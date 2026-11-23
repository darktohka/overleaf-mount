//! Overleaf REST client.
//!
//! Two flavours of authentication, tried in this order:
//!
//! 1. **Project-scoped token (`olp_`) + session handshake** — the mechanism the
//!    real Overleaf editor uses. `POST /project/:id/token` (with the project
//!    token as `Authorization`) creates a project-session cookie; from then on
//!    the cookie authorises the REST API and `/project/:id/socket.io`.
//!    This works against *any* Overleaf CE/Server instance.
//! 2. **Username/password login** — the mount logs in through `/login` and
//!    reuses the resulting session cookie, so it can be pointed at a plain dev
//!    server without minting tokens by hand.
//!
//! Everything is cached in memory where it makes sense (project metadata, the
//! cookie jar, the socket cookie) so that a `write()` on the filesystem turns
//! into exactly one HTTP request.

use anyhow::{anyhow, bail, Context, Result};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE, COOKIE};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use log::{debug, info, warn};
use url::Url;

use crate::ratelimit::{Endpoint, RateLimited, RateLimiter};
use crate::types::{Doc, DocEntry, Entity, ProjectMeta, RootFolder};

/// A single Overleaf project root folder (`rootFolder[0].docs` / `.fileRefs`).
#[derive(Debug, Clone, Deserialize)]
pub struct RawDocEntry {
    #[serde(rename = "_id")]
    pub id: String,
    pub name: String,
}

/// Result of `GET /project/:id/doc/:doc_id`.
#[derive(Debug, Clone, Deserialize)]
pub struct RawDoc {
    #[serde(rename = "_id")]
    pub id: String,
    pub lines: Vec<String>,
    pub version: i64,
    pub ranges: Option<Value>,
}

/// One line of `GET /project/:id/updates?min_count=`.
#[derive(Debug, Clone, Deserialize)]
pub struct RawUpdate {
    pub doc_id: String,
    pub v: i64,
    #[serde(default)]
    pub op: Option<Value>,
}

#[derive(Debug)]
pub struct OverleafClient {
    http: reqwest::Client,
    base: Url,
    project_id: String,
    auth: Auth,
    /// CSRF token for state-changing calls (from `window.csrfToken` or the
    /// `ol-csrfToken` cookie).
    csrf: Mutex<Option<String>>,
    /// Cookies observed on responses (we drive the jar manually so we can keep
    /// them across the socket upgrade as well).
    cookies: Mutex<BTreeMap<String, String>>,
    pub project: Mutex<Option<ProjectMeta>>,
    /// Optional credentials for web's private API (`/project/:id/doc/:id`),
    /// which is how document content is read/written when the socket is not
    /// available. Overleaf's own services use basic auth for this.
    api_auth: Option<(String, String)>,
    /// Cached id of the logged-in user (needed by the private `/join` API).
    user_id: Mutex<Option<String>>,
    /// Base URL for socket.io. In production nginx proxies `/socket.io` on the
    /// main host, so this is normally the same as `base`; for a bare dev stack
    /// it can point at the real-time service directly.
    realtime_base: String,
    /// Shared rate-limit state: per-endpoint pacing plus the global cooldown a
    /// 429 puts every request into. See `crate::ratelimit`.
    limiter: RateLimiter,
}

#[derive(Debug, Clone)]
enum Auth {
    ProjectToken(String),
    UserPass { email: String, password: String },
}

fn header_value(s: &str) -> Result<HeaderValue> {
    HeaderValue::from_str(s).context("invalid header value")
}

impl OverleafClient {
    /// Build a client for `project_id` on `base` (e.g. `http://localhost:8080`).
    pub async fn connect(
        base: &str,
        project_id: &str,
        project_token: Option<&str>,
        user: Option<(&str, &str)>,
    ) -> Result<Arc<Self>> {
        let base = Url::parse(base).context("invalid base URL")?;
        // Redirects are followed manually (see `send_with_auth`) so that the
        // `Set-Cookie` headers of every hop are captured: the login flow sets
        // the session cookie on a 302 and reqwest would otherwise drop it.
        let mut builder = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            // A wedged server must not block a document pump forever: without a
            // deadline a hung fallback request also blocks the retry that would
            // have used the (reconnected) socket.
            .timeout(std::time::Duration::from_secs(20))
            .connect_timeout(std::time::Duration::from_secs(5))
            .user_agent("overleaf-mount/0.1");
        if let Some(token) = project_token {
            let mut headers = HeaderMap::new();
            headers.insert(AUTHORIZATION, header_value(&format!("Bearer {token}"))?);
            builder = builder.default_headers(headers);
        }
        let http = builder.build()?;

        let auth = match (project_token, user) {
            (Some(token), _) => Auth::ProjectToken(token.to_string()),
            (None, Some((email, password))) => Auth::UserPass {
                email: email.to_string(),
                password: password.to_string(),
            },
            (None, None) => bail!("either --token or --email/--password is required for the Overleaf API"),
        };

        let client = Arc::new(Self {
            http,
            base,
            project_id: project_id.to_string(),
            auth,
            csrf: Mutex::new(None),
            cookies: Mutex::new(BTreeMap::new()),
            project: Mutex::new(None),
            api_auth: None,
            user_id: Mutex::new(None),
            realtime_base: String::new(),
            limiter: RateLimiter::new(),
        });
        client.handshake().await?;
        Ok(client)
    }

    pub fn base_url(&self) -> &Url {
        &self.base
    }

    /// The underlying HTTP client (used for the socket.io handshake).
    pub fn http_client(&self) -> &reqwest::Client {
        &self.http
    }

    /// Point socket.io at an explicit host (defaults to the main host).
    pub fn with_realtime(mut self: Arc<Self>, url: Option<&str>) -> Arc<Self> {
        let base = url
            .map(|u| u.trim_end_matches('/').to_string())
            .unwrap_or_else(|| self.base.as_str().trim_end_matches('/').to_string());
        // Safety: `Arc::get_mut` fails if other handles exist; we always call
        // this before cloning, so fall back to interior mutability if not.
        if let Some(inner) = Arc::get_mut(&mut self) {
            inner.realtime_base = base;
        }
        self
    }

    /// Supply credentials for web's private API.
    pub fn with_api_auth(mut self: Arc<Self>, user: Option<&str>, pass: Option<&str>) -> Arc<Self> {
        if let (Some(u), Some(p)) = (user, pass) {
            if let Some(inner) = Arc::get_mut(&mut self) {
                inner.api_auth = Some((u.to_string(), p.to_string()));
            } else {
                // Already cloned: fall back to storing it through the mutex-free
                // path by leaking a new Arc is not acceptable, so log instead.
                warn!("api credentials must be set before the client is shared");
            }
        }
        self
    }

    /// Base URL the socket.io client should use.
    pub fn realtime_base(&self) -> &str {
        if self.realtime_base.is_empty() {
            self.base.as_str().trim_end_matches('/')
        } else {
            &self.realtime_base
        }
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    fn url(&self, path: &str) -> Url {
        self.base.join(path.trim_start_matches('/')).expect("valid path")
    }

    /// Establish the session: project token handshake or user/password login.
    ///
    /// Overleaf protects state-changing routes with a session-bound CSRF token
    /// (`csurf`), so the login flow is: GET /login (obtain the session cookie
    /// and a token) -> POST /login with `_csrf` -> GET again for a fresh token,
    /// because logging in regenerates the session.
    async fn handshake(&self) -> Result<()> {
        match self.auth.clone() {
            Auth::ProjectToken(_) => {
                let url = self.url(&format!("/project/{}/token", self.project_id));
                let res = self
                    .send_with_auth(reqwest::Method::POST, url, None)
                    .await
                    .context("project token handshake")?;
                if !res.status().is_success() {
                    let status = res.status();
                    let retry = retry_after(res.headers());
                    let body = res.text().await.unwrap_or_default();
                    return Err(status_error(
                        "project token handshake",
                        Endpoint::Other,
                        status,
                        retry,
                        &body,
                    ));
                }
                info!("authenticated with project token (session cookie established)");
            }
            Auth::UserPass { email, password } => {
                // 1. Session cookie + CSRF token. `GET /login` is throttled per
                // IP, so a burst of mounts (or a previous restart loop) can
                // land on a 429 here; the shared limiter turns that into a wait.
                let login_url = self.url("/login");
                let mut page_attempt = 0;
                let page = loop {
                    let page = self.get(login_url.clone()).await?;
                    if page.status() == StatusCode::TOO_MANY_REQUESTS {
                        page_attempt += 1;
                        if page_attempt >= 6 {
                            return Err(status_error(
                                "login page",
                                Endpoint::Other,
                                page.status(),
                                retry_after(page.headers()),
                                "rate limited",
                            ));
                        }
                        self.wait_for_cooldown().await;
                        continue;
                    }
                    break page;
                };
                if !page.status().is_success() {
                    let status = page.status();
                    let retry = retry_after(page.headers());
                    return Err(status_error("login page", Endpoint::Other, status, retry, ""));
                }
                let html = page.text().await.unwrap_or_default();
                let csrf = extract_csrf(&html)
                    .ok_or_else(|| anyhow!("login page did not contain a CSRF token"))?;
                *self.csrf.lock().await = Some(csrf.clone());

                // 2. Post credentials.
                let body = format!(
                    "_csrf={}&email={}&password={}",
                    urlencode(&csrf),
                    urlencode(&email),
                    urlencode(&password)
                );
                // `LoginRateLimiter` allows 10 attempts per email over 120 s and
                // is cleared by a successful login, so a 429 here means the
                // budget is exhausted: wait out the cooldown and try again
                // instead of making the user rerun the mount.
                let mut attempt = 0;
                let (status, failure) = loop {
                    let mut login_req = self
                        .http
                        .post(login_url.clone())
                        .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
                        .body(body.clone())
                        .header("x-csrf-token", csrf.clone())
                        .header("referer", self.url("/login").to_string());
                    if let Some(c) = self.cookies_header() {
                        login_req = login_req.header(COOKIE, c);
                    }
                    let res = self
                        .send_once(login_req, Endpoint::Other)
                        .await
                        .context("login request")?;
                    let status = res.status();
                    if status == StatusCode::TOO_MANY_REQUESTS {
                        attempt += 1;
                        let wait = self.cooldown_remaining().unwrap_or_default();
                        warn!(
                            "login throttled (attempt {attempt}); waiting {:.0}s before retrying",
                            wait.as_secs_f32()
                        );
                        self.wait_for_cooldown().await;
                        if attempt < 6 {
                            continue;
                        }
                    }
                    // 302 -> follow manually so we keep the session cookie
                    if status.is_redirection() {
                        if let Some(loc) = res.headers().get(reqwest::header::LOCATION) {
                            let next = self
                                .url("/")
                                .join(loc.to_str().unwrap_or("/"))
                                .unwrap_or_else(|_| self.url("/project"));
                            let _ = self.get(next).await;
                        }
                    }
                    let body_text = if status.is_redirection() || status.is_success() {
                        String::new()
                    } else {
                        res.text().await.unwrap_or_default()
                    };
                    break (status, body_text);
                };
                if !(status.is_redirection() || status.is_success()) {
                    if status == StatusCode::TOO_MANY_REQUESTS {
                        return Err(RateLimited::new(
                            "login",
                            Endpoint::Other,
                            self.cooldown_remaining(),
                        )
                        .into());
                    }
                    bail!("login failed ({status}): {}", truncate(&failure, 300));
                }
                info!("authenticated as {email}");

                // 3. Fresh CSRF token for the new session.
                *self.csrf.lock().await = None;
                self.fetch_csrf().await?;
            }
        }
        if self.csrf_token().await.is_none() {
            self.fetch_csrf().await?;
        }
        Ok(())
    }

    /// Overleaf exposes a CSRF token the frontend picks up from the page; for
    /// API clients the token is accepted from the cookie (double-submit) as
    /// well, so we look for both.
    async fn fetch_csrf(&self) -> Result<()> {
        for path in [
            format!("/project/{}", self.project_id),
            "/project".to_string(),
        ] {
            let url = self.url(&path);
            let res = match self.get(url).await {
                Ok(r) => r,
                Err(e) => {
                    debug!("CSRF probe {path} failed: {e:#}");
                    continue;
                }
            };
            if !res.status().is_success() {
                continue;
            }
            let html = res.text().await.unwrap_or_default();
            if let Some(t) = extract_csrf(&html) {
                *self.csrf.lock().await = Some(t);
                return Ok(());
            }
        }
        debug!("no CSRF token found; continuing without one");
        Ok(())
    }

    async fn csrf_token(&self) -> Option<String> {
        self.csrf.lock().await.clone()
    }

    fn absorb_cookies(&self, res: &reqwest::Response) {
        let values: Vec<String> = res
            .headers()
            .get_all("set-cookie")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .collect();
        if values.is_empty() {
            return;
        }
        // Callers always update the jar from the same task, and every request
        // path awaits before the next one starts, so the try_lock fast path is
        // sufficient; if it is momentarily held we retry through a blocking
        // helper that only spins when the lock is contended.
        let mut guard = match self.cookies.try_lock() {
            Ok(g) => g,
            Err(_) => {
                spin_lock(&self.cookies);
                return;
            }
        };
        for v in values {
            if let Some((pair, _attrs)) = v.split_once(';') {
                if let Some((name, value)) = pair.split_once('=') {
                    guard.insert(name.trim().to_string(), value.trim().to_string());
                }
            }
        }
    }

    fn cookies_header(&self) -> Option<HeaderValue> {
        let guard = self.cookies.try_lock().ok()?;
        if guard.is_empty() {
            return None;
        }
        let joined = guard
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("; ");
        HeaderValue::from_str(&joined).ok()
    }

    /// Cookie header usable for the websocket upgrade.
    pub async fn cookie_header(&self) -> String {
        let guard = self.cookies.lock().await;
        guard
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// Time left on the server-wide 429 cooldown, if one is in effect.
    pub fn cooldown_remaining(&self) -> Option<std::time::Duration> {
        self.limiter.cooldown_remaining()
    }

    pub fn is_rate_limited(&self) -> bool {
        self.limiter.is_cooling_down()
    }

    /// Wait until the cooldown a 429 installed has lapsed.
    pub async fn wait_for_cooldown(&self) {
        self.limiter.wait_for_cooldown().await
    }

    /// Send one request through the shared limiter.
    ///
    /// Every HTTP request of the mount goes through here: the limiter reserves
    /// a slot in the endpoint's window (pacing) and holds the global cooldown a
    /// 429 installs, so a rejection on one call site slows down all of them.
    async fn send_once(
        &self,
        req: reqwest::RequestBuilder,
        endpoint: Endpoint,
    ) -> Result<reqwest::Response> {
        self.limiter.acquire(endpoint).await;
        let res = self.http.execute(req.build()?).await?;
        self.absorb_cookies(&res);
        match res.status() {
            StatusCode::TOO_MANY_REQUESTS => {
                let retry_after = retry_after(res.headers());
                let pause = self.limiter.note_rate_limited(retry_after);
                warn!(
                    "{} rate limit hit; pausing every request for {:.1}s",
                    endpoint.class(),
                    pause.as_secs_f32()
                );
            }
            s if s.is_success() => self.limiter.note_success(),
            _ => {}
        }
        Ok(res)
    }

    async fn send_with_auth(
        &self,
        method: reqwest::Method,
        url: Url,
        body: Option<Value>,
    ) -> Result<reqwest::Response> {
        let mut req = self.http.request(method.clone(), url.clone());
        if let Some(c) = self.cookies_header() {
            req = req.header(COOKIE, c);
        }
        if method != reqwest::Method::GET && method != reqwest::Method::HEAD {
            if let Some(t) = self.csrf_token().await {
                req = req
                    .header("x-csrf-token", t.clone())
                    .header("x-csrftoken", t);
            }
        }
        if let Some(b) = body {
            req = req.json(&b);
        }
        let mut req = req;
        let mut current = url.clone();
        let endpoint = Endpoint::classify(method.as_str(), current.path());
        for _ in 0..8 {
            let this = req
                .try_clone()
                .ok_or_else(|| anyhow!("request body is not reusable"))?;
            let res = self
                .send_once(this, endpoint)
                .await
                .with_context(|| format!("{method} {current}"))?;
            if res.status().is_redirection() {
                let Some(loc) = res.headers().get(reqwest::header::LOCATION) else {
                    return Ok(res);
                };
                let next = current
                    .join(loc.to_str().unwrap_or("/"))
                    .unwrap_or_else(|_| current.clone());
                let mut next_req = self.http.request(reqwest::Method::GET, next.clone());
                if let Some(c) = self.cookies_header() {
                    next_req = next_req.header(COOKIE, c);
                }
                req = next_req;
                current = next;
                continue;
            }
            return Ok(res);
        }
        bail!("too many redirects requesting {url}")
    }

    pub async fn get(&self, url: Url) -> Result<reqwest::Response> {
        let res = self
            .send_with_auth(reqwest::Method::GET, url.clone(), None)
            .await?;
        Ok(res)
    }

    async fn get_json<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T> {
        let url = self.url(path);
        let res = self.get(url).await?;
        let status = res.status();
        if !status.is_success() {
            let retry = retry_after(res.headers());
            let body = res.text().await.unwrap_or_default();
            return Err(status_error(format!("GET {path}"), Endpoint::Other, status, retry, &body));
        }
        let text = res.text().await?;
        serde_json::from_str(&text)
            .with_context(|| format!("parsing GET {path} response: {}", truncate(&text, 300)))
    }

    /// True when the private API credentials are configured, i.e. `/join` (the
    /// only HTTP endpoint that carries document ids) can be used.
    pub fn has_api_auth(&self) -> bool {
        self.api_auth.is_some()
    }

    /// Fetch project metadata: root folder docs + file refs + uploads.
    pub async fn fetch_project(&self) -> Result<ProjectMeta> {
        // Preferred: web's private `/project/:id/join`, the same call real-time
        // makes. Unlike `/entities` it includes document ids, which are required
        // to read or write any document.
        let mut meta = match self.fetch_project_via_join().await {
            Some(Ok(meta)) => meta,
            Some(Err(e)) => {
                debug!("private /join failed ({e:#}); falling back to /entities");
                self.fetch_project_entities().await?
            }
            None => self.fetch_project_entities().await?,
        };

        if meta.files.is_empty() {
            meta.files = self.fetch_files().await.unwrap_or_default();
        }
        *self.project.lock().await = Some(meta.clone());
        Ok(meta)
    }

    /// The logged-in user's id, read from the `ol-user_id` meta tag that every
    /// page carries. Needed by `/project/:id/join`.
    pub async fn current_user_id(&self) -> Option<String> {
        if let Some(id) = self.user_id.lock().await.clone() {
            return Some(id);
        }
        let res = self.get(self.url("/user/settings")).await.ok()?;
        let html = res.text().await.ok()?;
        let id = html
            .split("name=\"ol-user_id\" content=\"")
            .nth(1)?
            .split('"')
            .next()?
            .to_string();
        *self.user_id.lock().await = Some(id.clone());
        Some(id)
    }

    /// `POST /project/:id/join` — the private API real-time uses; returns the
    /// whole tree including document ids.
    async fn fetch_project_via_join(&self) -> Option<Result<ProjectMeta>> {
        let (user, pass) = self.api_auth.clone()?;
        let user_id = self.current_user_id().await?;
        let url = self.url(&format!("/project/{}/join", self.project_id));
        let res = self
            .send_once(
                self.http
                    .post(url)
                    .basic_auth(user, Some(pass))
                    .json(&json!({ "userId": user_id })),
                Endpoint::JoinProject,
            )
            .await
            .ok()?;
        if !res.status().is_success() {
            let status = res.status();
            let retry = retry_after(res.headers());
            return Some(Err(status_error("/join", Endpoint::JoinProject, status, retry, "")));
        }
        let v: Value = res.json().await.ok()?;
        let v = v.get("project").cloned().unwrap_or(v);
        Some(parse_project_meta(&self.project_id, &v))
    }

    /// `/project/:id/entities` — names and paths only, no document ids. Kept as
    /// a last resort so the mount still lists the project read-only.
    async fn fetch_project_entities(&self) -> Result<ProjectMeta> {
        let v: Value = self
            .get_json(&format!("/project/{}/entities", self.project_id))
            .await
            .context("fetching project")?;
        let mut meta = parse_project_meta(&self.project_id, &v)?;

        // Unlike `/join`, this endpoint answers with a flat
        // `{"entities":[{"path":"/a/b.tex","type":"doc"}, …]}` list, so the
        // `rootFolder` walk above sees nothing. Rebuild the directory skeleton
        // from the paths: `/entities` does not emit folder entries, but every
        // parent of an entity is a directory. Documents and files themselves are
        // dropped — without their ids they cannot be opened, and listing them
        // would make the mount look usable while every read fails.
        if let Some(list) = v.get("entities").and_then(|e| e.as_array()) {
            for e in list {
                let Some(path) = e.get("path").and_then(|p| p.as_str()) else {
                    continue;
                };
                let mut parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
                let is_folder = e.get("type").and_then(|t| t.as_str()) == Some("folder");
                // A folder entity names the directory; anything else names an
                // entry inside it, so its own path component is not a directory.
                if !is_folder {
                    parts.pop();
                }
                for depth in 1..=parts.len() {
                    let dir = parts[..depth].join("/");
                    if !meta.folders.iter().any(|f| f.path == dir) {
                        meta.folders.push(RootFolder {
                            id: String::new(),
                            path: dir,
                        });
                    }
                }
            }
        }
        Ok(meta)
    }

    /// `GET /project/:id/updates?min_count=1` → the doc tree with versions.
    pub async fn fetch_updates(&self) -> Result<Vec<RawUpdate>> {
        // Errors must reach the poll loop: swallowing a 429 made the loop keep
        // polling at full speed into a closed limiter window.
        let v: Value = self
            .get_json(&format!("/project/{}/updates?min_count=1", self.project_id))
            .await?;
        let mut out = Vec::new();
        let arr = v
            .get("updates")
            .and_then(|u| u.as_array())
            .cloned()
            .or_else(|| v.as_array().cloned())
            .unwrap_or_default();
        for item in arr {
            if let Ok(u) = serde_json::from_value::<RawUpdate>(item.clone()) {
                out.push(u);
            }
        }
        Ok(out)
    }

    /// Read a document (lines + version).
    ///
    /// Prefers web's private API (`GET /project/:id/doc/:id`), which is what
    /// document-updater itself calls; it works for any Overleaf server given
    /// API credentials and does not depend on the socket.
    pub async fn get_doc(&self, doc_id: &str) -> Result<Doc> {
        if let Some((user, pass)) = self.api_auth.clone() {
            let url = self.url(&format!("/project/{}/doc/{}", self.project_id, doc_id));
            let res = self
                .send_once(
                    self.http.get(url).basic_auth(user, Some(pass)),
                    Endpoint::Other,
                )
                .await
                .context("private API get_doc")?;
            if res.status().is_success() {
                let text = res.text().await?;
                let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                if let Some(lines) = v.get("lines").and_then(|l| l.as_array()) {
                    return Ok(Doc {
                        id: doc_id.to_string(),
                        lines: flatten_lines(lines),
                        version: v.get("version").and_then(|x| x.as_i64()).unwrap_or(0),
                        ranges: v.get("ranges").cloned().filter(|r| !r.is_null()),
                    });
                }
            } else {
                debug!("private API get_doc {doc_id} -> {}", res.status());
            }
        }
        // Session-authenticated fallback.
        let raw: RawDoc = self
            .get_json(&format!("/project/{}/doc/{}", self.project_id, doc_id))
            .await
            .with_context(|| format!("fetching doc {doc_id}"))?;
        Ok(Doc {
            id: raw.id,
            lines: raw.lines,
            version: raw.version,
            ranges: raw.ranges,
        })
    }

    /// Write a whole document through the HTTP fallback path
    /// (`POST /project/:id/doc/:doc_id` with `lines`). Used when the
    /// real-time/OT pipeline is unavailable (other Overleaf servers, binary
    /// docs, or after an out-of-sync error). The server turns the new text into
    /// an OT update itself, so other editors see a normal edit.
    /// Replace a document's content through web's private document API.
    ///
    /// The endpoint validates its body strictly: `lines`, `version` (the version
    /// the caller believes it is replacing) and `ranges` are all required.
    pub async fn set_doc(
        &self,
        doc_id: &str,
        lines: &[String],
        version: i64,
        ranges: Option<&Value>,
        source: &str,
    ) -> Result<()> {
        let path = format!("/project/{}/doc/{}", self.project_id, doc_id);
        let url = self.url(&path);
        let body = json!({
            "lines": lines,
            "version": version.max(0),
            "ranges": ranges.cloned().unwrap_or_else(|| json!({})),
            "source": source,
        });
        if let Some((user, pass)) = self.api_auth.clone() {
            let res = self
                .send_once(
                    self.http
                        .post(url)
                        .basic_auth(user, Some(pass))
                        .json(&body),
                    Endpoint::Other,
                )
                .await
                .context("private API set_doc")?;
            let status = res.status();
            if status.is_success() {
                return Ok(());
            }
            let text = res.text().await.unwrap_or_default();
            debug!("private API set_doc {doc_id} -> {status}: {}", truncate(&text, 200));
        }
        let res = self
            .send_with_auth(reqwest::Method::POST, self.url(&path), Some(body))
            .await?;
        let status = res.status();
        if !status.is_success() {
            let retry = retry_after(res.headers());
            let body = res.text().await.unwrap_or_default();
            return Err(status_error(
                format!("set_doc {doc_id}"),
                Endpoint::Other,
                status,
                retry,
                &body,
            ));
        }
        Ok(())
    }

    /// Delete a document (deletes the underlying doc + its entry in the tree).
    pub async fn delete_doc(&self, doc_id: &str) -> Result<()> {
        let url = self.url(&format!("/project/{}/doc/{}", self.project_id, doc_id));
        if let Some((user, pass)) = self.api_auth.clone() {
            let res = self
                .send_once(
                    self.http.delete(url.clone()).basic_auth(user, Some(pass)),
                    Endpoint::Other,
                )
                .await?;
            if res.status().is_success() {
                return Ok(());
            }
        }
        let res = self
            .send_with_auth(reqwest::Method::DELETE, url, None)
            .await?;
        if !res.status().is_success() {
            let status = res.status();
            let retry = retry_after(res.headers());
            return Err(status_error(
                format!("delete_doc {doc_id}"),
                Endpoint::Other,
                status,
                retry,
                "",
            ));
        }
        Ok(())
    }

    /// Create a text document in the root folder.
    pub async fn add_doc(
        &self,
        name: &str,
        lines: &[String],
        parent_folder_id: Option<&str>,
    ) -> Result<String> {
        let url = self.url(&format!("/project/{}/doc", self.project_id));
        // `parent_folder_id: null` makes web use the project root folder; it is
        // *not* the literal string "rootFolder" (that is not a folder id, and
        // the handler answers 404 when it cannot find it).
        let body = json!({
            "name": name,
            "lines": lines,
            "parent_folder_id": parent_folder_id,
        });
        let res = self
            .send_with_auth(reqwest::Method::POST, url, Some(body))
            .await?;
        let status = res.status();
        let retry = retry_after(res.headers());
        let text = res.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(status_error(
                format!("add_doc {name}"),
                Endpoint::AddDoc,
                status,
                retry,
                &text,
            ));
        }
        let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        doc_id_from_add_response(&v).ok_or_else(|| anyhow!("add_doc: no _id in response: {}", truncate(&text, 200)))
    }

    /// Create a folder (or a text doc) in `parent_folder_id` (`None` = root).
    pub async fn add_folder(&self, name: &str, parent_folder_id: Option<&str>) -> Result<String> {
        let url = self.url(&format!("/project/{}/folder", self.project_id));
        let body = json!({ "name": name, "parent_folder_id": parent_folder_id });
        let res = self
            .send_with_auth(reqwest::Method::POST, url, Some(body))
            .await?;
        let status = res.status();
        let retry = retry_after(res.headers());
        let text = res.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(status_error(
                format!("add_folder {name}"),
                Endpoint::AddFolder,
                status,
                retry,
                &text,
            ));
        }
        let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        doc_id_from_add_response(&v).ok_or_else(|| anyhow!("add_folder: no _id in response"))
    }

    /// Upload a binary file into `parent_folder_id` (`None` = project root).
    ///
    /// The web route is `POST /Project/:id/upload?folder_id=…` with multipart
    /// fields `name` and `qqfile`. Uploading a name that already exists in the
    /// folder replaces its content (`FileSystemImportManager` uses
    /// `upsertFile`), which is what makes binary files writable on the mount.
    pub async fn upload_file(
        &self,
        name: &str,
        bytes: Vec<u8>,
        parent_folder_id: Option<&str>,
    ) -> Result<String> {
        let mut url = self.url(&format!("/Project/{}/upload", self.project_id));
        if let Some(id) = parent_folder_id {
            url.query_pairs_mut().append_pair("folder_id", id);
        }
        let part = reqwest::multipart::Part::bytes(bytes)
            .file_name(name.to_string())
            .mime_str("application/octet-stream")?;
        let form = reqwest::multipart::Form::new()
            .text("name", name.to_string())
            .part("qqfile", part);
        let mut req = self.http.post(url);
        if let Some(c) = self.cookies_header() {
            req = req.header(COOKIE, c);
        }
        if let Some(t) = self.csrf_token().await {
            req = req.header("x-csrf-token", t);
        }
        let res = self.send_once(req.multipart(form), Endpoint::Upload).await?;
        let status = res.status();
        let retry = retry_after(res.headers());
        let text = res.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(status_error(
                format!("upload {name}"),
                Endpoint::Upload,
                status,
                retry,
                &text,
            ));
        }
        let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        let id = v
            .get("entity_id")
            .or_else(|| v.get("_id"))
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string();
        if id.is_empty() {
            bail!("upload {name}: no entity_id in response: {}", truncate(&text, 200));
        }
        Ok(id)
    }

    /// List uploaded files (`GET /project/:id/files`).
    async fn fetch_files(&self) -> Result<Vec<crate::types::FileEntry>> {
        let v: Value = self
            .get_json(&format!("/project/{}/files", self.project_id))
            .await?;
        let mut out = Vec::new();
        if let Some(files) = v.get("files").and_then(|f| f.as_array()) {
            for f in files {
                let id = f.get("_id").and_then(|x| x.as_str()).unwrap_or_default();
                let name = f.get("name").and_then(|x| x.as_str()).unwrap_or_default();
                let size = f.get("size").and_then(|x| x.as_i64()).unwrap_or(0);
                if !id.is_empty() && !name.is_empty() {
                    out.push(crate::types::FileEntry {
                        id: id.to_string(),
                        name: name.to_string(),
                        size,
                    });
                }
            }
        }
        Ok(out)
    }

    /// Download an uploaded file's content.
    ///
    /// The public route is capitalised (`/Project/:id/file/:file_id`); the
    /// lowercase path only exists as the `DELETE` in the editor API, so a
    /// lowercase GET falls through to the 404 page.
    pub async fn get_file(&self, file_id: &str) -> Result<Vec<u8>> {
        let url = self.url(&format!("/Project/{}/file/{}", self.project_id, file_id));
        let res = self.get(url).await?;
        if !res.status().is_success() {
            let status = res.status();
            let retry = retry_after(res.headers());
            return Err(status_error(
                format!("get_file {file_id}"),
                Endpoint::Other,
                status,
                retry,
                "",
            ));
        }
        Ok(res.bytes().await?.to_vec())
    }

    /// Size of an uploaded file without downloading it.
    ///
    /// `FileStoreController.getFileHead` answers `HEAD` on the same route the
    /// browser uses, so a directory listing gets real sizes for the cost of one
    /// header request per file.
    pub async fn get_file_size(&self, file_id: &str) -> Result<u64> {
        let url = self.url(&format!("/Project/{}/file/{}", self.project_id, file_id));
        let res = self
            .send_with_auth(reqwest::Method::HEAD, url, None)
            .await
            .context("private API get_file_size")?;
        if !res.status().is_success() {
            let status = res.status();
            let retry = retry_after(res.headers());
            return Err(status_error(
                format!("get_file_size {file_id}"),
                Endpoint::Other,
                status,
                retry,
                "",
            ));
        }
        // `Response::content_length()` reports 0 for a HEAD response (there is
        // no body to measure), so read the header itself.
        res.headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| anyhow!("get_file_size {file_id}: no content-length"))
    }

    /// Rename an entity (`POST /project/:id/:type/:id/rename`).
    pub async fn rename_entity(&self, entity_id: &str, entity_type: Entity, new_name: &str) -> Result<()> {
        let url = self.url(&format!(
            "/project/{}/{}/{}/rename",
            self.project_id, entity_type, entity_id
        ));
        self.entity_post(url, json!({ "name": new_name }), "rename", entity_type, entity_id)
            .await
    }

    /// Move an entity into `folder_id` (`None` = project root).
    pub async fn move_entity(
        &self,
        entity_id: &str,
        entity_type: Entity,
        folder_id: Option<&str>,
    ) -> Result<()> {
        let url = self.url(&format!(
            "/project/{}/{}/{}/move",
            self.project_id, entity_type, entity_id
        ));
        let body = match folder_id {
            Some(id) => json!({ "folder_id": id }),
            None => json!({}),
        };
        self.entity_post(url, body, "move", entity_type, entity_id)
            .await
    }

    /// Delete a doc, file or folder.
    pub async fn delete_entity(&self, entity_id: &str, entity_type: Entity) -> Result<()> {
        let url = self.url(&format!(
            "/project/{}/{}/{}",
            self.project_id, entity_type, entity_id
        ));
        let res = self
            .send_with_auth(reqwest::Method::DELETE, url, None)
            .await?;
        if !res.status().is_success() {
            let status = res.status();
            let retry = retry_after(res.headers());
            return Err(status_error(
                format!("delete {entity_type} {entity_id}"),
                Endpoint::Other,
                status,
                retry,
                "",
            ));
        }
        Ok(())
    }

    async fn entity_post(
        &self,
        url: Url,
        body: Value,
        action: &str,
        entity_type: Entity,
        entity_id: &str,
    ) -> Result<()> {
        let endpoint = Endpoint::classify("POST", url.path());
        let mut req = self.http.request(reqwest::Method::POST, url);
        if let Some(c) = self.cookies_header() {
            req = req.header(COOKIE, c);
        }
        if let Some(t) = self.csrf_token().await {
            req = req.header("x-csrf-token", t);
        }
        let res = self.send_once(req.json(&body), endpoint).await?;
        if !res.status().is_success() {
            let status = res.status();
            let retry = retry_after(res.headers());
            let text = res.text().await.unwrap_or_default();
            return Err(status_error(
                format!("{action} {entity_type} {entity_id}"),
                endpoint,
                status,
                retry,
                &text,
            ));
        }
        Ok(())
    }
}

/// `Retry-After` in seconds or as an HTTP-date. Overleaf's own limiter does not
/// send the header (its 429 body is plain text), but nginx and other reverse
/// proxies in front of a deployment may, so honour it when present.
fn retry_after(headers: &HeaderMap) -> Option<std::time::Duration> {
    let value = headers.get(reqwest::header::RETRY_AFTER)?.to_str().ok()?;
    if let Ok(secs) = value.trim().parse::<u64>() {
        return Some(std::time::Duration::from_secs(secs));
    }
    let when = chrono::DateTime::parse_from_rfc2822(value.trim()).ok()?;
    let now = chrono::Utc::now();
    (when.with_timezone(&chrono::Utc) - now)
        .to_std()
        .ok()
}

/// Error for a non-success response. 429s stay typed (`RateLimited`) so the
/// sync engine can recognise the limit instead of matching on the message.
fn status_error(
    action: impl Into<String>,
    endpoint: Endpoint,
    status: StatusCode,
    retry_after: Option<std::time::Duration>,
    body: &str,
) -> anyhow::Error {
    let action = action.into();
    if status == StatusCode::TOO_MANY_REQUESTS {
        return RateLimited::new(action, endpoint, retry_after).into();
    }
    anyhow!("{action} -> {status}: {}", truncate(body, 300))
}

fn spin_lock(jar: &Mutex<BTreeMap<String, String>>) {
    // Contention here is a bug or a re-entrant call; block the (async) caller
    // until the lock is free. Each blocking iteration is bounded, and the
    // critical section it guards is a few map inserts.
    while jar.try_lock().is_err() {
        std::thread::yield_now();
    }
}

fn parse_project_meta(project_id: &str, v: &Value) -> Result<ProjectMeta> {
    let name = v
        .get("name")
        .and_then(|n| n.as_str())
        .unwrap_or("project")
        .to_string();
    let mut docs = Vec::new();
    let mut folders = Vec::new();
    fn walk(folder: &Value, path: &mut Vec<String>, docs: &mut Vec<DocEntry>, folders: &mut Vec<RootFolder>) {
        let _id = folder.get("_id").and_then(|x| x.as_str()).unwrap_or("rootFolder");
        if let Some(ds) = folder.get("docs").and_then(|d| d.as_array()) {
            for d in ds {
                let doc_id = d.get("_id").and_then(|x| x.as_str()).unwrap_or_default();
                let name = d.get("name").and_then(|x| x.as_str()).unwrap_or_default();
                if doc_id.is_empty() || name.is_empty() {
                    continue;
                }
                let mut p = path.clone();
                p.push(name.to_string());
                docs.push(DocEntry {
                    id: doc_id.to_string(),
                    path: p.join("/"),
                    name: name.to_string(),
                });
            }
        }
        if let Some(refs) = folder.get("fileRefs").and_then(|d| d.as_array()) {
            for f in refs {
                let name = f.get("name").and_then(|x| x.as_str()).unwrap_or_default();
                if name.is_empty() {
                    continue;
                }
                // fileRefs point at uploaded files (binary, no OT)
                debug!("fileRef {name} in {}", path.join("/"));
            }
        }
        if let Some(fs) = folder.get("folders").and_then(|d| d.as_array()) {
            for f in fs {
                let fname = f.get("name").and_then(|x| x.as_str()).unwrap_or_default();
                if fname.is_empty() {
                    continue;
                }
                path.push(fname.to_string());
                folders.push(RootFolder {
                    id: f.get("_id").and_then(|x| x.as_str()).unwrap_or_default().to_string(),
                    path: path.join("/"),
                });
                walk(f, path, docs, folders);
                path.pop();
            }
        }
    }
    let v = v.get("project").unwrap_or(v);
    if let Some(root) = v.get("rootFolder").and_then(|r| r.as_array()).and_then(|r| r.first()) {
        let mut path = Vec::new();
        walk(root, &mut path, &mut docs, &mut folders);
    }
    Ok(ProjectMeta {
        id: project_id.to_string(),
        name,
        docs,
        folders,
        files: Vec::new(),
    })
}

fn doc_id_from_add_response(v: &Value) -> Option<String> {
    for key in ["doc_id", "_id", "docId"] {
        if let Some(s) = v.get(key).and_then(|x| x.as_str()) {
            return Some(s.to_string());
        }
    }
    if let Some(arr) = v.as_array() {
        for item in arr {
            if let Some(s) = doc_id_from_add_response(item) {
                return Some(s);
            }
        }
    }
    None
}

/// Reverse the WebSocket transport escaping Overleaf applies to document lines.
///
/// Real-time sends each line of a `sharejs-text-ot` document as
/// `unescape(encodeURIComponent(line))` — the UTF-8 bytes reinterpreted as
/// latin1 characters — and the browser reverses it with
/// `decodeURIComponent(escape(line))`. Without this step non-ASCII text appears
/// as mojibake (`café` -> `cafÃ©`), sizes are wrong, and every edit after a
/// multi-byte character is addressed at the wrong offset.
///
/// Already-decoded text is returned unchanged: pure ASCII is untouched, and a
/// non-ASCII string that is not a valid UTF-8 byte sequence (or contains code
/// points above U+00FF) is passed through.
pub fn decode_transport_utf8(s: &str) -> String {
    if s.is_ascii() || s.chars().any(|c| (c as u32) > 0xFF) {
        return s.to_string();
    }
    let bytes: Vec<u8> = s.chars().map(|c| c as u8).collect();
    match String::from_utf8(bytes) {
        Ok(decoded) => decoded,
        Err(_) => s.to_string(),
    }
}

/// Normalise the shapes an Overleaf `lines` array can take.
///
/// `StringFileData.getLines()` returns `string[]`, but the sharejs model — and
/// older payloads nested through `doc.lines` — can return an array of character
/// arrays. Treating those as empty strings silently blanks every document
/// loaded in that shape.
pub fn flatten_lines(lines: &[Value]) -> Vec<String> {
    lines
        .iter()
        .map(|l| match l {
            Value::String(s) => s.clone(),
            Value::Array(parts) => parts.iter().filter_map(|p| p.as_str()).collect(),
            _ => String::new(),
        })
        .collect()
}

fn extract_csrf(html: &str) -> Option<String> {
    // Overleaf renders the token twice: a <meta name="ol-csrfToken" content="…">
    // and a hidden input. Accept either, plus the inline JS form and the JSON
    // forms used by some pages.
    for marker in [
        "name=\"ol-csrfToken\" content=\"",
        "name=\"csrf-token\" content=\"",
        "value=\"",
        "window.csrfToken = \"",
        "csrfToken\":\"",
    ] {
        let mut from = 0usize;
        while let Some(rel) = html[from..].find(marker) {
            let idx = from + rel;
            // `value="` is only trusted when it sits in a csrf input
            if marker == "value=\"" {
                let window_start = idx.saturating_sub(200);
                let ctx = &html[window_start..idx];
                if !ctx.contains("csrf") {
                    from = idx + marker.len();
                    continue;
                }
            }
            let rest = &html[idx + marker.len()..];
            if let Some(end) = rest.find('"') {
                return Some(rest[..end].to_string());
            }
            from = idx + marker.len();
        }
    }
    None
}

fn urlencode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        format!("{}…", &s[..n])
    }
}

impl std::fmt::Display for Entity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Entity::Doc => "doc",
            Entity::Folder => "folder",
            Entity::File => "file",
        };
        write!(f, "{s}")
    }
}

/// Raw shape of `GET /project/:id/docs` — kept for API completeness/tests.
pub async fn try_status(client: &OverleafClient) -> bool {
    let url = client.url("/status");
    matches!(client.get(url).await.map(|r| r.status()), Ok(StatusCode::OK))
}

#[cfg(test)]
mod tests {
    use super::decode_transport_utf8;

    #[test]
    fn decodes_transport_escaped_text() {
        // `unescape(encodeURIComponent(x))` maps each UTF-8 byte to one char.
        assert_eq!(decode_transport_utf8("caf\u{00c3}\u{00a9}"), "café");
        assert_eq!(
            decode_transport_utf8(
                "\u{00e6}\u{0097}\u{00a5}\u{00e6}\u{009c}\u{00ac}\u{00e8}\u{00aa}\u{009e}"
            ),
            "日本語"
        );
        assert_eq!(
            decode_transport_utf8("\u{00f0}\u{009f}\u{0098}\u{0080}"),
            "😀"
        );
    }

    #[test]
    fn leaves_ascii_and_already_decoded_text_alone() {
        assert_eq!(decode_transport_utf8("plain ascii"), "plain ascii");
        assert_eq!(decode_transport_utf8("café"), "café");
        assert_eq!(decode_transport_utf8("日本語"), "日本語");
        assert_eq!(decode_transport_utf8(""), "");
    }

    #[test]
    fn leaves_invalid_utf8_byte_sequences_alone() {
        // U+00E9 alone is not a valid UTF-8 sequence; keep the original.
        assert_eq!(decode_transport_utf8("caf\u{00e9}"), "caf\u{00e9}");
    }
}
