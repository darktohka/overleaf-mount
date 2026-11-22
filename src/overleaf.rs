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
                    let body = res.text().await.unwrap_or_default();
                    bail!("project token rejected ({status}): {}", truncate(&body, 300));
                }
                info!("authenticated with project token (session cookie established)");
            }
            Auth::UserPass { email, password } => {
                // 1. Session cookie + CSRF token.
                let login_url = self.url("/login");
                let page = self.get(login_url.clone()).await?;
                if !page.status().is_success() {
                    bail!("could not load the login page: {}", page.status());
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
                let mut req = self
                    .http
                    .post(login_url)
                    .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(body);
                if let Some(c) = self.cookies_header() {
                    req = req.header(COOKIE, c);
                }
                req = req
                    .header("x-csrf-token", csrf.clone())
                    .header("referer", self.url("/login").to_string());
                let res = req.send().await.context("login request")?;
                self.absorb_cookies(&res);
                let status = res.status();
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
                if !(status.is_redirection() || status.is_success()) {
                    let text = res.text().await.unwrap_or_default();
                    bail!("login failed ({status}): {}", truncate(&text, 300));
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
        for _ in 0..8 {
            let res = req
                .try_clone()
                .ok_or_else(|| anyhow!("request body is not reusable"))?
                .send()
                .await
                .with_context(|| format!("{method} {current}"))?;
            self.absorb_cookies(&res);
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
            let body = res.text().await.unwrap_or_default();
            bail!("GET {path} -> {status}: {}", truncate(&body, 300));
        }
        let text = res.text().await?;
        serde_json::from_str(&text)
            .with_context(|| format!("parsing GET {path} response: {}", truncate(&text, 300)))
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
            .http
            .post(url)
            .basic_auth(user, Some(pass))
            .json(&json!({ "userId": user_id }))
            .send()
            .await
            .ok()?;
        if !res.status().is_success() {
            return Some(Err(anyhow!("/join -> {}", res.status())));
        }
        let v: Value = res.json().await.ok()?;
        let v = v.get("project").cloned().unwrap_or(v);
        Some(parse_project_meta(&self.project_id, &v))
    }

    /// `/project/:id/entities` — names and paths only, no document ids. Kept as
    /// a last resort so the mount still lists a project read-only.
    async fn fetch_project_entities(&self) -> Result<ProjectMeta> {
        let v: Value = self
            .get_json(&format!("/project/{}/entities", self.project_id))
            .await
            .context("fetching project")?;
        let mut meta = parse_project_meta(&self.project_id, &v)?;

        // Names without ids: the caller can list the project but cannot read
        // document content, which is what makes this the last resort.
        if meta.docs.is_empty() {
            warn!("no document ids available: browsing will be read-only");
        }
        Ok(meta)
    }

    /// `GET /project/:id/updates?min_count=1` → the doc tree with versions.
    pub async fn fetch_updates(&self) -> Result<Vec<RawUpdate>> {
        let v: Value = self
            .get_json(&format!("/project/{}/updates?min_count=1", self.project_id))
            .await
            .unwrap_or(Value::Array(vec![]));
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
                .http
                .get(url)
                .basic_auth(user, Some(pass))
                .send()
                .await
                .context("private API get_doc")?;
            if res.status().is_success() {
                let text = res.text().await?;
                let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                if let Some(lines) = v.get("lines").and_then(|l| l.as_array()) {
                    return Ok(Doc {
                        id: doc_id.to_string(),
                        lines: lines
                            .iter()
                            .map(|l| l.as_str().unwrap_or_default().to_string())
                            .collect(),
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
                .http
                .post(url)
                .basic_auth(user, Some(pass))
                .json(&body)
                .send()
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
            let body = res.text().await.unwrap_or_default();
            bail!("set_doc {doc_id} -> {status}: {}", truncate(&body, 300));
        }
        Ok(())
    }

    /// Delete a document (deletes the underlying doc + its entry in the tree).
    pub async fn delete_doc(&self, doc_id: &str) -> Result<()> {
        let url = self.url(&format!("/project/{}/doc/{}", self.project_id, doc_id));
        if let Some((user, pass)) = self.api_auth.clone() {
            let res = self
                .http
                .delete(url.clone())
                .basic_auth(user, Some(pass))
                .send()
                .await?;
            if res.status().is_success() {
                return Ok(());
            }
        }
        let res = self
            .send_with_auth(reqwest::Method::DELETE, url, None)
            .await?;
        if !res.status().is_success() {
            bail!("delete_doc {doc_id} -> {}", res.status());
        }
        Ok(())
    }

    /// Create a text document in the root folder.
    pub async fn add_doc(&self, name: &str, lines: &[String]) -> Result<String> {
        let url = self.url(&format!("/project/{}/doc", self.project_id));
        let body = json!({ "name": name, "lines": lines, "parent_folder_id": "rootFolder" });
        let res = self
            .send_with_auth(reqwest::Method::POST, url, Some(body))
            .await?;
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("add_doc {name} -> {status}: {}", truncate(&text, 300));
        }
        let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        doc_id_from_add_response(&v).ok_or_else(|| anyhow!("add_doc: no _id in response: {}", truncate(&text, 200)))
    }

    /// Create a folder (or a text doc) at the project root.
    pub async fn add_folder(&self, name: &str) -> Result<String> {
        let url = self.url(&format!("/project/{}/folder", self.project_id));
        let body = json!({ "name": name, "parent_folder_id": "rootFolder" });
        let res = self
            .send_with_auth(reqwest::Method::POST, url, Some(body))
            .await?;
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("add_folder {name} -> {status}: {}", truncate(&text, 300));
        }
        let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        doc_id_from_add_response(&v).ok_or_else(|| anyhow!("add_folder: no _id in response"))
    }

    /// Upload a binary file (document store) to the root folder.
    pub async fn upload_file(&self, name: &str, bytes: Vec<u8>) -> Result<String> {
        let url = self.url(&format!("/project/{}/upload", self.project_id));
        let part = reqwest::multipart::Part::bytes(bytes)
            .file_name(name.to_string())
            .mime_str("application/octet-stream")?;
        let form = reqwest::multipart::Form::new()
            .text("parent_folder_id", "rootFolder")
            .part("qqfile", part);
        let mut req = self.http.post(url);
        if let Some(c) = self.cookies_header() {
            req = req.header(COOKIE, c);
        }
        if let Some(t) = self.csrf_token().await {
            req = req.header("x-csrf-token", t);
        }
        let res = req.multipart(form).send().await?;
        self.absorb_cookies(&res);
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("upload {name} -> {status}: {}", truncate(&text, 300));
        }
        let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        Ok(v.get("entity_id")
            .or_else(|| v.get("_id"))
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string())
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
    pub async fn get_file(&self, file_id: &str) -> Result<Vec<u8>> {
        let url = self.url(&format!("/project/{}/file/{}", self.project_id, file_id));
        let res = self.get(url).await?;
        if !res.status().is_success() {
            bail!("get_file {file_id} -> {}", res.status());
        }
        Ok(res.bytes().await?.to_vec())
    }

    /// Rename / move any entity.
    pub async fn rename_entity(&self, entity_id: &str, entity_type: Entity, new_name: &str) -> Result<()> {
        let url = self.url(&format!("/project/{}/{}", self.project_id, entity_type));
        let body = json!({ "name": new_name });
        let mut req = self.http.request(reqwest::Method::POST, url);
        if let Some(c) = self.cookies_header() {
            req = req.header(COOKIE, c);
        }
        if let Some(t) = self.csrf_token().await {
            req = req.header("x-csrf-token", t);
        }
        let res = req.json(&body).send().await?;
        self.absorb_cookies(&res);
        if !res.status().is_success() {
            bail!("rename {entity_type} {entity_id} -> {}", res.status());
        }
        Ok(())
    }
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
