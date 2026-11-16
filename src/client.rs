use crate::fs_state::FsState;
use crate::ot;
use crate::protocol::{self, Packet, ProjectView, FolderView};
use futures_util::{SinkExt, StreamExt};
use reqwest::header::{HeaderMap, HeaderValue, COOKIE, SET_COOKIE};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use url::Url;

#[derive(Debug)]
pub enum SyncCommand {
    SyncDoc {
        doc_id: String,
    },
    UploadFile {
        file_id: String,
        name: String,
        parent_folder_id: String,
        content: Vec<u8>,
    },
    CreateDoc {
        parent_folder_id: String,
        name: String,
        content: String,
        reply: oneshot::Sender<Result<String, String>>,
    },
    CreateFolder {
        parent_folder_id: String,
        name: String,
        reply: oneshot::Sender<Result<String, String>>,
    },
    DeleteEntity {
        entity_type: String, // "doc", "file", "folder"
        entity_id: String,
    },
    RenameEntity {
        entity_type: String,
        entity_id: String,
        new_name: String,
    },
}

#[derive(Clone)]
pub struct OverleafClient {
    pub base_url: String,
    pub project_id: String,
    pub cookie: Arc<parking_lot::RwLock<String>>,
    pub csrf_token: Arc<parking_lot::RwLock<Option<String>>>,
    pub fs_state: FsState,
    pub command_tx: mpsc::UnboundedSender<SyncCommand>,
    pub fast_sync_supported: Arc<AtomicBool>,
    pub disable_fast_sync: bool,
    pub next_msg_id: Arc<AtomicU64>,
    pub public_id: Arc<parking_lot::RwLock<Option<String>>>,
    pub http_client: reqwest::Client,
}

impl OverleafClient {
    pub async fn new(
        base_url: String,
        project_id: String,
        email: Option<String>,
        password: Option<String>,
        initial_cookie: Option<String>,
        fs_state: FsState,
        disable_fast_sync: bool,
    ) -> Result<(Self, mpsc::UnboundedReceiver<SyncCommand>), Box<dyn std::error::Error + Send + Sync>> {
        let http_client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()?;

        let mut cookie_str = initial_cookie.unwrap_or_default();

        if cookie_str.is_empty() {
            if let (Some(email), Some(password)) = (email, password) {
                tracing::info!("Logging in as {} to {}", email, base_url);
                let (c, _) = Self::login(&http_client, &base_url, &email, &password).await?;
                cookie_str = c;
                tracing::info!("Login successful! Cookie obtained.");
            }
        }

        let csrf_opt = Self::fetch_csrf_token(&http_client, &base_url, &project_id, &cookie_str).await;
        tracing::info!("Authenticated project CSRF token: {:?}", csrf_opt);

        let (command_tx, command_rx) = mpsc::unbounded_channel();

        let client = OverleafClient {
            base_url,
            project_id,
            cookie: Arc::new(parking_lot::RwLock::new(cookie_str)),
            csrf_token: Arc::new(parking_lot::RwLock::new(csrf_opt)),
            fs_state,
            command_tx,
            fast_sync_supported: Arc::new(AtomicBool::new(!disable_fast_sync)),
            disable_fast_sync,
            next_msg_id: Arc::new(AtomicU64::new(1)),
            public_id: Arc::new(parking_lot::RwLock::new(None)),
            http_client,
        };

        Ok((client, command_rx))
    }

    pub async fn fetch_csrf_token(
        http_client: &reqwest::Client,
        base_url: &str,
        project_id: &str,
        cookie: &str,
    ) -> Option<String> {
        let url = format!("{}/project/{}", base_url.trim_end_matches('/'), project_id);
        let res = http_client
            .get(&url)
            .header(COOKIE, cookie)
            .send()
            .await
            .ok()?;
        let html = res.text().await.ok()?;
        extract_csrf_token(&html)
    }

    async fn login(
        http_client: &reqwest::Client,
        base_url: &str,
        email: &str,
        password: &str,
    ) -> Result<(String, String), Box<dyn std::error::Error + Send + Sync>> {
        // Step 1: GET /login to get CSRF token and initial cookie
        let login_page_url = format!("{}/login", base_url.trim_end_matches('/'));
        let res = http_client.get(&login_page_url).send().await?;

        let mut session_cookie = String::new();
        if let Some(cookie_header) = res.headers().get(SET_COOKIE) {
            let cookie_val = cookie_header.to_str()?.split(';').next().unwrap_or("");
            session_cookie = cookie_val.to_string();
        }

        let html = res.text().await?;
        let csrf_token = extract_csrf_token(&html).ok_or("Could not find CSRF token on login page")?;

        // Step 2: POST /login with email, password, and CSRF
        let mut headers = HeaderMap::new();
        headers.insert("Content-Type", HeaderValue::from_static("application/json"));
        headers.insert("X-Csrf-Token", HeaderValue::from_str(&csrf_token)?);
        if !session_cookie.is_empty() {
            headers.insert(COOKIE, HeaderValue::from_str(&session_cookie)?);
        }

        let body = serde_json::json!({
            "email": email,
            "password": password,
        });

        let post_res = http_client
            .post(&login_page_url)
            .headers(headers)
            .json(&body)
            .send()
            .await?;

        // Collect all set-cookie headers
        let mut final_cookies = Vec::new();
        for cookie_val in post_res.headers().get_all(SET_COOKIE) {
            let s = cookie_val.to_str()?.split(';').next().unwrap_or("");
            if !s.is_empty() {
                final_cookies.push(s.to_string());
            }
        }

        let cookie_str = if final_cookies.is_empty() {
            if !session_cookie.is_empty() {
                session_cookie
            } else {
                return Err("No session cookie returned after login".into());
            }
        } else {
            final_cookies.join("; ")
        };

        let post_html = post_res.text().await.unwrap_or_default();
        let auth_csrf = extract_csrf_token(&post_html).unwrap_or(csrf_token);

        Ok((cookie_str, auth_csrf))
    }

    /// Run the persistent WebSocket connection and sync loop.
    pub async fn run_sync_loop(
        self,
        mut command_rx: mpsc::UnboundedReceiver<SyncCommand>,
    ) {
        loop {
            tracing::info!("Starting WebSocket sync connection to Overleaf...");
            if let Err(e) = self.connect_and_process(&mut command_rx).await {
                tracing::warn!("WebSocket sync loop error: {:?}. Retrying in 2 seconds...", e);
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }

    async fn connect_and_process(
        &self,
        command_rx: &mut mpsc::UnboundedReceiver<SyncCommand>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let cookie_str = self.cookie.read().clone();
        let base_url = self.base_url.trim_end_matches('/');

        // 1. Socket.IO 0.9 handshake: GET /socket.io/1/?projectId=<pid>&t=<timestamp>
        let handshake_url = format!(
            "{}/socket.io/1/?projectId={}&t={}",
            base_url,
            self.project_id,
            chrono::Utc::now().timestamp_millis()
        );

        let mut hs_req = self.http_client.get(&handshake_url);
        if !cookie_str.is_empty() {
            hs_req = hs_req.header(COOKIE, &cookie_str);
        }
        let hs_res = hs_req.send().await?;
        let hs_body = hs_res.text().await?;

        let sid = hs_body
            .split(':')
            .next()
            .ok_or_else(|| format!("Invalid handshake response: {hs_body}"))?;

        tracing::info!("Socket.IO handshake successful, sid: {}", sid);

        // 2. Connect to WebSocket
        let ws_url_parsed = Url::parse(base_url)?;
        let scheme = if ws_url_parsed.scheme() == "https" {
            "wss"
        } else {
            "ws"
        };
        let ws_url = format!(
            "{}://{}/socket.io/1/websocket/{}?projectId={}",
            scheme,
            ws_url_parsed.host_str().unwrap_or("127.0.0.1"),
            sid,
            self.project_id
        );
        let ws_port = ws_url_parsed.port();
        let ws_url_with_port = if let Some(p) = ws_port {
            format!(
                "{}://{}:{}/socket.io/1/websocket/{}?projectId={}",
                scheme,
                ws_url_parsed.host_str().unwrap_or("127.0.0.1"),
                p,
                sid,
                self.project_id
            )
        } else {
            ws_url
        };

        tracing::info!("Connecting to WebSocket URL: {}", ws_url_with_port);

        let mut request = ws_url_with_port.as_str().into_client_request()?;
        if !cookie_str.is_empty() {
            request
                .headers_mut()
                .insert(COOKIE, HeaderValue::from_str(&cookie_str)?);
        }

        let (ws_stream, _) = tokio_tungstenite::connect_async(request).await?;
        let (mut ws_tx, mut ws_rx) = ws_stream.split();

        tracing::info!("WebSocket connected!");

        // Pending RPC callbacks map: msg_id -> doc_id
        let mut pending_joins: HashMap<String, String> = HashMap::new();
        let mut pending_ot_updates: HashMap<String, String> = HashMap::new();
        let mut pending_conflict_reconciliations: HashMap<String, (String, String)> = HashMap::new();

        let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(25));

        loop {
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    let _ = ws_tx.send(Message::Text("2::".into())).await;
                }

                Some(cmd) = command_rx.recv() => {
                    self.handle_sync_command(cmd, &mut ws_tx, &mut pending_ot_updates).await?;
                }

                msg_opt = ws_rx.next() => {
                    match msg_opt {
                        Some(Ok(Message::Text(text))) => {
                            self.handle_ws_message(
                                &text,
                                &mut ws_tx,
                                &mut pending_joins,
                                &mut pending_ot_updates,
                                &mut pending_conflict_reconciliations,
                            ).await?;
                        }
                        Some(Ok(Message::Ping(p))) => {
                            let _ = ws_tx.send(Message::Pong(p)).await;
                        }
                        Some(Ok(Message::Close(_))) => {
                            tracing::warn!("WebSocket closed by server");
                            break;
                        }
                        Some(Err(e)) => {
                            tracing::warn!("WebSocket error: {:?}", e);
                            break;
                        }
                        None => {
                            tracing::warn!("WebSocket stream ended");
                            break;
                        }
                        _ => {}
                    }
                }
            }
        }

        Ok(())
    }

    async fn handle_sync_command<S>(
        &self,
        cmd: SyncCommand,
        ws_tx: &mut S,
        pending_ot_updates: &mut HashMap<String, String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        S: futures_util::Sink<Message> + Unpin,
        <S as futures_util::Sink<Message>>::Error: std::fmt::Debug,
    {
        match cmd {
            SyncCommand::SyncDoc { doc_id } => {
                let (current_text, base_text, version, dirty) = {
                    let inner = self.fs_state.inner.read();
                    if let Some(ino) = inner.overleaf_id_to_ino.get(&doc_id) {
                        if let Some(data) = inner.inodes.get(ino) {
                            (
                                String::from_utf8_lossy(&data.content).to_string(),
                                data.base_text.clone(),
                                data.version,
                                data.dirty,
                            )
                        } else {
                            return Ok(());
                        }
                    } else {
                        return Ok(());
                    }
                };

                if !dirty {
                    return Ok(());
                }

                // If fast-sync is enabled and supported, try optimized REST endpoint first
                let fast_sync_enabled = !self.disable_fast_sync && self.fast_sync_supported.load(Ordering::SeqCst);
                if fast_sync_enabled {
                    match self.try_fast_sync_doc(&doc_id, version, &current_text).await {
                        Ok(new_v) => {
                            tracing::info!("Fast-sync succeeded for doc {} -> version {}", doc_id, new_v);
                            self.fs_state.ack_our_ot(&doc_id, new_v);
                            return Ok(());
                        }
                        Err(e) => {
                            tracing::warn!("Fast-sync returned error: {}. Falling back to standard OT sync.", e);
                            self.fast_sync_supported.store(false, Ordering::SeqCst);
                        }
                    }
                }

                // Standard fallback: compute diff and send applyOtUpdate over WebSocket
                let ops = ot::diff(&base_text, &current_text);
                if ops.is_empty() {
                    return Ok(());
                }

                // Update base_text and dirty flag immediately so duplicate triggers don't send identical ops
                {
                    let mut inner = self.fs_state.inner.write();
                    let target_ino = inner.overleaf_id_to_ino.get(&doc_id).copied();
                    if let Some(ino) = target_ino {
                        if let Some(data) = inner.inodes.get_mut(&ino) {
                            data.base_text = current_text.clone();
                            data.dirty = false;
                        }
                    }
                }

                let msg_id = self.next_msg_id.fetch_add(1, Ordering::SeqCst);
                let packet = protocol::encode_apply_ot(msg_id, &doc_id, version, &ops);
                pending_ot_updates.insert(msg_id.to_string(), doc_id.clone());

                tracing::info!("Sending applyOtUpdate for doc {} (version {}): {} ops", doc_id, version, ops.len());
                let _ = ws_tx.send(Message::Text(packet.into())).await;
            }

            SyncCommand::UploadFile { file_id: _, name, parent_folder_id, content } => {
                let _ = self.upload_file_http(&parent_folder_id, &name, content).await;
            }

            SyncCommand::CreateDoc { parent_folder_id, name, content, reply } => {
                let res = self.create_doc_http(&parent_folder_id, &name).await;
                match res {
                    Ok(new_doc_id) => {
                        let _ = reply.send(Ok(new_doc_id.clone()));
                        // Join new doc
                        let msg_id = self.next_msg_id.fetch_add(1, Ordering::SeqCst);
                        let packet = protocol::encode_join_doc(msg_id, &new_doc_id);
                        let _ = ws_tx.send(Message::Text(packet.into())).await;

                        if !content.is_empty() {
                            let fast_sync_enabled = !self.disable_fast_sync && self.fast_sync_supported.load(Ordering::SeqCst);
                            if fast_sync_enabled {
                                let _ = self.try_fast_sync_doc(&new_doc_id, 0, &content).await;
                            }
                        }
                    }
                    Err(e) => {
                        let _ = reply.send(Err(e.to_string()));
                    }
                }
            }

            SyncCommand::CreateFolder { parent_folder_id, name, reply } => {
                let res = self.create_folder_http(&parent_folder_id, &name).await;
                let _ = reply.send(res.map_err(|e| e.to_string()));
            }

            SyncCommand::DeleteEntity { entity_type, entity_id } => {
                let _ = self.delete_entity_http(&entity_type, &entity_id).await;
            }

            SyncCommand::RenameEntity { entity_type, entity_id, new_name } => {
                let _ = self.rename_entity_http(&entity_type, &entity_id, &new_name).await;
            }
        }

        Ok(())
    }

    async fn handle_ws_message<S>(
        &self,
        raw: &str,
        ws_tx: &mut S,
        pending_joins: &mut HashMap<String, String>,
        pending_ot_updates: &mut HashMap<String, String>,
        pending_conflict_reconciliations: &mut HashMap<String, (String, String)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        S: futures_util::Sink<Message> + Unpin,
        <S as futures_util::Sink<Message>>::Error: std::fmt::Debug,
    {
        let packet = match Packet::parse(raw) {
            Ok(p) => p,
            Err(e) => {
                tracing::debug!("Packet parse error: {:?} for raw: {}", e, raw);
                return Ok(());
            }
        };

        match packet {
            Packet::Heartbeat => {
                let _ = ws_tx.send(Message::Text("2::".into())).await;
            }

            Packet::Event { id: _, ack: _, name, args } => {
                if name == "joinProjectResponse" {
                    if let Some(first_arg) = args.first() {
                        let resp: protocol::JoinProjectResponse = serde_json::from_value(first_arg.clone())?;
                        if let Some(pub_id) = resp.public_id {
                            *self.public_id.write() = Some(pub_id.clone());
                            tracing::info!("Received joinProjectResponse. Our public_id: {}", pub_id);
                        }

                        // Populate project tree into FsState
                        self.populate_project_tree(&resp.project, ws_tx, pending_joins).await?;
                    }
                } else if name == "otUpdateApplied" {
                    if let Some(first_arg) = args.first() {
                        if let Ok(update) = serde_json::from_value::<protocol::OtUpdatePayload>(first_arg.clone()) {
                            let our_pub_id = self.public_id.read().clone();
                            let is_from_us = update
                                .meta
                                .as_ref()
                                .and_then(|m| m.source.as_ref())
                                .map(|s| Some(s) == our_pub_id.as_ref() || s == "fast-sync")
                                .unwrap_or(false);

                            if is_from_us {
                                tracing::info!("Our OT update applied to doc {} at version {}", update.doc, update.v + 1);
                                self.fs_state.ack_our_ot(&update.doc, update.v + 1);
                            } else {
                                tracing::info!(
                                    "Collaborator OT update on doc {} (v: {}, ops: {})",
                                    update.doc,
                                    update.v,
                                    update.op.len()
                                );
                                if let Some((doc_id, version, reconciled, _)) =
                                    self.fs_state.apply_remote_ot(&update.doc, update.v, &update.op)
                                {
                                    // Conflict/reconciliation triggered!
                                    let fast_sync_enabled = !self.disable_fast_sync && self.fast_sync_supported.load(Ordering::SeqCst);
                                    if fast_sync_enabled {
                                        tracing::info!(
                                            "Conflict/reconciliation on doc {}: applying authoritative reconciled version via fast-sync",
                                            doc_id
                                        );
                                        match self.try_fast_sync_doc(&doc_id, version, &reconciled).await {
                                            Ok(new_v) => {
                                                self.fs_state.ack_our_ot(&doc_id, new_v);
                                            }
                                            Err(e) => {
                                                tracing::warn!("Fast-sync reconciliation failed: {}. Falling back to OT diff.", e);
                                                let join_msg_id = self.next_msg_id.fetch_add(1, Ordering::SeqCst);
                                                let packet = protocol::encode_join_doc(join_msg_id, &doc_id);
                                                pending_conflict_reconciliations.insert(join_msg_id.to_string(), (doc_id, reconciled));
                                                let _ = ws_tx.send(Message::Text(packet.into())).await;
                                            }
                                        }
                                    } else {
                                        tracing::info!(
                                            "Conflict/reconciliation on doc {}: fetching current server state via joinDoc to enforce authoritative version",
                                            doc_id
                                        );
                                        let join_msg_id = self.next_msg_id.fetch_add(1, Ordering::SeqCst);
                                        let packet = protocol::encode_join_doc(join_msg_id, &doc_id);
                                        pending_conflict_reconciliations.insert(join_msg_id.to_string(), (doc_id, reconciled));
                                        let _ = ws_tx.send(Message::Text(packet.into())).await;
                                    }
                                }
                            }
                        } else if let Some(v) = first_arg.get("v").and_then(|v| v.as_i64()) {
                            // Sender acknowledgement format: { v: 1, doc: "..." }
                            if let Some(doc_id) = first_arg.get("doc").and_then(|d| d.as_str()) {
                                tracing::info!("Received OT ack for doc {} -> v {}", doc_id, v + 1);
                                self.fs_state.ack_our_ot(doc_id, v + 1);
                            }
                        }
                    }
                }
            }

            Packet::Ack { id, args } => {
                if let Some((doc_id, reconciled)) = pending_conflict_reconciliations.remove(&id) {
                    if args.len() >= 3 && args[0].is_null() {
                        let lines: Vec<String> = serde_json::from_value(args[1].clone()).unwrap_or_default();
                        let server_version: i64 = serde_json::from_value(args[2].clone()).unwrap_or(0);
                        let server_text = lines.join("\n");
                        let corrective_ops = ot::diff(&server_text, &reconciled);
                        tracing::info!(
                            "Resolving conflict on doc {}: server has v{} ({} lines), sending {} corrective ops to enforce authoritative version",
                            doc_id,
                            server_version,
                            lines.len(),
                            corrective_ops.len()
                        );
                        if !corrective_ops.is_empty() {
                            let apply_msg_id = self.next_msg_id.fetch_add(1, Ordering::SeqCst);
                            let packet = protocol::encode_apply_ot(apply_msg_id, &doc_id, server_version, &corrective_ops);
                            pending_ot_updates.insert(apply_msg_id.to_string(), doc_id.clone());
                            let _ = ws_tx.send(Message::Text(packet.into())).await;
                        }
                        self.fs_state.ack_our_ot(&doc_id, server_version);
                    }
                } else if let Some(doc_id) = pending_joins.remove(&id) {
                    // joinDoc ack format: [null, lines: Vec<String>, version: i64, ops, ranges, ...]
                    if args.len() >= 3 && args[0].is_null() {
                        let lines: Vec<String> = serde_json::from_value(args[1].clone()).unwrap_or_default();
                        let version: i64 = serde_json::from_value(args[2].clone()).unwrap_or(0);
                        let text = lines.join("\n");
                        tracing::info!("Joined doc {} (version {}, {} lines)", doc_id, version, lines.len());

                        // Update doc in FsState
                        let inner = self.fs_state.inner.read();
                        let target_ino = inner.overleaf_id_to_ino.get(&doc_id).copied();
                        drop(inner);
                        if let Some(ino) = target_ino {
                            let mut inner = self.fs_state.inner.write();
                            let is_gitignore = inner.inodes.get(&ino).map(|n| n.name == ".gitignore").unwrap_or(false);
                            if let Some(node) = inner.inodes.get_mut(&ino) {
                                if !node.dirty {
                                    node.content = text.as_bytes().to_vec();
                                    node.base_text = text;
                                    node.version = version;
                                }
                            }
                            if is_gitignore {
                                inner.rebuild_gitignore();
                            }
                        }
                    }
                } else if let Some(doc_id) = pending_ot_updates.remove(&id) {
                    tracing::info!("applyOtUpdate RPC acked for doc {}", doc_id);
                }
            }

            _ => {}
        }

        Ok(())
    }

    async fn populate_project_tree<S>(
        &self,
        project: &ProjectView,
        ws_tx: &mut S,
        pending_joins: &mut HashMap<String, String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        S: futures_util::Sink<Message> + Unpin,
        <S as futures_util::Sink<Message>>::Error: std::fmt::Debug,
    {
        if let Some(root_folder) = project.root_folder.first() {
            self.fs_state.set_root_folder_id(&root_folder._id);
            self.load_folder_recursive(1, root_folder, ws_tx, pending_joins).await?;
            self.fs_state.rebuild_gitignore();
        }
        Ok(())
    }

    async fn load_folder_recursive<S>(
        &self,
        parent_ino: u64,
        folder: &FolderView,
        ws_tx: &mut S,
        pending_joins: &mut HashMap<String, String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        S: futures_util::Sink<Message> + Unpin,
        <S as futures_util::Sink<Message>>::Error: std::fmt::Debug,
    {
        // Add docs
        for doc in &folder.docs {
            let doc_id = &doc._id;
            let name = &doc.name;

            // Check if already in FsState
            let exists = {
                let inner = self.fs_state.inner.read();
                inner.overleaf_id_to_ino.contains_key(doc_id)
            };
            if !exists {
                self.fs_state.add_doc(parent_ino, name, doc_id, "", 0);
            }

            // Emit joinDoc
            let msg_id = self.next_msg_id.fetch_add(1, Ordering::SeqCst);
            let packet = protocol::encode_join_doc(msg_id, doc_id);
            pending_joins.insert(msg_id.to_string(), doc_id.clone());
            let _ = ws_tx.send(Message::Text(packet.into())).await;
        }

        // Add binary files
        for file in &folder.file_refs {
            let file_id = &file._id;
            let name = &file.name;
            let hash = file.hash.clone();

            let exists = {
                let inner = self.fs_state.inner.read();
                inner.overleaf_id_to_ino.contains_key(file_id)
            };
            if !exists {
                // Fetch file content via HTTP in background or lazily
                let content = self.download_file_http(file_id).await.unwrap_or_default();
                self.fs_state.add_file(parent_ino, name, file_id, hash, content);
            }
        }

        // Add subfolders
        for subfolder in &folder.folders {
            let folder_id = &subfolder._id;
            let name = &subfolder.name;

            let folder_ino = {
                let inner = self.fs_state.inner.read();
                inner.overleaf_id_to_ino.get(folder_id).copied()
            };
            let sub_ino = match folder_ino {
                Some(ino) => ino,
                None => self.fs_state.add_folder(parent_ino, name, folder_id),
            };

            Box::pin(self.load_folder_recursive(sub_ino, subfolder, ws_tx, pending_joins)).await?;
        }

        Ok(())
    }

    /// Try the optimized fast-sync REST endpoint
    pub async fn try_fast_sync_doc(
        &self,
        doc_id: &str,
        base_version: i64,
        content: &str,
    ) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        let cookie_str = self.cookie.read().clone();
        let base_url = self.base_url.trim_end_matches('/');
        let url = format!("{}/project/{}/doc/{}/fast-sync", base_url, self.project_id, doc_id);

        let mut req = self.http_client.post(&url);
        if !cookie_str.is_empty() {
            req = req.header(COOKIE, &cookie_str);
        }

        let our_source = self.public_id.read().clone().unwrap_or_else(|| "fast-sync".to_string());
        let body = serde_json::json!({
            "content": content,
            "baseVersion": base_version,
            "source": our_source,
        });

        let res = req.json(&body).send().await?;
        if !res.status().is_success() {
            return Err(format!("Fast-sync HTTP status: {}", res.status()).into());
        }

        let resp_json: serde_json::Value = res.json().await?;
        let new_v = resp_json
            .get("version")
            .and_then(|v| v.as_i64())
            .ok_or("Missing version in fast-sync response")?;

        Ok(new_v)
    }

    pub async fn download_file_http(
        &self,
        file_id: &str,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let cookie_str = self.cookie.read().clone();
        let base_url = self.base_url.trim_end_matches('/');
        let url = format!("{}/project/{}/file/{}", base_url, self.project_id, file_id);

        let mut req = self.http_client.get(&url);
        if !cookie_str.is_empty() {
            req = req.header(COOKIE, &cookie_str);
        }

        let res = req.send().await?;
        if res.status().is_success() {
            Ok(res.bytes().await?.to_vec())
        } else {
            Err(format!("Download failed with status: {}", res.status()).into())
        }
    }

    pub async fn upload_file_http(
        &self,
        folder_id: &str,
        name: &str,
        content: Vec<u8>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let cookie_str = self.cookie.read().clone();
        let base_url = self.base_url.trim_end_matches('/');
        let url = format!("{}/project/{}/upload?folder_id={}", base_url, self.project_id, folder_id);

        let form = reqwest::multipart::Form::new().part(
            "qqfile",
            reqwest::multipart::Part::bytes(content).file_name(name.to_string()),
        );

        let mut req = self.http_client.post(&url);
        if !cookie_str.is_empty() {
            req = req.header(COOKIE, &cookie_str);
        }

        let res = req.multipart(form).send().await?;
        let status = res.status();
        let body = res.text().await?;
        if status.is_success() {
            tracing::info!("Uploaded file {} successfully: {}", name, body);
            Ok(body)
        } else {
            Err(format!("Upload failed (status {}): {}", status, body).into())
        }
    }

    pub async fn create_doc_http(
        &self,
        folder_id: &str,
        name: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let cookie_str = self.cookie.read().clone();
        let base_url = self.base_url.trim_end_matches('/');
        let url = format!("{}/project/{}/doc", base_url, self.project_id);

        let mut req = self.http_client.post(&url);
        if !cookie_str.is_empty() {
            req = req.header(COOKIE, &cookie_str);
        }
        if let Some(ref csrf) = *self.csrf_token.read() {
            req = req.header("X-Csrf-Token", csrf);
        }

        let mut body = serde_json::json!({
            "name": name,
        });
        if !folder_id.is_empty() && folder_id != "root" {
            body["parent_folder_id"] = serde_json::Value::String(folder_id.to_string());
        }

        let res = req.json(&body).send().await?;
        let status = res.status();
        let body_text = res.text().await.unwrap_or_default();
        tracing::info!("create_doc_http status: {}, body: {}", status, body_text);
        let val: serde_json::Value = serde_json::from_str(&body_text)?;
        let doc_id = val
            .get("_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("Invalid create doc response: {:?}", val))?;

        Ok(doc_id.to_string())
    }

    pub async fn create_folder_http(
        &self,
        folder_id: &str,
        name: &str,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let cookie_str = self.cookie.read().clone();
        let base_url = self.base_url.trim_end_matches('/');
        let url = format!("{}/project/{}/folder", base_url, self.project_id);

        let mut req = self.http_client.post(&url);
        if !cookie_str.is_empty() {
            req = req.header(COOKIE, &cookie_str);
        }
        if let Some(ref csrf) = *self.csrf_token.read() {
            req = req.header("X-Csrf-Token", csrf);
        }

        let mut body = serde_json::json!({
            "name": name,
        });
        if !folder_id.is_empty() && folder_id != "root" {
            body["parent_folder_id"] = serde_json::Value::String(folder_id.to_string());
        }

        let res = req.json(&body).send().await?;
        let val: serde_json::Value = res.json().await?;
        let id = val
            .get("_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| format!("Invalid create folder response: {:?}", val))?;

        Ok(id.to_string())
    }

    pub async fn delete_entity_http(
        &self,
        entity_type: &str,
        entity_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let cookie_str = self.cookie.read().clone();
        let base_url = self.base_url.trim_end_matches('/');
        let url = format!("{}/project/{}/{}/{}", base_url, self.project_id, entity_type, entity_id);

        let mut req = self.http_client.delete(&url);
        if !cookie_str.is_empty() {
            req = req.header(COOKIE, &cookie_str);
        }
        if let Some(ref csrf) = *self.csrf_token.read() {
            req = req.header("X-Csrf-Token", csrf);
        }

        let res = req.send().await?;
        tracing::info!("Deleted entity {}/{}, status: {}", entity_type, entity_id, res.status());
        Ok(())
    }

    pub async fn rename_entity_http(
        &self,
        entity_type: &str,
        entity_id: &str,
        new_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let cookie_str = self.cookie.read().clone();
        let base_url = self.base_url.trim_end_matches('/');
        let url = format!("{}/project/{}/{}/{}/rename", base_url, self.project_id, entity_type, entity_id);

        let mut req = self.http_client.post(&url);
        if !cookie_str.is_empty() {
            req = req.header(COOKIE, &cookie_str);
        }
        if let Some(ref csrf) = *self.csrf_token.read() {
            req = req.header("X-Csrf-Token", csrf);
        }

        let body = serde_json::json!({ "name": new_name });
        let res = req.json(&body).send().await?;
        tracing::info!("Renamed entity {}/{} to {}, status: {}", entity_type, entity_id, new_name, res.status());
        Ok(())
    }
}

fn extract_csrf_token(html: &str) -> Option<String> {
    if let Some(pos) = html.find("name=\"ol-csrfToken\"") {
        let slice = &html[pos..];
        if let Some(content_pos) = slice.find("content=\"") {
            let start = content_pos + 9;
            let end = slice[start..].find('"')?;
            return Some(slice[start..start + end].to_string());
        }
    }
    None
}
