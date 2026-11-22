//! Live synchronisation between the mount and Overleaf.
//!
//! # Design
//!
//! Two independent transports, both feeding the same in-memory model:
//!
//! * **WebSocket (primary).** `/project/:id/socket.io` — the same channel the
//!   Overleaf editor uses. We join the project, join each doc, and then receive
//!   `otUpdateApplied` for *every* edit made by *anyone*, and send our own edits
//!   as `applyOtUpdate`. This gives sub-second propagation in both directions
//!   and lets the server do the OT transform/ordering for us.
//! * **HTTP polling (fallback).** Used when the socket cannot be established
//!   (older Overleaf, no real-time service, socket auth denied) or when it drops
//!   for longer than `RECONNECT_GRACE`. We poll `GET /project/:id/updates` and
//!   `GET /project/:id/doc/:doc_id` and reconcile with the three-way merge.
//!
//! # Conflict resolution
//!
//! * Text docs while the socket is up: **server-side OT**. Our local change is
//!   diffed against the version we last saw and sent as an op with `v = <that
//!   version>`. If a remote op arrived first, document-updater transforms ours
//!   onto it, so concurrent edits to the same sentence both survive (interleaved
//!   by op order). We then apply the returned `otUpdateApplied` to our cache.
//! * Text docs while polling: **three-way merge** (`merge3`) against the
//!   common ancestor. Non-overlapping edits merge automatically; overlapping
//!   ones keep both sides inside `<<<<<<<`/`>>>>>>>` markers so nothing is lost.
//! * Binary uploads: no OT type exists, so we use last-writer-wins with mtime
//!   comparison and refuse to clobber a newer remote version silently.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use log::{debug, info, trace, warn};
use serde_json::{json, Value};
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

use crate::merge3;
use crate::ot::{self, TextOp};
use crate::overleaf::OverleafClient;
use crate::types::{Doc, Entity, Fs, NodeKind, ROOT_INO};

/// How long we tolerate a socket outage before falling back to polling.
const RECONNECT_GRACE: Duration = Duration::from_secs(2);
/// Poll interval in fallback mode.
const POLL_INTERVAL: Duration = Duration::from_millis(700);
/// How often to re-read project metadata (new files/folders).
const TREE_REFRESH: Duration = Duration::from_secs(10);

/// A local edit the filesystem wants pushed to Overleaf.
#[derive(Debug, Clone)]
pub enum Outgoing {
    /// The user wrote to a text document on the mount. `seq` is the order in
    /// which the filesystem observed the write; the engine ignores intents that
    /// arrive out of order (the kernel can deliver `truncate(0)` after the
    /// `write(2)` that follows it, and honouring both would empty the file).
    WriteDoc {
        doc_id: String,
        text: String,
        seq: u64,
    },
    /// The user created a new text file.
    CreateDoc { parent: u64, name: String, text: String },
    CreateDir { parent: u64, name: String },
    Rename { ino: u64, new_parent: u64, new_name: String },
    Delete { ino: u64 },
}

/// What we are waiting for when an acknowledgement frame arrives.
#[derive(Debug, Clone)]
enum Ack {
    JoinDoc(String),
    ApplyUpdate(String),
}

/// socket.io 0.9 event frame without an acknowledgement id.
fn socket_event(name: &str, args: Value) -> String {
    format!("5:::{}", json!({ "name": name, "args": args }))
}

/// socket.io 0.9 event frame that requests an acknowledgement.
fn socket_event_ack(ack_id: u64, name: &str, args: Value) -> String {
    format!("5:{ack_id}+::{}", json!({ "name": name, "args": args }))
}

fn chrono_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

/// Push a frame into the live socket, if any.
async fn out_rx_send(engine: &SyncEngine, frame: String) {
    let tx = engine.socket_tx.lock().await.clone();
    if let Some(tx) = tx {
        if tx.send(frame).is_err() {
            debug!("socket send channel closed");
        }
    }
}

/// Handle used by the FUSE layer to talk to the engine.
#[derive(Clone)]
pub struct SyncHandle {
    tx: mpsc::UnboundedSender<Outgoing>,
}

impl SyncHandle {
    pub fn send(&self, ev: Outgoing) {
        if self.tx.send(ev).is_err() {
            warn!("sync engine has shut down; dropping filesystem event");
        }
    }
}

/// Notifications from the engine back into the FUSE layer.
pub trait Notifier: Send + Sync + 'static {
    /// The content of `ino` changed on the server.
    fn invalidate(&self, ino: u64);
    /// The directory tree changed (created/removed/renamed nodes).
    fn tree_changed(&self);
}

/// The engine. Owns the Overleaf client and knows the doc ↔ inode mapping.
pub struct SyncEngine {
    pub fs: Arc<Fs>,
    pub client: Arc<OverleafClient>,
    pub notifier: Arc<dyn Notifier>,
    /// doc_id -> version we have cached.
    versions: Mutex<HashMap<String, i64>>,
    /// Requests from the filesystem.
    rx: Mutex<mpsc::UnboundedReceiver<Outgoing>>,
    /// Own writes, so we can ignore their echo.
    self_writes: Mutex<HashMap<String, usize>>,
    /// Sender into the live websocket task (installed while connected).
    socket_tx: Mutex<Option<mpsc::UnboundedSender<String>>>,
    /// Per document, the text the *pending local change* is relative to. A local
    /// edit is expressed as a diff from this text; when collaborators move the
    /// document underneath us the diff is rebased onto their version instead of
    /// overwriting it.
    desired_from: Mutex<HashMap<String, String>>,
    /// The operation we last sent per document, with the text it was computed
    /// against. Confirmation of our own edit carries no op, so the new base is
    /// derived from this — never from the visible file, which may already hold
    /// newer local typing.
    in_flight: Mutex<HashMap<String, (TextOp, String)>>,
    /// Documents where a collaborator's edit arrived while ours was in flight:
    /// the server transformed ours, so the result must be re-read.
    diverged: Mutex<std::collections::HashSet<String>>,
    /// Highest write-intent sequence number applied per document.
    latest_seq: Mutex<HashMap<String, u64>>,
    /// Latest text the filesystem wants the server to hold, per document.
    /// Newer writes overwrite older ones, which is what coalesces a
    /// `truncate(0)` + `write(2)` pair (and fast typists) into one operation.
    desired: Mutex<HashMap<String, String>>,
    /// Documents that currently have a pump task running.
    pumping: Mutex<std::collections::HashSet<String>>,
    /// Per-document confirmation counter: bumped every time `synced` advances,
    /// so a pump can wait for *its* operation to land before computing the next
    /// diff. A counter (not a flag) means a late bump can never be mistaken for
    /// the confirmation of a newer operation.
    confirm: Mutex<HashMap<String, tokio::sync::watch::Sender<u64>>>,
    /// The last text/version the *server* confirmed, per document. Local edits
    /// are diffed against this (never against the visible cache, which the FUSE
    /// layer updates optimistically) and remote ops are applied to it, so the
    /// mount always has a correct OT base.
    synced: Mutex<HashMap<String, (String, i64)>>,
    /// In-flight RPCs awaiting a socket.io 0.9 acknowledgement frame.
    acks: Mutex<HashMap<u64, Ack>>,
    /// Signalled as soon as the project tree has been received over the socket.
    ready: tokio::sync::Notify,
    /// Number of documents whose initial `joinDoc` snapshot is still outstanding.
    pending_docs: Mutex<usize>,
    /// Signalled once every initial document snapshot has arrived.
    contents_ready: tokio::sync::Notify,
    ack_counter: std::sync::atomic::AtomicU64,
    pub socket_up: Arc<std::sync::atomic::AtomicBool>,
}

impl SyncEngine {
    pub fn new(
        fs: Arc<Fs>,
        client: Arc<OverleafClient>,
        notifier: Arc<dyn Notifier>,
    ) -> (Arc<Self>, SyncHandle) {
        let (tx, rx) = mpsc::unbounded_channel();
        let engine = Arc::new(Self {
            fs,
            client,
            notifier,
            versions: Mutex::new(HashMap::new()),
            rx: Mutex::new(rx),
            self_writes: Mutex::new(HashMap::new()),
            socket_tx: Mutex::new(None),
            synced: Mutex::new(HashMap::new()),
            desired: Mutex::new(HashMap::new()),
            latest_seq: Mutex::new(HashMap::new()),
            desired_from: Mutex::new(HashMap::new()),
            in_flight: Mutex::new(HashMap::new()),
            diverged: Mutex::new(std::collections::HashSet::new()),
            pumping: Mutex::new(std::collections::HashSet::new()),
            confirm: Mutex::new(HashMap::new()),
            acks: Mutex::new(HashMap::new()),
            ready: tokio::sync::Notify::new(),
            pending_docs: Mutex::new(0),
            contents_ready: tokio::sync::Notify::new(),
            ack_counter: std::sync::atomic::AtomicU64::new(1),
            socket_up: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        });
        (engine, SyncHandle { tx })
    }

    /// Initial load.
    ///
    /// The authoritative project tree (with document ids) comes from the
    /// socket's `joinProjectResponse`, exactly like the Overleaf editor. We
    /// start the socket, wait briefly for the tree, and fall back to the HTTP
    /// endpoints when the socket is unavailable.
    pub async fn bootstrap(self: &Arc<Self>) -> Result<()> {
        // Kick off the socket first; `on_join_project` signals `ready`.
        let me = self.clone();
        tokio::spawn(async move {
            if let Err(e) = me.ws_supervisor().await {
                warn!("websocket supervisor stopped: {e:#}");
            }
        });
        let ready = tokio::time::timeout(Duration::from_secs(10), self.ready.notified()).await;
        if ready.is_ok() {
            // The tree is here; wait briefly for the document snapshots so the
            // mount is usable the moment it appears.
            if tokio::time::timeout(Duration::from_secs(10), self.contents_ready.notified())
                .await
                .is_err()
            {
                warn!("some document contents did not load before the mount started");
            }
        }
        if ready.is_err() {
            warn!("real-time did not deliver the project tree; using HTTP fallback");
            let meta = self.client.fetch_project().await?;
            self.rebuild_tree(&meta).await?;
            let mut versions = HashMap::new();
            for entry in &meta.docs {
                if let Ok(doc) = self.client.get_doc(&entry.id).await {
                    versions.insert(entry.id.clone(), doc.version);
                    self.store_doc(&entry.path, doc);
                }
            }
            *self.versions.lock().await = versions;
        }
        let docs = self.fs.doc_inodes.read().len();
        info!(
            "project '{}' loaded: {} documents, {} directories",
            self.fs.mount_point.read(),
            docs,
            self.fs
                .nodes
                .read()
                .values()
                .filter(|n| n.is_dir())
                .count()
        );
        self.notifier.tree_changed();
        Ok(())
    }

    /// Create the directory/doc nodes for a project tree.
    async fn rebuild_tree(&self, meta: &crate::types::ProjectMeta) -> Result<()> {
        let mut folders = meta.folders.clone();
        folders.sort_by_key(|f| f.path.matches('/').count());
        for f in &folders {
            self.ensure_dir(&f.path);
        }
        for d in &meta.docs {
            let (parent, name) = match d.path.rsplit_once('/') {
                Some((dir, name)) => (self.ensure_dir(dir), name.to_string()),
                None => (ROOT_INO, d.path.clone()),
            };
            let ino = self.fs.upsert(
                parent,
                &name,
                NodeKind::Doc {
                    doc_id: d.id.clone(),
                    ranges: None,
                },
                0,
            );
            self.fs.doc_inodes.write().insert(d.id.clone(), ino);
        }
        for f in &meta.files {
            self.fs.upsert(
                ROOT_INO,
                &f.name,
                NodeKind::File {
                    file_id: f.id.clone(),
                },
                f.size as u64,
            );
        }
        Ok(())
    }

    /// Resolve (creating as needed) a directory path to an inode.
    fn ensure_dir(&self, path: &str) -> u64 {
        let mut cur = ROOT_INO;
        for part in path.split('/').filter(|p| !p.is_empty()) {
            cur = match self.fs.lookup(cur, part) {
                Some(ino) => ino,
                None => self.fs.upsert(cur, part, NodeKind::Dir, 0),
            };
        }
        cur
    }

    /// Store a doc loaded from the server under its path.
    fn store_doc(&self, path: &str, doc: Doc) {
        let (parent, name) = match path.rsplit_once('/') {
            Some((dir, name)) => (self.ensure_dir(dir), name.to_string()),
            None => (ROOT_INO, path.to_string()),
        };
        let ino = self.fs.upsert(
            parent,
            &name,
            NodeKind::Doc {
                doc_id: doc.id.clone(),
                ranges: doc.ranges.clone(),
            },
            0,
        );
        self.fs.doc_inodes.write().insert(doc.id.clone(), ino);
        self.fs.set_doc(ino, doc);
    }

    /// Main loop. Runs forever; aborts only on fatal errors.
    pub async fn run(self: Arc<Self>) -> Result<()> {
        let me = self.clone();
        let mut poll = tokio::spawn(async move { me.poll_loop().await });
        let me = self.clone();
        let mut intents = tokio::spawn(async move { me.intent_loop().await });

        tokio::select! {
            r = &mut poll => warn!("poll loop exited: {:?}", r),
            r = &mut intents => warn!("intent loop exited: {:?}", r),
        }
        bail!("sync engine stopped")
    }

    /// Process filesystem -> server events.
    async fn intent_loop(self: Arc<Self>) -> Result<()> {
        loop {
            let ev = {
                let mut rx = self.rx.lock().await;
                rx.recv().await
            };
            let Some(ev) = ev else { return Ok(()) };
            let res = match ev {
                Outgoing::WriteDoc { doc_id, text, seq } => {
                    self.clone().request_write(doc_id, text, seq).await;
                    Ok(())
                }
                Outgoing::CreateDoc {
                    parent,
                    name,
                    text,
                } => self.create_doc(parent, &name, &text).await,
                Outgoing::CreateDir { parent, name } => self.create_dir(parent, &name).await,
                Outgoing::Rename {
                    ino,
                    new_parent,
                    new_name,
                } => self.rename(ino, new_parent, &new_name).await,
                Outgoing::Delete { ino } => self.delete(ino).await,
            };
            if let Err(e) = res {
                warn!("sync error: {e:#}");
            }
        }
    }

    /// Record that `doc_id` should hold `text`, and make sure a pump is running
    /// for it. Returns immediately: the network round-trip happens off the FUSE
    /// thread, and repeated writes to the same document collapse into one.
    async fn request_write(self: Arc<Self>, doc_id: String, text: String, seq: u64) {
        {
            let mut latest = self.latest_seq.lock().await;
            match latest.get(&doc_id) {
                Some(seen) if *seen > seq => {
                    debug!("doc {doc_id}: dropping out-of-order write intent ({seq} < {seen})");
                    return;
                }
                _ => {
                    latest.insert(doc_id.clone(), seq);
                }
            }
        }
        let first_of_burst = !self.desired.lock().await.contains_key(&doc_id);
        if first_of_burst {
            // Remember what the local change is relative to (the last text the
            // server confirmed), *before* taking the `desired` lock.
            let (base, _) = self.doc_base(&doc_id).await;
            self.desired_from.lock().await.insert(doc_id.clone(), base);
        }
        self.desired.lock().await.insert(doc_id.clone(), text);
        let already = {
            let mut pumping = self.pumping.lock().await;
            let already = pumping.contains(&doc_id);
            if !already {
                pumping.insert(doc_id.clone());
            }
            already
        };
        if already {
            // A pump is already running; it will pick up the newer text.
            self.wake_doc(&doc_id).await;
            return;
        }
        tokio::spawn(async move {
            loop {
                if let Err(e) = self.clone().pump_doc(&doc_id).await {
                    // Keep `desired` so the write is retried: a socket drop or a
                    // refused HTTP fallback must not lose the user's edit (they
                    // already got a successful write(2) from the kernel).
                    warn!("sync error for {doc_id}: {e:#}; will retry");
                    tokio::time::sleep(Duration::from_millis(750)).await;
                }
                // Stop unless a newer write arrived while we were finishing.
                if !self.desired.lock().await.contains_key(&doc_id) {
                    break;
                }
            }
            self.pumping.lock().await.remove(&doc_id);
        });
    }

    /// Sender/receiver pair for a document's confirmation counter.
    async fn confirm_channel(&self, doc_id: &str) -> tokio::sync::watch::Receiver<u64> {
        let mut map = self.confirm.lock().await;
        let tx = map
            .entry(doc_id.to_string())
            .or_insert_with(|| tokio::sync::watch::channel(0u64).0);
        tx.subscribe()
    }

    /// Tell a waiting pump that `synced` has advanced for this document.
    async fn bump_confirm(&self, doc_id: &str) {
        let mut map = self.confirm.lock().await;
        if let Some(tx) = map.get(doc_id) {
            tx.send_modify(|v| *v = v.wrapping_add(1));
        }
    }

    /// Wake a running pump so it re-reads `desired` (the text may have changed
    /// while it was waiting for a confirmation).
    async fn wake_doc(&self, doc_id: &str) {
        let mut map = self.confirm.lock().await;
        if let Some(tx) = map.get_mut(doc_id) {
            tx.send_modify(|v| *v = v.wrapping_add(1));
        }
    }

    /// Drive one document towards `desired`, one confirmed operation at a time.
    ///
    /// The crucial invariant is that every operation is computed against the
    /// text the server has *confirmed*, never against one that is still in
    /// flight. Sending two operations computed from the same base makes the
    /// server transform the second against the first, which duplicates or
    /// deletes text (this was a real bug: `truncate(0)` followed by `write(2)`
    /// wiped the document).
    async fn pump_doc(self: Arc<Self>, doc_id: &str) -> Result<()> {
        for _round in 0..64 {
            let want = match self.desired.lock().await.get(doc_id).cloned() {
                Some(w) => w,
                None => return Ok(()),
            };

            // Short coalescing window: a `truncate` + `write` pair (or a burst of
            // keystrokes) becomes a single operation.
            tokio::time::sleep(Duration::from_millis(20)).await;
            let newer = self.desired.lock().await.get(doc_id).cloned();
            if newer.as_deref().map(|n| n != want).unwrap_or(false) {
                debug!("pump {doc_id}: superseded by a newer write");
                continue;
            }

            let (base, version) = self.doc_base(doc_id).await;
            let from = self
                .desired_from
                .lock()
                .await
                .get(doc_id)
                .cloned()
                .unwrap_or_else(|| base.clone());
            // Rebase the local change onto whatever the server holds now, so a
            // collaborator's concurrent edit is kept instead of being overwritten
            // by our snapshot of the file.
            let target = rebase_local(&from, &want, &base);

            if base == target {
                let unchanged = self
                    .desired
                    .lock()
                    .await
                    .get(doc_id)
                    .map(|d| d == &want)
                    .unwrap_or(false);
                if unchanged {
                    self.desired.lock().await.remove(doc_id);
                    self.desired_from.lock().await.remove(doc_id);
                }
                return Ok(());
            }

            let op = ot::diff(&base, &target);
            if op.is_noop() {
                self.desired.lock().await.remove(doc_id);
                return Ok(());
            }
            let (ins, del) = op.change_size();

            if self.socket_up.load(std::sync::atomic::Ordering::SeqCst) {
                let mut rx = self.confirm_channel(doc_id).await;
                let generation = *rx.borrow();
                self.in_flight
                    .lock()
                    .await
                    .insert(doc_id.to_string(), (op.clone(), base.clone()));
                debug!("doc {doc_id}: pushing +{ins}/-{del} on v{version}");
                match self.send_via_socket(doc_id, &op, version, &base).await {
                    Ok(()) => {
                        let confirmed = tokio::time::timeout(Duration::from_secs(8), async {
                            loop {
                                if rx.changed().await.is_err() {
                                    return;
                                }
                                if *rx.borrow() > generation {
                                    return;
                                }
                            }
                        })
                        .await;
                        if confirmed.is_err() {
                            warn!("doc {doc_id}: no confirmation for our edit; resyncing");
                            self.in_flight.lock().await.remove(doc_id);
                            self.resync_doc(doc_id).await;
                        }
                        continue;
                    }
                    Err(e) => warn!("socket send failed ({e:#}); using HTTP fallback"),
                }
            }

            // HTTP fallback: PUT the whole document. Overleaf turns the new text
            // into an update itself, so other editors see a normal edit.
            let lines: Vec<String> = want.split('\n').map(String::from).collect();
            let ranges = self.fs.doc_inodes.read().get(doc_id).copied()
                .and_then(|ino| self.fs.doc(ino))
                .and_then(|d| d.ranges);
            self.client
                .set_doc(doc_id, &lines, version, ranges.as_ref(), "overleaf-mount")
                .await?;
            if let Ok(fresh) = self.client.get_doc(doc_id).await {
                self.store_remote_doc_full(doc_id, fresh.lines, fresh.version, fresh.ranges)
                    .await;
            }
            // The PUT already carries `want`; nothing further to send.
            let unchanged = self
                .desired
                .lock()
                .await
                .get(doc_id)
                .map(|d| d == &want)
                .unwrap_or(false);
            if unchanged {
                self.desired.lock().await.remove(doc_id);
                self.desired_from.lock().await.remove(doc_id);
            }
            return Ok(());
        }
        warn!("doc {doc_id}: gave up after 64 synchronisation rounds");
        Ok(())
    }

    /// The last server-confirmed text and version for a document, falling back
    /// to the visible cache before the first confirmation.
    async fn doc_base(&self, doc_id: &str) -> (String, i64) {
        if let Some(v) = self.synced.lock().await.get(doc_id).cloned() {
            return v;
        }
        let ino = self.fs.doc_inodes.read().get(doc_id).copied();
        match ino.and_then(|i| self.fs.doc(i)) {
            Some(d) => (d.text(), d.version),
            None => (String::new(), 0),
        }
    }

    async fn create_doc(self: &Arc<Self>, parent: u64, name: &str, text: &str) -> Result<()> {
        let lines: Vec<String> = text.split('\n').map(String::from).collect();
        let doc_id = if self.fs.is_ignored(&self.fs.path_of(parent)) {
            // local-only file: nothing to do
            return Ok(());
        } else {
            self.client.add_doc(name, &lines).await?
        };
        let ino = self.fs.upsert(
            parent,
            name,
            NodeKind::Doc {
                doc_id: doc_id.clone(),
                ranges: None,
            },
            text.len() as u64,
        );
        self.fs.doc_inodes.write().insert(doc_id.clone(), ino);
        self.fs.set_doc(
            ino,
            Doc {
                id: doc_id.clone(),
                lines,
                version: 0,
                ranges: None,
            },
        );
        self.versions.lock().await.insert(doc_id.clone(), 0);
        self.notifier.tree_changed();
        // Content written before the server id existed is sitting in the
        // in-memory image; push it now that the document is real.
        let text = self.fs.doc(ino).map(|d| d.text()).unwrap_or_default();
        if !text.is_empty() {
            self.clone().request_write(doc_id.clone(), text, 0).await;
        }
        debug!("created document {doc_id}");
        Ok(())
    }

    async fn create_dir(&self, parent: u64, name: &str) -> Result<()> {
        let path = {
            let parent_path = self.fs.path_of(parent);
            if parent_path.is_empty() {
                name.to_string()
            } else {
                format!("{parent_path}/{name}")
            }
        };
        if self.fs.is_ignored(&path) {
            return Ok(());
        }
        self.client.add_folder(name).await?;
        self.notifier.tree_changed();
        Ok(())
    }

    async fn rename(&self, ino: u64, new_parent: u64, new_name: &str) -> Result<()> {
        let node = match self.fs.node(ino) {
            Some(n) => n,
            None => return Ok(()),
        };
        if node.is_dir() {
            // Folders need a move API call; we keep the mount consistent and
            // let the next poll pick up the server state.
            debug!("folder rename is reflected on the next tree refresh");
            return Ok(());
        }
        let path = self.fs.path_of(ino);
        if self.fs.is_ignored(&path) {
            return Ok(());
        }
        match &node.kind {
            NodeKind::Doc { doc_id, .. } => {
                self.client
                    .rename_entity(doc_id, Entity::Doc, new_name)
                    .await?;
            }
            NodeKind::File { .. } => {
                debug!("binary renames are not supported by the upload API");
            }
            _ => {}
        }
        Ok(())
    }

    async fn delete(&self, ino: u64) -> Result<()> {
        let path = self.fs.path_of(ino);
        if self.fs.is_ignored(&path) {
            return Ok(());
        }
        if let Some(node) = self.fs.node(ino) {
            if let NodeKind::Doc { doc_id, .. } = &node.kind {
                if let Err(e) = self.client.delete_doc(doc_id).await {
                    warn!("could not delete doc {doc_id} on the server: {e:#}");
                }
            }
        }
        Ok(())
    }

    // ---------------------------------------------------------------- socket --

    async fn ws_supervisor(self: Arc<Self>) -> Result<()> {
        loop {
            match self.clone().ws_connect().await {
                Ok(()) => info!("websocket closed"),
                Err(e) => debug!("websocket unavailable: {e:#}"),
            }
            *self.socket_tx.lock().await = None;
            self.socket_up
                .store(false, std::sync::atomic::Ordering::SeqCst);
            tokio::time::sleep(RECONNECT_GRACE).await;
        }
    }

    /// Connect to Overleaf's real-time service and keep the mount in sync.
    ///
    /// The wire protocol is **socket.io 0.9** (Overleaf's fork), not Engine.IO.
    /// Frames look like `<type>:<ackId>:<endpoint>:<data>`:
    ///
    /// * `1::`                    connect
    /// * `2::`                    heartbeat (the server expects it)
    /// * `5:::{...}`              event: `{"name": "...", "args": [...]}`
    /// * `5:<id>+::{...}`         event expecting an acknowledgement
    /// * `6:::<id>+[...]`         acknowledgement for event `<id>`
    ///
    /// The handshake is a plain HTTP GET to `/socket.io/1/` that returns
    /// `<session>:<heartbeat>:<close>:<transports>`; the websocket then lives at
    /// `/socket.io/1/websocket/<session>`. Both must carry the session cookie
    /// *and* `?projectId=<id>` (real-time validates the project up front).
    async fn ws_connect(self: Arc<Self>) -> Result<()> {
        let rt = self.client.realtime_base().to_string();
        let project_id = self.client.project_id().to_string();
        let cookie = self.client.cookie_header().await;

        // 1. Handshake.
        let hs_url = format!(
            "{rt}/socket.io/1/?t={}&projectId={project_id}",
            chrono_millis()
        );
        let mut req = self.client.http_client().get(&hs_url);
        if !cookie.is_empty() {
            req = req.header("cookie", cookie.clone());
        }
        let hs = req.send().await.context("socket.io handshake")?;
        if !hs.status().is_success() {
            bail!("socket.io handshake failed: {}", hs.status());
        }
        let hs_body = hs.text().await.unwrap_or_default();
        let sid = hs_body
            .split(':')
            .next()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow!("socket.io handshake returned '{hs_body}'"))?;
        let heartbeat_secs: u64 = hs_body
            .split(':')
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(60);

        // 2. Websocket upgrade.
        let ws_base = rt.replacen("https://", "wss://", 1).replacen("http://", "ws://", 1);
        let url = format!("{ws_base}/socket.io/1/websocket/{sid}?projectId={project_id}");
        let mut request = url
            .as_str()
            .into_client_request()
            .context("building websocket request")?;
        if !cookie.is_empty() {
            request
                .headers_mut()
                .insert("cookie", cookie.parse().context("cookie header")?);
        }
        if let Ok(origin) = self.client.base_url().origin().ascii_serialization().parse() {
            request.headers_mut().insert("origin", origin);
        }

        let (mut ws, _resp) = tokio_tungstenite::connect_async(request)
            .await
            .context("connecting to real-time socket")?;
        info!("real-time websocket connected");
        let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();
        *self.socket_tx.lock().await = Some(out_tx);
        self.socket_up
            .store(true, std::sync::atomic::Ordering::SeqCst);
        *self.acks.lock().await = HashMap::new();

        // 3. Join the project; the response carries the whole tree.
        out_rx_send(
            &self,
            socket_event("joinProject", json!([{ "project_id": project_id }])),
        )
        .await;

        let mut ping = tokio::time::interval(Duration::from_secs(heartbeat_secs.max(10) / 2));
        ping.tick().await;

        loop {
            tokio::select! {
                msg = ws.next() => {
                    match msg {
                        Some(Ok(Message::Text(txt))) => {
                            self.clone().handle_socket_frame(&txt).await;
                        }
                        Some(Ok(Message::Binary(b))) => {
                            if let Ok(txt) = String::from_utf8(b) {
                                self.clone().handle_socket_frame(&txt).await;
                            }
                        }
                        Some(Ok(Message::Ping(p))) => { ws.send(Message::Pong(p)).await?; }
                        Some(Ok(Message::Close(_))) | None => {
                            info!("real-time socket closed");
                            break;
                        }
                        Some(Err(e)) => return Err(anyhow!("websocket error: {e}")),
                        _ => {}
                    }
                }
                Some(frame) = out_rx.recv() => {
                    ws.send(Message::Text(frame)).await?;
                }
                _ = ping.tick() => {
                    ws.send(Message::Text("2::".into())).await.ok();
                }
            }
        }
        *self.socket_tx.lock().await = None;
        self.socket_up
            .store(false, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    /// Handle one socket.io 0.9 frame.
    ///
    /// The framing is `<type>:<ack id?>:<endpoint>:<data>`, but the endpoint is
    /// usually empty, so the reliable way to read a frame is: take the leading
    /// type character, then treat everything before the first `[`/`{` as the
    /// header, from which an `<digits>+` acknowledgement id may be extracted.
    async fn handle_socket_frame(self: Arc<Self>, raw: &str) {
        trace!("socket frame: {raw}");
        let Some(kind) = raw.chars().next() else { return };
        let rest = raw.get(2..).unwrap_or("");
        let data_start = rest
            .find(['[', '{'])
            .unwrap_or(rest.len());
        let header = &rest[..data_start];
        let data = &rest[data_start..];
        let ack_id: u64 = header
            .split('+')
            .next()
            .unwrap_or("")
            .chars()
            .filter(|c| c.is_ascii_digit())
            .collect::<String>()
            .parse()
            .unwrap_or(0);

        match kind {
            // acknowledgement of one of our calls
            '6' => {
                let Ok(payload) = serde_json::from_str::<Value>(data) else {
                    return;
                };
                let purpose = self.acks.lock().await.remove(&ack_id);
                match purpose {
                    Some(Ack::JoinDoc(doc_id)) => self.on_join_doc_ack(&doc_id, &payload).await,
                    Some(Ack::ApplyUpdate(doc_id)) => {
                        if let Some(err) = payload.get(0).filter(|e| !e.is_null()) {
                            warn!("update for {doc_id} rejected: {err}");
                            self.resync_doc(&doc_id).await;
                        }
                    }
                    None => trace!("unexpected ack {ack_id}"),
                }
            }
            // event
            '5' => {
                let Ok(v) = serde_json::from_str::<Value>(data) else {
                    return;
                };
                let name = v.get("name").and_then(|n| n.as_str()).unwrap_or_default();
                let args = v.get("args").cloned().unwrap_or(Value::Array(vec![]));
                match name {
                    "joinProjectResponse" => {
                        if let Some(err) = args.get(0).and_then(|a| a.get("message")) {
                            warn!("joinProject rejected: {err}");
                            return;
                        }
                        let project = args
                            .get(0)
                            .and_then(|a| a.get("project"))
                            .cloned()
                            .unwrap_or(Value::Null);
                        self.clone().on_join_project(project).await;
                    }
                    "otUpdateApplied" => {
                        let doc_id = args
                            .get(0)
                            .and_then(|a| a.get("doc"))
                            .and_then(|d| d.as_str())
                            .unwrap_or_default()
                            .to_string();
                        let version = args
                            .get(0)
                            .and_then(|a| a.get("v"))
                            .and_then(|d| d.as_i64())
                            .unwrap_or(-1);
                        match args.get(0).and_then(|a| a.get("op")) {
                            Some(op) => {
                                if let Err(e) =
                                    self.apply_remote_update(&doc_id, version, op).await
                                {
                                    debug!("applying update for {doc_id} failed: {e:#}");
                                }
                            }
                            // An acknowledgement of *our own* edit: document-updater
                            // sends back `{doc, v}` with no op (nothing to re-apply).
                            None => self.confirm_own_write(&doc_id, version).await,
                        }
                    }
                    "otUpdateError" => {
                        // real-time disconnects the client after this, so treat it
                        // as "our view is stale" and re-read the document.
                        let doc_id = args
                            .get(0)
                            .and_then(|a| {
                                a.get("doc_id")
                                    .or_else(|| a.get("doc"))
                                    .and_then(|d| d.as_str())
                            })
                            .unwrap_or_default()
                            .to_string();
                        warn!(
                            "document-updater rejected an update for '{doc_id}': {}",
                            args.get(1).cloned().unwrap_or(Value::Null)
                        );
                        self.resync_doc(&doc_id).await;
                    }
                    "reciveNewDoc" | "reciveNewFile" | "reciveNewFolder" | "removeEntity"
                    | "reciveEntityRename" | "reciveEntityMove" => {
                        debug!("project structure changed ({name}); refreshing tree");
                        self.clone().spawn_tree_refresh();
                    }
                    "connectionRejected" => {
                        warn!("socket connection rejected: {}", args.get(0).cloned().unwrap_or_default());
                    }
                    other => trace!("ignoring socket event {other}"),
                }
            }
            _ => {}
        }
    }

    /// `joinProjectResponse`: build the filesystem tree from the server payload.
    async fn on_join_project(self: Arc<Self>, project: Value) {
        let name = project
            .get("name")
            .and_then(|n| n.as_str())
            .unwrap_or("project")
            .to_string();
        info!("joined project '{name}'");
        let mut doc_ids: Vec<String> = Vec::new();
        if let Some(root) = project
            .get("rootFolder")
            .and_then(|r| r.as_array())
            .and_then(|r| r.first())
        {
            self.walk_folder(root, ROOT_INO, &mut doc_ids);
        }
        self.notifier.tree_changed();
        self.ready.notify_one();
        // Seed content for every document, and let bootstrap wait for it: a
        // write that arrives before the snapshot would be diffed against an
        // empty base.
        {
            let mut pending = self.pending_docs.lock().await;
            *pending = doc_ids.len();
            if doc_ids.is_empty() {
                self.contents_ready.notify_one();
            }
        }
        for doc_id in doc_ids {
            self.request_join_doc(&doc_id).await;
        }
    }

    /// Recursively create dirs/docs from a server folder payload.
    fn walk_folder(&self, folder: &Value, parent: u64, out: &mut Vec<String>) {
        if let Some(folders) = folder.get("folders").and_then(|f| f.as_array()) {
            for f in folders {
                let Some(fname) = f.get("name").and_then(|n| n.as_str()) else {
                    continue;
                };
                let ino = self.fs.upsert(parent, fname, NodeKind::Dir, 0);
                self.walk_folder(f, ino, out);
            }
        }
        if let Some(docs) = folder.get("docs").and_then(|d| d.as_array()) {
            for d in docs {
                let (Some(id), Some(dname)) = (
                    d.get("_id").and_then(|x| x.as_str()),
                    d.get("name").and_then(|x| x.as_str()),
                ) else {
                    continue;
                };
                let ino = self.fs.upsert(
                    parent,
                    dname,
                    NodeKind::Doc {
                        doc_id: id.to_string(),
                        ranges: None,
                    },
                    0,
                );
                self.fs.doc_inodes.write().insert(id.to_string(), ino);
                out.push(id.to_string());
            }
        }
        if let Some(files) = folder.get("fileRefs").and_then(|f| f.as_array()) {
            for f in files {
                let (Some(id), Some(fname)) = (
                    f.get("_id").and_then(|x| x.as_str()),
                    f.get("name").and_then(|x| x.as_str()),
                ) else {
                    continue;
                };
                self.fs.upsert(
                    parent,
                    fname,
                    NodeKind::File {
                        file_id: id.to_string(),
                    },
                    0,
                );
            }
        }
    }

    /// Ask the server for a document's content (ack carries lines + version).
    async fn request_join_doc(&self, doc_id: &str) {
        let ack = self.next_ack();
        self.acks
            .lock()
            .await
            .insert(ack, Ack::JoinDoc(doc_id.to_string()));
        out_rx_send(
            self,
            socket_event_ack(
                ack,
                "joinDoc",
                json!([doc_id, { "encodeRanges": true }]),
            ),
        )
        .await;
    }

    fn next_ack(&self) -> u64 {
        self.ack_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// `joinDoc` acknowledgement: `[null, lines, version, ops, ranges, type, …]`.
    async fn on_join_doc_ack(&self, doc_id: &str, payload: &Value) {
        {
            let mut pending = self.pending_docs.lock().await;
            if *pending > 0 {
                *pending -= 1;
                if *pending == 0 {
                    self.contents_ready.notify_one();
                }
            }
        }
        let arr = match payload.as_array() {
            Some(a) => a,
            None => return,
        };
        if let Some(err) = arr.first().filter(|e| !e.is_null()) {
            debug!("joinDoc({doc_id}) error: {err}");
            return;
        }
        let lines: Vec<String> = arr
            .get(1)
            .and_then(|l| l.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| {
                        x.as_str()
                            .map(String::from)
                            // lines may arrive escaped (array of chars/nulls)
                            .or_else(|| x.as_array().map(|_| String::new()))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let version = arr.get(2).and_then(|v| v.as_i64()).unwrap_or(0);
        let ranges = arr.get(4).cloned().filter(|r| !r.is_null());
        self.store_remote_doc_full(doc_id, lines, version, ranges).await;
    }

    async fn store_remote_doc_full(
        &self,
        doc_id: &str,
        lines: Vec<String>,
        version: i64,
        ranges: Option<Value>,
    ) {
        let ino = match self.fs.doc_inodes.read().get(doc_id) {
            Some(i) => *i,
            None => return,
        };
        let current = self.fs.doc(ino);
        let changed = current
            .as_ref()
            .map(|d| d.lines != lines)
            .unwrap_or(true);
        let text = lines.join("\n");
        self.fs.set_doc(
            ino,
            Doc {
                id: doc_id.to_string(),
                lines,
                version,
                ranges,
            },
        );
        self.versions
            .lock()
            .await
            .insert(doc_id.to_string(), version);
        self.synced
            .lock()
            .await
            .insert(doc_id.to_string(), (text, version));
        self.bump_confirm(doc_id).await;
        if changed {
            self.notifier.invalidate(ino);
        }
    }

    /// Re-read one document from the server (after an out-of-sync error).
    async fn resync_doc(&self, doc_id: &str) {
        if self.socket_up.load(std::sync::atomic::Ordering::SeqCst) {
            self.request_join_doc(doc_id).await;
        } else if let Ok(fresh) = self.client.get_doc(doc_id).await {
            self.store_remote_doc_full(
                doc_id,
                fresh.lines,
                fresh.version,
                fresh.ranges,
            )
            .await;
        }
    }

    fn spawn_tree_refresh(self: Arc<Self>) {
        tokio::spawn(async move {
            if let Err(e) = self.refresh_tree().await {
                debug!("tree refresh failed: {e:#}");
            }
        });
    }

    /// Send one edit as `applyOtUpdate` over the live socket.
    ///
    /// The server transforms our op against anything that arrived first — that
    /// is exactly the "two people edit the same sentence" case — and broadcasts
    /// `otUpdateApplied`, which updates every peer including us.
    async fn send_via_socket(
        &self,
        doc_id: &str,
        op: &TextOp,
        version: i64,
        base: &str,
    ) -> Result<()> {
        let ack = self.next_ack();
        self.acks
            .lock()
            .await
            .insert(ack, Ack::ApplyUpdate(doc_id.to_string()));
        // The deleted characters must come from the base the operation was
        // computed against — never from the visible cache, which the FUSE layer
        // has already moved on to the new text.
        let update = json!([doc_id, {
            "doc": doc_id,
            "op": to_wire_op(op, base),
            "v": version,
            // `meta` is strictly validated by real-time (only `tc` is accepted);
            // it fills in `source`/`user_id` itself from the session.
        }]);
        out_rx_send(self, socket_event_ack(ack, "applyOtUpdate", update)).await;
        if self.socket_tx.lock().await.is_none() {
            bail!("websocket not connected");
        }
        Ok(())
    }

    /// Our own edit was accepted.
    ///
    /// `version` is the version the operation was applied **to** (sharejs
    /// semantics), so the document is now at `version + 1`. The resulting text
    /// is derived from the operation we sent, because the acknowledgement
    /// carries no op and the visible file may already contain newer typing.
    async fn confirm_own_write(&self, doc_id: &str, version: i64) {
        let ino = match self.fs.doc_inodes.read().get(doc_id) {
            Some(i) => *i,
            None => return,
        };
        {
            let mut sw = self.self_writes.lock().await;
            if let Some(count) = sw.get_mut(doc_id) {
                if *count > 0 {
                    *count -= 1;
                    if *count == 0 {
                        sw.remove(doc_id);
                    }
                }
            }
        }
        let new_version = version + 1;
        let entry = self.in_flight.lock().await.remove(doc_id);
        let diverged = self.diverged.lock().await.remove(doc_id);

        if diverged || entry.is_none() {
            // A collaborator's edit interleaved, so document-updater transformed
            // our operation; its result cannot be reconstructed from the op
            // alone. Re-read the document instead of guessing.
            debug!("doc {doc_id}: confirmed after a concurrent edit; re-reading");
            self.resync_doc(doc_id).await;
            let base = self
                .synced
                .lock()
                .await
                .get(doc_id)
                .map(|(t, _)| t.clone())
                .unwrap_or_default();
            self.settle_local_after_push(doc_id, &base).await;
            return;
        }

        let (op, base) = entry.expect("checked above");
        let new_text = match op.apply(&base) {
            Ok(t) => t,
            Err(e) => {
                warn!("doc {doc_id}: could not rebuild text after confirm ({e}); re-reading");
                self.resync_doc(doc_id).await;
                return;
            }
        };
        self.synced
            .lock()
            .await
            .insert(doc_id.to_string(), (new_text.clone(), new_version));
        self.versions
            .lock()
            .await
            .insert(doc_id.to_string(), new_version);
        if let Some(mut doc) = self.fs.doc(ino) {
            doc.version = new_version;
            self.fs.set_doc(ino, doc);
        }
        self.settle_local_after_push(doc_id, &new_text).await;
        self.publish_visible(doc_id, &new_text).await;
        self.bump_confirm(doc_id).await;
        debug!("doc {doc_id}: our edit confirmed (v{new_version})");
    }

    /// Make the mounted file show the merge of the server text and any local
    /// change that has not been confirmed yet.
    async fn publish_visible(&self, doc_id: &str, server_text: &str) {
        let ino = match self.fs.doc_inodes.read().get(doc_id) {
            Some(i) => *i,
            None => return,
        };
        let text = self
            .desired
            .lock()
            .await
            .get(doc_id)
            .cloned()
            .unwrap_or_else(|| server_text.to_string());
        if let Some(mut doc) = self.fs.doc(ino) {
            doc.set_text(&text);
            self.fs.set_doc(ino, doc);
        }
    }

    /// After one of our operations has been incorporated into the server text,
    /// move the reference point of the pending local change forward. Without
    /// this the next round would see the local change as still outstanding and
    /// re-insert it (growing the document on every round trip).
    async fn settle_local_after_push(&self, doc_id: &str, new_base: &str) {
        // Read both values first: holding one of these mutexes while locking the
        // other (or the same one again) deadlocks the pump.
        let from = self.desired_from.lock().await.get(doc_id).cloned();
        let Some(from) = from else { return };
        let want = self.desired.lock().await.get(doc_id).cloned();
        if let Some(want) = want {
            if want != new_base {
                let rebased = rebase_local(&from, &want, new_base);
                self.desired.lock().await.insert(doc_id.to_string(), rebased);
            }
        }
        self.desired_from
            .lock()
            .await
            .insert(doc_id.to_string(), new_base.to_string());
    }

    /// Apply an `otUpdateApplied` operation from the server.
    ///
    /// `version` is the operation's **base** version; afterwards the document is
    /// at `version + 1`. Applying the op to our confirmed base (never to the
    /// visible file) is what keeps the mount identical to every browser, even
    /// when two people edit the same sentence.
    async fn apply_remote_update(&self, doc_id: &str, version: i64, op_json: &Value) -> Result<()> {
        let ino = match self.fs.doc_inodes.read().get(doc_id) {
            Some(i) => *i,
            None => {
                debug!("update for unknown doc {doc_id}");
                return Ok(());
            }
        };

        // Our own edit is still in flight: its result will be transformed, so a
        // confirmation for it cannot be trusted — plan on re-reading instead.
        if self.in_flight.lock().await.contains_key(doc_id) {
            self.diverged.lock().await.insert(doc_id.to_string());
        }

        let (base, base_version) = match self.synced.lock().await.get(doc_id).cloned() {
            Some(v) => v,
            None => match self.fs.doc(ino) {
                Some(d) => (d.text(), d.version),
                None => return Ok(()),
            },
        };

        if base_version >= 0 && version != base_version {
            debug!("doc {doc_id}: update for v{version} but base is v{base_version}; refetching");
            self.resync_doc(doc_id).await;
            return Ok(());
        }

        let op = parse_wire_op(op_json, base.chars().count())?;
        let new_text = match op.apply(&base) {
            Ok(t) => t,
            Err(e) => {
                warn!("op did not apply to {doc_id} ({e}); refetching");
                self.resync_doc(doc_id).await;
                return Ok(());
            }
        };
        let new_version = version + 1;

        self.synced
            .lock()
            .await
            .insert(doc_id.to_string(), (new_text.clone(), new_version));
        self.versions
            .lock()
            .await
            .insert(doc_id.to_string(), new_version);
        if let Some(mut doc) = self.fs.doc(ino) {
            doc.version = new_version;
            self.fs.set_doc(ino, doc);
        }
        // A remote edit must not make a local, not-yet-confirmed edit vanish
        // from the mounted file: rebase it and show the merged result.
        self.settle_local_after_push(doc_id, &new_text).await;
        self.publish_visible(doc_id, &new_text).await;
        self.bump_confirm(doc_id).await;
        self.notifier.invalidate(ino);
        debug!("doc {doc_id}: remote edit applied (v{new_version})");
        Ok(())
    }

    // ------------------------------------------------------------------ poll --

    /// Fallback synchronisation: poll document versions and merge.
    async fn poll_loop(self: Arc<Self>) -> Result<()> {
        let mut tree_timer = tokio::time::interval(TREE_REFRESH);
        loop {
            tokio::select! {
                _ = tokio::time::sleep(POLL_INTERVAL) => {}
                _ = tree_timer.tick() => {
                    if let Err(e) = self.refresh_tree().await {
                        debug!("tree refresh failed: {e:#}");
                    }
                }
            }
            if self.socket_up.load(std::sync::atomic::Ordering::SeqCst) {
                continue; // socket keeps us up to date
            }
            if let Err(e) = self.poll_once().await {
                debug!("poll failed: {e:#}");
            }
        }
    }

    async fn refresh_tree(&self) -> Result<()> {
        let meta = self.client.fetch_project().await?;
        let mut before: Vec<String> = self.fs.nodes.read().values().map(|n| n.name.clone()).collect();
        before.sort();
        self.rebuild_tree(&meta).await?;
        let mut after: Vec<String> = self.fs.nodes.read().values().map(|n| n.name.clone()).collect();
        after.sort();
        if before != after {
            self.notifier.tree_changed();
        }
        Ok(())
    }

    async fn poll_once(&self) -> Result<()> {
        let updates = self.client.fetch_updates().await?;
        for u in updates {
            let known = self.versions.lock().await.get(&u.doc_id).copied();
            match known {
                Some(v) if v == u.v => continue,
                _ => {}
            }
            let ino = match self.fs.doc_inodes.read().get(&u.doc_id) {
                Some(i) => *i,
                None => continue,
            };
            let fresh = match self.client.get_doc(&u.doc_id).await {
                Ok(d) => d,
                Err(e) => {
                    debug!("get_doc {} failed: {e:#}", u.doc_id);
                    continue;
                }
            };
            let Some(local) = self.fs.doc(ino) else { continue };
            let remote_text = fresh.text();
            let local_text = local.text();
            if remote_text == local_text {
                self.versions
                    .lock()
                    .await
                    .insert(u.doc_id.clone(), fresh.version);
                continue;
            }
            // Optimistic three-way merge: the common ancestor is our cached
            // copy; a genuine overlap keeps both sides with markers.
            let merged = merge3::merge3(&remote_text, &local_text, &remote_text);
            if !merged.conflicts.is_empty() {
                warn!(
                    "doc {} has {} conflicting region(s); keeping both sides",
                    u.doc_id,
                    merged.conflicts.len()
                );
            }
            let mut doc = fresh.clone();
            doc.set_text(&merged.text);
            self.fs.set_doc(ino, doc);
            self.versions
                .lock()
                .await
                .insert(u.doc_id.clone(), fresh.version);
            self.notifier.invalidate(ino);
            info!("polled update for {} -> v{}", u.doc_id, fresh.version);
        }
        Ok(())
    }
}

/// Re-express the local change `from -> want` on top of a newer server text.
///
/// `from` is the text the local edit was made against, `base` is what the server
/// holds now. When they differ, a collaborator (or another device) edited the
/// document in between; transforming the local change against theirs keeps both
/// edits instead of reverting one of them.
fn rebase_local(from: &str, want: &str, base: &str) -> String {
    if from == base {
        return want.to_string();
    }
    let local = ot::diff(from, want);
    if local.is_noop() {
        return base.to_string();
    }
    let remote = ot::diff(from, base);
    if remote.is_noop() {
        return want.to_string();
    }
    match ot::TextOp::transform_pair(&local, &remote) {
        Ok((local_after, _remote_after)) => match local_after.apply(base) {
            Ok(text) => text,
            Err(e) => {
                debug!("rebase failed to apply ({e}); falling back to the local text");
                want.to_string()
            }
        },
        Err(e) => {
            debug!("rebase transform failed ({e}); falling back to the local text");
            want.to_string()
        }
    }
}

/// Convert an Overleaf wire operation into a `TextOp`.
///
/// document-updater's `sharejs-text-ot` type takes a **positional** op list:
/// `[{"p": 12, "i": "hello"}, {"p": 20, "d": "world"}]`, applied in order to the
/// evolving document. Older payloads (and the history-OT type) use a component
/// list instead, so both are accepted.
pub fn parse_wire_op(v: &Value, base_len: usize) -> Result<TextOp> {
    let arr = if let Some(a) = v.as_array() {
        a.clone()
    } else if let Some(a) = v.get("op").and_then(|o| o.as_array()) {
        a.clone()
    } else {
        bail!("unrecognised operation shape: {v}");
    };

    // Component form: [3, "inserted", {"d": "abc"}, -2]
    let is_component_form = arr.iter().all(|c| {
        c.is_i64() || c.get("i").is_some() || c.get("d").is_some()
    }) && arr
        .iter()
        .any(|c| c.is_i64() || c.get("i").is_some() || c.get("d").is_some())
        && !arr.iter().any(|c| c.get("p").is_some());
    if is_component_form && !arr.is_empty() {
        let mut op = TextOp::new();
        for comp in arr {
            if let Some(n) = comp.as_i64() {
                if n >= 0 {
                    op.retain(n as usize);
                } else {
                    op.remove((-n) as usize);
                }
                continue;
            }
            if let Some(s) = comp.get("i").and_then(|x| x.as_str()) {
                op.insert(s);
                continue;
            }
            if let Some(s) = comp.get("d").and_then(|x| x.as_str()) {
                op.remove(s.chars().count());
                continue;
            }
            if let Some(n) = comp.get("d").and_then(|x| x.as_i64()) {
                op.remove(n as usize);
                continue;
            }
            bail!("unrecognised operation component: {comp}");
        }
        if op.base_len < base_len {
            op.retain(base_len - op.base_len);
        }
        return Ok(op);
    }

    // Positional form: each entry edits the document produced so far.
    let mut op = TextOp::new();
    let mut cursor = 0usize; // position reached in the evolving document
    for entry in arr {
        let p = entry
            .get("p")
            .and_then(|x| x.as_u64())
            .ok_or_else(|| anyhow!("positional op without p: {entry}"))?
            as usize;
        if p < cursor {
            bail!("overlapping positional ops at p={p} (cursor {cursor})");
        }
        op.retain(p - cursor);
        if let Some(s) = entry.get("i").and_then(|x| x.as_str()) {
            op.insert(s);
            cursor = p + s.chars().count();
        } else if let Some(s) = entry.get("d").and_then(|x| x.as_str()) {
            op.remove(s.chars().count());
            cursor = p;
        } else {
            bail!("positional op without i/d: {entry}");
        }
    }
    // Text after the last op is kept: sharejs ops omit the trailing retain.
    if op.base_len < base_len {
        op.retain(base_len - op.base_len);
    }
    Ok(op)
}

/// Convert a `TextOp` into the positional wire form, using the current document
/// text to fill in the deleted characters (`document-updater` validates them).
pub fn to_wire_op(op: &TextOp, base: &str) -> Value {
    let chars: Vec<char> = base.chars().collect();
    let mut out = Vec::new();
    let mut pos = 0usize; // position in the doc as it evolves
    let mut base_pos = 0usize; // position in the original doc
    for c in &op.components {
        match c {
            crate::ot::Component::Retain(n) => {
                pos += n;
                base_pos += n;
            }
            crate::ot::Component::Insert(s) => {
                out.push(json!({ "p": pos, "i": s }));
                pos += s.chars().count();
            }
            crate::ot::Component::Remove(n) => {
                let end = (base_pos + n).min(chars.len());
                let deleted: String = chars[base_pos.min(chars.len())..end].iter().collect();
                out.push(json!({ "p": pos, "d": deleted }));
                base_pos += n;
                // `pos` stays: the following text shifts into this position
            }
        }
    }
    Value::Array(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rebase_keeps_a_collaborators_insert() {
        // We appended while somebody prepended: both must survive.
        assert_eq!(rebase_local("abc", "abcX", "PEER abc"), "PEER abcX");
    }

    #[test]
    fn rebase_is_identity_without_remote_changes() {
        assert_eq!(rebase_local("abc", "abcX", "abc"), "abcX");
    }

    #[test]
    fn rebase_keeps_local_deletion_and_remote_insert() {
        // Local change deleted " world"; the server gained a prefix meanwhile.
        assert_eq!(rebase_local("hello world", "hello", "Xhello world"), "Xhello");
    }

    #[test]
    fn wire_roundtrip_positional() {
        // retain 3 of "abcde", insert "hi", delete "de" -> "abchi"
        let mut op = TextOp::new();
        op.retain(3);
        op.insert("hi");
        op.remove(2);
        assert_eq!(op.apply("abcde").unwrap(), "abchi");
        let wire = to_wire_op(&op, "abcde");
        assert_eq!(wire, json!([{ "p": 3, "i": "hi" }, { "p": 5, "d": "de" }]));
        let back = parse_wire_op(&wire, 5).unwrap();
        assert_eq!(back.apply("abcde").unwrap(), "abchi");
        assert_eq!(back, op, "wire roundtrip must preserve the operation");
    }

    #[test]
    fn wire_positions_are_sequential() {
        // insert then delete after it: the delete position accounts for the insert
        let mut op = TextOp::new();
        op.retain(2);
        op.insert("XY");
        op.remove(3);
        op.retain(1);
        let base = "abcdef";
        let wire = to_wire_op(&op, base);
        assert_eq!(wire, json!([{ "p": 2, "i": "XY" }, { "p": 4, "d": "cde" }]));
        let back = parse_wire_op(&wire, base.len()).unwrap();
        assert_eq!(back.apply(base).unwrap(), op.apply(base).unwrap());
        assert_eq!(back.apply(base).unwrap(), "abXYf");
    }

    #[test]
    fn wire_parses_component_form_and_negative_delete() {
        let v = json!([2, -1, 1]);
        let op = parse_wire_op(&v, 4).unwrap();
        assert_eq!(op.apply("abcd").unwrap(), "abd");
    }

    #[test]
    fn wire_parses_positional_from_server() {
        // insert "# " at 0, then delete one char at evolving position 5 (= 'd')
        let v = json!([{ "p": 0, "i": "# " }, { "p": 5, "d": "d" }]);
        let op = parse_wire_op(&v, 6).unwrap();
        assert_eq!(op.apply("abcdef").unwrap(), "# abcef");
    }
}
