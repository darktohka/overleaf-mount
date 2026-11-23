//! Shared in-memory model.
//!
//! Everything the filesystem shows lives here. The FUSE layer only ever reads
//! and mutates this structure; network synchronisation (`sync/ot.rs`,
//! `sync/poll.rs`) reads/writes it and wakes the FUSE layer through the
//! notification kick. That keeps the kernel-facing side free of `.await`.

use crate::gitignore::Ignorer;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use parking_lot::RwLock;

/// A text document (Overleaf "doc"), with its server version.
#[derive(Debug, Clone)]
pub struct Doc {
    pub id: String,
    /// Document content, split on `\n` exactly like Overleaf's wire format.
    pub lines: Vec<String>,
    /// Overleaf document version this content corresponds to.
    pub version: i64,
    /// Track-changes ranges (`ranges` from the API) — opaque, kept so we do not
    /// destroy comments/tracked changes when we only touch text.
    pub ranges: Option<serde_json::Value>,
}

impl Doc {
    pub fn text(&self) -> String {
        self.lines.join("\n")
    }

    pub fn char_len(&self) -> usize {
        let mut n = 0usize;
        for (i, l) in self.lines.iter().enumerate() {
            n += l.chars().count();
            if i + 1 < self.lines.len() {
                n += 1;
            }
        }
        n
    }

    pub fn set_text(&mut self, text: &str) {
        self.lines = text.split('\n').map(|s| s.to_string()).collect();
    }
}

/// A binary upload (no OT). Cached in memory, read-only over the mount.
#[derive(Debug, Clone)]
pub struct FileEntry {
    pub id: String,
    pub name: String,
    pub size: i64,
}

#[derive(Debug, Clone)]
pub struct DocEntry {
    pub id: String,
    /// Path relative to the project root, e.g. `sections/intro.tex`.
    pub path: String,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct RootFolder {
    pub id: String,
    pub path: String,
}

#[derive(Debug, Clone, Default)]
pub struct ProjectMeta {
    pub id: String,
    pub name: String,
    pub docs: Vec<DocEntry>,
    pub folders: Vec<RootFolder>,
    pub files: Vec<FileEntry>,
}

/// What an inode can point at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entity {
    Doc,
    Folder,
    File,
}

#[derive(Debug, Clone)]
pub struct Attrs {
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
    pub atime: SystemTime,
    pub nlink: u32,
}

impl Default for Attrs {
    fn default() -> Self {
        let now = SystemTime::now();
        Self {
            mode: 0o644,
            uid: 0,
            gid: 0,
            mtime: now,
            ctime: now,
            atime: now,
            nlink: 1,
        }
    }
}

/// One node in the in-memory tree.
#[derive(Debug, Clone)]
pub enum NodeKind {
    Dir,
    /// A text document that is synchronised with Overleaf.
    Doc {
        doc_id: String,
        /// Tracked-changes ranges payload from the API (kept verbatim).
        ranges: Option<serde_json::Value>,
    },
    /// A binary upload, read-only.
    File { file_id: String },
    /// A local-only file (matched by .gitignore) — never sent to Overleaf.
    LocalFile,
}

#[derive(Debug, Clone)]
pub struct Node {
    pub ino: u64,
    pub name: String,
    pub parent: u64,
    pub kind: NodeKind,
    pub attrs: Attrs,
    pub size: u64,
    /// Content of `LocalFile` nodes (and cached binary uploads).
    pub data: Option<Vec<u8>>,
}

impl Node {
    pub fn is_dir(&self) -> bool {
        matches!(self.kind, NodeKind::Dir)
    }
}

/// The whole filesystem image.
pub struct Fs {
    pub nodes: RwLock<BTreeMap<u64, Node>>,
    /// name -> ino for every directory, to answer lookups in O(log n).
    pub children: RwLock<BTreeMap<u64, BTreeMap<String, u64>>>,
    /// doc_id -> inode, so OT updates can be routed to the right file.
    pub doc_inodes: RwLock<BTreeMap<String, u64>>,
    /// document contents by inode, and the version we believe is on the server.
    pub docs: RwLock<BTreeMap<u64, Doc>>,
    /// Local-only content (gitignored files) by inode.
    pub local: RwLock<BTreeMap<u64, Vec<u8>>>,
    /// path -> set of gitignore rules matching it (for logging/stat)
    pub ignored: RwLock<BTreeMap<String, ()>>,
    /// Live `.gitignore` rules. `ignored` only records paths that existed when
    /// the rules were loaded; files created later are matched against these.
    pub ignorer: parking_lot::RwLock<Ignorer>,
    /// Monotonic-ish timestamps/version used to order our own writes.
    pub next_ino: AtomicU64,
    /// Generation counter bumped on every structural or content change; the
    /// FUSE layer watches it to invalidate kernel caches.
    pub generation: AtomicU64,
    /// Number of open directory handles (for `statfs` style reporting).
    pub open_handles: AtomicI64,
    /// Where the mount point is (for logging).
    pub mount_point: parking_lot::RwLock<String>,
    /// Default owner applied to every node unless overridden with chmod/chown.
    pub default_uid: u32,
    pub default_gid: u32,
    /// attribute timeout used by the FUSE layer
    pub ttl: Duration,
}

pub const ROOT_INO: u64 = 1;

impl Fs {
    pub fn new(default_uid: u32, default_gid: u32, ttl: Duration) -> Self {
        let attrs = Attrs {
            mode: full_mode(true, 0o755),
            uid: default_uid,
            gid: default_gid,
            nlink: 2,
            ..Attrs::default()
        };
        let mut nodes = BTreeMap::new();
        nodes.insert(
            ROOT_INO,
            Node {
                ino: ROOT_INO,
                name: "/".to_string(),
                parent: ROOT_INO,
                kind: NodeKind::Dir,
                attrs,
                size: 0,
                data: None,
            },
        );
        let mut children = BTreeMap::new();
        children.insert(ROOT_INO, BTreeMap::new());
        Self {
            nodes: RwLock::new(nodes),
            children: RwLock::new(children),
            doc_inodes: RwLock::new(BTreeMap::new()),
            docs: RwLock::new(BTreeMap::new()),
            local: RwLock::new(BTreeMap::new()),
            ignored: RwLock::new(BTreeMap::new()),
            ignorer: parking_lot::RwLock::new(Ignorer::new()),
            next_ino: AtomicU64::new(2),
            generation: AtomicU64::new(1),
            open_handles: AtomicI64::new(0),
            mount_point: RwLock::new(String::new()),
            default_uid,
            default_gid,
            ttl,
        }
    }

    pub fn bump(&self) -> u64 {
        self.generation.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    pub fn alloc_ino(&self) -> u64 {
        self.next_ino.fetch_add(1, Ordering::SeqCst)
    }

    pub fn lookup(&self, parent: u64, name: &str) -> Option<u64> {
        self.children.read().get(&parent)?.get(name).copied()
    }

    pub fn node(&self, ino: u64) -> Option<Node> {
        self.nodes.read().get(&ino).cloned()
    }

    /// Full path of an inode (for logging and gitignore matching).
    pub fn path_of(&self, ino: u64) -> String {
        let nodes = self.nodes.read();
        let mut parts = Vec::new();
        let mut cur = ino;
        let mut guard = 0;
        while cur != ROOT_INO && guard < 4096 {
            match nodes.get(&cur) {
                Some(n) => {
                    parts.push(n.name.clone());
                    if n.parent == cur {
                        break;
                    }
                    cur = n.parent;
                }
                None => break,
            }
            guard += 1;
        }
        parts.reverse();
        parts.join("/")
    }

    /// Insert a node under `parent`. Returns the inode. Reuses the existing
    /// inode when the name is already there (so open handles stay valid) and
    /// keeps attrs that were set with chmod/chown.
    pub fn upsert(&self, parent: u64, name: &str, kind: NodeKind, size: u64) -> u64 {
        let existing = self.lookup(parent, name);
        let ino = match existing {
            Some(ino) => {
                // A tree refresh re-states every document's kind. Its size must
                // come from the loaded text, not from the refresh payload (which
                // passes 0) — otherwise `getattr` reports an empty file and
                // O_APPEND writes start at offset 0.
                let size = if matches!(kind, NodeKind::Doc { .. }) {
                    self.docs
                        .read()
                        .get(&ino)
                        .map(|d| d.text().len() as u64)
                        .unwrap_or(size)
                } else if matches!(kind, NodeKind::File { .. }) {
                    // A cached upload keeps its downloaded bytes (and the size
                    // they imply); a refresh payload re-states files with size 0
                    // and must not make them look empty again.
                    self.nodes
                        .read()
                        .get(&ino)
                        .map(|n| {
                            n.data
                                .as_ref()
                                .map(|d| d.len() as u64)
                                .unwrap_or(n.size)
                        })
                        .unwrap_or(size)
                } else {
                    size
                };
                let mut nodes = self.nodes.write();
                if let Some(n) = nodes.get_mut(&ino) {
                    n.kind = kind;
                    n.size = size;
                }
                ino
            }
            None => {
                let ino = self.alloc_ino();
                let now = SystemTime::now();
                let mode = match kind {
                    NodeKind::Dir => full_mode(true, 0o755),
                    _ => full_mode(false, default_mode_for(name)),
                };
                let n = Node {
                    ino,
                    name: name.to_string(),
                    parent,
                    kind,
                    attrs: Attrs {
                        mode,
                        uid: self.default_uid,
                        gid: self.default_gid,
                        mtime: now,
                        ctime: now,
                        atime: now,
                        nlink: 1,
                    },
                    size,
                    data: None,
                };
                self.nodes.write().insert(ino, n);
                self.children.write().entry(parent).or_default().insert(name.to_string(), ino);
                self.children.write().entry(ino).or_default();
                self.bump();
                ino
            }
        };
        ino
    }

    /// Remove a node and its subtree; returns removed doc ids.
    pub fn remove(&self, ino: u64) -> Vec<String> {
        let mut removed_docs = Vec::new();
        if ino == ROOT_INO {
            return removed_docs;
        }
        let (parent, name) = match self.nodes.read().get(&ino) {
            Some(n) => (n.parent, n.name.clone()),
            None => return removed_docs,
        };
        // collect subtree
        let mut stack = vec![ino];
        let mut all = Vec::new();
        while let Some(cur) = stack.pop() {
            all.push(cur);
            if let Some(kids) = self.children.read().get(&cur) {
                stack.extend(kids.values().copied());
            }
        }
        {
            let mut nodes = self.nodes.write();
            let mut docs = self.docs.write();
            let mut local = self.local.write();
            let mut doc_inodes = self.doc_inodes.write();
            let mut children = self.children.write();
            for cur in all {
                if let Some(n) = nodes.remove(&cur) {
                    match &n.kind {
                        NodeKind::Doc { doc_id, .. } => {
                            removed_docs.push(doc_id.clone());
                            doc_inodes.remove(doc_id);
                        }
                        _ => {}
                    }
                }
                docs.remove(&cur);
                local.remove(&cur);
                children.remove(&cur);
            }
            if let Some(kids) = children.get_mut(&parent) {
                kids.remove(&name);
            }
        }
        self.bump();
        removed_docs
    }

    /// Move/rename `ino` to `new_parent`/`new_name`.
    pub fn r#move(&self, ino: u64, new_parent: u64, new_name: &str) -> Result<u64, i32> {
        if self.lookup(new_parent, new_name).is_some() {
            return Err(libc::EEXIST);
        }
        let (old_parent, old_name) = match self.nodes.read().get(&ino) {
            Some(n) => (n.parent, n.name.clone()),
            None => return Err(libc::ENOENT),
        };
        if old_parent == new_parent && old_name == new_name {
            return Ok(ino);
        }
        // forbid moving a directory inside itself
        let mut cur = new_parent;
        let mut guard = 0;
        while guard < 4096 {
            if cur == ino {
                return Err(libc::EINVAL);
            }
            if cur == ROOT_INO {
                break;
            }
            match self.nodes.read().get(&cur).map(|n| n.parent) {
                Some(p) => cur = p,
                None => break,
            }
            guard += 1;
        }
        {
            let mut children = self.children.write();
            if let Some(kids) = children.get_mut(&old_parent) {
                kids.remove(&old_name);
            }
            children.entry(new_parent).or_default().insert(new_name.to_string(), ino);
        }
        {
            let mut nodes = self.nodes.write();
            if let Some(n) = nodes.get_mut(&ino) {
                n.parent = new_parent;
                n.name = new_name.to_string();
                n.attrs.ctime = SystemTime::now();
            }
        }
        self.bump();
        Ok(ino)
    }

    /// Read the full text content of a doc/local node.
    pub fn read(&self, ino: u64) -> Vec<u8> {
        if let Some(d) = self.docs.read().get(&ino) {
            return d.text().into_bytes();
        }
        if let Some(b) = self.local.read().get(&ino) {
            return b.clone();
        }
        match self.nodes.read().get(&ino) {
            Some(n) => n.data.clone().unwrap_or_default(),
            None => Vec::new(),
        }
    }

    pub fn doc(&self, ino: u64) -> Option<Doc> {
        self.docs.read().get(&ino).cloned()
    }

    pub fn set_doc(&self, ino: u64, doc: Doc) {
        self.docs.write().insert(ino, doc.clone());
        if let Some(n) = self.nodes.write().get_mut(&ino) {
            n.size = doc.text().len() as u64;
        }
        self.bump();
    }

    pub fn set_local(&self, ino: u64, data: Vec<u8>) {
        if let Some(n) = self.nodes.write().get_mut(&ino) {
            n.size = data.len() as u64;
            n.data = Some(data.clone());
        }
        self.local.write().insert(ino, data);
        self.bump();
    }

    /// Cache the bytes of an uploaded file in its inode (`NodeKind::File`).
    pub fn set_file_data(&self, ino: u64, data: Vec<u8>) {
        if let Some(n) = self.nodes.write().get_mut(&ino) {
            n.size = data.len() as u64;
            n.data = Some(data);
        }
        self.bump();
    }

    /// Record a size learned without downloading the file (a `HEAD` request).
    pub fn set_size(&self, ino: u64, size: u64) {
        if let Some(n) = self.nodes.write().get_mut(&ino) {
            n.size = size;
        }
    }

    pub fn set_attrs(&self, ino: u64, f: impl FnOnce(&mut Attrs)) -> Result<(), i32> {
        match self.nodes.write().get_mut(&ino) {
            Some(n) => {
                f(&mut n.attrs);
                n.attrs.ctime = SystemTime::now();
                self.bump();
                Ok(())
            }
            None => Err(libc::ENOENT),
        }
    }

    pub fn is_ignored(&self, path: &str) -> bool {
        if self.ignored.read().contains_key(path) {
            return true;
        }
        !path.is_empty() && self.ignorer.read().is_ignored(path)
    }

    /// Install the project's `.gitignore` rules (called once, after bootstrap).
    pub fn set_ignorer(&self, ignorer: Ignorer) {
        *self.ignorer.write() = ignorer;
    }

    pub fn mark_ignored(&self, path: &str, ignored: bool) {
        let mut g = self.ignored.write();
        if ignored {
            g.insert(path.to_string(), ());
        } else {
            g.remove(path);
        }
    }
}

/// `.sh` files are executable by default (per the mount contract); everything
/// else is owner-writable.
pub fn default_mode_for(name: &str) -> u32 {
    if name.ends_with(".sh") {
        0o755
    } else {
        0o644
    }
}

/// File-type bits + permission bits, which is what `st_mode` needs.
pub fn full_mode(is_dir: bool, perm: u32) -> u32 {
    let kind = if is_dir {
        libc::S_IFDIR as u32
    } else {
        libc::S_IFREG as u32
    };
    kind | (perm & 0o7777)
}

/// Just the permission bits of a (possibly chmod'ed) mode.
pub fn perm_bits(mode: u32) -> u32 {
    mode & 0o7777
}

pub fn is_dir_mode(mode: u32) -> bool {
    mode & (libc::S_IFMT as u32) == libc::S_IFDIR as u32
}

#[cfg(test)]
mod tests {
    #[test]
    fn gitignore_rules_apply_to_new_paths() {
        use super::Fs;
        use crate::gitignore::Ignorer;
        use std::time::Duration;
        let fs = Fs::new(0, 0, Duration::from_secs(1));
        fs.set_ignorer(Ignorer::with_root_rules("build/\n*.aux\n"));
        // Paths that did not exist when the rules were loaded must match too.
        assert!(fs.is_ignored("build/output.tex"));
        assert!(fs.is_ignored("main.aux"));
        assert!(!fs.is_ignored("main.tex"));
    }

    use super::*;

    #[test]
    fn sh_files_are_executable_by_default() {
        assert_eq!(default_mode_for("build.sh"), 0o755);
        assert_eq!(default_mode_for("main.tex"), 0o644);
        let fs = Fs::new(1000, 1000, Duration::from_secs(1));
        let ino = fs.upsert(ROOT_INO, "run.sh", NodeKind::LocalFile, 0);
        let n = fs.node(ino).unwrap();
        assert_eq!(perm_bits(n.attrs.mode), 0o755);
        assert_eq!(n.attrs.uid, 1000);
        assert_eq!(n.attrs.gid, 1000);
    }

    #[test]
    fn chmod_and_chown_live_in_memory_only() {
        let fs = Fs::new(0, 0, Duration::from_secs(1));
        let ino = fs.upsert(ROOT_INO, "a.tex", NodeKind::Doc { doc_id: "d1".into(), ranges: None }, 0);
        fs.set_attrs(ino, |a| {
            a.mode = full_mode(false, 0o600);
            a.uid = 4242;
            a.gid = 77;
        })
        .unwrap();
        let n = fs.node(ino).unwrap();
        assert_eq!(perm_bits(n.attrs.mode), 0o600);
        assert_eq!((n.attrs.uid, n.attrs.gid), (4242, 77));
        // re-syncing the same path keeps the attrs
        let ino2 = fs.upsert(ROOT_INO, "a.tex", NodeKind::Doc { doc_id: "d1".into(), ranges: None }, 10);
        assert_eq!(ino2, ino);
        let n = fs.node(ino).unwrap();
        assert_eq!(perm_bits(n.attrs.mode), 0o600);
    }

    #[test]
    fn path_and_move() {
        let fs = Fs::new(0, 0, Duration::from_secs(1));
        let d = fs.upsert(ROOT_INO, "sections", NodeKind::Dir, 0);
        let f = fs.upsert(d, "intro.tex", NodeKind::Doc { doc_id: "d".into(), ranges: None }, 0);
        assert_eq!(fs.path_of(f), "sections/intro.tex");
        let d2 = fs.upsert(ROOT_INO, "chapters", NodeKind::Dir, 0);
        fs.r#move(f, d2, "intro.tex").unwrap();
        assert_eq!(fs.path_of(f), "chapters/intro.tex");
        assert_eq!(fs.lookup(d, "intro.tex"), None);
    }
}

impl Clone for Fs {
    /// Only used by tests: rebuild an independent copy of the in-memory image.
    fn clone(&self) -> Self {
        let f = Fs::new(self.default_uid, self.default_gid, self.ttl);
        *f.nodes.write() = self.nodes.read().clone();
        *f.children.write() = self.children.read().clone();
        *f.docs.write() = self.docs.read().clone();
        *f.local.write() = self.local.read().clone();
        *f.doc_inodes.write() = self.doc_inodes.read().clone();
        f
    }
}
