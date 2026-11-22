//! The FUSE filesystem.
//!
//! All callbacks are synchronous and only touch the in-memory image (`Fs`);
//! writes hand an intent to the sync engine over an unbounded channel so a
//! network round-trip never blocks the kernel thread. The engine calls back into
//! `Notifier` to invalidate the kernel's page cache when the server sends us
//! somebody else's edit.

use std::ffi::OsStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use fuser::{
    FileAttr, FileType, Filesystem, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, Request,
};
use log::{debug, trace, warn};
use parking_lot::Mutex;

use crate::fsops::{self, decode_name, file_attr, new_attrs};
use crate::sync::{Notifier, Outgoing, SyncHandle};
use crate::types::{Doc, Entity, Fs, NodeKind, ROOT_INO};

pub const TTL: Duration = Duration::from_secs(1);
const BLOCK_SIZE: u32 = 512;

/// One open file handle.
///
/// Deliberately holds no content buffer: every write splices into the current
/// document text, so a truncate or a collaborator's edit can never be reverted
/// by a stale copy taken at `open(2)` time.
struct Handle {
    ino: u64,
    /// Set when the file was opened for writing, so flush/release re-push it.
    dirty: bool,
    dir_entries: Option<Vec<(u64, FileType, String)>>,
}

pub struct OverleafFs {
    pub fs: Arc<Fs>,
    pub sync: SyncHandle,
    handles: Mutex<std::collections::HashMap<u64, Handle>>,
    next_fh: AtomicU64,
    /// Order in which write intents were observed, used to discard ones that the
    /// kernel delivers late (a `truncate` can arrive after the `write` it
    /// preceded, and applying it would empty the document).
    write_seq: AtomicU64,
    /// Inode currently being flushed by the engine (to avoid echo loops).
    pub flushing: Mutex<Option<u64>>,
}

impl OverleafFs {
    pub fn new(fs: Arc<Fs>, sync: SyncHandle) -> Self {
        Self {
            fs,
            sync,
            handles: Mutex::new(std::collections::HashMap::new()),
            next_fh: AtomicU64::new(1),
            write_seq: AtomicU64::new(1),
            flushing: Mutex::new(None),
        }
    }

    fn attr_for(&self, ino: u64) -> Option<FileAttr> {
        let node = self.fs.node(ino)?;
        let size = match &node.kind {
            NodeKind::Dir => 0,
            NodeKind::Doc { .. } => self
                .fs
                .docs
                .read()
                .get(&ino)
                .map(|d| d.text().len() as u64)
                .unwrap_or(node.size),
            _ => node.size,
        };
        Some(file_attr(ino, &node.attrs, size))
    }

    fn fs_type(&self, ino: u64) -> Option<FileType> {
        let n = self.fs.node(ino)?;
        Some(if n.is_dir() {
            FileType::Directory
        } else {
            FileType::RegularFile
        })
    }

    /// Read current bytes of a file node.
    fn read_bytes(&self, ino: u64) -> Vec<u8> {
        self.fs.read(ino)
    }

    /// Push the node's current text to Overleaf, stamped with the next sequence
    /// number. Only text documents are synchronised.
    fn push_current(&self, ino: u64) {
        let Some(node) = self.fs.node(ino) else { return };
        if let NodeKind::Doc { doc_id, .. } = node.kind {
            // A freshly created document has only a placeholder id until the
            // engine has created it on the server; pushing now would POST to
            // `/doc/pending:<path>`. The engine pushes the content as soon as
            // it knows the real id.
            if doc_id.starts_with("pending:") {
                return;
            }
            let text = self
                .fs
                .doc(ino)
                .map(|d| d.text())
                .unwrap_or_default();
            self.sync.send(Outgoing::WriteDoc {
                doc_id,
                text,
                seq: self.write_seq.fetch_add(1, Ordering::SeqCst),
            });
        }
    }
}

/// Adapter so the sync engine can notify this filesystem.
pub struct FuseNotifier {
    pub fs: Arc<Fs>,
    pub mount_generation: Arc<AtomicU64>,
}

impl Notifier for FuseNotifier {
    fn invalidate(&self, _ino: u64) {
        // bumping the generation makes the FUSE layer serve fresh attrs with a
        // new mtime, which is what the kernel needs to drop its page cache
        self.fs.bump();
        self.mount_generation.fetch_add(1, Ordering::SeqCst);
    }

    fn tree_changed(&self) {
        self.fs.bump();
        self.mount_generation.fetch_add(1, Ordering::SeqCst);
    }
}

impl Filesystem for OverleafFs {
    fn init(
        &mut self,
        _req: &Request<'_>,
        _config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        debug!("fuse init");
        Ok(())
    }

    fn destroy(&mut self) {
        debug!("fuse destroy");
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let Some(name) = name.to_str() else {
            return reply.error(libc::EINVAL);
        };
        match self.fs.lookup(parent, name) {
            Some(ino) => match self.attr_for(ino) {
                Some(attr) => reply.entry(&TTL, &attr, 0),
                None => reply.error(libc::ENOENT),
            },
            None => reply.error(libc::ENOENT),
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, fh: Option<u64>, reply: ReplyAttr) {
        let _ = fh;
        debug!("getattr ino={ino}");
        match self.attr_for(ino) {
            Some(attr) => {
                debug!("getattr ino={ino} -> {:?}", attr.kind);
                reply.attr(&TTL, &attr)
            }
            None => {
                debug!("getattr ino={ino} -> ENOENT");
                reply.error(libc::ENOENT)
            }
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        // chmod / chown / utimes are kept in memory only (per the mount
        // contract); they are never sent to Overleaf.
        let mut err: Option<i32> = None;
        if let Err(e) = self.fs.set_attrs(ino, |a| {
            if let Some(m) = mode {
                // keep the file-type bits; chmod only sets permissions
                a.mode = (a.mode & libc::S_IFMT as u32) | (m & 0o7777);
            }
            if let Some(u) = uid {
                a.uid = u;
            }
            if let Some(g) = gid {
                a.gid = g;
            }
            if let Some(t) = &atime {
                a.atime = match t {
                    fuser::TimeOrNow::Now => SystemTime::now(),
                    fuser::TimeOrNow::SpecificTime(t) => *t,
                };
            }
            if let Some(t) = &mtime {
                a.mtime = match t {
                    fuser::TimeOrNow::Now => SystemTime::now(),
                    fuser::TimeOrNow::SpecificTime(t) => *t,
                };
            }
        }) {
            err = Some(e);
        }
        // truncate(2) arrives as size == 0; treat it as a content write.
        if let Some(new_size) = size {
            if let Some(node) = self.fs.node(ino) {
                if !node.is_dir() {
                    let mut data = self.read_bytes(ino);
                    data.resize(new_size as usize, 0);
                    match node.kind {
                        NodeKind::Doc { ref doc_id, .. } => {
                            let text = String::from_utf8_lossy(&data).to_string();
                            let mut doc = self
                                .fs
                                .doc(ino)
                                .unwrap_or_else(|| Doc {
                                    id: doc_id.clone(),
                                    lines: vec![String::new()],
                                    version: 0,
                                    ranges: None,
                                });
                            doc.set_text(&text);
                            self.fs.set_doc(ino, doc);
                            self.push_current(ino);
                        }
                        NodeKind::LocalFile => self.fs.set_local(ino, data),
                        _ => {}
                    }
                }
            }
        }
        if let Some(e) = err {
            return reply.error(e);
        }
        match self.attr_for(ino) {
            Some(attr) => reply.attr(&TTL, &attr),
            None => reply.error(libc::ENOENT),
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let Some(name) = name.to_str() else {
            return reply.error(libc::EINVAL);
        };
        if self.fs.lookup(parent, name).is_some() {
            return reply.error(libc::EEXIST);
        }
        let ino = self.fs.upsert(parent, name, NodeKind::Dir, 0);
        let _ = self.fs.set_attrs(ino, |a| {
            a.mode = crate::types::full_mode(true, (mode & 0o7777) & !umask)
        });
        self.sync.send(Outgoing::CreateDir {
            parent,
            name: name.to_string(),
        });
        match self.attr_for(ino) {
            Some(attr) => reply.entry(&TTL, &attr, 0),
            None => reply.error(libc::EIO),
        }
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let Some(name) = name.to_str() else {
            return reply.error(libc::EINVAL);
        };
        let Some(ino) = self.fs.lookup(parent, name) else {
            return reply.error(libc::ENOENT);
        };
        if self
            .fs
            .children
            .read()
            .get(&ino)
            .map(|c| !c.is_empty())
            .unwrap_or(false)
        {
            return reply.error(libc::ENOTEMPTY);
        }
        self.fs.remove(ino);
        self.sync.send(Outgoing::Delete { ino });
        reply.ok();
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let Some(name) = name.to_str() else {
            return reply.error(libc::EINVAL);
        };
        let Some(ino) = self.fs.lookup(parent, name) else {
            return reply.error(libc::ENOENT);
        };
        self.fs.remove(ino);
        self.sync.send(Outgoing::Delete { ino });
        reply.ok();
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        let (Some(name), Some(newname)) = (name.to_str(), newname.to_str()) else {
            return reply.error(libc::EINVAL);
        };
        let Some(ino) = self.fs.lookup(parent, name) else {
            return reply.error(libc::ENOENT);
        };
        match self.fs.r#move(ino, newparent, newname) {
            Ok(_) => {
                self.sync.send(Outgoing::Rename {
                    ino,
                    new_parent: newparent,
                    new_name: newname.to_string(),
                });
                reply.ok();
            }
            Err(e) => reply.error(e),
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        if self.fs.node(ino).is_none() {
            return reply.error(libc::ENOENT);
        }
        let write = flags & libc::O_ACCMODE as i32 != libc::O_RDONLY as i32;
        let fh = self.next_fh.fetch_add(1, Ordering::SeqCst);
        self.handles.lock().insert(
            fh,
            Handle {
                ino,
                dirty: write,
                dir_entries: None,
            },
        );
        reply.opened(fh, if write { fuser::consts::FOPEN_DIRECT_IO } else { 0 });
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyData,
    ) {
        let _ = fh;
        let data = self.read_bytes(ino);
        let start = offset.max(0) as usize;
        if start >= data.len() {
            return reply.data(&[]);
        }
        let end = (start + size as usize).min(data.len());
        trace!("read ino={ino} off={offset} len={}", end - start);
        reply.data(&data[start..end]);
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock: Option<u64>,
        reply: ReplyWrite,
    ) {
        let Some(node) = self.fs.node(ino) else {
            return reply.error(libc::ENOENT);
        };
        // Always splice into the *current* content, never into a buffer that was
        // captured at open(2) time: a truncate or a collaborator's edit since
        // then would otherwise be silently reverted by this write.
        let mut buf = self.read_bytes(ino);
        let start = offset.max(0) as usize;
        if buf.len() < start + data.len() {
            buf.resize(start + data.len(), 0);
        }
        buf[start..start + data.len()].copy_from_slice(data);
        if let Some(node_guard) = self.fs.nodes.read().get(&ino) {
            let _ = node_guard;
        }
        match node.kind {
            NodeKind::Doc { doc_id, .. } => {
                let text = String::from_utf8_lossy(&buf).to_string();
                let mut doc = self.fs.doc(ino).unwrap_or(Doc {
                    id: doc_id.clone(),
                    lines: vec![String::new()],
                    version: 0,
                    ranges: None,
                });
                doc.set_text(&text);
                self.fs.set_doc(ino, doc);
                if let Some(h) = self.handles.lock().get_mut(&fh) {
                    h.dirty = true;
                }
                // Push immediately so the edit is visible to collaborators as
                // fast as possible; the engine coalesces per-document anyway.
                self.push_current(ino);
            }
            NodeKind::LocalFile => {
                self.fs.set_local(ino, buf);
            }
            NodeKind::File { .. } => {
                return reply.error(libc::EROFS);
            }
            NodeKind::Dir => return reply.error(libc::EISDIR),
        }
        reply.written(data.len() as u32);
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        // On flush, re-push the document's current text: the write(2) calls
        // already did so, but a bare truncate does not always trigger one.
        let handle = self.handles.lock().remove(&fh);
        if let Some(h) = handle {
            if h.dirty {
                self.push_current(h.ino);
            }
        }
        let _ = ino;
        reply.ok();
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        self.fs.open_handles.fetch_sub(1, Ordering::SeqCst);
        let handle = self.handles.lock().remove(&fh);
        if let Some(h) = handle {
            if h.dirty {
                self.push_current(h.ino);
            }
        }
        let _ = ino;
        reply.ok();
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let Some(name) = name.to_str() else {
            return reply.error(libc::EINVAL);
        };
        if self.fs.lookup(parent, name).is_some() {
            return reply.error(libc::EEXIST);
        }
        // Path decide whether this file is synchronised or local-only.
        let parent_path = self.fs.path_of(parent);
        let full_path = if parent_path.is_empty() {
            name.to_string()
        } else {
            format!("{parent_path}/{name}")
        };
        let write = flags & libc::O_ACCMODE as i32 != libc::O_RDONLY as i32;
        let ino = if fsops::looks_like_text(name) && !self.fs.is_ignored(&full_path) {
            // Optimistically create a local doc node; the engine turns it into
            // a real Overleaf document and fills in the doc id.
            self.fs.upsert(
                parent,
                name,
                NodeKind::Doc {
                    doc_id: format!("pending:{full_path}"),
                    ranges: None,
                },
                0,
            )
        } else {
            self.fs.mark_ignored(&full_path, true);
            self.fs.upsert(parent, name, NodeKind::LocalFile, 0)
        };
        // `.sh` scripts are executable by default (the mount contract), even
        // though the kernel asks for plain 0644 when the file is created.
        let requested = mode & 0o7777;
        let default_perm = crate::types::default_mode_for(name);
        let _ = self.fs.set_attrs(ino, |a| {
            a.mode = crate::types::full_mode(
                false,
                ((requested | default_perm) & 0o7777) & !umask,
            )
        });
        let fh = self.next_fh.fetch_add(1, Ordering::SeqCst);
        self.handles.lock().insert(
            fh,
            Handle {
                ino,
                dirty: write,
                dir_entries: None,
            },
        );
        if write && matches!(self.fs.node(ino).map(|n| n.kind), Some(NodeKind::Doc { .. })) {
            self.sync.send(Outgoing::CreateDoc {
                parent,
                name: name.to_string(),
                text: String::new(),
            });
        }
        match self.attr_for(ino) {
            Some(attr) => reply.created(&TTL, &attr, 0, fh, 0),
            None => reply.error(libc::EIO),
        }
    }

    fn opendir(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        let Some(node) = self.fs.node(ino) else {
            return reply.error(libc::ENOENT);
        };
        if !node.is_dir() {
            return reply.error(libc::ENOTDIR);
        }
        let mut entries = vec![
            (ino, FileType::Directory, ".".to_string()),
            (node.parent, FileType::Directory, "..".to_string()),
        ];
        let children = self.fs.children.read().get(&ino).cloned().unwrap_or_default();
        for (name, child) in children {
            if let Some(t) = self.fs_type(child) {
                entries.push((child, t, name));
            }
        }
        let fh = self.next_fh.fetch_add(1, Ordering::SeqCst);
        self.handles.lock().insert(
            fh,
            Handle {
                ino,
                dirty: false,
                dir_entries: Some(entries),
            },
        );
        reply.opened(fh, 0);
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let entries = {
            let handles = self.handles.lock();
            handles.get(&fh).and_then(|h| h.dir_entries.clone())
        };
        let entries = match entries {
            Some(e) => e,
            None => return reply.error(libc::EBADF),
        };
        for (i, (child, kind, name)) in entries.iter().enumerate().skip(offset.max(0) as usize) {
            if reply.add(*child, (i + 1) as i64, *kind, name) {
                break;
            }
        }
        let _ = ino;
        reply.ok();
    }

    fn releasedir(&mut self, _req: &Request<'_>, _ino: u64, fh: u64, _flags: i32, reply: ReplyEmpty) {
        self.handles.lock().remove(&fh);
        reply.ok();
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyStatfs) {
        let nodes = self.fs.nodes.read().len() as u64;
        reply.statfs(
            nodes.max(1),
            nodes.max(1),
            nodes.max(1),
            nodes.max(1),
            0,
            BLOCK_SIZE,
            255,
            BLOCK_SIZE,
        );
    }

    fn access(&mut self, _req: &Request<'_>, ino: u64, _mask: i32, reply: ReplyEmpty) {
        if self.fs.node(ino).is_some() {
            reply.ok()
        } else {
            reply.error(libc::ENOENT)
        }
    }

    fn forget(&mut self, _req: &Request<'_>, _ino: u64, _nlookup: u64) {}

    fn fsync(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        // Push the document's current text: this is what makes `sync`/`fsync`
        // from userspace reach Overleaf immediately.
        let dirty = {
            let mut handles = self.handles.lock();
            handles.get_mut(&fh).map(|h| {
                h.dirty = true;
                h.ino
            })
        };
        if let Some(doc_ino) = dirty {
            self.push_current(doc_ino);
        }
        let _ = ino;
        reply.ok();
    }
}

impl OverleafFs {
    /// Exposed for tests: number of live handles.
    pub fn open_handle_count(&self) -> usize {
        self.handles.lock().len()
    }

    /// Root inode getter for callers that need it.
    pub fn root(&self) -> u64 {
        ROOT_INO
    }
}

/// Unused-import guard for `Entity` (it is part of the public sync API).
#[allow(dead_code)]
fn _entity_marker() -> Entity {
    Entity::Doc
}

#[allow(dead_code)]
fn _attr_marker(a: &crate::types::Attrs, n: &str) -> crate::types::Attrs {
    let _ = new_attrs(0, 0, n, false);
    a.clone()
}

#[allow(dead_code)]
fn _decode(name: &[u8]) -> Option<String> {
    decode_name(name)
}
