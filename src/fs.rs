use crate::client::{OverleafClient, SyncCommand};
use crate::fs_state::{FsState, InodeKind};
use fuser::{
    BsdFileFlags, Errno, FileHandle, Filesystem, FopenFlags, Generation, IoctlFlags,
    LockOwner, OpenFlags, RenameFlags, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyIoctl, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyXattr, Request, TimeOrNow,
    WriteFlags, INodeNo,
};
use std::ffi::OsStr;
use std::time::{Duration, SystemTime};
use tokio::sync::oneshot;

const TTL: Duration = Duration::from_secs(1);

pub struct OverleafFs {
    pub fs_state: FsState,
    pub client: OverleafClient,
}

impl OverleafFs {
    pub fn new(fs_state: FsState, client: OverleafClient) -> Self {
        Self { fs_state, client }
    }

    fn trigger_sync(&self, ino: u64) {
        if self.fs_state.is_ino_ignored(ino) {
            return;
        }

        if let Some((overleaf_id, kind, dirty)) = self.fs_state.get_info(ino) {
            if overleaf_id.starts_with("local_") {
                return;
            }
            if dirty {
                match kind {
                    InodeKind::Doc => {
                        let _ = self.client.command_tx.send(SyncCommand::SyncDoc {
                            doc_id: overleaf_id,
                        });
                    }
                    InodeKind::BinaryFile => {
                        if let Some((file_id, name, parent_ino, content)) =
                            self.fs_state.get_file_content(ino)
                        {
                            let parent_folder_id = self
                                .fs_state
                                .get_info(parent_ino)
                                .map(|(id, _, _)| id)
                                .unwrap_or_default();
                            let _ = self.client.command_tx.send(SyncCommand::UploadFile {
                                file_id,
                                name,
                                parent_folder_id,
                                content,
                            });
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

impl Filesystem for OverleafFs {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let name_str = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(Errno::ENOENT);
                return;
            }
        };

        if let Some(attr) = self.fs_state.lookup(parent.0, name_str) {
            reply.entry(&TTL, &attr, Generation(0));
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        if let Some(attr) = self.fs_state.get_attr(ino.0) {
            reply.attr(&TTL, &attr);
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let atime_val = match atime {
            Some(TimeOrNow::SpecificTime(t)) => Some(t),
            Some(TimeOrNow::Now) => Some(SystemTime::now()),
            None => None,
        };
        let mtime_val = match mtime {
            Some(TimeOrNow::SpecificTime(t)) => Some(t),
            Some(TimeOrNow::Now) => Some(SystemTime::now()),
            None => None,
        };

        tracing::info!(
            "setattr ino={:?} mode={:?} uid={:?} gid={:?} size={:?}",
            ino, mode, uid, gid, size
        );

        match self
            .fs_state
            .set_attr(ino.0, mode, uid, gid, size, atime_val, mtime_val)
        {
            Ok(attr) => {
                tracing::info!("setattr success: ino={:?} uid={} gid={} perm={:o}", ino, attr.uid, attr.gid, attr.perm);
                reply.attr(&TTL, &attr);
            }
            Err(err) => {
                tracing::warn!("setattr error: {}", err);
                reply.error(Errno::from_i32(err));
            }
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        if let Some(entries) = self.fs_state.readdir(ino.0) {
            for (i, (entry_ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize) {
                if reply.add(INodeNo(entry_ino), (i + 1) as u64, kind, name) {
                    break;
                }
            }
            reply.ok();
        } else {
            reply.error(Errno::ENOTDIR);
        }
    }

    fn open(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: ReplyOpen) {
        if self.fs_state.get_attr(ino.0).is_some() {
            reply.opened(FileHandle(0), FopenFlags::FOPEN_DIRECT_IO);
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        if let Some(data) = self.fs_state.read(ino.0, offset, size) {
            reply.data(&data);
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        match self.fs_state.write(ino.0, offset, data) {
            Ok(written) => {
                reply.written(written as u32);
            }
            Err(err) => {
                reply.error(Errno::from_i32(err));
            }
        }
    }

    fn flush(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        self.trigger_sync(ino.0);
        reply.ok();
    }

    fn release(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        self.trigger_sync(ino.0);
        reply.ok();
    }

    fn fsync(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        _datasync: bool,
        reply: ReplyEmpty,
    ) {
        self.trigger_sync(ino.0);
        reply.ok();
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let name_str = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(Errno::EINVAL);
                return;
            }
        };

        if self.fs_state.is_ignored(parent.0, name_str, false) {
            let ino = self.fs_state.add_doc(parent.0, name_str, "", "", 0);
            if name_str.ends_with(".sh") {
                let _ = self.fs_state.set_attr(ino, Some(0o755), None, None, None, None, None);
            } else if mode != 0 {
                let _ = self.fs_state.set_attr(ino, Some((mode & 0o7777) as u32), None, None, None, None, None);
            }
            if let Some(attr) = self.fs_state.get_attr(ino) {
                tracing::info!("Created ignored/local doc {} (ino: {}) in memory only", name_str, ino);
                reply.created(&TTL, &attr, Generation(0), FileHandle(0), FopenFlags::FOPEN_DIRECT_IO);
            } else {
                reply.error(Errno::EIO);
            }
            return;
        }

        let parent_folder_id = self
            .fs_state
            .get_info(parent.0)
            .map(|(id, _, _)| id)
            .unwrap_or_default();

        let (tx, rx) = oneshot::channel();
        let _ = self.client.command_tx.send(SyncCommand::CreateDoc {
            parent_folder_id,
            name: name_str.to_string(),
            content: String::new(),
            reply: tx,
        });

        match rx.blocking_recv() {
            Ok(Ok(doc_id)) => {
                let ino = self.fs_state.add_doc(parent.0, name_str, &doc_id, "", 0);
                if name_str.ends_with(".sh") {
                    let _ = self.fs_state.set_attr(
                        ino,
                        Some(0o755),
                        None,
                        None,
                        None,
                        None,
                        None,
                    );
                } else if mode != 0 {
                    let _ = self.fs_state.set_attr(
                        ino,
                        Some((mode & 0o7777) as u32),
                        None,
                        None,
                        None,
                        None,
                        None,
                    );
                }
                if let Some(attr) = self.fs_state.get_attr(ino) {
                    reply.created(&TTL, &attr, Generation(0), FileHandle(0), FopenFlags::FOPEN_DIRECT_IO);
                } else {
                    reply.error(Errno::EIO);
                }
            }
            Ok(Err(e)) => {
                tracing::error!("CreateDoc error from server: {}", e);
                reply.error(Errno::EIO);
            }
            Err(e) => {
                tracing::error!("CreateDoc oneshot channel recv error: {}", e);
                reply.error(Errno::EIO);
            }
        }
    }

    fn mknod(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        let name_str = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(Errno::EINVAL);
                return;
            }
        };

        let file_type = mode & libc::S_IFMT as u32;
        if file_type != 0 && file_type != libc::S_IFREG as u32 {
            reply.error(Errno::ENOTSUP);
            return;
        }

        if self.fs_state.is_ignored(parent.0, name_str, false) {
            let ino = self.fs_state.add_doc(parent.0, name_str, "", "", 0);
            if name_str.ends_with(".sh") {
                let _ = self.fs_state.set_attr(ino, Some(0o755), None, None, None, None, None);
            } else if mode != 0 {
                let _ = self.fs_state.set_attr(ino, Some((mode & 0o7777) as u32), None, None, None, None, None);
            }
            if let Some(attr) = self.fs_state.get_attr(ino) {
                tracing::info!("mknod created ignored/local doc {} (ino: {}) in memory only", name_str, ino);
                reply.entry(&TTL, &attr, Generation(0));
            } else {
                reply.error(Errno::EIO);
            }
            return;
        }

        let parent_folder_id = self
            .fs_state
            .get_info(parent.0)
            .map(|(id, _, _)| id)
            .unwrap_or_default();

        let (tx, rx) = oneshot::channel();
        let _ = self.client.command_tx.send(SyncCommand::CreateDoc {
            parent_folder_id,
            name: name_str.to_string(),
            content: String::new(),
            reply: tx,
        });

        match rx.blocking_recv() {
            Ok(Ok(doc_id)) => {
                let ino = self.fs_state.add_doc(parent.0, name_str, &doc_id, "", 0);
                if name_str.ends_with(".sh") {
                    let _ = self.fs_state.set_attr(ino, Some(0o755), None, None, None, None, None);
                } else if mode != 0 {
                    let _ = self.fs_state.set_attr(ino, Some((mode & 0o7777) as u32), None, None, None, None, None);
                }
                if let Some(attr) = self.fs_state.get_attr(ino) {
                    reply.entry(&TTL, &attr, Generation(0));
                } else {
                    reply.error(Errno::EIO);
                }
            }
            _ => reply.error(Errno::EIO),
        }
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let name_str = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(Errno::EINVAL);
                return;
            }
        };

        if self.fs_state.is_ignored(parent.0, name_str, true) {
            let ino = self.fs_state.add_folder(parent.0, name_str, "");
            if mode != 0 {
                let _ = self.fs_state.set_attr(
                    ino,
                    Some((mode & 0o7777) as u32),
                    None,
                    None,
                    None,
                    None,
                    None,
                );
            }
            if let Some(attr) = self.fs_state.get_attr(ino) {
                tracing::info!("Created ignored/local folder {} (ino: {}) in memory only", name_str, ino);
                reply.entry(&TTL, &attr, Generation(0));
            } else {
                reply.error(Errno::EIO);
            }
            return;
        }

        let parent_folder_id = self
            .fs_state
            .get_info(parent.0)
            .map(|(id, _, _)| id)
            .unwrap_or_default();

        let (tx, rx) = oneshot::channel();
        let _ = self.client.command_tx.send(SyncCommand::CreateFolder {
            parent_folder_id,
            name: name_str.to_string(),
            reply: tx,
        });

        match rx.blocking_recv() {
            Ok(Ok(folder_id)) => {
                let ino = self.fs_state.add_folder(parent.0, name_str, &folder_id);
                if mode != 0 {
                    let _ = self.fs_state.set_attr(
                        ino,
                        Some((mode & 0o7777) as u32),
                        None,
                        None,
                        None,
                        None,
                        None,
                    );
                }
                if let Some(attr) = self.fs_state.get_attr(ino) {
                    reply.entry(&TTL, &attr, Generation(0));
                } else {
                    reply.error(Errno::EIO);
                }
            }
            _ => reply.error(Errno::EIO),
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let name_str = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(Errno::EINVAL);
                return;
            }
        };

        let is_ignored = self.fs_state.is_ignored(parent.0, name_str, false);
        if let Some(removed) = self.fs_state.remove_node(parent.0, name_str) {
            if !is_ignored && !removed.overleaf_id.starts_with("local_") {
                let entity_type = match removed.kind {
                    InodeKind::Doc => "doc",
                    InodeKind::BinaryFile => "file",
                    InodeKind::Directory => "folder",
                };
                let _ = self.client.command_tx.send(SyncCommand::DeleteEntity {
                    entity_type: entity_type.to_string(),
                    entity_id: removed.overleaf_id,
                });
            }
            reply.ok();
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let name_str = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(Errno::EINVAL);
                return;
            }
        };

        let is_ignored = self.fs_state.is_ignored(parent.0, name_str, true);
        if let Some(removed) = self.fs_state.remove_node(parent.0, name_str) {
            if !is_ignored && !removed.overleaf_id.starts_with("local_") {
                let _ = self.client.command_tx.send(SyncCommand::DeleteEntity {
                    entity_type: "folder".to_string(),
                    entity_id: removed.overleaf_id,
                });
            }
            reply.ok();
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        _flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let old_name = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(Errno::EINVAL);
                return;
            }
        };
        let new_name = match newname.to_str() {
            Some(s) => s,
            None => {
                reply.error(Errno::EINVAL);
                return;
            }
        };

        if let Some(target_ino) = self.fs_state.lookup(parent.0, old_name).map(|a| a.ino.0) {
            if let Some((overleaf_id, kind, _)) = self.fs_state.get_info(target_ino) {
                let is_old_ignored = self.fs_state.is_ino_ignored(target_ino) || overleaf_id.starts_with("local_");
                let is_new_ignored = self.fs_state.is_ignored(newparent.0, new_name, kind == InodeKind::Directory);

                if !is_old_ignored && !is_new_ignored {
                    let entity_type = match kind {
                        InodeKind::Doc => "doc",
                        InodeKind::BinaryFile => "file",
                        InodeKind::Directory => "folder",
                    };
                    let _ = self.client.command_tx.send(SyncCommand::RenameEntity {
                        entity_type: entity_type.to_string(),
                        entity_id: overleaf_id,
                        new_name: new_name.to_string(),
                    });
                }
                let _ = self.fs_state.rename_node(parent.0, old_name, newparent.0, new_name);
                reply.ok();
                return;
            }
        }

        reply.error(Errno::ENOENT);
    }

    fn statfs(&self, _req: &Request, _ino: INodeNo, reply: ReplyStatfs) {
        reply.statfs(1000000, 500000, 500000, 1000000, 500000, 512, 255, 512);
    }

    fn getxattr(&self, _req: &Request, _ino: INodeNo, _name: &OsStr, _size: u32, reply: ReplyXattr) {
        reply.error(Errno::ENODATA);
    }

    fn listxattr(&self, _req: &Request, _ino: INodeNo, size: u32, reply: ReplyXattr) {
        if size == 0 {
            reply.size(0);
        } else {
            reply.data(&[]);
        }
    }

    fn setxattr(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _name: &OsStr,
        _value: &[u8],
        _flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        reply.error(Errno::ENOTSUP);
    }

    fn removexattr(&self, _req: &Request, _ino: INodeNo, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(Errno::ENODATA);
    }

    fn ioctl(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _flags: IoctlFlags,
        _cmd: u32,
        _in_data: &[u8],
        _out_size: u32,
        reply: ReplyIoctl,
    ) {
        reply.error(Errno::ENOTTY);
    }
}
