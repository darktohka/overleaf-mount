use crate::ot::{self, Component};
use fuser::{FileAttr, FileType};
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use parking_lot::RwLock;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

#[derive(Debug, Clone)]
pub struct SnapshotRecord {
    pub version: i64,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InodeKind {
    Directory,
    Doc,
    BinaryFile,
}

#[derive(Debug, Clone)]
pub struct InodeData {
    pub ino: u64,
    pub parent: u64,
    pub name: String,
    pub kind: InodeKind,
    pub overleaf_id: String,
    pub children: BTreeMap<String, u64>, // name -> ino (for directories)
    pub content: Vec<u8>,
    pub version: i64,
    pub base_text: String,
    pub dirty: bool,
    pub hash: Option<String>,
    pub authoritative_sentences: HashSet<String>,
    pub history: VecDeque<SnapshotRecord>,
    pub atime: SystemTime,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
    pub perm: u16,
    pub uid: u32,
    pub gid: u32,
}

impl InodeData {
    pub fn to_file_attr(&self) -> FileAttr {
        let kind = match self.kind {
            InodeKind::Directory => FileType::Directory,
            InodeKind::Doc | InodeKind::BinaryFile => FileType::RegularFile,
        };
        let nlink = if self.kind == InodeKind::Directory { 2 } else { 1 };

        FileAttr {
            ino: fuser::INodeNo(self.ino),
            size: self.content.len() as u64,
            blocks: ((self.content.len() + 511) / 512) as u64,
            atime: self.atime,
            mtime: self.mtime,
            ctime: self.ctime,
            crtime: self.ctime,
            kind,
            perm: self.perm,
            nlink,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            flags: 0,
            blksize: 512,
        }
    }
}

pub struct FsStateInner {
    pub next_ino: u64,
    pub inodes: HashMap<u64, InodeData>,
    pub overleaf_id_to_ino: HashMap<String, u64>,
    pub uid: u32,
    pub gid: u32,
    pub gitignores: Vec<(PathBuf, Gitignore)>,
}

impl FsStateInner {
    pub fn get_path(&self, ino: u64) -> Option<PathBuf> {
        if ino == 1 {
            return Some(PathBuf::new());
        }
        let mut components = Vec::new();
        let mut curr = ino;
        let mut visited = HashSet::new();

        while curr != 1 {
            if !visited.insert(curr) {
                return None;
            }
            let node = self.inodes.get(&curr)?;
            components.push(node.name.clone());
            curr = node.parent;
        }

        let mut path = PathBuf::new();
        for comp in components.into_iter().rev() {
            path.push(comp);
        }
        Some(path)
    }

    pub fn get_child_path(&self, parent_ino: u64, child_name: &str) -> PathBuf {
        match self.get_path(parent_ino) {
            Some(mut p) => {
                p.push(child_name);
                p
            }
            None => PathBuf::from(child_name),
        }
    }

    pub fn rebuild_gitignore(&mut self) {
        let mut gitignore_map: HashMap<PathBuf, Vec<String>> = HashMap::new();

        for (_ino, node) in &self.inodes {
            if node.name == ".gitignore" && (node.kind == InodeKind::Doc || node.kind == InodeKind::BinaryFile) {
                let dir_path = self.get_path(node.parent).unwrap_or_else(|| PathBuf::new());
                if let Ok(content_str) = std::str::from_utf8(&node.content) {
                    let lines: Vec<String> = content_str.lines().map(|s| s.to_string()).collect();
                    gitignore_map.insert(dir_path, lines);
                }
            }
        }

        let mut gitignores = Vec::new();
        for (dir, lines) in gitignore_map {
            let mut builder = GitignoreBuilder::new(&dir);
            let gitignore_file_path = if dir.as_os_str().is_empty() {
                PathBuf::from(".gitignore")
            } else {
                dir.join(".gitignore")
            };

            for line in lines {
                let _ = builder.add_line(Some(gitignore_file_path.clone()), &line);
            }

            if let Ok(gi) = builder.build() {
                gitignores.push((dir, gi));
            }
        }

        // Sort descending by depth (longest path first) so child gitignores take precedence
        gitignores.sort_by(|a, b| b.0.components().count().cmp(&a.0.components().count()));

        self.gitignores = gitignores;
    }

    pub fn is_path_ignored(&self, path: &Path, is_dir: bool) -> bool {
        if path.as_os_str().is_empty() {
            return false;
        }

        for (dir, gi) in &self.gitignores {
            let is_under = if dir.as_os_str().is_empty() {
                true
            } else {
                path.starts_with(dir)
            };

            if is_under {
                let m = gi.matched_path_or_any_parents(path, is_dir);
                if m.is_whitelist() {
                    return false;
                }
                if m.is_ignore() {
                    return true;
                }
            }
        }

        false
    }
}

#[derive(Clone)]
pub struct FsState {
    pub inner: Arc<RwLock<FsStateInner>>,
}

impl Default for FsState {
    fn default() -> Self {
        Self::new(None, None)
    }
}

impl FsState {
    pub fn new(uid: Option<u32>, gid: Option<u32>) -> Self {
        let default_uid = std::env::var("OVERLEAF_ORIG_UID")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or_else(|| unsafe { libc::getuid() });
        let default_gid = std::env::var("OVERLEAF_ORIG_GID")
            .ok()
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or_else(|| unsafe { libc::getgid() });

        let uid = uid.unwrap_or(default_uid);
        let gid = gid.unwrap_or(default_gid);
        let now = SystemTime::now();

        let root = InodeData {
            ino: 1,
            parent: 1,
            name: String::new(),
            kind: InodeKind::Directory,
            overleaf_id: "root".to_string(),
            children: BTreeMap::new(),
            content: Vec::new(),
            version: 0,
            base_text: String::new(),
            dirty: false,
            hash: None,
            authoritative_sentences: HashSet::new(),
            history: VecDeque::new(),
            atime: now,
            mtime: now,
            ctime: now,
            perm: 0o755,
            uid,
            gid,
        };

        let mut inodes = HashMap::new();
        inodes.insert(1, root);

        let mut overleaf_id_to_ino = HashMap::new();
        overleaf_id_to_ino.insert("root".to_string(), 1);

        FsState {
            inner: Arc::new(RwLock::new(FsStateInner {
                next_ino: 2,
                inodes,
                overleaf_id_to_ino,
                uid,
                gid,
                gitignores: Vec::new(),
            })),
        }
    }

    pub fn set_root_folder_id(&self, id: &str) {
        let mut inner = self.inner.write();
        inner.overleaf_id_to_ino.remove("root");
        if let Some(root) = inner.inodes.get_mut(&1) {
            root.overleaf_id = id.to_string();
        }
        inner.overleaf_id_to_ino.insert(id.to_string(), 1);
    }

    pub fn get_info(&self, ino: u64) -> Option<(String, InodeKind, bool)> {
        let inner = self.inner.read();
        let data = inner.inodes.get(&ino)?;
        Some((data.overleaf_id.clone(), data.kind.clone(), data.dirty))
    }

    pub fn get_file_content(&self, ino: u64) -> Option<(String, String, u64, Vec<u8>)> {
        let inner = self.inner.read();
        let data = inner.inodes.get(&ino)?;
        Some((data.overleaf_id.clone(), data.name.clone(), data.parent, data.content.clone()))
    }

    pub fn readdir(&self, ino: u64) -> Option<Vec<(u64, FileType, String)>> {
        let inner = self.inner.read();
        let data = inner.inodes.get(&ino)?;
        if data.kind != InodeKind::Directory {
            return None;
        }

        let mut entries = Vec::new();
        entries.push((ino, FileType::Directory, ".".to_string()));
        entries.push((data.parent, FileType::Directory, "..".to_string()));

        for (name, child_ino) in &data.children {
            if let Some(child_data) = inner.inodes.get(child_ino) {
                let kind = match child_data.kind {
                    InodeKind::Directory => FileType::Directory,
                    InodeKind::Doc | InodeKind::BinaryFile => FileType::RegularFile,
                };
                entries.push((*child_ino, kind, name.clone()));
            }
        }

        Some(entries)
    }

    pub fn get_attr(&self, ino: u64) -> Option<FileAttr> {
        let inner = self.inner.read();
        inner.inodes.get(&ino).map(|d| d.to_file_attr())
    }

    pub fn lookup(&self, parent: u64, name: &str) -> Option<FileAttr> {
        let inner = self.inner.read();
        let parent_data = inner.inodes.get(&parent)?;
        let child_ino = parent_data.children.get(name)?;
        inner.inodes.get(child_ino).map(|d| d.to_file_attr())
    }

    pub fn read(&self, ino: u64, offset: u64, size: u32) -> Option<Vec<u8>> {
        let inner = self.inner.read();
        let data = inner.inodes.get(&ino)?;
        let start = offset as usize;
        if start >= data.content.len() {
            return Some(Vec::new());
        }
        let end = std::cmp::min(start + size as usize, data.content.len());
        Some(data.content[start..end].to_vec())
    }

    pub fn is_ignored(&self, parent_ino: u64, name: &str, is_dir: bool) -> bool {
        let inner = self.inner.read();
        let path = inner.get_child_path(parent_ino, name);
        inner.is_path_ignored(&path, is_dir)
    }

    pub fn is_ino_ignored(&self, ino: u64) -> bool {
        let inner = self.inner.read();
        if let Some(node) = inner.inodes.get(&ino) {
            if node.overleaf_id.starts_with("local_") {
                return true;
            }
            if let Some(path) = inner.get_path(ino) {
                let is_dir = node.kind == InodeKind::Directory;
                return inner.is_path_ignored(&path, is_dir);
            }
        }
        false
    }

    pub fn rebuild_gitignore(&self) {
        let mut inner = self.inner.write();
        inner.rebuild_gitignore();
    }

    pub fn write(&self, ino: u64, offset: u64, bytes: &[u8]) -> Result<usize, libc::c_int> {
        let mut inner = self.inner.write();
        let is_gitignore = {
            let data = inner.inodes.get_mut(&ino).ok_or(libc::ENOENT)?;
            if data.kind == InodeKind::Directory {
                return Err(libc::EISDIR);
            }

            let offset = offset as usize;
            let required_len = offset + bytes.len();
            if required_len > data.content.len() {
                data.content.resize(required_len, 0);
            }
            data.content[offset..required_len].copy_from_slice(bytes);
            data.dirty = true;
            data.mtime = SystemTime::now();

            // If doc, track locally modified sentences
            if data.kind == InodeKind::Doc {
                if let Ok(new_text) = std::str::from_utf8(&data.content) {
                    let spans = ot::split_sentences(new_text);
                    data.authoritative_sentences.clear();
                    for span in spans {
                        if !data.base_text.contains(&span.text) {
                            data.authoritative_sentences.insert(span.text);
                        }
                    }
                }
            }
            data.name == ".gitignore"
        };

        if is_gitignore {
            inner.rebuild_gitignore();
        }

        Ok(bytes.len())
    }

    pub fn set_attr(
        &self,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<SystemTime>,
        mtime: Option<SystemTime>,
    ) -> Result<FileAttr, libc::c_int> {
        let mut inner = self.inner.write();
        let (attr, is_gitignore) = {
            let data = inner.inodes.get_mut(&ino).ok_or(libc::ENOENT)?;

            let mut changed_meta = false;

            if let Some(m) = mode {
                data.perm = (m & 0o7777) as u16;
                changed_meta = true;
            }

            if let Some(u) = uid {
                data.uid = u;
                changed_meta = true;
            }

            if let Some(g) = gid {
                data.gid = g;
                changed_meta = true;
            }

            if let Some(sz) = size {
                if data.kind == InodeKind::Directory {
                    return Err(libc::EISDIR);
                }
                data.content.resize(sz as usize, 0);
                data.dirty = true;
                data.mtime = SystemTime::now();
                changed_meta = true;

                if data.kind == InodeKind::Doc {
                    if let Ok(new_text) = std::str::from_utf8(&data.content) {
                        let spans = ot::split_sentences(new_text);
                        data.authoritative_sentences.clear();
                        for span in spans {
                            if !data.base_text.contains(&span.text) {
                                data.authoritative_sentences.insert(span.text);
                            }
                        }
                    }
                }
            }

            if let Some(at) = atime {
                data.atime = at;
            }
            if let Some(mt) = mtime {
                data.mtime = mt;
            }

            if changed_meta {
                data.ctime = SystemTime::now();
            }

            (data.to_file_attr(), data.name == ".gitignore" && size.is_some())
        };

        if is_gitignore {
            inner.rebuild_gitignore();
        }

        Ok(attr)
    }

    pub fn truncate(&self, ino: u64, size: u64) -> Result<FileAttr, libc::c_int> {
        self.set_attr(ino, None, None, None, Some(size), None, None)
    }

    pub fn add_doc(&self, parent: u64, name: &str, overleaf_id: &str, text: &str, version: i64) -> u64 {
        let mut inner = self.inner.write();
        let ino = inner.next_ino;
        inner.next_ino += 1;
        let now = SystemTime::now();
        let uid = inner.uid;
        let gid = inner.gid;

        let overleaf_id = if overleaf_id.is_empty() {
            format!("local_doc_{}", ino)
        } else {
            overleaf_id.to_string()
        };

        let mut history = VecDeque::new();
        history.push_back(SnapshotRecord {
            version,
            text: text.to_string(),
        });

        let perm = if name.ends_with(".sh") { 0o755 } else { 0o644 };

        let node = InodeData {
            ino,
            parent,
            name: name.to_string(),
            kind: InodeKind::Doc,
            overleaf_id: overleaf_id.clone(),
            children: BTreeMap::new(),
            content: text.as_bytes().to_vec(),
            version,
            base_text: text.to_string(),
            dirty: false,
            hash: None,
            authoritative_sentences: HashSet::new(),
            history,
            atime: now,
            mtime: now,
            ctime: now,
            perm,
            uid,
            gid,
        };

        inner.inodes.insert(ino, node);
        inner.overleaf_id_to_ino.insert(overleaf_id, ino);
        if let Some(parent_node) = inner.inodes.get_mut(&parent) {
            parent_node.children.insert(name.to_string(), ino);
        }

        if name == ".gitignore" {
            inner.rebuild_gitignore();
        }

        ino
    }

    pub fn add_file(&self, parent: u64, name: &str, overleaf_id: &str, hash: Option<String>, content: Vec<u8>) -> u64 {
        let mut inner = self.inner.write();
        let ino = inner.next_ino;
        inner.next_ino += 1;
        let now = SystemTime::now();
        let uid = inner.uid;
        let gid = inner.gid;

        let overleaf_id = if overleaf_id.is_empty() {
            format!("local_file_{}", ino)
        } else {
            overleaf_id.to_string()
        };

        let perm = if name.ends_with(".sh") { 0o755 } else { 0o644 };

        let node = InodeData {
            ino,
            parent,
            name: name.to_string(),
            kind: InodeKind::BinaryFile,
            overleaf_id: overleaf_id.clone(),
            children: BTreeMap::new(),
            content,
            version: 0,
            base_text: String::new(),
            dirty: false,
            hash,
            authoritative_sentences: HashSet::new(),
            history: VecDeque::new(),
            atime: now,
            mtime: now,
            ctime: now,
            perm,
            uid,
            gid,
        };

        inner.inodes.insert(ino, node);
        inner.overleaf_id_to_ino.insert(overleaf_id, ino);
        if let Some(parent_node) = inner.inodes.get_mut(&parent) {
            parent_node.children.insert(name.to_string(), ino);
        }

        if name == ".gitignore" {
            inner.rebuild_gitignore();
        }

        ino
    }

    pub fn add_folder(&self, parent: u64, name: &str, overleaf_id: &str) -> u64 {
        let mut inner = self.inner.write();
        let ino = inner.next_ino;
        inner.next_ino += 1;
        let now = SystemTime::now();
        let uid = inner.uid;
        let gid = inner.gid;

        let overleaf_id = if overleaf_id.is_empty() {
            format!("local_folder_{}", ino)
        } else {
            overleaf_id.to_string()
        };

        let node = InodeData {
            ino,
            parent,
            name: name.to_string(),
            kind: InodeKind::Directory,
            overleaf_id: overleaf_id.clone(),
            children: BTreeMap::new(),
            content: Vec::new(),
            version: 0,
            base_text: String::new(),
            dirty: false,
            hash: None,
            authoritative_sentences: HashSet::new(),
            history: VecDeque::new(),
            atime: now,
            mtime: now,
            ctime: now,
            perm: 0o755,
            uid,
            gid,
        };

        inner.inodes.insert(ino, node);
        inner.overleaf_id_to_ino.insert(overleaf_id, ino);
        if let Some(parent_node) = inner.inodes.get_mut(&parent) {
            parent_node.children.insert(name.to_string(), ino);
        }

        ino
    }

    pub fn apply_remote_ot(
        &self,
        doc_id: &str,
        update_v: i64,
        ops: &[Component],
    ) -> Option<(String, i64, String, Vec<Component>)> {
        let mut inner = self.inner.write();
        let ino = *inner.overleaf_id_to_ino.get(doc_id)?;
        let is_gitignore = inner.inodes.get(&ino).map(|n| n.name == ".gitignore").unwrap_or(false);
        let data = inner.inodes.get_mut(&ino)?;

        let current_text = match std::str::from_utf8(&data.content) {
            Ok(s) => s.to_string(),
            Err(_) => return None,
        };

        // Find common ancestor, compute remote_text, and compute server_text_now
        let (ancestor_text, remote_text, server_text_now) = {
            // 1. Try applying directly to data.base_text
            if let Ok(rt) = ot::apply(&data.base_text, ops) {
                (data.base_text.clone(), rt.clone(), rt)
            } else {
                // 2. Try applying to historical snapshots (newest to oldest)
                let mut found = None;
                for record in data.history.iter().rev() {
                    if let Ok(rt) = ot::apply(&record.text, ops) {
                        found = Some((record.text.clone(), rt));
                        break;
                    }
                }

                if let Some((anc, rt)) = found {
                    let local_ops = ot::diff(&anc, &data.base_text);
                    let transformed_ops = ot::transform(ops, &local_ops, ot::Side::Left);
                    let st = ot::apply(&data.base_text, &transformed_ops)
                        .or_else(|_| ot::apply_fuzzy(&data.base_text, &transformed_ops))
                        .unwrap_or_else(|_| data.base_text.clone());
                    (anc, rt, st)
                } else {
                    // 3. Fallback: fuzzy application
                    let rt = ot::apply_fuzzy(&current_text, ops).unwrap_or_else(|_| current_text.clone());
                    (data.base_text.clone(), rt.clone(), rt)
                }
            }
        };

        let next_v = update_v + 1;
        let has_divergence = data.dirty || current_text != ancestor_text;

        let result = if !has_divergence {
            // Clean remote update: no local divergence
            data.content = remote_text.as_bytes().to_vec();
            data.base_text = remote_text.clone();
            data.version = next_v;
            data.mtime = SystemTime::now();
            data.history.push_back(SnapshotRecord {
                version: next_v,
                text: remote_text,
            });
            if data.history.len() > 50 {
                data.history.pop_front();
            }
            None
        } else {
            // CONFLICT / CONCURRENT EDITS: 3-way reconciliation!
            // "If both people edit the same part / sentence at the same time, and there is no way to tell which is the authoritative, then use our version as authoritative."
            let res = ot::reconcile_conflict(
                &ancestor_text,
                &current_text,
                &remote_text,
            );

            data.content = res.final_text.as_bytes().to_vec();
            data.base_text = res.final_text.clone();
            data.version = next_v;
            data.mtime = SystemTime::now();
            data.history.push_back(SnapshotRecord {
                version: next_v,
                text: res.final_text.clone(),
            });
            if data.history.len() > 50 {
                data.history.pop_front();
            }

            if res.has_conflict {
                // When an authoritative conflict resolution overrides remote changes, diff server_text_now to final_text
                let corrective_ops = ot::diff(&server_text_now, &res.final_text);
                if !corrective_ops.is_empty() {
                    Some((doc_id.to_string(), next_v, res.final_text, corrective_ops))
                } else {
                    data.dirty = false;
                    None
                }
            } else {
                data.dirty = false;
                None
            }
        };

        if is_gitignore {
            inner.rebuild_gitignore();
        }

        result
    }

    pub fn ack_our_ot(&self, doc_id: &str, new_v: i64) {
        let mut inner = self.inner.write();
        let target_ino = inner.overleaf_id_to_ino.get(doc_id).copied();
        if let Some(ino) = target_ino {
            if let Some(data) = inner.inodes.get_mut(&ino) {
                data.version = new_v;
                if let Ok(text) = std::str::from_utf8(&data.content) {
                    data.base_text = text.to_string();
                    data.history.push_back(SnapshotRecord {
                        version: new_v,
                        text: text.to_string(),
                    });
                    if data.history.len() > 50 {
                        data.history.pop_front();
                    }
                }
                data.dirty = false;
            }
        }
    }

    pub fn remove_node(&self, parent: u64, name: &str) -> Option<InodeData> {
        let mut inner = self.inner.write();
        let child_ino = {
            let parent_data = inner.inodes.get_mut(&parent)?;
            parent_data.children.remove(name)?
        };
        let removed = inner.inodes.remove(&child_ino)?;
        inner.overleaf_id_to_ino.remove(&removed.overleaf_id);
        if removed.name == ".gitignore" {
            inner.rebuild_gitignore();
        }
        Some(removed)
    }

    pub fn rename_node(&self, old_parent: u64, old_name: &str, new_parent: u64, new_name: &str) -> Result<(), libc::c_int> {
        let mut inner = self.inner.write();
        let is_gitignore = old_name == ".gitignore" || new_name == ".gitignore";
        let ino = {
            let p = inner.inodes.get_mut(&old_parent).ok_or(libc::ENOENT)?;
            p.children.remove(old_name).ok_or(libc::ENOENT)?
        };

        let target_existing = {
            let new_p = inner.inodes.get(&new_parent).ok_or(libc::ENOENT)?;
            new_p.children.get(new_name).copied()
        };
        if let Some(existing_ino) = target_existing {
            inner.inodes.remove(&existing_ino);
        }

        if let Some(node) = inner.inodes.get_mut(&ino) {
            node.parent = new_parent;
            node.name = new_name.to_string();
            if new_name.ends_with(".sh") {
                node.perm |= 0o111;
            }
            node.ctime = SystemTime::now();
        }

        if let Some(new_p) = inner.inodes.get_mut(&new_parent) {
            new_p.children.insert(new_name.to_string(), ino);
        }

        if is_gitignore {
            inner.rebuild_gitignore();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_permissions_and_sh_files() {
        let fs = FsState::new(Some(1000), Some(1000));

        // Root directory should have perm 0755
        let root_attr = fs.get_attr(1).expect("root attr");
        assert_eq!(root_attr.perm, 0o755);
        assert_eq!(root_attr.uid, 1000);
        assert_eq!(root_attr.gid, 1000);

        // Regular doc should have perm 0644
        let doc_ino = fs.add_doc(1, "main.tex", "d1", "hello", 1);
        let doc_attr = fs.get_attr(doc_ino).expect("doc attr");
        assert_eq!(doc_attr.perm, 0o644);

        // Shell script doc should have perm 0755 (+x by default)
        let sh_ino = fs.add_doc(1, "build.sh", "d2", "#!/bin/bash", 1);
        let sh_attr = fs.get_attr(sh_ino).expect("sh doc attr");
        assert_eq!(sh_attr.perm, 0o755);

        // Binary file should have perm 0644
        let bin_ino = fs.add_file(1, "image.png", "f1", None, vec![1, 2, 3]);
        let bin_attr = fs.get_attr(bin_ino).expect("bin attr");
        assert_eq!(bin_attr.perm, 0o644);

        // Shell script binary file should have perm 0755
        let bin_sh_ino = fs.add_file(1, "script.sh", "f2", None, vec![4, 5, 6]);
        let bin_sh_attr = fs.get_attr(bin_sh_ino).expect("bin sh attr");
        assert_eq!(bin_sh_attr.perm, 0o755);

        // Folder should have perm 0755
        let folder_ino = fs.add_folder(1, "scripts", "fol1");
        let folder_attr = fs.get_attr(folder_ino).expect("folder attr");
        assert_eq!(folder_attr.perm, 0o755);
    }

    #[test]
    fn test_chmod_and_chown_in_memory() {
        let fs = FsState::new(Some(1000), Some(1000));
        let doc_ino = fs.add_doc(1, "main.tex", "d1", "content", 1);

        // chmod to 0700
        let updated = fs.set_attr(doc_ino, Some(0o700), None, None, None, None, None).unwrap();
        assert_eq!(updated.perm, 0o700);
        assert_eq!(fs.get_attr(doc_ino).unwrap().perm, 0o700);

        // chown to uid 2000, gid 3000
        let updated = fs.set_attr(doc_ino, None, Some(2000), Some(3000), None, None, None).unwrap();
        assert_eq!(updated.uid, 2000);
        assert_eq!(updated.gid, 3000);
        assert_eq!(fs.get_attr(doc_ino).unwrap().uid, 2000);
        assert_eq!(fs.get_attr(doc_ino).unwrap().gid, 3000);

        // perm is preserved
        assert_eq!(fs.get_attr(doc_ino).unwrap().perm, 0o700);
    }

    #[test]
    fn test_rename_to_sh_adds_executable_bit() {
        let fs = FsState::new(Some(1000), Some(1000));
        let doc_ino = fs.add_doc(1, "deploy", "d1", "echo hi", 1);
        assert_eq!(fs.get_attr(doc_ino).unwrap().perm, 0o644);

        // Rename to deploy.sh
        fs.rename_node(1, "deploy", 1, "deploy.sh").unwrap();
        assert_eq!(fs.get_attr(doc_ino).unwrap().perm, 0o755);
    }

    #[test]
    fn test_gitignore_matching() {
        let fs = FsState::new(Some(1000), Some(1000));

        let gi_content = "*.log\nbuild/\n!keep.log\n";
        let _gi_ino = fs.add_doc(1, ".gitignore", "gi_1", gi_content, 1);

        assert_eq!(fs.is_ignored(1, "test.log", false), true);
        assert_eq!(fs.is_ignored(1, "keep.log", false), false);
        assert_eq!(fs.is_ignored(1, "main.tex", false), false);
        assert_eq!(fs.is_ignored(1, ".gitignore", false), false);

        assert_eq!(fs.is_ignored(1, "build", true), true);

        // Subfolder under build/
        let build_ino = fs.add_folder(1, "build", "b1");
        assert_eq!(fs.is_ignored(build_ino, "output.pdf", false), true);
        assert_eq!(fs.is_ignored(build_ino, "nested", true), true);

        let test_log_ino = fs.add_doc(1, "test.log", "", "log data", 1);
        assert_eq!(fs.is_ino_ignored(test_log_ino), true);

        let main_tex_ino = fs.add_doc(1, "main.tex", "doc_main", "tex data", 1);
        assert_eq!(fs.is_ino_ignored(main_tex_ino), false);
    }

    #[test]
    fn test_dynamic_gitignore_update() {
        let fs = FsState::new(Some(1000), Some(1000));
        assert_eq!(fs.is_ignored(1, "cache.tmp", false), false);

        // Create empty .gitignore
        let gi_ino = fs.add_doc(1, ".gitignore", "gi_1", "", 1);
        assert_eq!(fs.is_ignored(1, "cache.tmp", false), false);

        // Write to .gitignore
        fs.write(gi_ino, 0, b"*.tmp\n").unwrap();
        assert_eq!(fs.is_ignored(1, "cache.tmp", false), true);
        assert_eq!(fs.is_ignored(1, "notes.txt", false), false);

        // Remove .gitignore
        fs.remove_node(1, ".gitignore");
        assert_eq!(fs.is_ignored(1, "cache.tmp", false), false);
    }

    #[test]
    fn test_nested_gitignore() {
        let fs = FsState::new(Some(1000), Some(1000));

        let sub_ino = fs.add_folder(1, "sub", "sub1");
        fs.add_doc(sub_ino, ".gitignore", "gi_sub", "secret.txt\n", 1);

        assert_eq!(fs.is_ignored(sub_ino, "secret.txt", false), true);
        assert_eq!(fs.is_ignored(1, "secret.txt", false), false);
    }

    #[test]
    fn test_local_id_ignored() {
        let fs = FsState::new(Some(1000), Some(1000));
        let local_ino = fs.add_doc(1, "local_file.txt", "", "local only", 1);
        assert_eq!(fs.is_ino_ignored(local_ino), true);
    }
}
