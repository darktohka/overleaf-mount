//! Filesystem operations and attribute plumbing shared by the FUSE layer and
//! the synchroniser.
//!
//! The FUSE callbacks never block on the network: they mutate the in-memory
//! image and hand *intents* to the sync engine, which owns the Overleaf
//! connection. That split is what allows the mount to stay responsive while a
//! write is in flight and keeps OT state consistent (the engine serialises
//! updates per document).

use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::types::{default_mode_for, Attrs, NodeKind, ROOT_INO};

/// An operation the filesystem wants the synchroniser to perform.
#[derive(Debug, Clone)]
pub enum Intent {
    /// Whole new content for a text document (inode + doc id).
    WriteDoc {
        ino: u64,
        doc_id: String,
        data: Vec<u8>,
        version: i64,
    },
    /// Create a doc file from a local write (`fs::write` creates it).
    CreateDoc {
        parent: u64,
        name: String,
        data: Vec<u8>,
    },
    CreateDir {
        parent: u64,
        name: String,
    },
    Rename {
        ino: u64,
        new_parent: u64,
        new_name: String,
    },
    Remove {
        ino: u64,
    },
}

/// A request to flush/close a file handle.
#[derive(Debug, Clone)]
pub struct Flush {
    pub ino: u64,
    pub path: String,
}

pub fn to_system_time(secs: i64) -> SystemTime {
    if secs <= 0 {
        SystemTime::now()
    } else {
        UNIX_EPOCH + Duration::from_secs(secs as u64)
    }
}

pub fn system_time_secs(t: SystemTime) -> (i64, u32) {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => (d.as_secs() as i64, d.subsec_nanos()),
        Err(_) => (0, 0),
    }
}

/// Build a `fuser::FileAttr` from our in-memory attributes.
pub fn file_attr(ino: u64, attrs: &Attrs, size: u64) -> fuser::FileAttr {
    let (mtime, _mtime_nsec) = system_time_secs(attrs.mtime);
    let (ctime, _ctime_nsec) = system_time_secs(attrs.ctime);
    let (atime, _atime_nsec) = system_time_secs(attrs.atime);
    fuser::FileAttr {
        ino,
        size,
        blocks: size.div_ceil(512),
        atime: to_system_time(atime),
        mtime: to_system_time(mtime),
        ctime: to_system_time(ctime),
        crtime: to_system_time(ctime),
        kind: if crate::types::is_dir_mode(attrs.mode) {
            fuser::FileType::Directory
        } else {
            fuser::FileType::RegularFile
        },
        perm: crate::types::perm_bits(attrs.mode) as u16,
        nlink: attrs.nlink,
        uid: attrs.uid,
        gid: attrs.gid,
        rdev: 0,
        flags: 0,
        blksize: 4096,
    }
}

/// Attributes for a brand new node (respects the mount's uid/gid defaults and
/// makes `*.sh` executable).
pub fn new_attrs(default_uid: u32, default_gid: u32, name: &str, is_dir: bool) -> Attrs {
    let now = SystemTime::now();
    Attrs {
        mode: crate::types::full_mode(is_dir, if is_dir { 0o755 } else { default_mode_for(name) }),
        uid: default_uid,
        gid: default_gid,
        mtime: now,
        ctime: now,
        atime: now,
        nlink: if is_dir { 2 } else { 1 },
    }
}

/// Encode a directory entry name for `readdir` (skipping invalid UTF-8 names is
/// fine for Overleaf projects, which are always UTF-8).
pub fn decode_name(name: &[u8]) -> Option<String> {
    std::str::from_utf8(name).ok().map(|s| s.to_string())
}

/// True for the doc types Overleaf treats as text (they get OT updates).
pub fn looks_like_text(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    const TEXT_EXT: &[&str] = &[
        "tex", "bib", "cls", "sty", "bst", "bbx", "cbx", "lbx", "def", "clo", "ltx", "dtx", "ins",
        "txt", "md", "markdown", "rst", "org", "json", "yml", "yaml", "toml", "ini", "cfg",
        "conf", "csv", "tsv", "js", "mjs", "cjs", "ts", "tsx", "jsx", "py", "rb", "sh", "bash",
        "zsh", "fish", "ps1", "bat", "c", "h", "cc", "cpp", "cxx", "hpp", "hh", "java", "kt",
        "go", "rs", "php", "pl", "lua", "sql", "r", "m", "html", "htm", "css", "scss", "sass",
        "less", "xml", "svg", "gitignore", "gitattributes", "editorconfig", "env", "makefile",
        "mk", "dockerfile", "lp", "sage", "asy", "pgf", "tikz",
    ];
    // Only *known* text extensions take the optimistic document path. Names
    // without an extension need their content to decide (a Makefile is text,
    // `.git/index` or a loose git object is not), so they are classified on
    // their first flush instead.
    TEXT_EXT.iter().any(|e| lower.ends_with(&format!(".{e}")))
}

/// Format a `NodeKind` for logs.
pub fn kind_str(k: &NodeKind) -> &'static str {
    match k {
        NodeKind::Dir => "dir",
        NodeKind::Doc { .. } => "doc",
        NodeKind::File { .. } => "file",
        NodeKind::LocalFile => "local",
    }
}

/// Root inode constant re-export for the FUSE layer.
pub const ROOT: u64 = ROOT_INO;

/// Bump the generation counter and return it (helper for the FUSE layer).
pub fn touch(fs: &crate::types::Fs) -> u64 {
    fs.generation.fetch_add(1, Ordering::SeqCst) + 1
}
