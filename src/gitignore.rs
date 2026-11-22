//! `.gitignore` handling.
//!
//! Uses the `ignore` crate (the one ripgrep is built on): it parses the full
//! gitignore syntax (globs, `!` negation, `**`, directory-only `dir/`, `/`
//! anchoring) and gives us a fast matcher. Files that match are kept in memory
//! only — they are created on the mount as ordinary files, never sent to
//! Overleaf, and they are *not* overwritten by remote changes.

use std::path::Path;

use ignore::gitignore::{Gitignore, GitignoreBuilder};

/// A set of `.gitignore` files for one Overleaf project.
///
/// Every folder in the project may carry its own `.gitignore`; like git, rules
/// from deeper files win. We keep one matcher per directory prefix.
#[derive(Debug)]
pub struct Ignorer {
    root: Gitignore,
    /// directory path (relative to project root) -> matcher for that dir
    nested: Vec<(String, Gitignore)>,
    /// debugging aid: last pattern that matched a path
    pub last_match: parking_lot::Mutex<Option<String>>,
}

impl Ignorer {
    pub fn new() -> Self {
        Self {
            root: Gitignore::empty(),
            nested: Vec::new(),
            last_match: parking_lot::Mutex::new(None),
        }
    }

    /// Load the root `.gitignore` contents (may be empty).
    pub fn with_root_rules(contents: &str) -> Self {
        let mut b = GitignoreBuilder::new("");
        for line in contents.lines() {
            let _ = b.add_line(None, line);
        }
        Self {
            root: b.build().unwrap_or_else(|_| Gitignore::empty()),
            nested: Vec::new(),
            last_match: parking_lot::Mutex::new(None),
        }
    }

    /// Read `.gitignore` from a directory on the mount (if present).
    pub fn load_from_dir(root: &Path) -> Self {
        let gi = root.join(".gitignore");
        match std::fs::read_to_string(&gi) {
            Ok(contents) => Self::with_root_rules(&contents),
            Err(_) => Self::new(),
        }
    }

    /// Add/refresh the matcher for a nested `.gitignore` (path relative to root).
    pub fn add_nested(&mut self, dir: &str, contents: &str) {
        let mut b = GitignoreBuilder::new(dir);
        for line in contents.lines() {
            let _ = b.add_line(None, line);
        }
        if let Ok(gi) = b.build() {
            self.nested.retain(|(d, _)| d != dir);
            self.nested.push((dir.to_string(), gi));
            // deepest first
            self.nested.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
        }
    }

    /// Does `path` (relative to the project root) match any ignore rule?
    ///
    /// A path is ignored when the deepest matching rule (root first, then
    /// nested from shallow to deep) says so. Negations (`!file`) are handled by
    /// the `ignore` crate itself per matcher; across matchers we follow git:
    /// the deepest file that has an opinion wins.
    pub fn is_ignored(&self, path: &str) -> bool {
        // Git treats a path as ignored when the path itself matches, or when
        // any ancestor directory matches a `dir/` rule. `ignore`'s matcher is
        // per-path, so walk the ancestors ourselves.
        let mut candidates: Vec<(String, bool)> = Vec::new();
        candidates.push((path.to_string(), false));
        let mut acc = String::new();
        let parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
        for part in parts.iter().take(parts.len().saturating_sub(1)) {
            if !acc.is_empty() {
                acc.push('/');
            }
            acc.push_str(part);
            candidates.push((acc.clone(), true));
        }

        let mut decision: Option<(usize, bool)> = None;
        let mut reason: Option<String> = None;

        // Root .gitignore first, then nested ones (deepest wins).
        let mut matchers: Vec<(usize, &Gitignore)> = vec![(0, &self.root)];
        for (dir, gi) in &self.nested {
            matchers.push((dir.len() + 1, gi));
        }
        matchers.sort_by_key(|(depth, _)| *depth);

        for (depth, gi) in matchers {
            let dir = if depth == 0 {
                String::new()
            } else {
                self.nested
                    .iter()
                    .find(|(_, g)| std::ptr::eq(g, gi))
                    .map(|(d, _)| d.clone())
                    .unwrap_or_default()
            };
            for (cand, is_dir) in &candidates {
                let rel = match &dir {
                    d if d.is_empty() => cand.clone(),
                    d => match cand.strip_prefix(d.as_str()) {
                        Some(rest) => rest.trim_start_matches('/').to_string(),
                        None => continue,
                    },
                };
                if rel.is_empty() {
                    continue;
                }
                let m = gi.matched(&rel, *is_dir);
                if m.is_ignore() {
                    decision = Some((depth, true));
                    reason = Some(format!("{path}: '{rel}' ignored by {dir}/.gitignore"));
                } else if m.is_whitelist() {
                    decision = Some((depth, false));
                    reason = Some(format!("{path}: '{rel}' re-included by {dir}/.gitignore"));
                }
            }
        }
        if let Some(r) = reason {
            *self.last_match.lock() = Some(r);
        }
        decision.map(|(_, ignore)| ignore).unwrap_or(false)
    }

    /// Convenience: should a path be synchronised (i.e. not ignored)?
    pub fn should_sync(&self, path: &str) -> bool {
        !self.is_ignored(path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_rules() {
        let ig = Ignorer::with_root_rules(
            "*.aux\n*.log\nbuild/\n!keep.log\nsections/private.tex\n**/tmp\n",
        );
        assert!(ig.is_ignored("main.aux"));
        assert!(ig.is_ignored("chapters/intro.aux"));
        assert!(ig.is_ignored("build/output.tex"));
        assert!(!ig.is_ignored("keep.log"), "negation must win");
        assert!(ig.is_ignored("sections/private.tex"));
        assert!(!ig.is_ignored("sections/public.tex"));
        assert!(ig.is_ignored("a/b/tmp"));
        assert!(!ig.is_ignored("main.tex"));
    }

    #[test]
    fn nested_rules_override_root() {
        let mut ig = Ignorer::with_root_rules("*.tex\n");
        ig.add_nested("docs", "notes.md\n!*.tex\n");
        assert!(ig.is_ignored("main.tex"));
        assert!(!ig.is_ignored("docs/chapter.tex"), "deep !*.tex wins");
        assert!(ig.is_ignored("docs/notes.md"));
    }

    #[test]
    fn comments_and_blank_lines_are_ignored() {
        let ig = Ignorer::with_root_rules("# comment\n\n*.bak\n");
        assert!(ig.is_ignored("x.bak"));
        assert!(!ig.is_ignored("comment"));
    }
}
