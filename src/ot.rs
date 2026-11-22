//! A small, self-contained port of the ShareJS / ot.js operation model used by
//! Overleaf's document-updater (`sharejs-text-ot`).
//!
//! An Overleaf plain-text document is a string with `\n` separators. An update
//! is an *operation*: a sequence of components
//!   * `retain(n)`  – keep the next n characters,
//!   * `insert(s)`  – insert string `s`,
//!   * `remove(n)`  – remove the next n characters,
//! that exactly consumes the base document (`base_len`) and produces a new
//! document of length `target_len`. On the wire (`{"op":[{"i":"x","p":3}]}`)
//! components are numbers (`3` = retain 3) or objects (`{"i":s}` insert,
//! `{"d":s}` delete).
//!
//! Text operations form a *transformable* pair: given two concurrent
//! operations A and B on the same document S, `transform_pair` yields A' and B'
//! with `apply(apply(S, A), B') == apply(apply(S, B), A')`. That is what makes
//! concurrent editing of the same document from the mount and from a browser
//! converge (OT, Jupiter/ShareJS flavour), including when both edit the same
//! sentence at the same time.
//!
//! For non-text (binary) files Overleaf has no OT type, so the mount uses a
//! three-way text merge / last-writer-wins strategy (see `merge3.rs`).

use std::collections::VecDeque;
use std::fmt;

/// A single component of an operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Component {
    /// Keep `n` characters.
    Retain(usize),
    /// Insert a string.
    Insert(String),
    /// Remove `n` characters.
    Remove(usize),
}

/// A ShareJS text operation.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct TextOp {
    pub base_len: usize,
    pub target_len: usize,
    pub components: Vec<Component>,
}

/// An operation sequence that does not fit the document it is applied to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtError(pub String);

impl fmt::Display for OtError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for OtError {}

impl TextOp {
    pub fn new() -> Self {
        Self::default()
    }

    /// Recompute `base_len`/`target_len` from the component list.
    ///
    /// `base_len` = characters consumed from the source document
    ///              (retains + removes),
    /// `target_len` = characters produced (retains + inserts).
    /// A remove consumes input and produces nothing, so it never contributes
    /// to `target_len`.
    fn recompute(&mut self) {
        let mut base = 0usize;
        let mut target = 0usize;
        for c in &self.components {
            match c {
                Component::Retain(n) => {
                    base += n;
                    target += n;
                }
                Component::Insert(s) => target += s.chars().count(),
                Component::Remove(n) => base += n,
            }
        }
        self.base_len = base;
        self.target_len = target;
    }

    pub fn check_len(&self) -> Result<(), OtError> {
        let mut copy = self.clone();
        copy.recompute();
        if copy.base_len != self.base_len || copy.target_len != self.target_len {
            return Err(OtError(format!(
                "operation length mismatch: declared base={} target={}, computed base={} target={} (components {:?})",
                self.base_len, self.target_len, copy.base_len, copy.target_len, self.components
            )));
        }
        Ok(())
    }

    /// Append a retain, merging runs.
    pub fn retain(&mut self, n: usize) {
        if n == 0 {
            return;
        }
        match self.components.last_mut() {
            Some(Component::Retain(m)) => *m += n,
            _ => self.components.push(Component::Retain(n)),
        }
        self.recompute();
    }

    /// Append an insert, merging runs.
    pub fn insert(&mut self, s: &str) {
        if s.is_empty() {
            return;
        }
        match self.components.last_mut() {
            Some(Component::Insert(prev)) => prev.push_str(s),
            _ => self.components.push(Component::Insert(s.to_string())),
        }
        self.recompute();
    }

    /// Append a remove, merging runs.
    pub fn remove(&mut self, n: usize) {
        if n == 0 {
            return;
        }
        match self.components.last_mut() {
            Some(Component::Remove(m)) => *m += n,
            _ => self.components.push(Component::Remove(n)),
        }
        self.recompute();
    }

    /// Normalise: drop trailing retains are kept (they matter for length), but
    /// adjacent components of the same kind are merged.
    pub fn normalise(&mut self) {
        let mut out: Vec<Component> = Vec::with_capacity(self.components.len());
        for c in self.components.drain(..) {
            match (out.last_mut(), c) {
                (Some(Component::Retain(a)), Component::Retain(b)) => *a += b,
                (Some(Component::Remove(a)), Component::Remove(b)) => *a += b,
                (Some(Component::Insert(a)), Component::Insert(b)) => a.push_str(&b),
                (_, c) => out.push(c),
            }
        }
        self.components = out;
        self.recompute();
    }

    /// True when the operation changes nothing.
    pub fn is_noop(&self) -> bool {
        self.components
            .iter()
            .all(|c| matches!(c, Component::Retain(_)))
    }

    /// (characters inserted, characters removed) — for change detection/logging.
    pub fn change_size(&self) -> (usize, usize) {
        let mut ins = 0;
        let mut del = 0;
        for c in &self.components {
            match c {
                Component::Insert(s) => ins += s.chars().count(),
                Component::Remove(n) => del += n,
                _ => {}
            }
        }
        (ins, del)
    }

    /// Apply to a document represented as lines (Overleaf's wire format).
    pub fn apply_to_lines(&self, lines: &[String]) -> Result<Vec<String>, OtError> {
        let doc = lines.join("\n");
        let out = self.apply(&doc)?;
        Ok(split_lines(&out))
    }

    /// Apply to an in-memory string.
    pub fn apply(&self, doc: &str) -> Result<String, OtError> {
        let chars: Vec<char> = doc.chars().collect();
        if chars.len() != self.base_len {
            return Err(OtError(format!(
                "apply: document length {} != operation base length {}",
                chars.len(),
                self.base_len
            )));
        }
        self.check_len()?;
        let mut out = String::with_capacity(self.target_len + 16);
        let mut pos = 0usize;
        for c in &self.components {
            match c {
                Component::Retain(n) => {
                    if pos + n > chars.len() {
                        return Err(OtError("apply: retain past end of document".into()));
                    }
                    out.extend(&chars[pos..pos + n]);
                    pos += n;
                }
                Component::Insert(s) => out.push_str(s),
                Component::Remove(n) => {
                    if pos + n > chars.len() {
                        return Err(OtError("apply: remove past end of document".into()));
                    }
                    pos += n;
                }
            }
        }
        if pos != chars.len() {
            return Err(OtError(format!(
                "apply: operation consumed {} of {} characters",
                pos,
                chars.len()
            )));
        }
        Ok(out)
    }

    /// Transform `a` against `b` (concurrent, same base document).
    ///
    /// Returns `(a', b')` with `apply(apply(S,a), b') == apply(apply(S,b), a')`.
    /// When both sides insert at the same position the `a` side goes first,
    /// which is the ot.js/Jupiter tie-break; both peers see the operations in
    /// the same order (document-updater orders by op version), so both reach
    /// the same document.
    pub fn transform_pair(a: &TextOp, b: &TextOp) -> Result<(TextOp, TextOp), OtError> {
        if a.base_len != b.base_len {
            return Err(OtError(format!(
                "transform: concurrent operations have base lengths {} and {}",
                a.base_len, b.base_len
            )));
        }
        a.check_len()?;
        b.check_len()?;
        let mut a2 = TextOp::new();
        let mut b2 = TextOp::new();
        let mut ca: VecDeque<Component> = a.components.iter().cloned().collect();
        let mut cb: VecDeque<Component> = b.components.iter().cloned().collect();
        let mut guard = 0usize;
        loop {
            guard += 1;
            if guard > 1_000_000 {
                return Err(OtError("transform did not terminate".into()));
            }
            match (ca.front().cloned(), cb.front().cloned()) {
                (None, None) => break,
                // a's insert lands before anything b does at this position
                (Some(Component::Insert(sa)), _) => {
                    b2.retain(sa.chars().count());
                    a2.insert(&sa);
                    ca.pop_front();
                }
                (_, Some(Component::Insert(sb))) => {
                    a2.retain(sb.chars().count());
                    b2.insert(&sb);
                    cb.pop_front();
                }
                (Some(Component::Retain(na)), Some(Component::Retain(nb))) => {
                    let n = na.min(nb);
                    a2.retain(n);
                    b2.retain(n);
                    step(&mut ca, n);
                    step(&mut cb, n);
                }
                (Some(Component::Remove(na)), Some(Component::Remove(nb))) => {
                    // Both delete the same characters: after either operation
                    // they are gone, so neither side has to delete them again.
                    let n = na.min(nb);
                    step(&mut ca, n);
                    step(&mut cb, n);
                }
                (Some(Component::Remove(na)), Some(Component::Retain(nb))) => {
                    // A deletes text B keeps: A' must still delete it after B
                    // has been applied; B' must not retain it (it is gone).
                    let n = na.min(nb);
                    a2.remove(n);
                    step(&mut ca, n);
                    step(&mut cb, n);
                }
                (Some(Component::Retain(na)), Some(Component::Remove(nb))) => {
                    // Mirror of the case above: B' deletes, A' drops the retain.
                    let n = na.min(nb);
                    b2.remove(n);
                    step(&mut ca, n);
                    step(&mut cb, n);
                }
                (None, Some(_)) | (Some(_), None) => {
                    // Both operations are complete (base_len checked above), so
                    // a side can only run out after the other side's remaining
                    // components are inserts — handled by the arms above.
                    return Err(OtError(
                        "transform: unbalanced operation lengths".into(),
                    ));
                }
            }
        }
        a2.check_len()?;
        b2.check_len()?;
        Ok((a2, b2))
    }

    /// Compose two consecutive operations:
    /// `apply(apply(S,a), b) == apply(S, compose(a,b))`.
    pub fn compose(a: &TextOp, b: &TextOp) -> Result<TextOp, OtError> {
        if a.target_len != b.base_len {
            return Err(OtError(format!(
                "compose: a.target_len {} != b.base_len {}",
                a.target_len, b.base_len
            )));
        }
        let mut out = TextOp::new();
        let mut ca: VecDeque<Component> = a.components.iter().cloned().collect();
        let mut cb: VecDeque<Component> = b.components.iter().cloned().collect();
        let mut guard = 0usize;
        loop {
            guard += 1;
            if guard > 1_000_000 {
                return Err(OtError("compose did not terminate".into()));
            }
            match (ca.front().cloned(), cb.front().cloned()) {
                (None, None) => break,
                // a deletes text: it never reaches b's output
                (Some(Component::Remove(n)), _) => {
                    out.remove(n);
                    ca.pop_front();
                }
                // b deletes text that a inserted
                (Some(Component::Insert(sa)), Some(Component::Remove(nb))) => {
                    let len = sa.chars().count();
                    let taken = nb.min(len);
                    let rest: String = sa.chars().skip(taken).collect();
                    if !rest.is_empty() {
                        out.insert(&rest);
                    }
                    ca.pop_front();
                    if taken == len {
                        // fully consumed by b's delete; shorten its remove
                        if nb > len {
                            cb.pop_front();
                            cb.push_front(Component::Remove(nb - len));
                        } else {
                            cb.pop_front();
                        }
                    } else {
                        cb.pop_front();
                    }
                }
                // b inserts: lands at the current position of a's output
                (_, Some(Component::Insert(sb))) => {
                    out.insert(&sb);
                    cb.pop_front();
                }
                // a inserts, b keeps it
                (Some(Component::Insert(sa)), _) => {
                    let len = sa.chars().count();
                    out.insert(&sa);
                    ca.pop_front();
                    step(&mut cb, len);
                }
                (Some(Component::Retain(na)), Some(Component::Retain(nb))) => {
                    let n = na.min(nb);
                    out.retain(n);
                    step(&mut ca, n);
                    step(&mut cb, n);
                }
                (Some(Component::Retain(na)), Some(Component::Remove(nb))) => {
                    let n = na.min(nb);
                    out.remove(n);
                    step(&mut ca, n);
                    step(&mut cb, n);
                }
                (None, Some(_)) | (Some(_), None) => {
                    return Err(OtError("compose: unbalanced operation lengths".into()));
                }
            }
        }
        out.check_len()?;
        Ok(out)
    }
}

/// Consume `n` characters from the component at the front of `q`.
///
/// `n` is always `<=` the front component's length when the kinds match; a
/// `Remove` against an `Insert` front (b deleting newly inserted text) pops the
/// whole insert, which is what the callers above intend.
fn step(q: &mut VecDeque<Component>, n: usize) {
    let Some(front) = q.front_mut() else { return };
    match front {
        Component::Retain(x) | Component::Remove(x) => {
            if n >= *x {
                q.pop_front();
            } else {
                *x -= n;
            }
        }
        Component::Insert(s) => {
            let len = s.chars().count();
            if n >= len {
                q.pop_front();
            } else if n > 0 {
                *s = s.chars().skip(n).collect();
            }
        }
    }
}

/// Split a document string into Overleaf's line array.
pub fn split_lines(doc: &str) -> Vec<String> {
    doc.split('\n').map(|s| s.to_string()).collect()
}

/// Compute the operation turning `old` into `new` by trimming the common
/// prefix and suffix. Overleaf's frontend does the same with `fast-diff`; for
/// whole-file uploads this is optimal for any contiguous change (the common
/// case) and never worse than delete-all/insert-all.
pub fn diff(old: &str, new: &str) -> TextOp {
    let a: Vec<char> = old.chars().collect();
    let b: Vec<char> = new.chars().collect();
    let mut op = TextOp::new();
    if a == b {
        op.retain(a.len());
        return op;
    }
    let mut prefix = 0usize;
    let max_prefix = a.len().min(b.len());
    while prefix < max_prefix && a[prefix] == b[prefix] {
        prefix += 1;
    }
    let mut suffix = 0usize;
    let max_suffix = (a.len() - prefix).min(b.len() - prefix);
    while suffix < max_suffix && a[a.len() - 1 - suffix] == b[b.len() - 1 - suffix] {
        suffix += 1;
    }
    // Equal characters on both sides, so the changed regions are the middles.
    let old_mid = a.len() - prefix - suffix;
    let new_mid = b.len() - prefix - suffix;
    op.retain(prefix);
    if new_mid > 0 {
        let ins: String = b[prefix..b.len() - suffix].iter().collect();
        op.insert(&ins);
    }
    op.remove(old_mid);
    op.retain(suffix);
    op
}

/// Build an operation that replaces the whole document.
pub fn replace_all(old_len: usize, new: &str) -> TextOp {
    let mut op = TextOp::new();
    op.remove(old_len);
    op.insert(new);
    op
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_roundtrip() {
        let cases = [
            ("hello world", "hello brave world"),
            ("abc\n", "abc"),
            ("", "x"),
            ("x", ""),
            ("a\nb\nc", "a\nB\nc"),
            ("same", "same"),
            ("日本語のテキスト", "日本語の新しいテキスト"),
            ("abcdef", "abcXYZdef"),
        ];
        for (old, new) in cases {
            let op = diff(old, new);
            assert_eq!(op.apply(old).unwrap(), new, "diff {:?} -> {:?}", old, new);
        }
    }

    #[test]
    fn diff_uses_minimal_prefix_suffix() {
        let op = diff("abcXYZdef", "abcdef");
        assert_eq!(op.apply("abcXYZdef").unwrap(), "abcdef");
        assert_eq!(op.change_size(), (0, 3));
    }

    #[test]
    fn transform_concurrent_inserts_converge() {
        let base = "abc";
        let mut a = TextOp::new();
        a.retain(1);
        a.insert("X");
        a.retain(2);
        let mut b = TextOp::new();
        b.retain(2);
        b.insert("Y");
        b.retain(1);

        let (a2, b2) = TextOp::transform_pair(&a, &b).unwrap();
        let ca = b2.apply(&a.apply(base).unwrap()).unwrap();
        let cb = a2.apply(&b.apply(base).unwrap()).unwrap();
        assert_eq!(ca, cb, "concurrent inserts must converge");
        assert_eq!(ca, "aXbYc");
    }

    #[test]
    fn transform_insert_vs_delete_same_spot() {
        let base = "hello world";
        let mut a = TextOp::new();
        a.retain(5);
        a.remove(6);
        let mut b = TextOp::new();
        b.retain(11);
        b.insert("!");

        let (a2, b2) = TextOp::transform_pair(&a, &b).unwrap();
        let ca = b2.apply(&a.apply(base).unwrap()).unwrap();
        let cb = a2.apply(&b.apply(base).unwrap()).unwrap();
        assert_eq!(ca, cb);
        assert_eq!(ca, "hello!");
    }

    #[test]
    fn transform_same_position_inserts_are_ordered() {
        let base = "ab";
        let mut a = TextOp::new();
        a.insert("[A]");
        a.retain(2);
        let mut b = TextOp::new();
        b.insert("[B]");
        b.retain(2);
        let (a2, b2) = TextOp::transform_pair(&a, &b).unwrap();
        let ca = b2.apply(&a.apply(base).unwrap()).unwrap();
        let cb = a2.apply(&b.apply(base).unwrap()).unwrap();
        assert_eq!(ca, cb);
        assert_eq!(ca, "[A][B]ab");
    }

    #[test]
    fn transform_replace_whole_document_vs_edit() {
        let base = "line1\nline2";
        let a = replace_all(base.chars().count(), "totally different");
        let mut b = TextOp::new();
        b.retain(base.chars().count());
        b.insert("\nline3");
        let (a2, b2) = TextOp::transform_pair(&a, &b).unwrap();
        let ca = b2.apply(&a.apply(base).unwrap()).unwrap();
        let cb = a2.apply(&b.apply(base).unwrap()).unwrap();
        assert_eq!(ca, cb);
    }

    #[test]
    fn transform_two_people_editing_the_same_sentence() {
        // Both fix the same typo differently: OT must not corrupt the doc and
        // both outcomes must agree.
        let base = "The quick brown fox.";
        let mut a = TextOp::new();
        a.retain(10);
        a.remove(5);
        a.insert("red");
        a.retain(5);
        let mut b = TextOp::new();
        b.retain(10);
        b.remove(5);
        b.insert("blue");
        b.retain(5);
        let (a2, b2) = TextOp::transform_pair(&a, &b).unwrap();
        let ca = b2.apply(&a.apply(base).unwrap()).unwrap();
        let cb = a2.apply(&b.apply(base).unwrap()).unwrap();
        assert_eq!(ca, cb);
        assert!(ca.contains("red") || ca.contains("blue"));
    }

    #[test]
    fn compose_delete_then_insert() {
        let base = "abcdef";
        let mut a = TextOp::new();
        a.retain(2);
        a.remove(2); // "abef"
        a.retain(2);
        let mut b = TextOp::new();
        b.retain(2);
        b.insert("XY"); // "abXYef"
        b.retain(2);
        let composed = TextOp::compose(&a, &b).unwrap();
        assert_eq!(
            composed.apply(base).unwrap(),
            b.apply(&a.apply(base).unwrap()).unwrap()
        );
    }

    #[test]
    fn lines_roundtrip() {
        let lines = vec!["hello".to_string(), "world".to_string()];
        let mut op = TextOp::new();
        op.retain(11);
        op.insert("\n!");
        let out = op.apply_to_lines(&lines).unwrap();
        assert_eq!(out, vec!["hello", "world", "!"]);
    }

    #[test]
    fn fuzz_transform_converges_for_random_concurrent_edits() {
        use rand::{rngs::StdRng, Rng, SeedableRng};
        let mut rng = StdRng::seed_from_u64(0xC0FFEE);
        let alphabet = ['a', 'b', 'c', '\n', ' ', 'é'];
        for round in 0..2000 {
            let base_len = rng.gen_range(0..30);
            let base: String = (0..base_len)
                .map(|_| alphabet[rng.gen_range(0..alphabet.len())])
                .collect();
            let mut edits = Vec::new();
            for _ in 0..2 {
                let mut s: Vec<char> = base.chars().collect();
                for _ in 0..rng.gen_range(1..5) {
                    if !s.is_empty() && rng.gen_bool(0.5) {
                        let p = rng.gen_range(0..s.len());
                        s.remove(p);
                    } else {
                        let p = rng.gen_range(0..=s.len());
                        s.insert(p, alphabet[rng.gen_range(0..alphabet.len())]);
                    }
                }
                let new: String = s.into_iter().collect();
                edits.push(diff(&base, &new));
            }
            let (a, b) = (&edits[0], &edits[1]);
            let (a2, b2) = TextOp::transform_pair(a, b)
                .unwrap_or_else(|e| panic!("round {round}: transform failed: {e}"));
            let ca = a.apply(&base).and_then(|d| b2.apply(&d));
            let cb = b.apply(&base).and_then(|d| a2.apply(&d));
            match (ca, cb) {
                (Ok(x), Ok(y)) => assert_eq!(x, y, "round {round}: divergence base={base:?}"),
                (Err(e), _) => panic!("round {round}: apply failed: {e}"),
                (_, Err(e)) => panic!("round {round}: apply failed: {e}"),
            }
        }
    }
}
