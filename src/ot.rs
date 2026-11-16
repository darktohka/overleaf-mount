use serde::{Deserialize, Serialize};
use similar::{Algorithm, DiffOp};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InsertComponent {
    pub p: usize,
    pub i: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeleteComponent {
    pub p: usize,
    pub d: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum Component {
    Insert { p: usize, i: String },
    Delete { p: usize, d: String },
}

impl Component {
    pub fn position(&self) -> usize {
        match self {
            Component::Insert { p, .. } => *p,
            Component::Delete { p, .. } => *p,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Left,
    Right,
}

#[derive(Debug)]
pub enum OtError {
    DeleteMismatch {
        pos: usize,
        expected: String,
        actual: String,
    },
    OutOfBounds(usize, usize),
}

impl std::fmt::Display for OtError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OtError::DeleteMismatch { pos, expected, actual } => {
                write!(f, "Delete text mismatch at position {pos}: expected {expected:?}, found {actual:?}")
            }
            OtError::OutOfBounds(pos, len) => {
                write!(f, "Position {pos} out of bounds for text length {len}")
            }
        }
    }
}

impl std::error::Error for OtError {}

/// Apply a sequence of ShareJS text operations to snapshot string.
pub fn apply(snapshot: &str, ops: &[Component]) -> Result<String, OtError> {
    let mut current = snapshot.to_string();
    for op in ops {
        match op {
            Component::Insert { p, i } => {
                if *p > current.chars().count() {
                    return Err(OtError::OutOfBounds(*p, current.chars().count()));
                }
                let byte_offset = char_to_byte_index(&current, *p);
                current.insert_str(byte_offset, i);
            }
            Component::Delete { p, d } => {
                let char_count = current.chars().count();
                let del_char_count = d.chars().count();
                if *p + del_char_count > char_count {
                    return Err(OtError::OutOfBounds(*p + del_char_count, char_count));
                }
                let start_byte = char_to_byte_index(&current, *p);
                let end_byte = char_to_byte_index(&current, *p + del_char_count);
                let actual = &current[start_byte..end_byte];
                if actual != d {
                    return Err(OtError::DeleteMismatch {
                        pos: *p,
                        expected: d.clone(),
                        actual: actual.to_string(),
                    });
                }
                current.drain(start_byte..end_byte);
            }
        }
    }
    Ok(current)
}

/// Fuzzy apply sequence of ShareJS text operations to snapshot string.
/// If exact position matching fails, attempts substring matching for replaces/deletes.
pub fn apply_fuzzy(snapshot: &str, ops: &[Component]) -> Result<String, OtError> {
    if let Ok(res) = apply(snapshot, ops) {
        return Ok(res);
    }

    let mut current = snapshot.to_string();
    let mut i = 0;
    while i < ops.len() {
        if i + 1 < ops.len() {
            if let (Component::Delete { p: del_p, d }, Component::Insert { p: ins_p, i: ins_str }) = (&ops[i], &ops[i + 1]) {
                if del_p == ins_p {
                    if let Some(pos) = current.find(d) {
                        current.replace_range(pos..pos + d.len(), ins_str);
                        i += 2;
                        continue;
                    }
                }
            }
        }

        match &ops[i] {
            Component::Insert { p, i: ins_str } => {
                let char_count = current.chars().count();
                let actual_p = std::cmp::min(*p, char_count);
                let byte_offset = char_to_byte_index(&current, actual_p);
                current.insert_str(byte_offset, ins_str);
            }
            Component::Delete { p: _, d } => {
                if let Some(pos) = current.find(d) {
                    current.replace_range(pos..pos + d.len(), "");
                }
            }
        }
        i += 1;
    }

    Ok(current)
}

/// Convert character index to byte index in UTF-8 string.
fn char_to_byte_index(s: &str, char_idx: usize) -> usize {
    s.char_indices()
        .nth(char_idx)
        .map(|(byte_idx, _)| byte_idx)
        .unwrap_or(s.len())
}

/// Compute ShareJS OT components to transform `before` into `after`.
pub fn diff(before: &str, after: &str) -> Vec<Component> {
    if before == after {
        return Vec::new();
    }
    let before_chars: Vec<char> = before.chars().collect();
    let after_chars: Vec<char> = after.chars().collect();

    let diff_ops = similar::capture_diff_slices(
        Algorithm::Myers,
        &before_chars,
        &after_chars,
    );

    let mut ops = Vec::new();
    let mut cursor = 0;

    for op in diff_ops {
        match op {
            DiffOp::Equal { len, .. } => {
                cursor += len;
            }
            DiffOp::Delete { old_index, old_len, .. } => {
                let deleted_str: String = before_chars[old_index..old_index + old_len].iter().collect();
                ops.push(Component::Delete {
                    p: cursor,
                    d: deleted_str,
                });
                // In ShareJS sequential execution, after deletion, the following characters shift down to cursor.
            }
            DiffOp::Insert { new_index, new_len, .. } => {
                let inserted_str: String = after_chars[new_index..new_index + new_len].iter().collect();
                ops.push(Component::Insert {
                    p: cursor,
                    i: inserted_str,
                });
                cursor += new_len;
            }
            DiffOp::Replace { old_index, old_len, new_index, new_len } => {
                let deleted_str: String = before_chars[old_index..old_index + old_len].iter().collect();
                ops.push(Component::Delete {
                    p: cursor,
                    d: deleted_str,
                });
                let inserted_str: String = after_chars[new_index..new_index + new_len].iter().collect();
                ops.push(Component::Insert {
                    p: cursor,
                    i: inserted_str,
                });
                cursor += new_len;
            }
        }
    }

    ops
}

/// Transform position `pos` against an operation component.
pub fn transform_position(pos: usize, c: &Component, insert_after: bool) -> usize {
    match c {
        Component::Insert { p, i } => {
            let i_len = i.chars().count();
            if *p < pos || (*p == pos && insert_after) {
                pos + i_len
            } else {
                pos
            }
        }
        Component::Delete { p, d } => {
            let d_len = d.chars().count();
            if pos <= *p {
                pos
            } else if pos <= *p + d_len {
                *p
            } else {
                pos - d_len
            }
        }
    }
}

/// Transform single component `c` against `other`, appending result to `dest`.
pub fn transform_component(dest: &mut Vec<Component>, c: &Component, other: &Component, side: Side) {
    let insert_after = side == Side::Right;
    match c {
        Component::Insert { p, i } => {
            let new_p = transform_position(*p, other, insert_after);
            dest.push(Component::Insert {
                p: new_p,
                i: i.clone(),
            });
        }
        Component::Delete { p, d } => {
            let d_len = d.chars().count();
            match other {
                Component::Insert { p: other_p, i: other_i } => {
                    let other_i_len = other_i.chars().count();
                    let d_chars: Vec<char> = d.chars().collect();
                    if *p < *other_p && *other_p < *p + d_len {
                        let split = *other_p - *p;
                        let s1: String = d_chars[..split].iter().collect();
                        let s2: String = d_chars[split..].iter().collect();
                        dest.push(Component::Delete { p: *p, d: s1 });
                        dest.push(Component::Delete { p: *p + other_i_len, d: s2 });
                    } else if *other_p <= *p {
                        dest.push(Component::Delete {
                            p: *p + other_i_len,
                            d: d.clone(),
                        });
                    } else {
                        dest.push(Component::Delete {
                            p: *p,
                            d: d.clone(),
                        });
                    }
                }
                Component::Delete { p: other_p, d: other_d } => {
                    let other_d_len = other_d.chars().count();
                    if *p >= *other_p + other_d_len {
                        dest.push(Component::Delete {
                            p: *p - other_d_len,
                            d: d.clone(),
                        });
                    } else if *p + d_len <= *other_p {
                        dest.push(Component::Delete {
                            p: *p,
                            d: d.clone(),
                        });
                    } else {
                        // Overlapping deletes
                        let d_chars: Vec<char> = d.chars().collect();
                        let mut new_d = String::new();
                        if *p < *other_p {
                            let part: String = d_chars[..*other_p - *p].iter().collect();
                            new_d.push_str(&part);
                        }
                        if *p + d_len > *other_p + other_d_len {
                            let start = *other_p + other_d_len - *p;
                            let part: String = d_chars[start..].iter().collect();
                            new_d.push_str(&part);
                        }
                        if !new_d.is_empty() {
                            let new_p = transform_position(*p, other, false);
                            dest.push(Component::Delete {
                                p: new_p,
                                d: new_d,
                            });
                        }
                    }
                }
            }
        }
    }
}

/// Transform list of components `op` by list of components `other_op`.
pub fn transform(op: &[Component], other_op: &[Component], side: Side) -> Vec<Component> {
    let mut result = op.to_vec();
    for other in other_op {
        let mut new_result = Vec::new();
        for c in &result {
            transform_component(&mut new_result, c, other, side);
        }
        result = new_result;
    }
    result
}

/// Split text into units (sentences or lines) for 3-way alignment.
pub fn split_sentences_or_lines(text: &str) -> Vec<String> {
    let mut result = Vec::new();
    let chars: Vec<char> = text.chars().collect();
    let mut start = 0;
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];
        let is_terminator = (c == '.' || c == '!' || c == '?')
            && (i + 1 == chars.len() || chars[i + 1].is_whitespace());
        let is_newline = c == '\n';

        if is_terminator || is_newline {
            let mut end = i + 1;
            while end < chars.len() && (chars[end] == ' ' || chars[end] == '\t') {
                end += 1;
            }
            let s_text: String = chars[start..end].iter().collect();
            result.push(s_text);
            start = end;
            i = end;
        } else {
            i += 1;
        }
    }

    if start < chars.len() {
        let s_text: String = chars[start..].iter().collect();
        result.push(s_text);
    }

    result
}

/// Split text into sentences/segments with their character start/end ranges.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SentenceSpan {
    pub start: usize,
    pub end: usize,
    pub text: String,
}

pub fn split_sentences(text: &str) -> Vec<SentenceSpan> {
    let mut spans = Vec::new();
    let chars: Vec<char> = text.chars().collect();
    let mut start = 0;
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];
        let is_terminator = (c == '.' || c == '!' || c == '?')
            && (i + 1 == chars.len() || chars[i + 1].is_whitespace());
        let is_newline_break = c == '\n' && (i + 1 < chars.len() && chars[i + 1] == '\n');

        if is_terminator || is_newline_break {
            let mut end = i + 1;
            while end < chars.len() && chars[end].is_whitespace() {
                end += 1;
            }
            let s_text: String = chars[start..end].iter().collect();
            spans.push(SentenceSpan {
                start,
                end,
                text: s_text,
            });
            start = end;
            i = end;
        } else {
            i += 1;
        }
    }

    if start < chars.len() {
        let s_text: String = chars[start..].iter().collect();
        spans.push(SentenceSpan {
            start,
            end: chars.len(),
            text: s_text,
        });
    }

    spans
}

/// Reconciliation outcome indicating merged text, whether a conflict occurred, and corrective ops.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationResult {
    pub final_text: String,
    pub has_conflict: bool,
    pub corrective_ops: Vec<Component>,
}

/// Robust Conflict Resolution:
/// "If both people edit the same part / sentence at the same time, and there is no way to tell which is the authoritative, then use our version as authoritative."
///
/// Parameters:
/// - `base_text`: the common ancestor document text before concurrent edits.
/// - `our_text`: our locally edited document text (authoritative for our changes).
/// - `remote_text`: the document text produced by remote collaborators.
///
/// Returns:
/// - `ReconciliationResult`: containing merged_text, has_conflict flag, and corrective_ops (if conflict occurred).
pub fn reconcile_conflict(
    base_text: &str,
    our_text: &str,
    remote_text: &str,
) -> ReconciliationResult {
    if our_text == remote_text {
        return ReconciliationResult {
            final_text: our_text.to_string(),
            has_conflict: false,
            corrective_ops: Vec::new(),
        };
    }
    if our_text == base_text {
        return ReconciliationResult {
            final_text: remote_text.to_string(),
            has_conflict: false,
            corrective_ops: Vec::new(),
        };
    }
    if remote_text == base_text {
        return ReconciliationResult {
            final_text: our_text.to_string(),
            has_conflict: false,
            corrective_ops: Vec::new(),
        };
    }

    let base_units = split_sentences_or_lines(base_text);
    let our_units = split_sentences_or_lines(our_text);
    let remote_units = split_sentences_or_lines(remote_text);

    let base_refs: Vec<&str> = base_units.iter().map(|s| s.as_str()).collect();
    let our_refs: Vec<&str> = our_units.iter().map(|s| s.as_str()).collect();
    let remote_refs: Vec<&str> = remote_units.iter().map(|s| s.as_str()).collect();

    let diff_our = similar::capture_diff_slices(Algorithm::Myers, &base_refs, &our_refs);
    let diff_remote = similar::capture_diff_slices(Algorithm::Myers, &base_refs, &remote_refs);

    use std::collections::HashMap;
    let mut our_modified_base: HashMap<usize, Vec<String>> = HashMap::new();
    let mut our_inserted_after: HashMap<usize, Vec<String>> = HashMap::new();

    for op in diff_our {
        match op {
            DiffOp::Equal { .. } => {}
            DiffOp::Delete { old_index, old_len, .. } => {
                for i in old_index..old_index + old_len {
                    our_modified_base.insert(i, Vec::new());
                }
            }
            DiffOp::Insert { old_index, new_index, new_len } => {
                let inserted = our_units[new_index..new_index + new_len].to_vec();
                our_inserted_after.insert(old_index, inserted);
            }
            DiffOp::Replace { old_index, old_len, new_index, new_len } => {
                let replacements = our_units[new_index..new_index + new_len].to_vec();
                for i in old_index..old_index + old_len {
                    if i == old_index {
                        our_modified_base.insert(i, replacements.clone());
                    } else {
                        our_modified_base.insert(i, Vec::new());
                    }
                }
            }
        }
    }

    let mut remote_modified_base: HashMap<usize, Vec<String>> = HashMap::new();
    let mut remote_inserted_after: HashMap<usize, Vec<String>> = HashMap::new();

    for op in diff_remote {
        match op {
            DiffOp::Equal { .. } => {}
            DiffOp::Delete { old_index, old_len, .. } => {
                for i in old_index..old_index + old_len {
                    remote_modified_base.insert(i, Vec::new());
                }
            }
            DiffOp::Insert { old_index, new_index, new_len } => {
                let inserted = remote_units[new_index..new_index + new_len].to_vec();
                remote_inserted_after.insert(old_index, inserted);
            }
            DiffOp::Replace { old_index, old_len, new_index, new_len } => {
                let replacements = remote_units[new_index..new_index + new_len].to_vec();
                for i in old_index..old_index + old_len {
                    if i == old_index {
                        remote_modified_base.insert(i, replacements.clone());
                    } else {
                        remote_modified_base.insert(i, Vec::new());
                    }
                }
            }
        }
    }

    // Determine if an authoritative conflict occurred
    let mut has_conflict = false;
    for (idx, our_mod) in &our_modified_base {
        if let Some(rem_mod) = remote_modified_base.get(idx) {
            if our_mod != rem_mod {
                has_conflict = true;
                break;
            }
        }
    }

    if !has_conflict {
        for (idx, our_ins) in &our_inserted_after {
            if let Some(rem_ins) = remote_inserted_after.get(idx) {
                if our_ins != rem_ins {
                    has_conflict = true;
                    break;
                }
            }
        }
    }

    let mut result = Vec::new();
    for i in 0..=base_units.len() {
        if let Some(our_ins) = our_inserted_after.get(&i) {
            result.extend(our_ins.clone());
            if let Some(rem_ins) = remote_inserted_after.get(&i) {
                if rem_ins != our_ins {
                    result.extend(rem_ins.clone());
                }
            }
        } else if let Some(rem_ins) = remote_inserted_after.get(&i) {
            result.extend(rem_ins.clone());
        }

        if i < base_units.len() {
            if let Some(our_mod) = our_modified_base.get(&i) {
                // OUR VERSION IS AUTHORITATIVE ON CONFLICT!
                result.extend(our_mod.clone());
            } else if let Some(rem_mod) = remote_modified_base.get(&i) {
                // Remote modified non-conflicting sentence/part
                result.extend(rem_mod.clone());
            } else {
                result.push(base_units[i].clone());
            }
        }
    }

    let final_text = result.concat();
    let corrective_ops = if has_conflict {
        diff(remote_text, &final_text)
    } else {
        Vec::new()
    };

    ReconciliationResult {
        final_text,
        has_conflict,
        corrective_ops,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_diff_and_apply_basic() {
        let before = "Hello world!";
        let after = "Hello beautiful world!";
        let ops = diff(before, after);
        let result = apply(before, &ops).unwrap();
        assert_eq!(result, after);
    }

    #[test]
    fn test_diff_and_apply_multiline() {
        let before = "\\documentclass{article}\n\\begin{document}\nInitial content.\n\\end{document}\n";
        let after = "\\documentclass{article}\n\\begin{document}\nUpdated line 1.\nAdded line 2.\n\\end{document}\n";
        let ops = diff(before, after);
        let result = apply(before, &ops).unwrap();
        assert_eq!(result, after);
    }

    #[test]
    fn test_ot_transform_non_conflicting() {
        // Base document
        let base = "Line 1: Apples\nLine 2: Bananas\nLine 3: Cherries\n";
        // User A edits line 1
        let after_a = "Line 1: Green Apples\nLine 2: Bananas\nLine 3: Cherries\n";
        // User B edits line 3
        let after_b = "Line 1: Apples\nLine 2: Bananas\nLine 3: Sweet Cherries\n";

        let op_a = diff(base, after_a);
        let op_b = diff(base, after_b);

        // Server accepts A first, so doc becomes after_a
        // Transform B's op against A's op
        let op_b_prime = transform(&op_b, &op_a, Side::Right);
        let final_doc = apply(after_a, &op_b_prime).unwrap();

        assert!(final_doc.contains("Green Apples"));
        assert!(final_doc.contains("Sweet Cherries"));
    }

    #[test]
    fn test_conflict_resolution_our_version_authoritative() {
        let base = "\\section{Conclusion}\nThe proven result is that Algorithm Alpha is the most efficient choice.\n";
        // Collaborator modifies sentence to:
        let remote = "\\section{Conclusion}\nThe proven result is that Algorithm Beta is clearly superior to Alpha.\n";
        // We modify the exact same sentence to:
        let our = "\\section{Conclusion}\nThe proven result is that Algorithm Gamma is conclusively the undisputed winner.\n";

        let res = reconcile_conflict(base, our, remote);
        // Our version must be authoritative!
        assert!(res.has_conflict);
        assert!(res.final_text.contains("Algorithm Gamma is conclusively the undisputed winner"));
        assert!(!res.final_text.contains("Algorithm Beta"));
        // And corrective ops applied to remote yields our text
        let applied_corrective = apply(remote, &res.corrective_ops).unwrap();
        assert_eq!(applied_corrective, res.final_text);
    }

    #[test]
    fn test_reconcile_concurrent_non_conflicting() {
        let base = "\\section{Introduction}\nSection 1: Original introduction.\n\n\\section{Methodology}\nSection 2: Original methodology.\n";
        let our = "\\section{Introduction}\nSection 1: Local FUSE author comprehensive introduction.\n\n\\section{Methodology}\nSection 2: Original methodology.\n";
        let remote = "\\section{Introduction}\nSection 1: Original introduction.\n\n\\section{Methodology}\nSection 2: Remote collaborator enhanced experimental methodology.\n";

        let res = reconcile_conflict(base, our, remote);
        assert!(!res.has_conflict);
        assert!(res.final_text.contains("Local FUSE author comprehensive introduction"));
        assert!(res.final_text.contains("Remote collaborator enhanced experimental methodology"));
        assert!(res.corrective_ops.is_empty());
    }
}
