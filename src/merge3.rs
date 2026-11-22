//! Three-way merge for plain text.
//!
//! Used for two situations Overleaf's OT cannot express directly:
//!   1. we failed to send an update through the OT pipeline (fallback path:
//!      we PUT the whole file) and somebody else changed the doc meanwhile;
//!   2. the project uses a document type without OT (or the server has no
//!      real-time service at all — "other Overleaf server" fallback), where
//!      concurrent writes are reconciled optimistically.
//!
//! The algorithm is a standard line-based diff3 merge:
//!   * diff `base -> ours` and `base -> theirs` into hunks,
//!   * walk both hunk lists; non-overlapping hunks are taken from either side,
//!   * overlapping hunks are merged when they produce the same text, and are
//!     reported as conflicts otherwise.
//!
//! A `MergeResult` carries both the merged text and a *conflict-free*
//! resolution that keeps both sides: when regions conflict we keep ours,
//! followed by theirs in a clearly delimited block, so no user edit is ever
//! silently dropped (`--` / `#` markers match `git`'s style so users recognise
//! them).

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeResult {
    pub text: String,
    pub conflicts: Vec<Conflict>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Conflict {
    /// 1-based line in the base document where the conflicting region starts.
    pub base_line: usize,
    pub ours: String,
    pub theirs: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Hunk {
    base_start: usize, // line index into base
    base_end: usize,   // exclusive
    lines: Vec<String>,
}

fn split(s: &str) -> Vec<&str> {
    s.split('\n').collect()
}

fn lcs_hunks(base: &[&str], other: &[&str]) -> Vec<Hunk> {
    // Myers-style LCS table. Documents are editor buffers (thousands of lines
    // at most), so the O(n*m) table is fine and keeps the code obvious.
    let n = base.len();
    let m = other.len();
    let mut dp = vec![vec![0usize; m + 1]; n + 1];
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            dp[i][j] = if base[i] == other[j] {
                dp[i + 1][j + 1] + 1
            } else {
                dp[i + 1][j].max(dp[i][j + 1])
            };
        }
    }
    let mut hunks = Vec::new();
    let (mut i, mut j) = (0usize, 0usize);
    let mut cur: Option<Hunk> = None;
    while i < n || j < m {
        if i < n && j < m && base[i] == other[j] {
            if let Some(h) = cur.take() {
                hunks.push(h);
            }
            i += 1;
            j += 1;
            continue;
        }
        let h = cur.get_or_insert_with(|| Hunk {
            base_start: i,
            base_end: i,
            lines: Vec::new(),
        });
        let take_other = j < m && (i == n || dp[i][j + 1] >= dp[i + 1][j]);
        if take_other {
            h.lines.push(other[j].to_string());
            j += 1;
            // base_end stays: this is an insertion at base_start
        } else {
            h.base_end += 1;
            i += 1;
        }
    }
    if let Some(h) = cur {
        hunks.push(h);
    }
    hunks
}

/// Merge `ours` and `theirs`, both derived from `base`.
pub fn merge3(base: &str, ours: &str, theirs: &str) -> MergeResult {
    if ours == theirs {
        return MergeResult {
            text: ours.to_string(),
            conflicts: vec![],
        };
    }
    if ours == base {
        return MergeResult {
            text: theirs.to_string(),
            conflicts: vec![],
        };
    }
    if theirs == base {
        return MergeResult {
            text: ours.to_string(),
            conflicts: vec![],
        };
    }
    let b = split(base);
    let a_hunks = lcs_hunks(&b, &split(ours));
    let t_hunks = lcs_hunks(&b, &split(theirs));

    let mut out: Vec<String> = Vec::new();
    let mut conflicts = Vec::new();
    let (mut ai, mut ti) = (0usize, 0usize);
    let mut pos = 0usize; // current base line

    while ai < a_hunks.len() || ti < t_hunks.len() {
        let next_a = a_hunks.get(ai);
        let next_t = t_hunks.get(ti);
        // Next hunk start (insertions sort before replacements at the same pos)
        let start = match (next_a, next_t) {
            (Some(x), Some(y)) => x.base_start.min(y.base_start),
            (Some(x), None) => x.base_start,
            (None, Some(y)) => y.base_start,
            (None, None) => break,
        };
        out.extend(b[pos..start].iter().map(|s| s.to_string()));
        pos = start;

        // Collect all hunks that touch [start, ...) from either side
        let mut group_a: Vec<Hunk> = Vec::new();
        let mut group_t: Vec<Hunk> = Vec::new();
        let mut end = start;
        loop {
            let mut grew = false;
            while let Some(h) = a_hunks.get(ai) {
                if h.base_start <= end {
                    end = end.max(h.base_end).max(h.base_start);
                    group_a.push(h.clone());
                    ai += 1;
                    grew = true;
                } else {
                    break;
                }
            }
            while let Some(h) = t_hunks.get(ti) {
                if h.base_start <= end {
                    end = end.max(h.base_end).max(h.base_start);
                    group_t.push(h.clone());
                    ti += 1;
                    grew = true;
                } else {
                    break;
                }
            }
            if !grew {
                break;
            }
        }
        // insertions at the very same point extend `end` without consuming base
        let base_region: Vec<String> = b[start..end].iter().map(|s| s.to_string()).collect();
        let ours_region = apply_local(&base_region, start, &group_a);
        let theirs_region = apply_local(&base_region, start, &group_t);
        if ours_region == theirs_region {
            out.extend(ours_region);
        } else if one_side_unchanged(&ours_region, &base_region) {
            out.extend(theirs_region);
        } else if one_side_unchanged(&theirs_region, &base_region) {
            out.extend(ours_region);
        } else {
            // Genuine conflict: keep ours and append theirs in markers so both
            // edits survive and the user can resolve manually.
            conflicts.push(Conflict {
                base_line: start + 1,
                ours: ours_region.join("\n"),
                theirs: theirs_region.join("\n"),
            });
            let mut merged = Vec::new();
            merged.push("<<<<<<< local (overleaf-mount)".to_string());
            merged.extend(ours_region);
            merged.push("=======".to_string());
            merged.extend(theirs_region);
            merged.push(">>>>>>> remote (overleaf)".to_string());
            out.extend(merged);
        }
        pos = end;
    }
    out.extend(b[pos..].iter().map(|s| s.to_string()));
    MergeResult {
        text: out.join("\n"),
        conflicts,
    }
}

fn one_side_unchanged(region: &[String], base_region: &[String]) -> bool {
    region == base_region
}

/// Apply the hunks of one side that fall inside `[start, end)` of the base.
fn apply_local(base_region: &[String], start: usize, hunks: &[Hunk]) -> Vec<String> {
    let mut out = Vec::new();
    let mut pos = start;
    for h in hunks {
        let rel_start = h.base_start.saturating_sub(start);
        let rel_pos = pos.saturating_sub(start);
        out.extend(base_region[rel_pos..rel_start].iter().cloned());
        out.extend(h.lines.iter().cloned());
        pos = h.base_end;
    }
    let rel_pos = pos.saturating_sub(start);
    if rel_pos < base_region.len() {
        out.extend(base_region[rel_pos..].iter().cloned());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_overlapping_edits_both_applied() {
        let base = "a\nb\nc\nd\ne";
        let ours = "A\nb\nc\nd\ne"; // edit line 1
        let theirs = "a\nb\nc\nd\nE"; // edit line 5
        let m = merge3(base, ours, theirs);
        assert!(m.conflicts.is_empty());
        assert_eq!(m.text, "A\nb\nc\nd\nE");
    }

    #[test]
    fn identical_edits_merge_cleanly() {
        let base = "a\nb\nc";
        let ours = "a\nB\nc";
        let m = merge3(base, ours, ours);
        assert!(m.conflicts.is_empty());
        assert_eq!(m.text, "a\nB\nc");
    }

    #[test]
    fn conflicting_edit_keeps_both_sides() {
        let base = "a\nb\nc";
        let ours = "a\nOURS\nc";
        let theirs = "a\nTHEIRS\nc";
        let m = merge3(base, ours, theirs);
        assert_eq!(m.conflicts.len(), 1);
        assert!(m.text.contains("OURS"));
        assert!(m.text.contains("THEIRS"));
    }

    #[test]
    fn same_sentence_edit_from_both_sides_is_a_conflict_not_data_loss() {
        let base = "The quick brown fox jumps over the lazy dog.";
        let ours = "The quick red fox jumps over the lazy dog.";
        let theirs = "The quick blue fox jumps over the lazy dog.";
        let m = merge3(base, ours, theirs);
        assert_eq!(m.conflicts.len(), 1);
        assert!(m.text.contains("red"));
        assert!(m.text.contains("blue"));
    }

    #[test]
    fn insertion_by_both_sides_different_lines() {
        let base = "one\ntwo";
        let ours = "one\ninserted-a\ntwo";
        let theirs = "one\ntwo\ninserted-b";
        let m = merge3(base, ours, theirs);
        assert!(m.conflicts.is_empty(), "{:?}", m.conflicts);
        assert_eq!(m.text, "one\ninserted-a\ntwo\ninserted-b");
    }

    #[test]
    fn ours_unchanged_takes_theirs() {
        let m = merge3("x", "x", "y");
        assert_eq!(m.text, "y");
    }
}
