//! Integration tests for the text / OT model (`overleaf_mount::ot`).
//!
//! These are black-box tests: they only use the public API exported from the
//! library. Everything is deterministic — the PRNG below is a hand-rolled
//! xorshift64* so a failure is always reproducible from the printed seed and
//! the failing assertion's message.
//!
//! What is being protected here:
//!
//! * **Minimality.** Writing one character to a 100 KB document must produce a
//!   one-entry wire operation, not a whole-file rewrite. That is the feature
//!   the mount exists for, so it is asserted with exact sizes.
//! * **Diff/apply round-trip** over an alphabet that contains every character
//!   class that historically broke length arithmetic: `\n`, `\r`, `\t`,
//!   quotes, backslashes, combining sequences and astral (4-byte) characters.
//! * **Transform convergence.** Concurrent edits must converge no matter where
//!   they land; this is the OT invariant that keeps two people editing the same
//!   sentence from corrupting the document.

use overleaf_mount::ot::{diff, replace_all, split_lines, Component, TextOp};
use overleaf_mount::sync::to_wire_op;

// ------------------------------------------------------------------ helpers --

/// Deterministic xorshift64* PRNG (self-contained: no `rand` needed, so the
/// test binary stays reproducible forever).
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        // xorshift must never be seeded with 0.
        Rng(seed | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next_u64() % n as u64) as usize
        }
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.next_u64() % 100 < percent
    }
}

/// An alphabet that deliberately mixes every awkward character class.
///
/// `e\u{301}` is a two-`char` grapheme, `😀`/`𝄞` are astral (2 UTF-16 units,
/// 4 UTF-8 bytes), `日`/`本`/`語` are BMP non-ASCII. The in-tree fuzz test only
/// uses `a b c \n space é`, so astral characters are covered for the first time
/// here.
const ALPHABET: &[&str] = &[
    "a", "b", "c", "Z", "0", " ", "\n", "\r", "\t", "\"", "\\", "é", "日", "本", "語", "😀", "𝄞",
    "e\u{301}",
];

fn random_text(rng: &mut Rng, max_tokens: usize) -> String {
    let n = rng.below(max_tokens + 1);
    let mut out = String::new();
    for _ in 0..n {
        out.push_str(ALPHABET[rng.below(ALPHABET.len())]);
    }
    out
}

/// Apply `edits` random insertions/deletions to `s`, returning the new text.
fn mutate(rng: &mut Rng, s: &str, edits: usize) -> String {
    let mut chars: Vec<char> = s.chars().collect();
    for _ in 0..edits {
        if !chars.is_empty() && rng.chance(45) {
            let p = rng.below(chars.len());
            chars.remove(p);
        } else {
            let p = rng.below(chars.len() + 1);
            let token = ALPHABET[rng.below(ALPHABET.len())];
            // Insert the token's chars in reverse so they keep their order.
            for ch in token.chars().rev() {
                chars.insert(p, ch);
            }
        }
    }
    chars.into_iter().collect()
}

/// Build a random *valid* operation over `base`: it consumes exactly
/// `base.chars().count()` characters. Unlike `diff`, this produces
/// multi-region operations (several retains/removes/inserts), which is what
/// exercises the transform/compose state machines.
fn random_op(rng: &mut Rng, base: &str) -> TextOp {
    let len = base.chars().count();
    let mut op = TextOp::new();
    let mut pos = 0usize;
    let mut guard = 0usize;
    while pos < len {
        guard += 1;
        assert!(guard <= 10_000, "random_op failed to terminate");
        match rng.below(3) {
            0 => {
                let n = 1 + rng.below((len - pos).min(3));
                op.retain(n);
                pos += n;
            }
            1 => {
                let n = 1 + rng.below((len - pos).min(3));
                op.remove(n);
                pos += n;
            }
            _ => {
                let n = 1 + rng.below(3);
                let mut s = String::new();
                for _ in 0..n {
                    s.push_str(ALPHABET[rng.below(ALPHABET.len())]);
                }
                op.insert(&s);
            }
        }
    }
    if rng.chance(30) {
        op.insert("tail");
    }
    op
}

fn char_len(s: &str) -> usize {
    s.chars().count()
}

/// Insert `ins` at `char` index `idx` (not a byte index).
fn insert_at(s: &str, idx: usize, ins: &str) -> String {
    let mut out: String = s.chars().take(idx).collect();
    out.push_str(ins);
    out.extend(s.chars().skip(idx));
    out
}

/// Remove `n` chars starting at `char` index `idx`.
fn remove_at(s: &str, idx: usize, n: usize) -> String {
    let mut out: String = s.chars().take(idx).collect();
    out.extend(s.chars().skip(idx + n));
    out
}

/// A realistic multi-line document of about `target_bytes` bytes.
fn large_document(target_bytes: usize) -> String {
    let mut doc = String::with_capacity(target_bytes + 128);
    let mut i = 0usize;
    while doc.len() < target_bytes {
        if i > 0 {
            doc.push('\n');
        }
        doc.push_str(&format!(
            "line {i:05}: the quick brown fox jumps over the lazy dog 0123456789"
        ));
        i += 1;
    }
    doc
}

fn wire_bytes(op: &TextOp, base: &str) -> usize {
    to_wire_op(op, base).to_string().len()
}

fn wire_entries(op: &TextOp, base: &str) -> Vec<serde_json::Value> {
    let v = to_wire_op(op, base);
    v.as_array()
        .unwrap_or_else(|| panic!("to_wire_op returned {v}, expected a JSON array"))
        .clone()
}

// ------------------------------------------------------ 1. minimality ------

#[test]
fn minimal_insert_in_the_middle_of_a_100kb_document() {
    let doc = large_document(100 * 1024);
    let lines = split_lines(&doc).len();
    let mid = char_len(&doc) / 2;
    let new = insert_at(&doc, mid, "X");

    let op = diff(&doc, &new);
    assert_eq!(
        op.change_size(),
        (1, 0),
        "inserting one character must insert exactly one and delete none"
    );
    assert!(
        op.components.len() <= 3,
        "a single contiguous insertion needs at most retain/insert/retain, got {} components: {:?}",
        op.components.len(),
        op.components
    );
    let bytes = wire_bytes(&op, &doc);
    println!(
        "minimality/100KB mid-insert: doc={} bytes, {lines} lines, components={}, wire={} bytes -> {}",
        doc.len(),
        op.components.len(),
        bytes,
        to_wire_op(&op, &doc)
    );
    assert!(
        bytes < 80,
        "wire op for a one-character insertion into a 100 KB document was {bytes} bytes \
         (expected < 80): {}",
        to_wire_op(&op, &doc)
    );
    let entries = wire_entries(&op, &doc);
    assert_eq!(entries.len(), 1, "expected one positional entry, got {entries:?}");
    assert_eq!(op.apply(&doc).expect("apply must succeed"), new);
}

#[test]
fn minimal_insert_at_the_end_of_a_100kb_document() {
    let doc = large_document(100 * 1024);
    let new = insert_at(&doc, char_len(&doc), "!");
    let op = diff(&doc, &new);

    assert_eq!(op.change_size(), (1, 0));
    assert!(
        op.components.len() <= 3,
        "append needs retain+insert at most, got {:?}",
        op.components
    );
    let bytes = wire_bytes(&op, &doc);
    println!("minimality/100KB append: wire={bytes} bytes -> {}", to_wire_op(&op, &doc));
    assert!(
        bytes < 80,
        "appending one character to a 100 KB document produced a {bytes}-byte wire op: {}",
        to_wire_op(&op, &doc)
    );
    assert_eq!(op.apply(&doc).unwrap(), new);
}

#[test]
fn minimal_insert_at_the_start_of_a_100kb_document() {
    let doc = large_document(100 * 1024);
    let new = insert_at(&doc, 0, ">");
    let op = diff(&doc, &new);

    assert_eq!(op.change_size(), (1, 0));
    assert!(
        op.components.len() <= 3,
        "prepend needs insert+retain at most, got {:?}",
        op.components
    );
    let bytes = wire_bytes(&op, &doc);
    println!("minimality/100KB prepend: wire={bytes} bytes -> {}", to_wire_op(&op, &doc));
    assert!(
        bytes < 80,
        "prepending one character to a 100 KB document produced a {bytes}-byte wire op: {}",
        to_wire_op(&op, &doc)
    );
    assert_eq!(op.apply(&doc).unwrap(), new);
}

#[test]
fn minimal_word_replacement_in_the_middle_of_a_100kb_document() {
    let doc = large_document(100 * 1024);
    let pos = doc
        .find("quick")
        .expect("the generated document contains 'quick'");
    let new = format!("{}planet{}", &doc[..pos], &doc[pos + "quick".len()..]);

    let op = diff(&doc, &new);
    assert_eq!(
        op.change_size(),
        ("planet".len(), "quick".len()),
        "replacing one word must touch only that word (got {:?}, components {:?})",
        op.change_size(),
        op.components
    );
    assert!(
        op.components.len() <= 4,
        "one word replacement needs retain/insert/remove/retain, got {:?}",
        op.components
    );
    let entries = wire_entries(&op, &doc);
    assert_eq!(
        entries.len(),
        2,
        "expected exactly one insert and one delete entry, got {entries:?}"
    );
    let bytes = wire_bytes(&op, &doc);
    println!(
        "minimality/100KB word swap: wire={bytes} bytes, components={} -> {}",
        op.components.len(),
        to_wire_op(&op, &doc)
    );
    assert!(
        bytes < 120,
        "word replacement wire op was {bytes} bytes: {}",
        to_wire_op(&op, &doc)
    );
    assert_eq!(op.apply(&doc).unwrap(), new);
}

#[test]
fn minimal_single_character_delete_in_a_200kb_document() {
    let doc = large_document(200 * 1024);
    let mid = char_len(&doc) / 2;
    let victim = doc.chars().nth(mid).expect("document is non-empty");
    let new = remove_at(&doc, mid, 1);

    let op = diff(&doc, &new);
    assert_eq!(
        op.change_size(),
        (0, 1),
        "deleting one character must insert nothing and delete exactly one"
    );
    assert!(
        op.components.len() <= 3,
        "a single contiguous deletion needs at most retain/remove/retain, got {:?}",
        op.components
    );
    let entries = wire_entries(&op, &doc);
    assert_eq!(entries.len(), 1, "expected exactly one positional entry, got {entries:?}");
    let victim_text = victim.to_string();
    assert_eq!(
        entries[0].get("d").and_then(|d| d.as_str()),
        Some(victim_text.as_str()),
        "the delete entry must spell out the removed character"
    );
    let bytes = wire_bytes(&op, &doc);
    println!(
        "minimality/200KB delete: doc={} bytes, wire={bytes} bytes -> {}",
        doc.len(),
        to_wire_op(&op, &doc)
    );
    assert!(
        bytes < 80,
        "deleting one character from a 200 KB document produced a {bytes}-byte wire op: {}",
        to_wire_op(&op, &doc)
    );
    assert_eq!(op.apply(&doc).unwrap(), new);
}

#[test]
fn diff_of_identical_documents_is_a_noop_and_a_rewrite_is_not() {
    let doc = large_document(64 * 1024);
    let noop = diff(&doc, &doc);
    assert!(
        noop.is_noop(),
        "diff(doc, doc) must be a no-op, got {:?}",
        noop.components
    );
    assert_eq!(noop.change_size(), (0, 0));
    assert_eq!(noop.apply(&doc).unwrap(), doc);

    let rewritten = diff("abc", "xyz");
    assert!(
        !rewritten.is_noop(),
        "a fully different document must not produce a no-op"
    );
    let (ins, del) = rewritten.change_size();
    assert!(ins > 0 && del > 0, "full rewrite should delete and insert, got {ins}/{del}");

    let swapped = diff("ab", "ba");
    assert!(!swapped.is_noop(), "swapping the document must not be a no-op");
    assert_eq!(swapped.apply("ab").unwrap(), "ba");
}

// ------------------------------------------- 2. diff/apply invariants ------

#[test]
fn diff_apply_roundtrip_over_random_corpus() {
    let mut rng = Rng::new(0x5EED_1234_ABCD);
    let mut cases = 0usize;
    // Half the cases are completely independent strings (worst case for the
    // prefix/suffix trim), half are mutations of a base (the realistic case:
    // a small edit inside a large common body).
    for case in 0..600 {
        let (a, b) = if case % 2 == 0 {
            (random_text(&mut rng, 40), random_text(&mut rng, 40))
        } else {
            let a = random_text(&mut rng, 60);
            let edits = 1 + rng.below(4);
            let b = mutate(&mut rng, &a, edits);
            (a, b)
        };
        let op = diff(&a, &b);
        let got = op.apply(&a).unwrap_or_else(|e| {
            panic!("case {case}: apply failed for diff({a:?}, {b:?}): {e}; components={:?}", op.components)
        });
        assert_eq!(
            got, b,
            "case {case}: diff({a:?}, {b:?}) applied to the base gave {got:?}"
        );
        assert_eq!(
            op.base_len,
            char_len(&a),
            "case {case}: base_len must be the source char count"
        );
        assert_eq!(
            op.target_len,
            char_len(&b),
            "case {case}: target_len must be the destination char count"
        );
        cases += 1;
    }
    assert!(cases >= 500, "expected at least 500 corpus cases, ran {cases}");
    println!("diff/apply corpus: {cases} cases, all round-tripped");
}

#[test]
fn diff_application_is_repeatable_and_self_diff_is_a_noop() {
    let mut rng = Rng::new(0x11DE_A5E5);
    for case in 0..200 {
        let a = random_text(&mut rng, 30);
        let b = mutate(&mut rng, &a, 3);

        assert_eq!(diff(&a, &a), diff(&a, &a), "diff must be deterministic");
        assert!(
            diff(&a, &a).is_noop(),
            "case {case}: diff(a, a) must be a no-op for {a:?}"
        );

        let op = diff(&a, &b);
        let first = op.apply(&a).unwrap();
        let second = op.apply(&a).unwrap();
        assert_eq!(
            first, second,
            "case {case}: applying the same op to the same base twice must agree"
        );
        assert_eq!(first, b, "case {case}: op did not produce the target");
    }
}

#[test]
fn apply_rejects_a_base_of_the_wrong_length() {
    let doc = "hello world";
    let op = diff(doc, "hello brave world");
    assert!(op.apply(doc).is_ok());

    let short = op.apply("hell");
    let err = short.expect_err("applying to a base that is too short must fail");
    assert!(
        err.to_string().contains("length"),
        "error should mention the length mismatch, got: {err}"
    );

    let long = op.apply("hello world and then some more text");
    let err = long.expect_err("applying to a base that is too long must fail");
    assert!(
        err.to_string().contains("length"),
        "error should mention the length mismatch, got: {err}"
    );

    // Deleting everything from a one-character document must not silently pass
    // on an empty base either.
    let del = diff("x", "");
    assert!(del.apply("").is_err(), "remove(1) must not apply to an empty document");
    assert_eq!(del.apply("x").unwrap(), "");
}

#[test]
fn lines_roundtrip_for_all_line_shapes() {
    let docs = [
        "trailing newline\n",
        "no trailing newline",
        "",
        "\n",
        "\n\n\n",
        "a\nb\n\n",
        "x\n\ny",
    ];
    for doc in docs {
        assert_eq!(
            split_lines(doc).join("\n"),
            doc,
            "split_lines must be the exact inverse of join(\"\\n\") for {doc:?}"
        );

        let len = char_len(doc);
        let new = insert_at(doc, len / 2, "Z");
        let op = diff(doc, &new);
        let lines = split_lines(doc);
        let got = op
            .apply_to_lines(&lines)
            .unwrap_or_else(|e| panic!("apply_to_lines failed for {doc:?}: {e}"));
        assert_eq!(
            got,
            split_lines(&new),
            "apply_to_lines(split_lines(doc)) must equal split_lines(target) for {doc:?} -> {new:?}"
        );
        assert_eq!(got.join("\n"), new, "lines round-trip lost text for {doc:?}");
    }
}

// ------------------------------------------ 3. transform convergence ------

#[test]
fn transform_converges_for_random_concurrent_pairs() {
    let mut rng = Rng::new(0x0000_1CEB_00C5);
    let mut rounds = 0usize;
    for round in 0..2200 {
        let base = random_text(&mut rng, 12);
        // Alternate between diff-derived ops (single contiguous change) and
        // random multi-region ops, so both code paths in transform_pair see
        // traffic.
        let (a, b) = if round % 2 == 0 {
            let e1 = 1 + rng.below(3);
            let e2 = 1 + rng.below(3);
            let s1 = mutate(&mut rng, &base, e1);
            let s2 = mutate(&mut rng, &base, e2);
            (diff(&base, &s1), diff(&base, &s2))
        } else {
            (random_op(&mut rng, &base), random_op(&mut rng, &base))
        };

        let (a2, b2) = TextOp::transform_pair(&a, &b).unwrap_or_else(|e| {
            panic!("round {round}: transform_pair failed for base={base:?} a={a:?} b={b:?}: {e}")
        });

        assert_eq!(
            a2.base_len, b.target_len,
            "round {round}: a' must apply to b's output (len {}), got {}",
            b.target_len, a2.base_len
        );
        assert_eq!(
            b2.base_len, a.target_len,
            "round {round}: b' must apply to a's output (len {}), got {}",
            a.target_len, b2.base_len
        );

        let ca = a
            .apply(&base)
            .and_then(|d| b2.apply(&d))
            .unwrap_or_else(|e| panic!("round {round}: apply path A then B' failed: {e}"));
        let cb = b
            .apply(&base)
            .and_then(|d| a2.apply(&d))
            .unwrap_or_else(|e| panic!("round {round}: apply path B then A' failed: {e}"));
        assert_eq!(
            ca, cb,
            "round {round}: concurrent edits diverged\n  base={base:?}\n  a={a:?}\n  b={b:?}\n  a'={a2:?}\n  b'={b2:?}\n  A then B'={ca:?}\n  B then A'={cb:?}"
        );

        // Transform must not invent text: neither side can grow beyond what the
        // original operations inserted (and can only lose deletions).
        let (ai, ad) = a.change_size();
        let (bi, bd) = b.change_size();
        let (a2i, a2d) = a2.change_size();
        let (b2i, b2d) = b2.change_size();
        assert!(
            a2i <= ai + bi && b2i <= bi + ai,
            "round {round}: transform inflated insertions: a=+{ai} b=+{bi} -> a'=+{a2i} b'=+{b2i}"
        );
        assert!(
            a2d <= ad + bd && b2d <= bd + ad,
            "round {round}: transform inflated deletions: a=-{ad} b=-{bd} -> a'=-{a2d} b'=-{b2d}"
        );
        rounds += 1;
    }
    assert!(rounds >= 2000, "expected at least 2000 transform rounds, ran {rounds}");
    println!("transform convergence: {rounds} random pairs converged");
}

#[test]
fn transform_insert_versus_insert_at_the_same_offset() {
    let base = "abc";
    let mut a = TextOp::new();
    a.insert("[A]");
    a.retain(3);
    let mut b = TextOp::new();
    b.insert("[B]");
    b.retain(3);

    let (a2, b2) = TextOp::transform_pair(&a, &b).expect("transform must succeed");
    let ca = b2.apply(&a.apply(base).unwrap()).unwrap();
    let cb = a2.apply(&b.apply(base).unwrap()).unwrap();
    assert_eq!(ca, cb, "concurrent same-offset inserts diverged");
    assert_eq!(
        ca.matches("[A]").count(),
        1,
        "marker [A] must appear exactly once in {ca:?}"
    );
    assert_eq!(
        ca.matches("[B]").count(),
        1,
        "marker [B] must appear exactly once in {ca:?}"
    );
    assert_eq!(ca, "[A][B]abc", "tie-break order changed: {ca:?}");
}

#[test]
fn transform_delete_versus_insert_at_the_same_offset() {
    // A deletes "world", B inserts "!!!" at the same position.
    let base = "hello world";
    let mut a = TextOp::new();
    a.retain(6);
    a.remove(5);
    let mut b = TextOp::new();
    b.retain(6);
    b.insert("!!!");
    b.retain(5);

    let (a2, b2) = TextOp::transform_pair(&a, &b).expect("transform must succeed");
    let ca = b2.apply(&a.apply(base).unwrap()).unwrap();
    let cb = a2.apply(&b.apply(base).unwrap()).unwrap();
    assert_eq!(ca, cb, "delete-vs-insert at the same offset diverged");
    assert_eq!(ca, "hello !!!", "expected the deletion and the insertion to compose");
    assert_eq!(
        ca.matches("!!!").count(),
        1,
        "inserted marker duplicated in {ca:?}"
    );
    assert!(!ca.contains("world"), "deleted text survived in {ca:?}");
}

#[test]
fn transform_overlapping_deletions() {
    // a deletes bcd, b deletes cdef; the union is gone, nothing is deleted twice.
    let base = "abcdefgh";
    let mut a = TextOp::new();
    a.retain(1);
    a.remove(3);
    a.retain(4);
    let mut b = TextOp::new();
    b.retain(2);
    b.remove(4);
    b.retain(2);

    let (a2, b2) = TextOp::transform_pair(&a, &b).expect("transform must succeed");
    let ca = b2.apply(&a.apply(base).unwrap()).unwrap();
    let cb = a2.apply(&b.apply(base).unwrap()).unwrap();
    assert_eq!(ca, cb, "overlapping deletions diverged");
    assert_eq!(ca, "agh", "overlapping deletions should remove b..f exactly once");
    let (_, a2d) = a2.change_size();
    let (_, b2d) = b2.change_size();
    assert!(
        a2d + b2d <= 7,
        "overlapping deletes must not be applied twice: a'={a2d} b'={b2d}"
    );
}

#[test]
fn transform_whole_document_replace_versus_small_edit() {
    let base = "line1\nline2";
    let a = replace_all(char_len(base), "totally different");
    let mut b = TextOp::new();
    b.retain(char_len(base));
    b.insert("\nline3");

    let (a2, b2) = TextOp::transform_pair(&a, &b).expect("transform must succeed");
    let ca = b2.apply(&a.apply(base).unwrap()).unwrap();
    let cb = a2.apply(&b.apply(base).unwrap()).unwrap();
    assert_eq!(ca, cb, "replace-all vs append diverged");
    assert_eq!(ca, "totally different\nline3");
    assert!(
        ca.contains("totally different"),
        "the replacement was lost in {ca:?}"
    );
}

#[test]
fn transform_with_a_noop_leaves_the_other_side_unchanged() {
    let base = "some text here";
    let target = "some TEXT here";
    let op = diff(base, target);
    let noop = diff(base, base);
    assert!(noop.is_noop());

    // `transform_pair(op, noop)` returns `(op', noop')`, where `op'` applies
    // *after* the no-op (so it must reproduce the edit) and `noop'` applies
    // after the edit (so it must change nothing).
    let (op_after_noop, noop_after_op) = TextOp::transform_pair(&op, &noop)
        .expect("transform of op against a no-op must succeed");
    let text = op_after_noop
        .apply(&noop.apply(base).unwrap())
        .expect("applying the transformed edit must work");
    assert_eq!(text, target, "a no-op on the other side changed the result");
    let text_noop_last = noop_after_op
        .apply(&op.apply(base).unwrap())
        .expect("applying the transformed no-op must work");
    assert_eq!(
        text_noop_last, target,
        "a no-op that follows the edit must not change the text"
    );
    assert_eq!(
        op_after_noop.change_size(),
        op.change_size(),
        "transforming against a no-op altered the change size"
    );
    assert_eq!(
        noop_after_op.change_size(),
        (0, 0),
        "a no-op must stay a no-op after being transformed: {:?}",
        noop_after_op.components
    );

    // Same in the other argument order.
    let (noop_after_op2, op_after_noop2) = TextOp::transform_pair(&noop, &op)
        .expect("transform of a no-op against an op must succeed");
    assert_eq!(
        noop_after_op2.change_size(),
        (0, 0),
        "the no-op side must remain a no-op"
    );
    assert_eq!(
        op_after_noop2.change_size(),
        op.change_size(),
        "the real edit must be preserved regardless of argument order"
    );
    let text2 = op_after_noop2
        .apply(&noop.apply(base).unwrap())
        .expect("apply must succeed");
    assert_eq!(text2, target);
}

// ------------------------------------------------------- 4. composition --

#[test]
fn compose_matches_sequential_application() {
    let mut rng = Rng::new(0xC0_0A5E_77);
    for round in 0..600 {
        let s = random_text(&mut rng, 14);
        let e1 = 1 + rng.below(3);
        let e2 = 1 + rng.below(3);
        let s1 = mutate(&mut rng, &s, e1);
        let s2 = mutate(&mut rng, &s1, e2);

        let a = diff(&s, &s1);
        let b = diff(&s1, &s2);
        let composed = TextOp::compose(&a, &b)
            .unwrap_or_else(|e| panic!("round {round}: compose failed for {s:?}->{s1:?}->{s2:?}: {e}"));

        let sequential = b
            .apply(&a.apply(&s).unwrap())
            .unwrap_or_else(|e| panic!("round {round}: sequential apply failed: {e}"));
        let flat = composed
            .apply(&s)
            .unwrap_or_else(|e| panic!("round {round}: applying the composed op failed: {e}"));
        assert_eq!(
            flat, sequential,
            "round {round}: compose(a,b).apply(S) != b.apply(a.apply(S)) for S={s:?} S1={s1:?} S2={s2:?}"
        );
        assert_eq!(flat, s2, "round {round}: composition did not reach the target text");

        let (ci, cd) = composed.change_size();
        let (ai, ad) = a.change_size();
        let (bi, bd) = b.change_size();
        assert!(
            ci <= ai + bi && cd <= ad + bd,
            "round {round}: compose inflated the change: a=+{ai}/-{ad}, b=+{bi}/-{bd}, composed=+{ci}/-{cd}"
        );
    }
    println!("compose: 600 consecutive op pairs matched sequential application");
}

#[test]
fn compose_matches_sequential_application_for_random_component_ops() {
    // `diff`-derived ops are single-region, so they rarely exercise the case
    // where the second operation's components fall *inside* the first
    // operation's insertion. Multi-region random ops do that constantly.
    let mut rng = Rng::new(0xC0_11_7E50);
    let mut checked = 0usize;
    for round in 0..400 {
        let s = random_text(&mut rng, 3);
        let a = random_op(&mut rng, &s);
        let mid = a
            .apply(&s)
            .unwrap_or_else(|e| panic!("round {round}: building the intermediate text failed: {e}"));
        let b = random_op(&mut rng, &mid);

        let expected = b
            .apply(&mid)
            .unwrap_or_else(|e| panic!("round {round}: sequential apply failed: {e}"));
        let composed = TextOp::compose(&a, &b).unwrap_or_else(|e| {
            panic!(
                "round {round}: compose rejected two valid consecutive operations: {e}\n  S={s:?}\n  a={:?}\n  S1={mid:?}\n  b={:?}",
                a.components, b.components
            )
        });
        assert_eq!(
            composed.apply(&s).unwrap(),
            expected,
            "round {round}: compose(a,b).apply(S) != b.apply(a.apply(S))\n  S={s:?}\n  a={:?}\n  b={:?}\n  composed={:?}",
            a.components,
            b.components,
            composed.components
        );
        checked += 1;
    }
    println!("compose: {checked} random multi-region op pairs matched sequential application");
}

#[test]
fn compose_handles_b_operations_remaining_after_a_is_exhausted() {
    // Same root cause as the test above, but surfacing as an error instead of
    // a wrong result: `a` inserts four characters, `b` retains two of them and
    // inserts, then deletes past the end of `a`'s remaining components.
    let base = "abcd";
    let mut a = TextOp::new();
    a.retain(1); // "a"
    a.insert("WXYZ"); // "aWXYZ"
    a.remove(2); // drops "bc"
    a.retain(1); // "d"  => "aWXYZd"
    let after_a = a.apply(base).unwrap();
    assert_eq!(after_a, "aWXYZd");

    let mut b = TextOp::new();
    b.retain(2); // "aW"
    b.insert("!!");
    b.remove(4); // "XYZd"
    let after_b = b.apply(&after_a).unwrap();
    assert_eq!(after_b, "aW!!");

    let composed = TextOp::compose(&a, &b).unwrap_or_else(|e| {
        panic!(
            "compose must produce an op for these valid consecutive operations, got: {e}; \
             a={:?} b={:?} base={base:?}",
            a.components, b.components
        )
    });
    assert_eq!(composed.apply(base).unwrap(), after_b);
}

/// Every operation over a `len`-character base built from retains, removes and
/// (at most one per position) inserts of `"X"`/`"YZ"`. Used for exhaustive
/// small-input testing of transform/compose.
fn small_op_family(len: usize) -> Vec<TextOp> {
    fn rec(pos: usize, len: usize, cur: &mut TextOp, out: &mut Vec<TextOp>) {
        if pos == len {
            out.push(cur.clone());
            for ins in ["X", "YZ"] {
                let mut o = cur.clone();
                o.insert(ins);
                out.push(o);
            }
            return;
        }
        for ins in [None, Some("X"), Some("YZ")] {
            let mut next = cur.clone();
            if let Some(s) = ins {
                next.insert(s);
            }
            for k in 1..=(len - pos) {
                let mut keep = next.clone();
                keep.retain(k);
                rec(pos + k, len, &mut keep, out);

                let mut drop = next.clone();
                drop.remove(k);
                rec(pos + k, len, &mut drop, out);
            }
        }
    }
    let mut out = Vec::new();
    rec(0, len, &mut TextOp::new(), &mut out);
    out
}

/// A focused edit family over a document of `len` chars: no-op, insert at every
/// boundary, delete every range, and replace every range. Small enough to pair
/// exhaustively, broad enough to put the second operation's components *inside*
/// the first operation's insertion.
fn targeted_edits(len: usize) -> Vec<TextOp> {
    let mut ops = Vec::new();
    let mut noop = TextOp::new();
    noop.retain(len);
    ops.push(noop);
    for pos in 0..=len {
        let mut o = TextOp::new();
        o.retain(pos);
        o.insert("I");
        o.retain(len - pos);
        ops.push(o);
        for k in 1..=(len - pos) {
            let mut o = TextOp::new();
            o.retain(pos);
            o.remove(k);
            o.retain(len - pos - k);
            ops.push(o);

            let mut o = TextOp::new();
            o.retain(pos);
            o.insert("R");
            o.remove(k);
            o.retain(len - pos - k);
            ops.push(o);
        }
    }
    ops
}

#[test]
fn transform_converges_for_every_small_op_pair() {
    let base = "ab";
    let ops = small_op_family(base.chars().count());
    let mut checked = 0usize;
    let mut diverged: Vec<String> = Vec::new();
    for a in &ops {
        for b in &ops {
            let (a2, b2) = match TextOp::transform_pair(a, b) {
                Ok(pair) => pair,
                Err(e) => {
                    diverged.push(format!("transform error for a={:?} b={:?}: {e}", a.components, b.components));
                    continue;
                }
            };
            let ca = a.apply(base).and_then(|d| b2.apply(&d));
            let cb = b.apply(base).and_then(|d| a2.apply(&d));
            match (ca, cb) {
                (Ok(x), Ok(y)) if x == y => checked += 1,
                (Ok(x), Ok(y)) => diverged.push(format!(
                    "a={:?} b={:?}: A then B'={x:?} but B then A'={y:?}",
                    a.components, b.components
                )),
                (Err(e), _) | (_, Err(e)) => {
                    diverged.push(format!("apply failed for a={:?} b={:?}: {e}", a.components, b.components))
                }
            }
        }
        if diverged.len() > 5 {
            break;
        }
    }
    assert!(
        diverged.is_empty(),
        "{} of {} exhaustive transform pairs failed, first ones:\n  {}",
        diverged.len(),
        ops.len() * ops.len(),
        diverged.join("\n  ")
    );
    println!("transform: all {checked} exhaustive small op pairs converged");
}

#[test]
fn compose_is_correct_for_every_small_op_pair() {
    // Enumerate valid consecutive pairs: `a` from the full component family,
    // `b` from the targeted edit family applied to `a`'s output.
    let base = "ab";
    let a_ops = small_op_family(base.chars().count());
    let mut correct = 0usize;
    let mut rejected: Vec<String> = Vec::new();
    let mut wrong: Vec<String> = Vec::new();

    for a in &a_ops {
        let Ok(mid) = a.apply(base) else { continue };
        for b in targeted_edits(mid.chars().count()) {
            let expected = match b.apply(&mid) {
                Ok(t) => t,
                Err(_) => continue,
            };
            match TextOp::compose(a, &b) {
                Ok(composed) => match composed.apply(base) {
                    Ok(got) if got == expected => correct += 1,
                    Ok(got) => wrong.push(format!(
                        "a={:?} b={:?}: composed.apply(S)={got:?} but b.apply(a.apply(S))={expected:?}",
                        a.components, b.components
                    )),
                    Err(e) => wrong.push(format!(
                        "a={:?} b={:?}: composed op {composed:?} does not apply to the base: {e}",
                        a.components, b.components
                    )),
                },
                Err(e) => rejected.push(format!("a={:?} b={:?}: {e}", a.components, b.components)),
            }
            if wrong.len() > 5 {
                break;
            }
        }
        if wrong.len() > 5 {
            break;
        }
    }

    assert!(
        wrong.is_empty(),
        "{} composed operations produced the wrong text, first ones:\n  {}",
        wrong.len(),
        wrong.join("\n  ")
    );
    assert!(
        rejected.is_empty(),
        "{} of {} valid consecutive op pairs were rejected by compose, first ones:\n  {}",
        rejected.len(),
        correct + rejected.len() + wrong.len(),
        rejected.join("\n  ")
    );
    println!("compose: all {correct} exhaustive small op pairs matched sequential application");
}

#[test]
fn compose_of_an_append_followed_by_a_delete_of_the_appended_text() {
    // The smallest failing case found by the exhaustive probe: append "YZ" to
    // "ab", then delete only the "Z". The composed operation should keep exactly
    // "abY".
    let base = "ab";
    let mut a = TextOp::new();
    a.retain(2);
    a.insert("YZ"); // "abYZ"
    let after_a = a.apply(base).unwrap();
    assert_eq!(after_a, "abYZ");

    let mut b = TextOp::new();
    b.retain(3); // "abY"
    b.remove(1); // drops "Z"
    let expected = b.apply(&after_a).unwrap();
    assert_eq!(expected, "abY");

    let composed = TextOp::compose(&a, &b).unwrap_or_else(|e| {
        panic!(
            "compose must accept an append followed by a partial delete of the \
             appended text, got: {e}; a={:?} b={:?} base={base:?}",
            a.components, b.components
        )
    });
    assert_eq!(
        composed.apply(base).unwrap(),
        expected,
        "compose(a, b).apply(S) != b.apply(a.apply(S)); composed={:?}",
        composed.components
    );
}

#[test]
fn compose_of_noop_and_delete_then_insert() {
    let base = "abcdef";
    let noop = diff(base, base);
    let mut a = TextOp::new();
    a.retain(2);
    a.remove(2); // abef
    a.retain(2);
    let mut b = TextOp::new();
    b.retain(2);
    b.insert("XY"); // abXYef
    b.retain(2);

    let with_noop = TextOp::compose(&noop, &a).expect("compose(noop, a)");
    assert_eq!(
        with_noop.apply(base).unwrap(),
        a.apply(base).unwrap(),
        "composing a leading no-op must be the identity"
    );
    let noop_last = TextOp::compose(&a, &diff(&a.apply(base).unwrap(), &a.apply(base).unwrap()))
        .expect("compose(a, noop)");
    assert_eq!(noop_last.apply(base).unwrap(), a.apply(base).unwrap());

    let composed = TextOp::compose(&a, &b).expect("compose delete-then-insert");
    assert_eq!(
        composed.apply(base).unwrap(),
        b.apply(&a.apply(base).unwrap()).unwrap()
    );
    assert_eq!(composed.apply(base).unwrap(), "abXYef");
}

// ------------------------------------------------------- 5. edge text -----

#[test]
fn edge_text_roundtrips() {
    let docs: Vec<(&str, String)> = vec![
        ("empty", String::new()),
        ("single newline", "\n".to_string()),
        ("three newlines", "\n\n\n".to_string()),
        ("nul bytes", "line\0one\nline\0two".to_string()),
        ("utf-8 BOM", "\u{feff}bom at the front\nsecond line".to_string()),
        ("crlf only", "a\r\nb\r\nc\r\n".to_string()),
        ("crlf empty lines", "\r\n\r\n".to_string()),
        (
            "many trailing newlines",
            "text ends here\n\n\n\n".to_string(),
        ),
        ("lone cr", "a\rb\rc".to_string()),
        ("astral only", "😀𝄞😀".to_string()),
        ("combining only", "e\u{301}e\u{301}".to_string()),
    ];

    for (name, doc) in docs {
        let len = char_len(&doc);
        // A mid-document insertion, a mid-document deletion, and a full rewrite.
        let candidates = [
            insert_at(&doc, len / 2, "Z"),
            if len > 0 {
                remove_at(&doc, len / 2, 1)
            } else {
                "now non-empty".to_string()
            },
            format!("rewritten:{doc}"),
        ];
        for (i, new) in candidates.iter().enumerate() {
            let op = diff(&doc, new);
            let applied = op
                .apply(&doc)
                .unwrap_or_else(|e| panic!("{name}: apply failed (variant {i}): {e}"));
            assert_eq!(applied, *new, "{name}: diff/apply mismatch (variant {i})");

            let lines = split_lines(&doc);
            let line_out = op
                .apply_to_lines(&lines)
                .unwrap_or_else(|e| panic!("{name}: apply_to_lines failed (variant {i}): {e}"));
            assert_eq!(
                line_out,
                split_lines(new),
                "{name}: apply_to_lines disagrees with apply (variant {i})"
            );
        }

        if name == "utf-8 BOM" {
            // The BOM is a character like any other: it must not be stripped.
            assert!(op_preserves_prefix(&doc), "diff dropped the BOM");
        }
    }
}

fn op_preserves_prefix(doc: &str) -> bool {
    let new = insert_at(doc, char_len(doc), "!");
    diff(doc, &new).apply(doc).unwrap().starts_with('\u{feff}')
}

#[test]
fn megabyte_single_line_edit_is_minimal() {
    let line: String = "abcdefghij".repeat(100_000); // 1 000 000 bytes, one line
    assert_eq!(line.len(), 1_000_000);
    assert_eq!(split_lines(&line).len(), 1, "the document is a single line");

    let mid = char_len(&line) / 2;
    let inserted = insert_at(&line, mid, "X");
    let op = diff(&line, &inserted);
    assert_eq!(
        op.change_size(),
        (1, 0),
        "one edit in a 1 MB single-line document must insert exactly one char"
    );
    assert!(
        op.components.len() <= 3,
        "components for a single insertion: {:?}",
        op.components
    );
    let bytes = wire_bytes(&op, &line);
    println!("edge/1MB single line: wire={bytes} bytes -> {}", to_wire_op(&op, &line));
    assert!(bytes < 80, "1 MB single-line insertion produced {bytes} wire bytes");
    assert_eq!(op.apply(&line).unwrap(), inserted);
    assert_eq!(
        op.apply_to_lines(&split_lines(&line)).unwrap().join("\n"),
        inserted,
        "apply_to_lines must agree with apply on a 1 MB single-line document"
    );

    let deleted = remove_at(&line, mid, 1);
    let del_op = diff(&line, &deleted);
    assert_eq!(del_op.change_size(), (0, 1));
    assert_eq!(del_op.apply(&line).unwrap(), deleted);

    // A 1 MB document that differs at *both* ends still round-trips. `diff` is
    // a prefix/suffix (single contiguous region) diff, so two disjoint changes
    // legitimately degenerate into a whole-document replacement — the point
    // here is correctness, not minimality.
    let both_ends = format!("{}{}{}", "S", &line[1..line.len() - 1], "E");
    let op = diff(&line, &both_ends);
    assert_eq!(op.apply(&line).unwrap(), both_ends);
    let (ins, del) = op.change_size();
    assert!(
        ins >= 1 && del >= 1,
        "changing both ends must still round-trip, got +{ins}/-{del}"
    );
    println!(
        "edge/1MB both ends: change_size=({ins},{del}) (single-region diff degenerates to a rewrite)"
    );
}

#[test]
fn component_helpers_keep_lengths_consistent() {
    let mut op = TextOp::new();
    op.retain(3);
    op.insert("xy");
    op.remove(2);
    op.retain(4);
    assert_eq!(op.base_len, 9, "base_len counts retains + removes");
    assert_eq!(op.target_len, 9, "target_len counts retains + inserts");

    let mut merged = TextOp::new();
    merged.retain(1);
    merged.retain(2);
    merged.insert("a");
    merged.insert("b");
    merged.remove(1);
    merged.remove(2);
    merged.normalise();
    assert_eq!(
        merged.components,
        vec![
            Component::Retain(3),
            Component::Insert("ab".into()),
            Component::Remove(3)
        ],
        "normalise must merge adjacent runs of the same kind"
    );
    assert_eq!(merged.change_size(), (2, 3));
    assert!(!merged.is_noop());

    // replace_all is delete-then-insert and applies to any document of that
    // length.
    let op = replace_all(5, "brand new");
    assert_eq!(op.apply("abcde").unwrap(), "brand new");
    assert_eq!(op.change_size(), (9, 5));
}
