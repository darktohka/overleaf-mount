//! Integration tests for the wire codec (`overleaf_mount::sync`):
//! `to_wire_op`, `parse_wire_op` and `utf16_len`.
//!
//! The single most important property here is that positions on the wire are
//! **UTF-16 code units**, because `document-updater` is JavaScript and indexes
//! with string positions. A Rust `char` is a Unicode scalar value, so every
//! astral character (emoji, U+1D100-range symbols, some CJK extensions) is one
//! `char` but two code units. Getting this wrong does not fail loudly — edits
//! silently land a character to the left of where the user typed — so it is
//! asserted directly, with exact numbers, not just via round-trips.

use overleaf_mount::ot::{diff, replace_all, Component, TextOp};
use overleaf_mount::sync::{parse_wire_op, to_wire_op, utf16_len};
use serde_json::{json, Value};

// ------------------------------------------------------------------ helpers --

/// Deterministic xorshift64* PRNG (self-contained: no `rand` needed).
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
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

/// Astral-heavy alphabet: the wire codec is only interesting when the base
/// contains characters whose `char` length differs from their UTF-16 length.
const ASTRAL_ALPHABET: &[&str] = &[
    "a", "b", " ", "\n", "日", "😀", "𝄞", "🎼", "e\u{301}", "\\", "\"",
];

fn random_text(rng: &mut Rng, max_tokens: usize) -> String {
    let n = rng.below(max_tokens + 1);
    let mut out = String::new();
    for _ in 0..n {
        out.push_str(ASTRAL_ALPHABET[rng.below(ASTRAL_ALPHABET.len())]);
    }
    out
}

fn mutate(rng: &mut Rng, s: &str, edits: usize) -> String {
    let mut chars: Vec<char> = s.chars().collect();
    for _ in 0..edits {
        if !chars.is_empty() && rng.chance(45) {
            let p = rng.below(chars.len());
            chars.remove(p);
        } else {
            let p = rng.below(chars.len() + 1);
            let token = ASTRAL_ALPHABET[rng.below(ASTRAL_ALPHABET.len())];
            for ch in token.chars().rev() {
                chars.insert(p, ch);
            }
        }
    }
    chars.into_iter().collect()
}

/// Mutate with a random number of edits (avoids double-borrowing the PRNG in
/// argument position).
fn mutate_rand(rng: &mut Rng, s: &str) -> String {
    let n = 1 + rng.below(3);
    mutate(rng, s, n)
}

fn char_len(s: &str) -> usize {
    s.chars().count()
}

fn entries(wire: &Value) -> Vec<Value> {
    wire.as_array()
        .unwrap_or_else(|| panic!("expected a JSON array from the wire codec, got {wire}"))
        .clone()
}

/// A single-insertion op at `char` index `idx`.
fn insert_op(base: &str, idx: usize, text: &str) -> TextOp {
    let mut op = TextOp::new();
    op.retain(idx);
    op.insert(text);
    op.retain(char_len(base) - idx);
    op
}

// ---------------------------------------------------- 1. UTF-16 positions --

#[test]
fn utf16_len_counts_code_units() {
    assert_eq!(utf16_len(""), 0);
    assert_eq!(utf16_len("abc"), 3);
    assert_eq!(utf16_len("日本語"), 3, "BMP CJK is one code unit each");
    assert_eq!(utf16_len("😀"), 2, "astral characters are surrogate pairs");
    assert_eq!(utf16_len("𝄞"), 2);
    assert_eq!(utf16_len("a😀b"), 4);
    assert_eq!(utf16_len("e\u{301}"), 2, "a combining sequence is two code units");
    assert_eq!(
        utf16_len("😀😀"),
        4,
        "utf16_len must not be confused with char count"
    );
}

#[test]
fn wire_positions_are_utf16_after_astral_characters() {
    // (base, char index of the insertion, expected `p` in UTF-16 units)
    let cases: &[(&str, usize, usize)] = &[
        ("a😀b", 2, 3),      // 'a'=1 + emoji=2
        ("a𝄞b", 2, 3),
        ("日本語", 2, 2),     // BMP only: units == chars
        ("😀😀ab", 3, 5),     // char index 3 is before 'b': 2+2+1 = 5 units
        ("e\u{301}x", 2, 2), // 'e' + combining acute
        ("😀", 0, 0),
        ("😀", 1, 2),
    ];

    for (base, idx, expect_p) in cases {
        let prefix: String = base.chars().take(*idx).collect();
        assert_eq!(
            utf16_len(&prefix),
            *expect_p,
            "test fixture error: prefix {prefix:?} is not {expect_p} units"
        );

        let op = insert_op(base, *idx, "X");
        let target = op.apply(base).unwrap();
        let wire = to_wire_op(&op, base);
        let es = entries(&wire);
        assert_eq!(es.len(), 1, "one insertion must produce one entry: {wire}");
        let p = es[0]
            .get("p")
            .and_then(|p| p.as_u64())
            .unwrap_or_else(|| panic!("entry has no numeric p: {}", es[0])) as usize;
        assert_eq!(
            p, *expect_p,
            "base {base:?}, insertion after char {idx}: wire position must be {expect_p} \
             UTF-16 units, got {p} (wire {wire})"
        );
        // The char index and the wire position differ exactly for astral text.
        if utf16_len(base) != char_len(base) && *idx > 0 {
            assert_ne!(
                p, *idx,
                "base {base:?}: the test would not prove anything if p == char index"
            );
        }

        let back = parse_wire_op(&wire, base)
            .unwrap_or_else(|e| panic!("parsing our own wire op failed for {base:?}: {e}"));
        assert_eq!(
            back.apply(base).unwrap(),
            target,
            "round-trip through UTF-16 positions changed the result for {base:?}"
        );
        assert_eq!(back.change_size(), (1, 0));
    }

    // Stated explicitly, as in the task specification:
    let op = insert_op("a😀b", 2, "X");
    assert_eq!(
        to_wire_op(&op, "a😀b"),
        json!([{ "p": 3, "i": "X" }]),
        "inserting between 😀 and b in \"a😀b\" must use p=3, not p=2"
    );
}

#[test]
fn parse_server_op_after_astral_character_applies_correctly() {
    // A payload as the browser would send it: the document is "😀hello" and the
    // user typed "!" right after the emoji, i.e. at UTF-16 offset 2.
    let base = "😀hello";
    let wire = json!([{ "p": 2, "i": "!" }]);
    let op = parse_wire_op(&wire, base).expect("server op must parse");
    assert_eq!(op.apply(base).unwrap(), "😀!hello");
    assert_eq!(op.base_len, char_len(base));
    assert_eq!(op.change_size(), (1, 0));

    // The next case separates "unit offset" from "char index" unambiguously:
    // reading p=4 as a char index would index past the end of "😀😀ab".
    let base = "😀😀ab";
    // Units: 😀=[0,2) 😀=[2,4) a=[4,5) b=[5,6). Insert before 'a' => p=4.
    let wire = json!([{ "p": 4, "i": "|" }]);
    let op = parse_wire_op(&wire, base).expect("server op must parse");
    assert_eq!(op.apply(base).unwrap(), "😀😀|ab");

    // A server op with several entries after an astral character: insert "1"
    // at unit 2 (after the first emoji), then delete the *second* emoji, which
    // now starts at unit 4 because "1" occupies unit 2..3.
    let base = "😀日😀x";
    let wire = json!([{ "p": 2, "i": "1" }, { "p": 4, "d": "😀" }]);
    let op = parse_wire_op(&wire, base).expect("multi-entry server op must parse");
    assert_eq!(
        op.apply(base).unwrap(),
        "😀1日x",
        "multi-entry op with astral positions applied to the wrong characters"
    );
}

#[test]
fn wire_delete_of_an_astral_character_names_exactly_that_character() {
    let base = "a😀b";
    let mut op = TextOp::new();
    op.retain(1);
    op.remove(1); // one char == the emoji == two UTF-16 units
    op.retain(1);
    assert_eq!(op.apply(base).unwrap(), "ab");

    let wire = to_wire_op(&op, base);
    let es = entries(&wire);
    assert_eq!(es.len(), 1, "one deletion must produce one entry: {wire}");
    assert_eq!(
        es[0].get("d").and_then(|d| d.as_str()),
        Some("😀"),
        "the delete entry must spell out the whole emoji, not half a surrogate pair: {wire}"
    );
    assert_eq!(
        utf16_len(es[0]["d"].as_str().unwrap()),
        2,
        "the deleted text is two UTF-16 units"
    );

    let back = parse_wire_op(&wire, base).expect("delete op must parse");
    assert_eq!(
        back.change_size(),
        (0, 1),
        "deleting an astral character removes one char (two units): {:?}",
        back.components
    );
    assert_eq!(back.apply(base).unwrap(), "ab");
}

#[test]
fn wire_rejects_a_position_inside_a_surrogate_pair() {
    // "a😀b": unit offsets 0 (before 'a'), 1 (before 😀), 3 (between 😀 and 'b'),
    // 4 (end). Offset 2 is *inside* the surrogate pair and must be rejected
    // rather than silently rounding.
    let base = "a😀b";
    for wire in [
        json!([{ "p": 2, "i": "X" }]),
        json!([{ "p": 2, "d": "b" }]),
    ] {
        let err = parse_wire_op(&wire, base)
            .err()
            .unwrap_or_else(|| panic!("p=2 splits a surrogate pair, expected Err for {wire}"));
        let msg = err.to_string();
        assert!(
            msg.contains("surrogate"),
            "error should explain the surrogate-pair split, got: {msg}"
        );
    }

    // The component form carries offsets too, so it must reject it as well.
    let err = parse_wire_op(&json!([2, { "i": "X" }]), base)
        .err()
        .expect("component retain of 2 units splits the surrogate pair");
    assert!(
        err.to_string().contains("surrogate"),
        "component form error should mention the surrogate pair, got: {err}"
    );

    // Sanity: the neighbouring offsets are accepted.
    for p in [0u64, 1, 3, 4] {
        let wire = json!([{ "p": p, "i": "X" }]);
        assert!(
            parse_wire_op(&wire, base).is_ok(),
            "valid UTF-16 offset {p} was rejected for {base:?}"
        );
    }
}

#[test]
fn wire_rejects_a_position_past_the_end() {
    assert!(
        parse_wire_op(&json!([{ "p": 5, "i": "X" }]), "abc").is_err(),
        "p=5 is past the end of \"abc\""
    );
    assert!(
        parse_wire_op(&json!([{ "p": 6, "i": "X" }]), "a😀b").is_err(),
        "p=6 is past the end of \"a😀b\" (4 units)"
    );
    // p == len is legal: it is an append.
    let op = parse_wire_op(&json!([{ "p": 3, "i": "!" }]), "abc").expect("p == len is an append");
    assert_eq!(op.apply("abc").unwrap(), "abc!");
    let op = parse_wire_op(&json!([{ "p": 4, "i": "!" }]), "a😀b").expect("p == len is an append");
    assert_eq!(op.apply("a😀b").unwrap(), "a😀b!");

    // Deletions past the end are rejected too.
    assert!(
        parse_wire_op(&json!([{ "p": 1, "d": "xyz" }]), "abc").is_err(),
        "deleting 3 chars at offset 1 of \"abc\" runs past the end"
    );
}

#[test]
fn wire_roundtrip_for_random_astral_edits() {
    let mut rng = Rng::new(0xA57A_1BEE_F00D);
    let mut cases = 0usize;
    for case in 0..350 {
        let base = random_text(&mut rng, 14);
        let target = if case % 3 == 0 {
            random_text(&mut rng, 14)
        } else {
            mutate_rand(&mut rng, &base)
        };
        let op = diff(&base, &target);

        let wire = to_wire_op(&op, &base);
        let es = entries(&wire);
        assert_eq!(
            es.len(),
            op.components
                .iter()
                .filter(|c| !matches!(c, Component::Retain(_)))
                .count(),
            "case {case}: one wire entry per insert/remove component, base={base:?}"
        );

        // Positional entries must be ordered: the server validates them.
        let mut last_p = 0u64;
        for entry in &es {
            let p = entry
                .get("p")
                .and_then(|p| p.as_u64())
                .unwrap_or_else(|| panic!("case {case}: entry without p: {entry}"));
            assert!(
                p >= last_p,
                "case {case}: wire positions are not non-decreasing ({last_p} then {p}) in {wire}"
            );
            last_p = p;
            assert!(
                entry.get("i").is_some() || entry.get("d").is_some(),
                "case {case}: entry with neither i nor d: {entry}"
            );
        }

        let parsed = parse_wire_op(&wire, &base).unwrap_or_else(|e| {
            panic!("case {case}: parsing our own wire op failed for base={base:?} wire={wire}: {e}")
        });
        assert_eq!(
            parsed.change_size(),
            op.change_size(),
            "case {case}: change size changed in the round-trip, base={base:?}"
        );
        let got = parsed.apply(&base).unwrap_or_else(|e| {
            panic!("case {case}: applying the parsed op failed for base={base:?}: {e}")
        });
        assert_eq!(
            got, target,
            "case {case}: wire round-trip changed the text\n  base={base:?}\n  target={target:?}\n  wire={wire}"
        );
        cases += 1;
    }
    assert!(cases >= 300, "expected at least 300 wire round-trips, ran {cases}");
    println!("wire round-trip: {cases} random astral edits recovered exactly");
}

// ---------------------------------------------------------- 2. wire shapes --

#[test]
fn positional_roundtrip_insert_then_delete() {
    // "AAAA BBBB CCCC": insert "xyz" after "AA", then delete " BBBB".
    let base = "AAAA BBBB CCCC";
    assert_eq!(char_len(base), 14);
    let mut op = TextOp::new();
    op.retain(2);
    op.insert("xyz");
    op.retain(2);
    op.remove(5); // " BBBB"
    op.retain(5); // " CCCC"
    let target = op.apply(base).unwrap();
    assert_eq!(target, "AAxyzAA CCCC");

    let wire = to_wire_op(&op, base);
    let es = entries(&wire);
    assert_eq!(es.len(), 2, "expected one insert and one delete entry: {wire}");
    assert_eq!(
        es[0].get("p").and_then(|p| p.as_u64()),
        Some(2),
        "the insertion is at offset 2: {wire}"
    );
    assert_eq!(
        es[1].get("p").and_then(|p| p.as_u64()),
        Some(7),
        "the delete position (2 + 3 inserted + 2 retained) must account for the insert: {wire}"
    );
    assert_eq!(
        es[1].get("d").and_then(|d| d.as_str()),
        Some(" BBBB"),
        "the delete entry must name the removed text: {wire}"
    );

    let back = parse_wire_op(&wire, base).expect("must parse");
    assert_eq!(back.apply(base).unwrap(), target);
    assert_eq!(back.change_size(), op.change_size());
}

#[test]
fn positional_roundtrip_delete_then_insert_deeper() {
    let base = "0123456789";
    let mut op = TextOp::new();
    op.remove(2); // deletes "01"
    op.retain(4); // keeps "2345"
    op.insert("Z");
    op.retain(4);
    let target = op.apply(base).unwrap();
    assert_eq!(target, "2345Z6789");

    let wire = to_wire_op(&op, base);
    let es = entries(&wire);
    assert_eq!(es.len(), 2, "expected one delete and one insert entry: {wire}");
    assert_eq!(es[0].get("d").and_then(|d| d.as_str()), Some("01"));
    assert_eq!(
        es[1].get("p").and_then(|p| p.as_u64()),
        Some(4),
        "after deleting 2 units the insertion point is 4: {wire}"
    );

    let back = parse_wire_op(&wire, base).expect("must parse");
    assert_eq!(back.apply(base).unwrap(), target);
}

#[test]
fn component_form_with_objects_parses() {
    // insert "x" at the start, then delete the first three chars of the base.
    let wire = json!([{ "i": "x" }, { "d": "abc" }]);
    let op = parse_wire_op(&wire, "abcdef").expect("component form must parse");
    assert_eq!(op.apply("abcdef").unwrap(), "xdef");
    assert_eq!(op.change_size(), (1, 3));

    // retain 3, insert "X", and keep the rest.
    let wire = json!([3, { "i": "X" }]);
    let op = parse_wire_op(&wire, "abcde").expect("component form must parse");
    assert_eq!(op.apply("abcde").unwrap(), "abcXde");
    assert_eq!(op.change_size(), (1, 0));
}

#[test]
fn component_form_with_bare_string_component_parses() {
    // This is the canonical sharejs component form: a bare string is an insert.
    // Base "abcdefg": retain 3, insert "x", remove 2 ("de"), keep "fg".
    let wire = json!([3, "x", -2]);
    let op = parse_wire_op(&wire, "abcdefg")
        .unwrap_or_else(|e| panic!("component form [3,\"x\",-2] must parse, got: {e}"));
    assert_eq!(op.apply("abcdefg").unwrap(), "abcxfg");
    assert_eq!(op.change_size(), (1, 2));
}

#[test]
fn component_numerics_count_utf16_units() {
    // Base "😀b" is 3 UTF-16 units but 2 chars. Retaining 2 units must retain
    // the whole emoji (one char), not one unit.
    let wire = json!([2, { "i": "X" }]);
    let op = parse_wire_op(&wire, "😀b").expect("component form must parse");
    assert_eq!(
        op.apply("😀b").unwrap(),
        "😀Xb",
        "numeric components must count UTF-16 units, not chars"
    );
    assert_eq!(op.base_len, 2, "base_len is still a char count");
    assert_eq!(op.change_size(), (1, 0));

    // A negative numeric component is a deletion, also in units: retain 2
    // units keeps the first emoji, then -2 removes the second one.
    let wire = json!([2, -2]);
    let op = parse_wire_op(&wire, "😀😀").expect("negative component must parse");
    assert_eq!(
        op.apply("😀😀").unwrap(),
        "😀",
        "retain 2 units keeps one emoji; remove 2 units deletes one whole emoji"
    );
    assert_eq!(op.change_size(), (0, 1));

    // Deleting all four units removes both emoji.
    let op = parse_wire_op(&json!([-4]), "😀😀").expect("negative component must parse");
    assert_eq!(
        op.apply("😀😀").unwrap(),
        "",
        "removing 4 UTF-16 units removes two astral chars"
    );
    assert_eq!(op.change_size(), (0, 2));
}

#[test]
fn overlapping_positional_ops_are_rejected() {
    let base = "abcdefghij";
    let wire = json!([{ "p": 5, "i": "X" }, { "p": 3, "i": "Y" }]);
    let err = parse_wire_op(&wire, base)
        .err()
        .expect("a positional entry before the cursor must be rejected");
    assert!(
        err.to_string().contains("overlap"),
        "error should mention the overlap, got: {err}"
    );

    // A deletion that would start before the cursor (because a previous insert
    // moved it) is equally invalid.
    let wire = json!([{ "p": 4, "i": "XY" }, { "p": 2, "d": "ab" }]);
    assert!(
        parse_wire_op(&wire, base).is_err(),
        "p=2 after an insert at p=4 is behind the cursor"
    );

    // Equal positions are legal (insert at p, then delete at the same p).
    let wire = json!([{ "p": 4, "i": "XY" }, { "p": 6, "d": "ef" }]);
    let op = parse_wire_op(&wire, base).expect("p equal to the cursor is legal");
    assert_eq!(op.apply(base).unwrap(), "abcdXYghij");
}

#[test]
fn malformed_positional_entries_are_rejected() {
    // An entry with a position but neither i nor d.
    let err = parse_wire_op(&json!([{ "p": 1 }]), "abc")
        .err()
        .expect("an entry with neither i nor d must be rejected");
    assert!(
        err.to_string().contains("i/d") || err.to_string().contains("without"),
        "error should say the entry has no i/d, got: {err}"
    );

    // An entry with no position at all.
    assert!(
        parse_wire_op(&json!([{ "i": "x" }, { "q": 1 }]), "abc").is_err(),
        "an entry without p must be rejected"
    );

    // A non-string, non-number component.
    assert!(
        parse_wire_op(&json!([true]), "abc").is_err(),
        "a boolean component must be rejected"
    );
}

#[test]
fn unrecognised_shapes_are_rejected() {
    for bad in [
        json!({ "foo": 1 }),
        json!("hello"),
        json!(42),
        json!(null),
        json!({ "op": "not-an-array" }),
        json!([{ "p": "one", "i": "x" }]),
    ] {
        assert!(
            parse_wire_op(&bad, "abc").is_err(),
            "shape {bad} should have been rejected"
        );
    }

    // A wrapped `{"op": [...]}` payload is accepted (history-OT shape).
    let wrapped = json!({ "op": [{ "p": 1, "i": "X" }] });
    let op = parse_wire_op(&wrapped, "abc").expect("{\"op\": [...]} must parse");
    assert_eq!(op.apply("abc").unwrap(), "aXbc");
}

#[test]
fn trailing_retain_is_added_when_the_wire_op_stops_early() {
    // sharejs omits the trailing retain; the client must add it or the op
    // cannot be applied at all.
    let op = parse_wire_op(&json!([{ "p": 1, "i": "X" }]), "abcdef").expect("must parse");
    assert_eq!(
        op.base_len, 6,
        "base_len must cover the whole document, got {} (components {:?})",
        op.base_len, op.components
    );
    assert!(
        matches!(op.components.last(), Some(Component::Retain(5))),
        "expected a trailing Retain(5), got {:?}",
        op.components
    );
    assert_eq!(op.apply("abcdef").unwrap(), "aXbcdef");

    // An empty array means "no change" and must still cover the document.
    let noop = parse_wire_op(&json!([]), "abcdef").expect("empty op must parse");
    assert!(noop.is_noop(), "an empty op list is a no-op: {:?}", noop.components);
    assert_eq!(noop.base_len, 6);
    assert_eq!(noop.apply("abcdef").unwrap(), "abcdef");
}

#[test]
fn whole_document_replacement_roundtrips() {
    let base = "old text with 😀 and 日本語\nsecond line";
    let new = "completely different\ncontent 🎼\n";
    let op = replace_all(char_len(base), new);
    assert_eq!(op.apply(base).unwrap(), new);

    let wire = to_wire_op(&op, base);
    let es = entries(&wire);
    assert_eq!(es.len(), 2, "replace-all is one delete plus one insert: {wire}");
    assert_eq!(
        es[0].get("d").and_then(|d| d.as_str()),
        Some(base),
        "the delete entry must spell out the whole old document"
    );

    let back = parse_wire_op(&wire, base).expect("replace-all must round-trip");
    assert_eq!(back.apply(base).unwrap(), new);
    assert_eq!(back.change_size(), op.change_size());
    assert_eq!(back.base_len, char_len(base));
}

#[test]
fn wire_op_is_small_for_a_small_edit_in_a_120kb_document() {
    let mut doc = String::with_capacity(120 * 1024 + 16);
    let mut i = 0usize;
    while doc.len() < 120 * 1024 {
        if i > 0 {
            doc.push('\n');
        }
        doc.push_str(&format!("l{i:05} the quick brown fox jumps over the lazy dog 0123456789"));
        i += 1;
    }
    assert_eq!(utf16_len(&doc), doc.len(), "fixture must be pure ASCII");

    let pos = char_len(&doc) / 2;
    let mut new_doc: String = doc.chars().take(pos).collect();
    new_doc.push_str("HELLO");
    new_doc.extend(doc.chars().skip(pos));

    let op = diff(&doc, &new_doc);
    assert_eq!(op.change_size(), (5, 0));

    let wire = to_wire_op(&op, &doc);
    let es = entries(&wire);
    assert_eq!(
        es.len(),
        1,
        "a 5-char insertion must be exactly one positional entry, got {wire}"
    );
    let bytes = wire.to_string().len();
    println!(
        "efficiency/120KB: doc={} bytes, wire={bytes} bytes -> {wire}",
        doc.len()
    );
    assert!(
        bytes < 120,
        "wire op for a 5-char edit in a 120 KB document was {bytes} bytes: {wire}"
    );

    let back = parse_wire_op(&wire, &doc).expect("must parse");
    assert_eq!(
        back.change_size(),
        (5, 0),
        "the recovered op must insert 5 and delete 0, got {:?} from {wire}",
        back.change_size()
    );
    assert_eq!(back.apply(&doc).unwrap(), new_doc);
}

#[test]
fn wire_op_for_a_deletion_spells_out_the_text() {
    // document-updater validates `d`, so a purely numeric delete is not enough.
    let base = "0123456789";
    let op = diff(base, "012345789");
    let wire = to_wire_op(&op, base);
    let es = entries(&wire);
    assert_eq!(es.len(), 1, "one deletion entry expected: {wire}");
    assert_eq!(
        es[0].get("d").and_then(|d| d.as_str()),
        Some("6"),
        "the delete must name the removed character: {wire}"
    );
    assert_eq!(es[0].get("i"), None, "a deletion carries no i: {wire}");
    let back = parse_wire_op(&wire, base).unwrap();
    assert_eq!(back.change_size(), (0, 1));
    assert_eq!(back.apply(base).unwrap(), "012345789");
}

#[test]
fn wire_op_for_a_noop_is_empty() {
    let base = "unchanged document";
    let op = diff(base, base);
    assert!(op.is_noop());
    let wire = to_wire_op(&op, base);
    assert_eq!(wire, json!([]), "a no-op has no positional entries");
    let back = parse_wire_op(&wire, base).unwrap();
    assert!(back.is_noop());
    assert_eq!(back.apply(base).unwrap(), base);
}
