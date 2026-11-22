//! Integration tests for `overleaf_mount::overleaf::flatten_lines`.
//!
//! `joinDoc`/`get_doc` payloads are *usually* `lines: string[]`, but the sharejs
//! document model can hand back one array of characters per line. A document
//! must load to exactly the same text either way — the old shortcut that
//! treated an array line as an empty line silently blanked such documents, so
//! both shapes are pinned here.

use overleaf_mount::overleaf::flatten_lines;
use serde_json::{json, Value};

fn strings(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

#[test]
fn plain_string_lines_pass_through() {
    let lines = vec![json!("a"), json!("b")];
    assert_eq!(
        flatten_lines(&lines),
        strings(&["a", "b"]),
        "string lines must be copied verbatim"
    );

    let lines = vec![json!("first line"), json!(""), json!("third")];
    assert_eq!(flatten_lines(&lines), strings(&["first line", "", "third"]));
}

#[test]
fn char_array_lines_are_concatenated() {
    let lines = vec![json!(["a", "b"]), json!(["c"])];
    assert_eq!(
        flatten_lines(&lines),
        strings(&["ab", "c"]),
        "an array line is the concatenation of its character strings"
    );
}

#[test]
fn nulls_inside_char_array_lines_are_skipped() {
    let lines = vec![json!(["a", Value::Null, "b"])];
    assert_eq!(
        flatten_lines(&lines),
        strings(&["ab"]),
        "null placeholders in a character array must not become text"
    );

    // A null first/last character is equally harmless.
    let lines = vec![json!([Value::Null, "x", Value::Null])];
    assert_eq!(flatten_lines(&lines), strings(&["x"]));
}

#[test]
fn empty_and_non_string_entries_become_empty_lines() {
    let lines = vec![json!([]), json!("x"), Value::Null];
    assert_eq!(
        flatten_lines(&lines),
        strings(&["", "x", ""]),
        "an empty array is an empty line; null is an empty line"
    );

    // Numbers/booleans are not text: they degrade to an empty line rather than
    // panicking or producing \"42\".
    let lines = vec![json!(42), json!(true), json!("keep")];
    assert_eq!(flatten_lines(&lines), strings(&["", "", "keep"]));
}

#[test]
fn mixed_nested_and_plain_lines() {
    let lines = vec![
        json!("plain"),
        json!(["n", "e", "s", "t"]),
        json!([]),
        json!(["a", Value::Null, "b"]),
        json!(["日本語"]),
        json!("tail"),
    ];
    assert_eq!(
        flatten_lines(&lines),
        strings(&["plain", "nest", "", "ab", "日本語", "tail"]),
        "mixed shapes must be flattened independently, in order"
    );
}

#[test]
fn flatten_lines_of_empty_payload_is_empty() {
    assert_eq!(flatten_lines(&[]), Vec::<String>::new());
}

#[test]
fn document_text_matches_between_char_array_and_string_shapes() {
    // The same logical document, once as `string[]` and once as `(string[])[]`.
    let logical = ["hello", "", "wörld", "😀 line", "日本語のテキスト", "end"];

    let string_shape: Vec<Value> = logical.iter().map(|l| json!(l)).collect();

    // Each line becomes an array of its characters; a null is sprinkled in to
    // mimic sharejs's representation of skipped positions. Note that an astral
    // character is one element here (JSON lone surrogates are rejected by
    // serde_json, so a code-unit split cannot be expressed in this shape).
    let char_shape: Vec<Value> = logical
        .iter()
        .map(|line| {
            let mut parts: Vec<Value> = line.chars().map(|c| json!(c.to_string())).collect();
            if parts.len() > 2 {
                parts.insert(1, Value::Null);
            }
            Value::Array(parts)
        })
        .collect();

    let from_strings = flatten_lines(&string_shape);
    let from_chars = flatten_lines(&char_shape);

    assert_eq!(
        from_chars, from_strings,
        "the char-array shape and the string shape must load to identical lines"
    );
    assert_eq!(
        from_chars, strings(&logical),
        "flattened lines lost or reordered content"
    );
    assert_eq!(
        from_chars.join("\n"),
        logical.join("\n"),
        "the document text must be identical regardless of wire shape"
    );

    // And the text survives a per-line multi-byte payload unchanged.
    assert_eq!(
        from_strings[3], "😀 line",
        "astral characters must survive verbatim"
    );
}

#[test]
fn char_array_shape_roundtrips_through_the_line_model() {
    // Splitting the joined document back into lines reproduces the payload:
    // flatten_lines and split("\n") are inverse for these inputs.
    use overleaf_mount::ot::split_lines;

    let logical = ["one", "two", "", "three"];
    let char_shape: Vec<Value> = logical
        .iter()
        .map(|line| Value::Array(line.chars().map(|c| json!(c.to_string())).collect()))
        .collect();

    let text = flatten_lines(&char_shape).join("\n");
    assert_eq!(text, "one\ntwo\n\nthree");
    assert_eq!(split_lines(&text), flatten_lines(&char_shape));
}
