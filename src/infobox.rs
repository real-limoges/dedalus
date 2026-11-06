//! Brace-matching `{{Infobox ...}}` parser for structured key-value extraction.
//!
//! Correctly handles nested `{{...}}` templates using depth tracking with
//! SIMD-accelerated `memchr` to skip over plain text between brace pairs.

use memchr::{memchr, memchr2};
use serde::{Deserialize, Serialize};

/// A parsed `{{Infobox ...}}` template with its type name and key-value fields.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Infobox {
    pub infobox_type: String,
    pub fields: Vec<(String, String)>,
}

/// Extracts all `{{Infobox ...}}` templates from article wikitext.
pub fn extract_infoboxes(text: &str) -> Vec<Infobox> {
    let mut results = Vec::new();
    let bytes = text.as_bytes();
    let mut search_from = 0;

    while let Some(pos) = find_infobox_start(&bytes[search_from..]) {
        let abs_pos = search_from + pos;
        if let Some(close_pos) = find_matching_close(bytes, abs_pos) {
            let inner = &text[abs_pos + 2..close_pos];
            if let Some(infobox) = parse_infobox_inner(inner) {
                results.push(infobox);
            }
            search_from = close_pos + 2;
        } else {
            search_from = abs_pos + 2;
        }
    }

    results
}

/// Byte offset of the first `{{Infobox` (case-insensitive, whitespace allowed
/// after the braces). Uses SIMD-accelerated memchr to jump between `{` candidates.
fn find_infobox_start(bytes: &[u8]) -> Option<usize> {
    let mut offset = 0;
    while let Some(pos) = memchr(b'{', &bytes[offset..]) {
        let i = offset + pos;
        if is_infobox_open(&bytes[i..]) {
            return Some(i);
        }
        offset = i + 1;
    }
    None
}

/// True if `bytes` starts with `{{`, optional whitespace, then the word
/// `infobox` ending at a space, underscore, newline, `|` or `}`.
fn is_infobox_open(bytes: &[u8]) -> bool {
    let word = b"infobox";
    let Some(after_braces) = bytes.strip_prefix(b"{{") else {
        return false;
    };
    let rest = after_braces.trim_ascii_start();
    if rest.len() < word.len() || !rest[..word.len()].eq_ignore_ascii_case(word) {
        return false;
    }
    match rest.get(word.len()) {
        None => true,
        Some(next) => matches!(next, b' ' | b'_' | b'\n' | b'\r' | b'|' | b'}'),
    }
}

/// Finds the matching `}}` for a `{{` starting at `start`.
/// Uses SIMD-accelerated memchr2 to skip over plain text between braces.
fn find_matching_close(bytes: &[u8], start: usize) -> Option<usize> {
    let mut depth: i32 = 0;
    let mut i = start;
    while i + 1 < bytes.len() {
        if bytes[i] == b'{' && bytes[i + 1] == b'{' {
            depth += 1;
            i += 2;
        } else if bytes[i] == b'}' && bytes[i + 1] == b'}' {
            depth -= 1;
            if depth == 0 {
                return Some(i);
            }
            i += 2;
        } else {
            // SIMD jump to next brace character
            match memchr2(b'{', b'}', &bytes[i..]) {
                Some(offset) if offset > 0 => i += offset,
                _ => i += 1,
            }
        }
    }
    None
}

fn parse_infobox_inner(inner: &str) -> Option<Infobox> {
    let segments = split_at_depth_zero(inner);
    if segments.is_empty() {
        return None;
    }

    let infobox_type = segments[0].trim().to_string();
    if infobox_type.is_empty() {
        return None;
    }

    let mut fields = Vec::new();
    for segment in &segments[1..] {
        let trimmed = segment.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some((key, value)) = split_field(trimmed)
            && !key.is_empty()
        {
            fields.push((key.to_string(), value.to_string()));
        }
    }

    Some(Infobox {
        infobox_type,
        fields,
    })
}

/// Splits `key = value` at the first `=`, unless that `=` sits inside a nested
/// template or link, in which case the segment is positional and has no key.
fn split_field(segment: &str) -> Option<(&str, &str)> {
    let eq_pos = segment.find('=')?;
    let key = &segment[..eq_pos];
    if key.contains("{{") || key.contains("[[") {
        return None;
    }
    Some((key.trim(), segment[eq_pos + 1..].trim()))
}

/// Splits on `|` outside any nested `{{ }}` template or `[[ ]]` link, so a
/// piped link such as `[[London|London, England]]` stays in one segment.
fn split_at_depth_zero(content: &str) -> Vec<&str> {
    let mut segments = Vec::new();
    let bytes = content.as_bytes();
    let mut template_depth: i32 = 0;
    let mut link_depth: i32 = 0;
    let mut last_split = 0;
    let mut i = 0;

    while i < bytes.len() {
        let pair = bytes.get(i..i + 2);
        if pair == Some(b"{{") {
            template_depth += 1;
            i += 2;
        } else if pair == Some(b"}}") {
            template_depth -= 1;
            i += 2;
        } else if pair == Some(b"[[") {
            link_depth += 1;
            i += 2;
        } else if pair == Some(b"]]") {
            link_depth -= 1;
            i += 2;
        } else if bytes[i] == b'|' && template_depth <= 0 && link_depth <= 0 {
            segments.push(&content[last_split..i]);
            last_split = i + 1;
            i += 1;
        } else {
            // Jump to the next byte that can change depth or split.
            let skip = bytes[i + 1..]
                .iter()
                .position(|b| matches!(b, b'{' | b'}' | b'[' | b']' | b'|'))
                .map_or(bytes.len() - i, |p| p + 1);
            i += skip;
        }
    }
    segments.push(&content[last_split..]);
    segments
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_simple_infobox() {
        let text = "{{Infobox person\n| name = John Doe\n| birth_date = 1990-01-01\n}}";
        let infoboxes = extract_infoboxes(text);
        assert_eq!(infoboxes.len(), 1);
        assert_eq!(infoboxes[0].infobox_type, "Infobox person");
        assert_eq!(infoboxes[0].fields.len(), 2);
        assert_eq!(infoboxes[0].fields[0].0, "name");
        assert_eq!(infoboxes[0].fields[0].1, "John Doe");
        assert_eq!(infoboxes[0].fields[1].0, "birth_date");
        assert_eq!(infoboxes[0].fields[1].1, "1990-01-01");
    }

    #[test]
    fn extract_infobox_with_nested_template() {
        let text = "{{Infobox person\n| name = John\n| birth_date = {{birth date|1990|1|1}}\n}}";
        let infoboxes = extract_infoboxes(text);
        assert_eq!(infoboxes.len(), 1);
        assert_eq!(infoboxes[0].fields.len(), 2);
        assert_eq!(infoboxes[0].fields[1].0, "birth_date");
        assert!(infoboxes[0].fields[1].1.contains("{{birth date|1990|1|1}}"));
    }

    #[test]
    fn extract_multiple_infoboxes() {
        let text =
            "{{Infobox person\n| name = A\n}}\nSome text\n{{Infobox settlement\n| name = B\n}}";
        let infoboxes = extract_infoboxes(text);
        assert_eq!(infoboxes.len(), 2);
        assert_eq!(infoboxes[0].infobox_type, "Infobox person");
        assert_eq!(infoboxes[1].infobox_type, "Infobox settlement");
    }

    #[test]
    fn extract_no_infobox() {
        let text = "This is a regular article with no infobox.";
        let infoboxes = extract_infoboxes(text);
        assert!(infoboxes.is_empty());
    }

    #[test]
    fn extract_lowercase_infobox() {
        let text = "{{infobox country\n| name = Testland\n}}";
        let infoboxes = extract_infoboxes(text);
        assert_eq!(infoboxes.len(), 1);
        assert_eq!(infoboxes[0].infobox_type, "infobox country");
    }

    #[test]
    fn extract_infobox_with_underscore() {
        let text = "{{Infobox_person\n| name = Test\n}}";
        let infoboxes = extract_infoboxes(text);
        assert_eq!(infoboxes.len(), 1);
        assert_eq!(infoboxes[0].infobox_type, "Infobox_person");
    }

    #[test]
    fn extract_infobox_empty_field_value() {
        let text = "{{Infobox person\n| name = \n| age = 30\n}}";
        let infoboxes = extract_infoboxes(text);
        assert_eq!(infoboxes[0].fields.len(), 2);
        assert_eq!(infoboxes[0].fields[0].0, "name");
        assert_eq!(infoboxes[0].fields[0].1, "");
    }

    #[test]
    fn unmatched_braces_returns_empty() {
        let text = "{{Infobox person\n| name = broken";
        let infoboxes = extract_infoboxes(text);
        assert!(infoboxes.is_empty());
    }

    #[test]
    fn does_not_match_non_infobox_templates() {
        let text = "{{cite web|url=http://example.com}} and {{reflist}}";
        let infoboxes = extract_infoboxes(text);
        assert!(infoboxes.is_empty());
    }

    #[test]
    fn infobox_with_surrounding_text() {
        let text = "Some intro text.\n{{Infobox person\n| name = Test\n}}\nMore text after.";
        let infoboxes = extract_infoboxes(text);
        assert_eq!(infoboxes.len(), 1);
        assert_eq!(infoboxes[0].fields[0].1, "Test");
    }

    #[test]
    fn find_matching_close_basic() {
        let text = b"{{hello}}";
        assert_eq!(find_matching_close(text, 0), Some(7));
    }

    #[test]
    fn find_matching_close_nested() {
        let text = b"{{outer {{inner}} end}}";
        assert_eq!(find_matching_close(text, 0), Some(21));
    }

    #[test]
    fn split_at_depth_zero_basic() {
        let result = split_at_depth_zero("a|b|c");
        assert_eq!(result, vec!["a", "b", "c"]);
    }

    #[test]
    fn split_at_depth_zero_nested() {
        let result = split_at_depth_zero("a|b={{x|y}}|c");
        assert_eq!(result, vec!["a", "b={{x|y}}", "c"]);
    }

    #[test]
    fn split_at_depth_zero_keeps_piped_links_whole() {
        let result = split_at_depth_zero("a|place=[[London|London, England]]|c");
        assert_eq!(result, vec!["a", "place=[[London|London, England]]", "c"]);
    }

    #[test]
    fn infobox_value_with_piped_link_is_not_truncated() {
        let text = "{{Infobox person\n| birth_place = [[London|London, England]]\n| name = X\n}}";
        let infoboxes = extract_infoboxes(text);
        assert_eq!(
            infoboxes[0].fields,
            vec![
                (
                    "birth_place".to_string(),
                    "[[London|London, England]]".to_string()
                ),
                ("name".to_string(), "X".to_string()),
            ]
        );
    }

    #[test]
    fn positional_template_parameter_has_no_key() {
        let text = "{{Infobox person\n| {{birth date|df=yes|1990}}\n| name = X\n}}";
        let infoboxes = extract_infoboxes(text);
        assert_eq!(
            infoboxes[0].fields,
            vec![("name".to_string(), "X".to_string())]
        );
    }

    #[test]
    fn infobox_with_space_after_braces_is_found() {
        let infoboxes = extract_infoboxes("{{ Infobox person\n| name = X\n}}");
        assert_eq!(infoboxes.len(), 1);
        assert_eq!(infoboxes[0].infobox_type, "Infobox person");
    }

    #[test]
    fn infobox_prefix_of_longer_word_is_ignored() {
        assert!(extract_infoboxes("{{Infoboxes are great}}").is_empty());
    }

    #[test]
    fn extract_infobox_with_non_ascii_before() {
        let text = "Ünîcödé text here.\n{{Infobox person\n| name = Test\n}}";
        let infoboxes = extract_infoboxes(text);
        assert_eq!(infoboxes.len(), 1);
        assert_eq!(infoboxes[0].infobox_type, "Infobox person");
        assert_eq!(infoboxes[0].fields[0].1, "Test");
    }

    #[test]
    fn infobox_serialization_roundtrip() {
        let infobox = Infobox {
            infobox_type: "Infobox person".to_string(),
            fields: vec![
                ("name".to_string(), "John".to_string()),
                ("age".to_string(), "30".to_string()),
            ],
        };
        let json = serde_json::to_string(&infobox).unwrap();
        let deserialized: Infobox = serde_json::from_str(&json).unwrap();
        assert_eq!(infobox, deserialized);
    }
}
