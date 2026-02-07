use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Infobox {
    pub infobox_type: String,
    pub fields: Vec<(String, String)>,
}

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

/// Case-insensitive search on raw bytes to preserve byte offsets with non-ASCII text.
fn find_infobox_start(bytes: &[u8]) -> Option<usize> {
    let needle = b"{{infobox";
    if bytes.len() < needle.len() {
        return None;
    }
    for i in 0..=bytes.len() - needle.len() {
        if bytes[i..i + needle.len()]
            .iter()
            .zip(needle.iter())
            .all(|(a, b)| a.to_ascii_lowercase() == *b)
        {
            let next_idx = i + needle.len();
            if next_idx >= bytes.len() {
                return Some(i);
            }
            let next = bytes[next_idx];
            if next == b' '
                || next == b'_'
                || next == b'\n'
                || next == b'\r'
                || next == b'|'
                || next == b'}'
            {
                return Some(i);
            }
        }
    }
    None
}

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
            i += 1;
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
        if let Some(eq_pos) = trimmed.find('=') {
            let key = trimmed[..eq_pos].trim().to_string();
            let value = trimmed[eq_pos + 1..].trim().to_string();
            if !key.is_empty() {
                fields.push((key, value));
            }
        }
    }

    Some(Infobox {
        infobox_type,
        fields,
    })
}

/// Splits on `|` at brace depth 0, respecting nested `{{ }}`.
fn split_at_depth_zero(content: &str) -> Vec<&str> {
    let mut segments = Vec::new();
    let bytes = content.as_bytes();
    let mut depth: i32 = 0;
    let mut last_split = 0;
    let mut i = 0;

    while i < bytes.len() {
        if i + 1 < bytes.len() && bytes[i] == b'{' && bytes[i + 1] == b'{' {
            depth += 1;
            i += 2;
        } else if i + 1 < bytes.len() && bytes[i] == b'}' && bytes[i + 1] == b'}' {
            depth -= 1;
            i += 2;
        } else if bytes[i] == b'|' && depth == 0 {
            segments.push(&content[last_split..i]);
            last_split = i + 1;
            i += 1;
        } else {
            i += 1;
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
