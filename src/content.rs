use once_cell::sync::Lazy;
use regex::Regex;

static CATEGORY_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\[\[Category:([^|\]]+?)(?:\|[^\]]+)?\]\]").unwrap());

static SECTION_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?m)^(={2,})\s*(.+?)\s*={2,}\s*$").unwrap());

static IMAGE_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\[\[(?:File|Image):([^|\]]+?)(?:\|[^\]]*)*\]\]").unwrap());

static EXTERNAL_LINK_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\[(https?://\S+?)(?:\s[^\]]+)?\]").unwrap());

static DISAMBIG_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\{\{(?:disambig(?:uation)?|dab|hndis|geodis|disamb|surname|given name|human name disambiguation|place name disambiguation|hospital disambiguation|airport disambiguation|letter-numbercombdisambig|school disambiguation|road disambiguation|biology disambiguation|taxonomy disambiguation|species latin name disambiguation|mathematical disambiguation|chemistry disambiguation|music disambiguation)\b").unwrap()
});

static SEE_ALSO_HEADER: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?mi)^={2,}\s*See\s+also\s*={2,}\s*$").unwrap());

static NEXT_SECTION_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?m)^={2,}\s*[^=]").unwrap());

pub static LINK_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\[\[([^|\]]+?)(?:\|[^\]]+)?\]\]").unwrap());

/// Returns the lead section (before the first `==` heading) with templates stripped.
pub fn extract_abstract(text: &str) -> String {
    // Strip templates first so headings inside {{Infobox ...}} don't truncate the lead.
    let stripped = strip_templates(text);

    let end_pos = SECTION_REGEX
        .find(&stripped)
        .map(|m| m.start())
        .unwrap_or(stripped.len());

    stripped[..end_pos]
        .lines()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

pub fn extract_sections(text: &str) -> Vec<String> {
    SECTION_REGEX
        .captures_iter(text)
        .map(|c| c[2].trim().to_string())
        .collect()
}

pub fn extract_see_also_links(text: &str) -> Vec<String> {
    let see_also_match = match SEE_ALSO_HEADER.find(text) {
        Some(m) => m,
        None => return Vec::new(),
    };

    let after_header = &text[see_also_match.end()..];

    let section_end = NEXT_SECTION_REGEX
        .find(after_header)
        .map(|m| m.start())
        .unwrap_or(after_header.len());

    let see_also_text = &after_header[..section_end];

    LINK_REGEX
        .captures_iter(see_also_text)
        .map(|c| c[1].trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

pub fn extract_categories(text: &str) -> Vec<String> {
    CATEGORY_REGEX
        .captures_iter(text)
        .map(|c| sanitize_field(c[1].trim()))
        .filter(|s| !s.is_empty())
        .collect()
}

/// Collapses newlines into spaces so CSV fields stay on a single line.
fn sanitize_field(s: &str) -> String {
    if s.contains('\n') || s.contains('\r') {
        s.replace(['\n', '\r'], " ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    } else {
        s.to_string()
    }
}

pub fn extract_images(text: &str) -> Vec<String> {
    IMAGE_REGEX
        .captures_iter(text)
        .map(|c| sanitize_field(c[1].trim()))
        .filter(|s| !s.is_empty())
        .collect()
}

pub fn extract_external_links(text: &str) -> Vec<String> {
    EXTERNAL_LINK_REGEX
        .captures_iter(text)
        .map(|c| sanitize_field(c[1].trim()))
        .filter(|s| !s.is_empty())
        .collect()
}

/// Byte offset of the "See also" header, for position-based edge classification.
pub fn see_also_section_start(text: &str) -> Option<usize> {
    SEE_ALSO_HEADER.find(text).map(|m| m.start())
}

pub fn is_disambiguation(text: &str) -> bool {
    DISAMBIG_REGEX.is_match(text)
}

fn strip_templates(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let bytes = text.as_bytes();
    let mut i = 0;
    let mut run_start = 0;

    while i < bytes.len() {
        if i + 1 < bytes.len() && bytes[i] == b'{' && bytes[i + 1] == b'{' {
            if run_start < i {
                result.push_str(&text[run_start..i]);
            }
            let mut depth: i32 = 0;
            while i + 1 < bytes.len() {
                if bytes[i] == b'{' && bytes[i + 1] == b'{' {
                    depth += 1;
                    i += 2;
                } else if bytes[i] == b'}' && bytes[i + 1] == b'}' {
                    depth -= 1;
                    if depth == 0 {
                        i += 2;
                        break;
                    }
                    i += 2;
                } else {
                    i += 1;
                }
            }
            run_start = i;
        } else {
            i += 1;
        }
    }

    if run_start < bytes.len() {
        result.push_str(&text[run_start..]);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn category_simple() {
        let cats = extract_categories("[[Category:Science]]");
        assert_eq!(cats, vec!["Science"]);
    }

    #[test]
    fn category_with_sort_key() {
        let cats = extract_categories("[[Category:People|Smith, John]]");
        assert_eq!(cats, vec!["People"]);
    }

    #[test]
    fn category_multiple() {
        let text = "[[Category:Science]]\n[[Category:Physics]]";
        let cats = extract_categories(text);
        assert_eq!(cats, vec!["Science", "Physics"]);
    }

    #[test]
    fn category_newlines_sanitized() {
        let text = "[[Category:Explorers from n\nNew France]]";
        let cats = extract_categories(text);
        assert_eq!(cats, vec!["Explorers from n New France"]);
    }

    #[test]
    fn category_does_not_match_regular_links() {
        let cats = extract_categories("[[Rust]] and [[Python]]");
        assert!(cats.is_empty());
    }

    #[test]
    fn sections_basic() {
        let text = "Intro\n== History ==\nSome history\n== See also ==\nLinks\n";
        let sections = extract_sections(text);
        assert_eq!(sections, vec!["History", "See also"]);
    }

    #[test]
    fn sections_nested_levels() {
        let text = "== Level 2 ==\n=== Level 3 ===\n== Another ==\n";
        let sections = extract_sections(text);
        assert_eq!(sections, vec!["Level 2", "Level 3", "Another"]);
    }

    #[test]
    fn sections_none() {
        let text = "Just a paragraph with no headings.";
        let sections = extract_sections(text);
        assert!(sections.is_empty());
    }

    #[test]
    fn see_also_basic() {
        let text = "Intro text.\n== History ==\nSome history.\n== See also ==\n* [[Rust]]\n* [[Python]]\n== References ==\nRefs here.";
        let links = extract_see_also_links(text);
        assert_eq!(links, vec!["Rust", "Python"]);
    }

    #[test]
    fn see_also_none() {
        let text = "No see also section here.\n== References ==\nRefs.";
        let links = extract_see_also_links(text);
        assert!(links.is_empty());
    }

    #[test]
    fn see_also_at_end() {
        let text = "Intro.\n== See also ==\n* [[Rust]]";
        let links = extract_see_also_links(text);
        assert_eq!(links, vec!["Rust"]);
    }

    #[test]
    fn images_basic() {
        let text = "[[File:Example.jpg|thumb|Caption]] and [[Image:Logo.png]]";
        let images = extract_images(text);
        assert_eq!(images, vec!["Example.jpg", "Logo.png"]);
    }

    #[test]
    fn images_none() {
        let text = "No images here, just [[a link]].";
        let images = extract_images(text);
        assert!(images.is_empty());
    }

    #[test]
    fn images_case_insensitive() {
        let text = "[[file:lower.jpg]] and [[IMAGE:upper.png]]";
        let images = extract_images(text);
        assert_eq!(images, vec!["lower.jpg", "upper.png"]);
    }

    #[test]
    fn external_links_basic() {
        let text = "[https://example.com Example] and [http://test.org Test Site]";
        let links = extract_external_links(text);
        assert_eq!(links, vec!["https://example.com", "http://test.org"]);
    }

    #[test]
    fn external_links_none() {
        let text = "No external links, just [[internal links]].";
        let links = extract_external_links(text);
        assert!(links.is_empty());
    }

    #[test]
    fn disambiguation_true() {
        assert!(is_disambiguation("{{disambiguation}}"));
        assert!(is_disambiguation("{{Disambiguation}}"));
        assert!(is_disambiguation("{{disambig}}"));
        assert!(is_disambiguation("{{dab}}"));
        assert!(is_disambiguation("{{surname}}"));
        assert!(is_disambiguation("{{given name}}"));
        assert!(is_disambiguation("{{geodis}}"));
        assert!(is_disambiguation("{{hndis}}"));
    }

    #[test]
    fn disambiguation_false() {
        assert!(!is_disambiguation("Regular article text."));
        assert!(!is_disambiguation("{{cite web|url=...}}"));
    }

    #[test]
    fn abstract_before_heading() {
        let text = "This is the abstract.\n\n== History ==\nSome history.";
        let abs = extract_abstract(text);
        assert_eq!(abs, "This is the abstract.");
    }

    #[test]
    fn abstract_strips_templates() {
        let text = "{{Infobox person|name=Test}}\nThis is the abstract.\n== Section ==\n";
        let abs = extract_abstract(text);
        assert_eq!(abs, "This is the abstract.");
    }

    #[test]
    fn abstract_no_headings() {
        let text = "Just a simple article with no headings.";
        let abs = extract_abstract(text);
        assert_eq!(abs, "Just a simple article with no headings.");
    }

    #[test]
    fn abstract_empty_lead() {
        let text = "== Section ==\nContent.";
        let abs = extract_abstract(text);
        assert_eq!(abs, "");
    }

    #[test]
    fn strip_templates_basic() {
        let result = strip_templates("{{template}} text after");
        assert_eq!(result, " text after");
    }

    #[test]
    fn strip_templates_nested() {
        let result = strip_templates("{{outer {{inner}} end}} text");
        assert_eq!(result, " text");
    }

    #[test]
    fn strip_templates_no_templates() {
        let result = strip_templates("plain text");
        assert_eq!(result, "plain text");
    }

    #[test]
    fn strip_templates_multiple() {
        let result = strip_templates("{{a}} middle {{b}} end");
        assert_eq!(result, " middle  end");
    }

    #[test]
    fn see_also_section_start_found() {
        let text = "Intro.\n== History ==\nSome history.\n== See also ==\n* [[Rust]]";
        let start = see_also_section_start(text);
        assert!(start.is_some());
        assert!(text[start.unwrap()..].starts_with("== See also =="));
    }

    #[test]
    fn see_also_section_start_not_found() {
        let text = "No see also section.\n== References ==\nRefs.";
        assert!(see_also_section_start(text).is_none());
    }

    #[test]
    fn strip_templates_unclosed_does_not_hang() {
        let result = strip_templates("{{unclosed template text after");
        assert!(!result.contains("unclosed"));
    }
}
