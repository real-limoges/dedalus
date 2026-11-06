//! Regex-based text extraction from Wikipedia article wikitext.
//!
//! Provides functions for extracting abstracts, section headings, see-also links,
//! categories, images, external links, and disambiguation detection. Uses
//! SIMD-accelerated `memchr` for fast template stripping.

use crate::title::normalize_title;
use memchr::memchr2;
use once_cell::sync::Lazy;
use regex::Regex;
use std::borrow::Cow;
use std::ops::Range;

// Link-shaped patterns exclude `[` and `]` from the target and the piped text,
// so `[[File:X.jpg|See [[Rust]] here]]` cannot swallow the inner `[[Rust]]`,
// and accept an empty piped part for the pipe trick (`[[Foo (bar)|]]`).

static CATEGORY_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\[\[\s*Category\s*:([^|\[\]]+?)(?:\|[^\[\]]*)?\]\]").unwrap());

static SECTION_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?m)^(={2,})\s*(.+?)\s*={2,}\s*$").unwrap());

static IMAGE_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)\[\[(?:File|Image):([^|\]]+?)(?:\|[^\]]*)*\]\]").unwrap());

static EXTERNAL_LINK_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\[(https?://\S+?)(?:\s[^\]]+)?\]").unwrap());

static DISAMBIG_REGEX: Lazy<Regex> = Lazy::new(|| {
    // The name must end at `|` or `}}`, so inline cleanup tags such as
    // `{{Disambiguation needed}}` on ordinary articles do not match.
    Regex::new(r"(?i)\{\{\s*(?:disambig(?:uation)?|dab|hndis|geodis|disamb|surname|given name|human name disambiguation|place name disambiguation|hospital disambiguation|airport disambiguation|letter-numbercombdisambig|school disambiguation|road disambiguation|biology disambiguation|taxonomy disambiguation|species latin name disambiguation|mathematical disambiguation|chemistry disambiguation|music disambiguation)\s*[|}]").unwrap()
});

static SEE_ALSO_HEADER: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?mi)^={2,}\s*See\s+also\s*={2,}\s*$").unwrap());

/// A level-2 heading (`== X ==`), which ends the section before it.
static LEVEL_TWO_HEADING: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?m)^==[^=]").unwrap());

pub static LINK_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\[\[([^|\[\]]+?)(?:\|[^\[\]]*)?\]\]").unwrap());

static HTML_COMMENT_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?s)<!--.*?-->").unwrap());

/// A footnote: self-closing `<ref name="a"/>` first, so it is never read as
/// the opening of a paired `<ref>...</ref>`.
static REF_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?is)<ref\b[^>]*/>|<ref\b[^>]*>.*?</ref\s*>").unwrap());

/// A bracketed external link, `[https://example.org label]`; group 1 is the label.
static BRACKETED_URL_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\[(?:https?:)?//[^\s\]]+(?:\s+([^\]]*))?\]").unwrap());

/// Returns the lead section (before the first `==` heading) as plain text:
/// templates, comments and footnotes removed, links shown as their visible
/// text, and file and category embeds dropped.
#[must_use]
pub fn extract_abstract(text: &str) -> String {
    // Strip templates first so headings inside {{Infobox ...}} don't truncate the lead.
    let stripped = strip_templates(text);

    let end_pos = SECTION_REGEX
        .find(&stripped)
        .map(|m| m.start())
        .unwrap_or(stripped.len());
    let lead = to_plain_text(&stripped[..end_pos]);

    // Build result string directly instead of collect + join to reduce allocations
    let mut result = String::with_capacity(lead.len());
    for line in lead.lines() {
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            if !result.is_empty() {
                result.push('\n');
            }
            result.push_str(trimmed);
        }
    }
    result
}

/// Renders template-free wikitext as the text a reader sees.
fn to_plain_text(wikitext: &str) -> String {
    let text = HTML_COMMENT_REGEX.replace_all(wikitext, "");
    let text = REF_REGEX.replace_all(&text, "");
    let text = render_wikilinks(&text);
    let text = BRACKETED_URL_REGEX.replace_all(&text, "$1");
    text.replace("'''", "").replace("''", "")
}

/// Replaces each `[[...]]` with its visible text. Nested links (inside an
/// image caption) are matched as a unit, and an unclosed `[[` drops the rest
/// of the text, as an unclosed template does in `strip_templates`.
fn render_wikilinks(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find("[[") {
        out.push_str(&rest[..start]);
        let inside = &rest[start + 2..];
        match closing_brackets(inside) {
            Some(end) => {
                out.push_str(link_text(&inside[..end]));
                rest = &inside[end + 2..];
            }
            None => return out,
        }
    }
    out.push_str(rest);
    out
}

/// The byte offset of the `]]` that closes a link whose `[[` came just
/// before `inside`, skipping any links nested within it.
fn closing_brackets(inside: &str) -> Option<usize> {
    let bytes = inside.as_bytes();
    let mut depth = 0usize;
    let mut i = 0;
    while i + 1 < bytes.len() {
        match (bytes[i], bytes[i + 1]) {
            (b'[', b'[') => {
                depth += 1;
                i += 2;
            }
            (b']', b']') if depth == 0 => return Some(i),
            (b']', b']') => {
                depth -= 1;
                i += 2;
            }
            _ => i += 1,
        }
    }
    None
}

/// The visible text of one link's inside (`Target`, `Target|Label`).
///
/// File, image, media and category links embed or tag rather than show
/// text, so they render as nothing; a leading `:` (`[[:Category:X]]`)
/// makes such a link an ordinary visible one. The pipe trick
/// (`[[Paris (band)|]]`) shows the target without its parenthetical.
fn link_text(inside: &str) -> &str {
    let (visible_link, inside) = match inside.strip_prefix(':') {
        Some(rest) => (true, rest),
        None => (false, inside),
    };
    if !visible_link && is_embed(inside) {
        return "";
    }
    match inside.split_once('|') {
        Some((_, label)) if !label.trim().is_empty() => label.trim(),
        Some((target, _)) => target.split(" (").next().unwrap_or(target).trim(),
        None => inside.trim(),
    }
}

fn is_embed(inside: &str) -> bool {
    inside.split_once(':').is_some_and(|(prefix, _)| {
        ["file", "image", "media", "category"]
            .iter()
            .any(|ns| prefix.trim().eq_ignore_ascii_case(ns))
    })
}

/// Extracts section heading names from the article text.
#[must_use]
pub fn extract_sections(text: &str) -> Vec<String> {
    SECTION_REGEX
        .captures_iter(text)
        .map(|c| c[2].trim().to_string())
        .collect()
}

/// Extracts category names from `[[Category:...]]` links, in canonical title
/// form, without duplicates, in order of first appearance.
#[must_use]
pub fn extract_categories(text: &str) -> Vec<Cow<'_, str>> {
    let names = CATEGORY_REGEX
        .captures_iter(text)
        .filter_map(|c| c.get(1).map(|m| normalize_title(m.as_str())));
    unique_nonempty(names)
}

/// Keeps the first occurrence of each non-empty item, in order.
fn unique_nonempty<'a>(items: impl Iterator<Item = Cow<'a, str>>) -> Vec<Cow<'a, str>> {
    let mut seen = std::collections::HashSet::new();
    items
        .filter(|s| !s.is_empty())
        .filter(|s| seen.insert(s.clone()))
        .collect()
}

/// Collapses newlines into spaces so CSV fields stay on a single line.
/// Returns `Cow::Borrowed` when no transformation is needed (>99% of inputs),
/// avoiding allocation for clean strings.
/// Uses SIMD-accelerated memchr2 for the fast-path check.
fn sanitize_field(s: &str) -> Cow<'_, str> {
    if memchr2(b'\n', b'\r', s.as_bytes()).is_none() {
        return Cow::Borrowed(s);
    }
    let mut result = String::with_capacity(s.len());
    let mut last_was_space = false;
    for c in s.chars() {
        if c == '\n' || c == '\r' || c.is_whitespace() {
            if !last_was_space && !result.is_empty() {
                result.push(' ');
                last_was_space = true;
            }
        } else {
            result.push(c);
            last_was_space = false;
        }
    }
    if result.ends_with(' ') {
        result.pop();
    }
    Cow::Owned(result)
}

/// Extracts image filenames from `[[File:...]]` and `[[Image:...]]` links,
/// in canonical title form, without duplicates, in order of first appearance.
#[must_use]
pub fn extract_images(text: &str) -> Vec<Cow<'_, str>> {
    let names = IMAGE_REGEX
        .captures_iter(text)
        .filter_map(|c| c.get(1).map(|m| normalize_title(m.as_str())));
    unique_nonempty(names)
}

/// Extracts URLs from `[http(s)://...]` external links, without duplicates,
/// in order of first appearance.
#[must_use]
pub fn extract_external_links(text: &str) -> Vec<Cow<'_, str>> {
    let urls = EXTERNAL_LINK_REGEX
        .captures_iter(text)
        .filter_map(|c| c.get(1).map(|m| sanitize_field(m.as_str().trim())));
    unique_nonempty(urls)
}

/// Byte range of the "See also" section: from its heading to the next
/// level-2 heading, or to the end of the text.
#[must_use]
pub fn see_also_section_range(text: &str) -> Option<Range<usize>> {
    let header = SEE_ALSO_HEADER.find(text)?;
    let end = LEVEL_TWO_HEADING
        .find_at(text, header.end())
        .map(|m| m.start())
        .unwrap_or(text.len());
    Some(header.start()..end)
}

/// Returns `true` if the article contains a disambiguation template.
#[must_use]
pub fn is_disambiguation(text: &str) -> bool {
    DISAMBIG_REGEX.is_match(text)
}

/// Strips `{{...}}` templates from text, handling nested braces.
/// Uses SIMD-accelerated memchr2 to skip over plain text between brace pairs.
fn strip_templates(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let bytes = text.as_bytes();
    let mut i = 0;
    let mut run_start = 0;

    // Outer loop: use SIMD to jump to the next '{' character
    while i < bytes.len() {
        match memchr2(b'{', b'}', &bytes[i..]) {
            None => break, // No more braces; the rest is plain text
            Some(offset) => {
                i += offset;
                if i + 1 < bytes.len() && bytes[i] == b'{' && bytes[i + 1] == b'{' {
                    // Found '{{': flush the text before it
                    if run_start < i {
                        result.push_str(&text[run_start..i]);
                    }
                    // Inner loop: find matching '}}' using SIMD to skip between braces
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
                            // SIMD jump to next brace inside template
                            match memchr2(b'{', b'}', &bytes[i..]) {
                                Some(inner_offset) if inner_offset > 0 => i += inner_offset,
                                _ => {
                                    i += 1;
                                }
                            }
                        }
                    }
                    // An unclosed template runs to the end of the text, so
                    // drop the rest. Stopping at the last byte instead would
                    // keep a stray byte and could split a UTF-8 character.
                    if depth != 0 {
                        i = bytes.len();
                    }
                    run_start = i;
                } else {
                    // Lone '{' or '}' is not a template marker; skip past it
                    i += 1;
                }
            }
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
    fn see_also_range_ends_at_next_level_two_heading() {
        let text = "Intro.\n== See also ==\n* [[Rust]]\n=== Sub ===\n* [[Go]]\n== References ==\n[[Oxford University Press]]";
        let range = see_also_section_range(text).unwrap();
        let section = &text[range];
        assert!(section.starts_with("== See also =="));
        assert!(section.contains("[[Go]]"));
        assert!(!section.contains("References"));
        assert!(!section.contains("Oxford"));
    }

    #[test]
    fn see_also_range_runs_to_end_when_last() {
        let text = "Intro.\n== See also ==\n* [[Rust]]";
        let range = see_also_section_range(text).unwrap();
        assert_eq!(range.end, text.len());
    }

    #[test]
    fn see_also_range_none_without_section() {
        let text = "No see also section.\n== References ==\nRefs.";
        assert!(see_also_section_range(text).is_none());
    }

    #[test]
    fn link_regex_finds_links_inside_image_captions() {
        let text = "[[File:X.jpg|thumb|See [[Rust]] here]]";
        let targets: Vec<&str> = LINK_REGEX
            .captures_iter(text)
            .map(|c| c.get(1).unwrap().as_str())
            .collect();
        assert_eq!(targets, vec!["Rust"]);
    }

    #[test]
    fn link_regex_accepts_pipe_trick() {
        let caps = LINK_REGEX.captures("[[Foo (bar)|]]").unwrap();
        assert_eq!(&caps[1], "Foo (bar)");
    }

    #[test]
    fn category_with_empty_sort_key() {
        assert_eq!(extract_categories("[[Category:Science|]]"), vec!["Science"]);
    }

    #[test]
    fn category_names_are_normalized_and_deduplicated() {
        let text = "[[Category:Living_people]] [[category: living people]] [[Category:Living people|Smith]]";
        assert_eq!(extract_categories(text), vec!["Living people"]);
    }

    #[test]
    fn category_leading_colon_is_a_link_not_a_category() {
        assert!(extract_categories("[[:Category:Physics]]").is_empty());
    }

    #[test]
    fn images_are_normalized_and_deduplicated() {
        let text = "[[File:rust_logo.svg]] [[Image:Rust logo.svg|thumb]]";
        assert_eq!(extract_images(text), vec!["Rust logo.svg"]);
    }

    #[test]
    fn external_links_are_deduplicated() {
        let text = "[https://example.com A] [https://example.com B]";
        assert_eq!(extract_external_links(text), vec!["https://example.com"]);
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
        assert_eq!(images, vec!["Lower.jpg", "Upper.png"]);
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
    fn disambiguation_needed_tag_is_not_a_disambiguation_page() {
        assert!(!is_disambiguation(
            "See [[Mercury]]{{Disambiguation needed|date=May 2020}}."
        ));
        assert!(is_disambiguation("{{Disambiguation|geo}}"));
        assert!(is_disambiguation("{{ disambiguation }}"));
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
    fn strip_templates_unclosed_drops_the_rest() {
        assert_eq!(strip_templates("{{unclosed template text after"), "");
        assert_eq!(strip_templates("before {{a}"), "before ");
    }

    #[test]
    fn strip_templates_unclosed_ending_in_multibyte_text() {
        assert_eq!(strip_templates("{{a é"), "");
        assert_eq!(strip_templates("Café {{note|naïve"), "Café ");
    }

    #[test]
    fn abstract_shows_links_as_their_visible_text() {
        assert_eq!(
            extract_abstract(
                "'''Rust''' is a [[programming language]] by [[Mozilla|the Mozilla people]], \
                 not [[Paris (band)|]], see [[:Category:Languages]]."
            ),
            "Rust is a programming language by the Mozilla people, not Paris, \
             see Category:Languages."
        );
    }

    #[test]
    fn abstract_drops_embeds_footnotes_comments_and_bare_urls() {
        let text = "[[File:Logo.svg|thumb|The [[Rust]] logo]]\n\
                    Lead<ref name=\"a\">Cite [[Book]]</ref> text<ref name=\"b\"/>.<!-- hidden -->\n\
                    [https://www.rust-lang.org Official site] and [https://example.org].\n\
                    [[Category:Languages]]";
        assert_eq!(extract_abstract(text), "Lead text.\nOfficial site and .");
    }

    #[test]
    fn abstract_drops_text_after_an_unclosed_link() {
        assert_eq!(extract_abstract("Lead [[Broken link text"), "Lead");
    }

    #[test]
    fn abstract_survives_unclosed_template_with_multibyte_text() {
        assert_eq!(
            extract_abstract("Lead text.\n{{Infobox|name=Zoë"),
            "Lead text."
        );
    }
}
