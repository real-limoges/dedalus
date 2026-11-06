//! Canonical page titles, following MediaWiki's title rules.
//!
//! Wikitext writes the same page many ways: `[[computer]]`, `[[Computer]]`,
//! `[[ Computer ]]`, `[[Computer#History]]`. MediaWiki treats underscores as
//! spaces, collapses runs of whitespace, ignores a leading colon and the
//! section anchor, and uppercases the first character. `normalize_title`
//! applies those rules so every title in the index, every link target and
//! every category or image name is compared in one form.

use std::borrow::Cow;

/// Namespaces whose pages are not articles. Compared case-insensitively.
const NON_ARTICLE_NAMESPACES: &[&str] = &[
    "Category",
    "File",
    "Image",
    "Media",
    "Template",
    "Wikipedia",
    "WP",
    "Help",
    "Portal",
    "Draft",
    "User",
    "Module",
    "MediaWiki",
    "Special",
    "Talk",
    "TimedText",
];

/// Returns the canonical form of a title.
///
/// Drops a `#section` anchor and a leading `:`, turns `_` into a space,
/// collapses whitespace runs to one space, trims, and uppercases the first
/// character. Borrows when the title is already canonical, which is the
/// common case for titles taken from the dump itself.
#[must_use]
pub fn normalize_title(raw: &str) -> Cow<'_, str> {
    let without_anchor = raw.split('#').next().unwrap_or(raw);
    if is_canonical(without_anchor) {
        return Cow::Borrowed(without_anchor);
    }

    let spaced = without_anchor.replace('_', " ");
    let collapsed = spaced.split_whitespace().collect::<Vec<_>>().join(" ");
    let without_colon = collapsed
        .strip_prefix(':')
        .unwrap_or(&collapsed)
        .trim_start();
    Cow::Owned(uppercase_first(without_colon))
}

/// `normalize_title` for an owned title, reusing its allocation when the
/// title is already canonical (a borrowed result is always a prefix of it).
#[must_use]
pub fn into_canonical(title: String) -> String {
    let canonical_len = match normalize_title(&title) {
        Cow::Owned(normalized) => return normalized,
        Cow::Borrowed(prefix) => prefix.len(),
    };
    let mut title = title;
    title.truncate(canonical_len);
    title
}

/// True if a normalized title names a page outside the article namespace,
/// such as `Category:Physics` or `file:Logo.png`.
#[must_use]
pub fn is_namespace_link(title: &str) -> bool {
    match title.split_once(':') {
        Some((prefix, _)) => {
            let prefix = prefix.trim();
            let base = prefix
                .strip_suffix(" talk")
                .or_else(|| prefix.strip_suffix(" Talk"))
                .unwrap_or(prefix);
            NON_ARTICLE_NAMESPACES
                .iter()
                .any(|ns| ns.eq_ignore_ascii_case(base))
        }
        None => false,
    }
}

/// True if `normalize_title` would leave this (anchor-free) title unchanged.
fn is_canonical(title: &str) -> bool {
    let starts_cleanly = match title.chars().next() {
        None => true,
        Some(first) => !first.is_lowercase() && !first.is_whitespace() && first != ':',
    };
    starts_cleanly
        && !title.ends_with(char::is_whitespace)
        && !title.contains('_')
        && !title.contains("  ")
        && !title.contains(|c: char| c.is_whitespace() && c != ' ')
}

fn uppercase_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_title_is_borrowed() {
        assert!(matches!(normalize_title("Rust"), Cow::Borrowed("Rust")));
        assert!(matches!(
            normalize_title("United States"),
            Cow::Borrowed("United States")
        ));
    }

    #[test]
    fn lowercase_first_letter_is_uppercased() {
        assert_eq!(normalize_title("computer"), "Computer");
        assert_eq!(normalize_title("éclair"), "Éclair");
    }

    #[test]
    fn underscores_become_spaces() {
        assert_eq!(normalize_title("United_States"), "United States");
    }

    #[test]
    fn whitespace_is_trimmed_and_collapsed() {
        assert_eq!(normalize_title("  Rust  "), "Rust");
        assert_eq!(normalize_title("New   York"), "New York");
        assert_eq!(normalize_title("New\nYork"), "New York");
    }

    #[test]
    fn section_anchor_is_dropped() {
        assert_eq!(normalize_title("Rust#History"), "Rust");
        assert_eq!(normalize_title("rust_lang#Syntax"), "Rust lang");
    }

    #[test]
    fn leading_colon_is_dropped() {
        assert_eq!(normalize_title(":Category:Physics"), "Category:Physics");
        assert_eq!(normalize_title(": rust"), "Rust");
    }

    #[test]
    fn empty_title_stays_empty() {
        assert_eq!(normalize_title(""), "");
        assert_eq!(normalize_title("#Anchor only"), "");
        assert_eq!(normalize_title("   "), "");
    }

    #[test]
    fn into_canonical_matches_normalize_title() {
        assert_eq!(into_canonical("Rust".to_string()), "Rust");
        assert_eq!(into_canonical("Rust#History".to_string()), "Rust");
        assert_eq!(into_canonical("united_states".to_string()), "United states");
    }

    #[test]
    fn namespace_links_are_detected_case_insensitively() {
        assert!(is_namespace_link("Category:Physics"));
        assert!(is_namespace_link("CATEGORY:Physics"));
        assert!(is_namespace_link("File:Logo.png"));
        assert!(is_namespace_link("Talk:Rust"));
        assert!(is_namespace_link("User talk:Someone"));
        assert!(is_namespace_link("Special:Random"));
    }

    #[test]
    fn article_titles_are_not_namespace_links() {
        assert!(!is_namespace_link("Rust"));
        assert!(!is_namespace_link("Star Wars: A New Hope"));
        assert!(!is_namespace_link("Mission: Impossible"));
    }
}
