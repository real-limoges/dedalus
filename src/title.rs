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

    Cow::Owned(canonicalize(without_anchor))
}

/// The slow path of `normalize_title`, built in one `String`: words split at
/// `_` and whitespace, joined by single spaces, one leading `:` dropped, and
/// the first character uppercased.
fn canonicalize(title: &str) -> String {
    let mut out = String::with_capacity(title.len());
    let words = title
        .split(|c: char| c == '_' || c.is_whitespace())
        .filter(|word| !word.is_empty());
    for word in words {
        if !out.is_empty() {
            out.push(' ');
        }
        out.push_str(word);
    }
    let colon_and_space = out
        .strip_prefix(':')
        .map(|rest| out.len() - rest.trim_start().len());
    if let Some(len) = colon_and_space {
        out.drain(..len);
    }
    if let Some(first) = out.chars().next() {
        out.replace_range(..first.len_utf8(), &first.to_uppercase().to_string());
    }
    out
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

/// True if `normalize_title` would leave this (anchor-free) title unchanged:
/// it does not start with a lowercase letter, whitespace or `:`, and has no
/// `_`, no whitespace other than single spaces, and no trailing space.
/// One pass over the title, since nearly every title from the dump is canonical.
fn is_canonical(title: &str) -> bool {
    let mut chars = title.chars();
    let Some(first) = chars.next() else {
        return true;
    };
    if first.is_lowercase() || first.is_whitespace() || first == ':' || first == '_' {
        return false;
    }
    let mut after_space = false;
    for c in chars {
        if c == '_' || (c.is_whitespace() && (c != ' ' || after_space)) {
            return false;
        }
        after_space = c == ' ';
    }
    !after_space
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

    /// A simple, multi-allocation `normalize_title`. The single-pass version
    /// must agree with it.
    fn reference_normalize(raw: &str) -> String {
        let without_anchor = raw.split('#').next().unwrap_or(raw);
        let starts_cleanly = match without_anchor.chars().next() {
            None => true,
            Some(first) => !first.is_lowercase() && !first.is_whitespace() && first != ':',
        };
        if starts_cleanly
            && !without_anchor.ends_with(char::is_whitespace)
            && !without_anchor.contains('_')
            && !without_anchor.contains("  ")
            && !without_anchor.contains(|c: char| c.is_whitespace() && c != ' ')
        {
            return without_anchor.to_string();
        }
        let spaced = without_anchor.replace('_', " ");
        let collapsed = spaced.split_whitespace().collect::<Vec<_>>().join(" ");
        let without_colon = collapsed
            .strip_prefix(':')
            .unwrap_or(&collapsed)
            .trim_start();
        let mut chars = without_colon.chars();
        match chars.next() {
            Some(first) => first.to_uppercase().chain(chars).collect(),
            None => String::new(),
        }
    }

    #[test]
    fn normalize_title_agrees_with_the_reference() {
        let inputs = [
            "",
            " ",
            "_",
            ":",
            "::Foo",
            ": foo",
            " :foo",
            "foo",
            "Foo",
            "foo_bar  baz ",
            "\tfoo\nbar",
            "Foo\u{a0}Bar",
            "Foo  Bar",
            "Foo ",
            " Foo",
            "_Foo",
            "Foo_",
            "ß-Bahn",
            "ǆungla",
            "ÿ",
            "élan vital",
            "Rust#History",
            "#Anchor",
            "Foo:bar",
            "Category:physics",
            "a",
            "1st",
            "İstanbul",
            "ǅ",
        ];
        for raw in inputs {
            assert_eq!(
                normalize_title(raw),
                reference_normalize(raw),
                "input {raw:?}"
            );
        }
    }
}
