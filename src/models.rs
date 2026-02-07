use crate::infobox::Infobox;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub enum PageType {
    Article,
    Redirect(String),
    Special,
}

#[derive(Debug, Clone)]
pub struct WikiPage {
    pub id: u32,
    pub title: String,
    pub page_type: PageType,
    /// `None` during the indexing pass (skip_text mode).
    pub text: Option<String>,
    /// Namespace number from `<ns>` tag.
    pub ns: Option<i32>,
    /// Revision timestamp from `<timestamp>` tag.
    pub timestamp: Option<String>,
}

fn is_false(v: &bool) -> bool {
    !*v
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct ArticleBlob {
    pub id: u32,
    pub title: String,
    pub abstract_text: String,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub categories: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub infoboxes: Vec<Infobox>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub sections: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub timestamp: Option<String>,
    #[serde(skip_serializing_if = "is_false", default)]
    pub is_disambiguation: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn article_blob_serialization() {
        let blob = ArticleBlob {
            id: 42,
            title: "Test Article".to_string(),
            abstract_text: "Hello world".to_string(),
            categories: vec![],
            infoboxes: vec![],
            sections: vec![],
            timestamp: None,
            is_disambiguation: false,
        };
        let json = serde_json::to_string(&blob).unwrap();
        assert!(json.contains("\"id\":42"));
        assert!(json.contains("\"title\":\"Test Article\""));
        assert!(json.contains("\"abstract_text\":\"Hello world\""));
        // Empty vecs and false bools should be omitted
        assert!(!json.contains("categories"));
        assert!(!json.contains("infoboxes"));
        assert!(!json.contains("sections"));
        assert!(!json.contains("timestamp"));
        assert!(!json.contains("is_disambiguation"));
    }

    #[test]
    fn article_blob_with_all_fields() {
        let blob = ArticleBlob {
            id: 42,
            title: "Test".to_string(),
            abstract_text: "Abstract".to_string(),
            categories: vec!["Science".to_string()],
            infoboxes: vec![Infobox {
                infobox_type: "Infobox person".to_string(),
                fields: vec![("name".to_string(), "Test".to_string())],
            }],
            sections: vec!["History".to_string()],
            timestamp: Some("2024-01-01T00:00:00Z".to_string()),
            is_disambiguation: true,
        };
        let json = serde_json::to_string(&blob).unwrap();
        let deserialized: ArticleBlob = serde_json::from_str(&json).unwrap();
        assert_eq!(blob, deserialized);
    }

    #[test]
    fn article_blob_roundtrip() {
        let original = ArticleBlob {
            id: 100,
            title: "Roundtrip Test".to_string(),
            abstract_text: "Content with special chars: <>&\"'".to_string(),
            categories: vec!["Test".to_string()],
            infoboxes: vec![],
            sections: vec![],
            timestamp: None,
            is_disambiguation: false,
        };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: ArticleBlob = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn article_blob_pretty_json() {
        let blob = ArticleBlob {
            id: 1,
            title: "Pretty".to_string(),
            abstract_text: "Content".to_string(),
            categories: vec![],
            infoboxes: vec![],
            sections: vec![],
            timestamp: None,
            is_disambiguation: false,
        };
        let json = serde_json::to_string_pretty(&blob).unwrap();
        assert!(json.contains('\n'));
        let deserialized: ArticleBlob = serde_json::from_str(&json).unwrap();
        assert_eq!(blob, deserialized);
    }

    #[test]
    fn article_blob_backward_compatible_deserialization() {
        // Minimal JSON without optional fields should deserialize with defaults
        let json = r#"{"id":1,"title":"Test","abstract_text":"Content"}"#;
        let blob: ArticleBlob = serde_json::from_str(json).unwrap();
        assert_eq!(blob.id, 1);
        assert!(blob.categories.is_empty());
        assert!(blob.infoboxes.is_empty());
        assert!(blob.sections.is_empty());
        assert!(blob.timestamp.is_none());
        assert!(!blob.is_disambiguation);
    }

    #[test]
    fn page_type_article() {
        let page = WikiPage {
            id: 1,
            title: "Test".to_string(),
            page_type: PageType::Article,
            text: Some("Content".to_string()),
            ns: None,
            timestamp: None,
        };
        assert!(matches!(page.page_type, PageType::Article));
    }

    #[test]
    fn page_type_redirect_holds_target() {
        let page = WikiPage {
            id: 2,
            title: "Old Name".to_string(),
            page_type: PageType::Redirect("New Name".to_string()),
            text: None,
            ns: None,
            timestamp: None,
        };
        match &page.page_type {
            PageType::Redirect(target) => assert_eq!(target, "New Name"),
            _ => panic!("Expected Redirect variant"),
        }
    }

    #[test]
    fn page_type_special() {
        let page = WikiPage {
            id: 3,
            title: "File:Example.jpg".to_string(),
            page_type: PageType::Special,
            text: None,
            ns: None,
            timestamp: None,
        };
        assert!(matches!(page.page_type, PageType::Special));
    }

    #[test]
    fn wiki_page_optional_text() {
        let with_text = WikiPage {
            id: 1,
            title: "A".to_string(),
            page_type: PageType::Article,
            text: Some("content".to_string()),
            ns: None,
            timestamp: None,
        };
        let without_text = WikiPage {
            id: 2,
            title: "B".to_string(),
            page_type: PageType::Article,
            text: None,
            ns: None,
            timestamp: None,
        };
        assert!(with_text.text.is_some());
        assert!(without_text.text.is_none());
    }
}
