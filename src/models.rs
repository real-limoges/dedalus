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
    pub text: Option<String>, // the first pass doesn't use this
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct ArticleBlob {
    pub id: u32,
    pub title: String,
    pub text: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn article_blob_serialization() {
        let blob = ArticleBlob {
            id: 42,
            title: "Test Article".to_string(),
            text: "Hello world".to_string(),
        };
        let json = serde_json::to_string(&blob).unwrap();
        assert!(json.contains("\"id\":42"));
        assert!(json.contains("\"title\":\"Test Article\""));
        assert!(json.contains("\"text\":\"Hello world\""));
    }

    #[test]
    fn article_blob_deserialization() {
        let json = r#"{"id":42,"title":"Test Article","text":"Hello world"}"#;
        let blob: ArticleBlob = serde_json::from_str(json).unwrap();
        assert_eq!(blob.id, 42);
        assert_eq!(blob.title, "Test Article");
        assert_eq!(blob.text, "Hello world");
    }

    #[test]
    fn article_blob_roundtrip() {
        let original = ArticleBlob {
            id: 100,
            title: "Roundtrip Test".to_string(),
            text: "Content with special chars: <>&\"'".to_string(),
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
            text: "Content".to_string(),
        };
        let json = serde_json::to_string_pretty(&blob).unwrap();
        assert!(json.contains('\n'));
        let deserialized: ArticleBlob = serde_json::from_str(&json).unwrap();
        assert_eq!(blob, deserialized);
    }

    #[test]
    fn page_type_article() {
        let page = WikiPage {
            id: 1,
            title: "Test".to_string(),
            page_type: PageType::Article,
            text: Some("Content".to_string()),
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
        };
        let without_text = WikiPage {
            id: 2,
            title: "B".to_string(),
            page_type: PageType::Article,
            text: None,
        };
        assert!(with_text.text.is_some());
        assert!(without_text.text.is_none());
    }
}
