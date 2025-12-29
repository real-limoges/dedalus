#![allow(dead_code)]

use serde::Serialize;

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
    pub text: Option<String> // the first pass doesn't use this
}

#[derive(Serialize)]
pub struct ArticleBlob {
    pub id: u32,
    pub title: String,
    pub text: String,
}
