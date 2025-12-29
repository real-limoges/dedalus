#![allow(dead_code)]

use std::collections::HashMap;

pub struct WikiIndex {
    title_to_id: HashMap<String, u32>,
    redirects: HashMap<String, String>,
}
