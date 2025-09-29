use std::collections::HashMap;

use crate::core::objects::Metadata;
use crate::vectors::embeddings::make_embeddings;

pub mod core;
pub mod vectors;


fn main() {
    let mut new_data = HashMap::new();
    new_data.insert("name".to_string(), "kek".to_string());
    let meta = Metadata::new(Some(new_data));
    println!("{}", meta.to_string());

    match make_embeddings("English") {
        Ok(embeddings) => {println!("Embedding dimension: {}", embeddings.len())}
        Err(_e) => {}
    }
}