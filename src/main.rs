use std::collections::HashMap;

use crate::core::objects::Metadata;

pub mod core;

fn main() {
    let mut new_data = HashMap::new();
    new_data.insert("name".to_string(), "kek".to_string());
    let meta = Metadata::new(new_data);
    println!("{}", meta.to_string());
}