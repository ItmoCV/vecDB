use std::hash::{DefaultHasher, Hash, Hasher};
use std::collections::HashMap;
use serde::{Serialize, Deserialize};

// util types


#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct StorageVector {
    pub data: Vec<f32>,
    pub timestamp: i64,
    pub metadata: HashMap<String, String>,
    pub hash_id: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct StorageCollection {
    pub name: String,
    pub id: u64,
    pub lsh_metric: String, // Сохраняем как строку для сериализации
    pub vector_dimension: usize,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct StorageBucket {
    pub id: u64,
    pub created_at: i64,
    pub updated_at: i64,
}

// utils func

pub fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}