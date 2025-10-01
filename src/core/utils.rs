use std::hash::{DefaultHasher, Hash, Hasher};
use std::collections::HashMap;
use crate::core::objects::{Vector};
use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};

// util types

pub struct VectorResponse {
    vectors: Vec<Vector>,
    score: Vec<i8>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct StorageVector {
    pub data: Vec<u32>,
    pub timestamp: DateTime<Utc>,
    pub meta_hash_id: u64,
    pub hash_id: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct StorageMetadata {
    pub data: HashMap<String, String>,
    pub vector_hash_id: String,
    pub hash_id: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct StorageCollection {
    pub name: String,
    pub hash_id: u64,
    pub vector_length: u8,
    pub metrics: String
}

// utils func

pub fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}