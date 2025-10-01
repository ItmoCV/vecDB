use std::hash::{DefaultHasher, Hash, Hasher};
use std::collections::HashMap;
use crate::core::objects::{Vector};
use serde::{Serialize, Deserialize};

// util types

pub struct VectorResponse {
    vectors: Vec<Vector>,
    score: Vec<i8>,
}

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
    pub hash_id: u64,
}

// utils func

pub fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}