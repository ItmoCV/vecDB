use std::hash::{DefaultHasher, Hash, Hasher};

use crate::core::objects::Vector;

// util types

pub struct VectorResponse {
    vectors: Vec<Vector>,
    score: Vec<i8>,
}

// utils func

pub fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}