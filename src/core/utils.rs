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

/// Вычисляет хеш с улучшенным распределением для шардирования
pub fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    let hash = s.finish();
    
    // Применяем дополнительное перемешивание для лучшего распределения
    // Используем MurmurHash3 финализатор для улучшения распределения
    let mut h = hash;
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
    h ^= h >> 33;
    h
}