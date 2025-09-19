use std::{collections::HashMap};
use crate::core::utils::{calculate_hash, StorageMetadata, StorageVector};
use serde::{Serialize, Deserialize};
use std::fmt;
use std::cell::RefCell;

// structs define

#[allow(dead_code)]
pub trait Object {
    fn load(&mut self, raw_data: Vec<u8>) -> u64;
    fn dump(&self) -> Result<(Vec<u8>, u64), ()>;
    fn hash_id(&self) -> u64;
    fn set_hash_id(&mut self, id: u64);
}

#[derive(Debug, Clone)]
pub struct Metadata {
    pub data: HashMap<String, String>,
    vector_hash_id: Option<u64>,
    hash_id: u64,
}

#[derive(Debug, Clone)]
pub struct Vector {
    pub data: Vec<u32>,
    pub timestamp: i64,
    meta_hash_id: Option<u64>,
    hash_id: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Collection {
    pub name: String,
    hash_id: u64,
}

// Impl block

//  Metadata impl

impl Object for Metadata {
    fn load(&mut self, raw_data: Vec<u8>) -> u64{
        let decoded: StorageMetadata = bincode::deserialize(&raw_data[..])
            .expect("Ошибка");

        self.data = decoded.data;
        self.hash_id = decoded.hash_id;
        self.vector_hash_id = Some(decoded.vector_hash_id);

        decoded.vector_hash_id
    }

    fn dump(&self) -> Result<(Vec<u8>, u64), ()> {
        let vector_hash_id = match self.vector_hash_id {
            Some(vector_hash) => vector_hash,
            None => 0,
        };
        let storage_data = StorageMetadata { 
            data: self.data.clone(),
            vector_hash_id: vector_hash_id,
            hash_id: self.hash_id,
        }; 

        let encoded = bincode::serialize(&storage_data)
            .expect("Ошибка сериализации Metadata");

        Ok((encoded, self.hash_id))
    }

    fn hash_id(&self) -> u64 {
        self.hash_id
    }

    fn set_hash_id(&mut self, id: u64) {
        self.hash_id = id;
    }
}

impl Metadata {
    pub fn calculate_hash(data: HashMap<String, String>) -> u64 {
        let mut sorted_data: Vec<(&String, &String)> = data.iter().collect();
        sorted_data.sort_by(|a, b| a.0.cmp(b.0));

        calculate_hash(&sorted_data)
    }

    pub fn new(new_data: HashMap<String, String>) -> Metadata {
        let hash_id = Metadata::calculate_hash(new_data.clone());
        Metadata { data: new_data , vector_hash_id: None, hash_id: hash_id}
    }

    pub fn add_vector(&mut self, parent_vector: Vector) {
        self.vector_hash_id = Some(parent_vector.hash_id);
    }
}

impl fmt::Display for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.vector_hash_id {
            Some(vector_hash) => {
                write!(
                    f,
                    "Metadata: {}, hash: {:?}, vector_hash_id: {}",
                    format!("{:?}", self.data),
                    self.hash_id,
                    vector_hash
                )
            },
            None => {
                write!(
                    f,
                    "Metadata: {}, hash: {:?}, vector_hash_id: None",
                    format!("{:?}", self.data),
                    self.hash_id
                )
            }
        }
        
    }
}

//  Vector impl

impl Object for Vector {
    fn load(&mut self, raw_data: Vec<u8>) -> u64{
        let decoded: StorageVector = bincode::deserialize(&raw_data[..])
            .expect("Ошибка");

        self.data = decoded.data;
        self.hash_id = decoded.hash_id;
        self.timestamp = decoded.timestamp;
        self.meta_hash_id = Some(decoded.meta_hash_id);

        decoded.meta_hash_id
    }

    fn dump(&self) -> Result<(Vec<u8>, u64), ()> {
        let meta_hash_id = match self.meta_hash_id {
            Some(meta_hash) => meta_hash,
            None => 0,
        };
        let storage_data = StorageVector { 
            data: self.data.to_vec(),
            timestamp: self.timestamp,
            meta_hash_id,
            hash_id: self.hash_id,
        };

        let encoded = bincode::serialize(&storage_data)
            .expect("Ошибка сериализации Vector");

        Ok((encoded, self.hash_id))
    }

    fn hash_id(&self) -> u64 {
        self.hash_id
    }


    fn set_hash_id(&mut self, id: u64) {
        self.hash_id = id;
    }
}

impl Vector {
    pub fn new(data: Vec<u32>, timestamp: i64) -> Vector {
        // TODO calculate_hash
        Vector { data: data, timestamp: timestamp, meta_hash_id: None, hash_id: 0}
    }

    pub fn add_metadata(&mut self, child_meta: Metadata) {
        self.meta_hash_id = Some(child_meta.hash_id);
    }
}

impl fmt::Display for Vector {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.meta_hash_id {
            Some(meta_hash) => {
                    write!(
                        f,
                        "Vector: {:?}, timestamp: {}, hash: {}, meta: {}",
                        self.data,
                        self.timestamp,
                        self.hash_id,
                        meta_hash
                    )
            }
            None => {
                write!(
                    f,
                    "Vector: {:?}, timestamp: {}, hash: {:?}, meta: None",
                    self.data,
                    self.timestamp,
                    self.hash_id
                )
            }
        }
    }
}

//  Collection impl

impl Object for Collection {
    fn load(&mut self, raw_data: Vec<u8>) -> u64 {
        let decoded: Collection = bincode::deserialize(&raw_data[..])
            .expect("Ошибка");

        self.name = decoded.name;
        self.hash_id = decoded.hash_id;

        0
    }

    fn dump(&self) -> Result<(Vec<u8>, u64), ()> {
        let encoded = bincode::serialize(&self)
            .expect("Ошибка сериализации Collection");
        
        Ok((encoded, self.hash_id))
    }

    fn hash_id(&self) -> u64 {
        self.hash_id
    }

    fn set_hash_id(&mut self, id: u64) {
        self.hash_id = id;
    }
}

impl Collection {
    pub fn new(name: String) -> Collection {
        // TODO calculate_hash
        Collection { name: name, hash_id: 0 }
    }
}