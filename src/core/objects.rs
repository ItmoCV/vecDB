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
    pub vector: Option<std::rc::Weak<RefCell<Vector>>>,
    hash_id: u64,
}

#[derive(Debug, Clone)]
pub struct Vector {
    pub data: Vec<u32>,
    pub timestamp: i64,
    pub meta: Option<std::rc::Weak<RefCell<Metadata>>>,
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

        decoded.vector_hash_id
    }

    fn dump(&self) -> Result<(Vec<u8>, u64), ()> {
        let vector_hash_id = match &self.vector {
            Some(vector_weak) => {
                if let Some(vector_rc) = vector_weak.upgrade() {
                    vector_rc.borrow().hash_id
                } else {
                    0
                }
            },
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
        Metadata { data: new_data , hash_id: hash_id, vector : None }
    }

    pub fn add_vector(&mut self, parent_vector: std::rc::Rc<RefCell<Vector>>) {
        self.vector = Some(std::rc::Rc::downgrade(&parent_vector));
    }

    pub fn get_vector(&self) -> Option<Vector> {
        match &self.vector {
            Some(vector_weak) => vector_weak.upgrade().map(|rc| (*rc.borrow()).clone()),
            None => None,
        }
    }
}

impl fmt::Display for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Metadata: {}, hash: {:?}",
            format!("{:?}", self.data),
            self.hash_id
        )
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

        decoded.meta_hash_id
    }

    fn dump(&self) -> Result<(Vec<u8>, u64), ()> {
        let meta_hash_id = match &self.meta {
            Some(meta_weak) => {
                if let Some(meta_rc) = meta_weak.upgrade() {
                    meta_rc.borrow().hash_id
                } else {
                    0
                }
            },
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
        Vector { data: data, timestamp: timestamp, meta: None, hash_id: 0}
    }

    pub fn add_metadata(&mut self, child_meta: std::rc::Rc<RefCell<Metadata>>) {
        self.meta = Some(std::rc::Rc::downgrade(&child_meta));
    }

    pub fn get_meta(&self) -> Option<Metadata> {
        match &self.meta {
            Some(meta_weak) => meta_weak.upgrade().map(|rc| (*rc.borrow()).clone()),
            None => None,
        }
    }
}

impl fmt::Display for Vector {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.meta {
            Some(meta_weak) => {
                if let Some(meta_rc) = meta_weak.upgrade() {
                    write!(
                        f,
                        "Vector: {:?}, timestamp: {}, hash: {:?}, meta: {}",
                        self.data,
                        self.timestamp,
                        self.hash_id,
                        meta_rc.borrow()
                    )
                } else {
                    write!(
                        f,
                        "Vector: {:?}, timestamp: {}, hash: {:?}, meta: None (слабая ссылка недействительна)",
                        self.data,
                        self.timestamp,
                        self.hash_id
                    )
                }
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