use std::{collections::HashMap};
use crate::core::utils::{calculate_hash};
use std::fmt;

// structs define

#[allow(dead_code)]
pub trait Object {
    fn load(&mut self);
    fn dump(&self);
    fn hash_id(&self) -> u64;
    fn set_hash_id(&mut self, id: u64);
}

pub struct Metadata {
    pub data: HashMap<String, String>,
    hash_id: u64,
}

pub struct Vector {
    pub data: Vec<u32>,
    pub timestamp: i64,
    pub meta: Option<Metadata>,
    hash_id: u64,
}

pub struct Collection {
    pub name: String,
    hash_id: u64,
}

// Impl block

//  Metadata impl

impl Object for Metadata {
    fn load(&mut self) {
        // Реализация загрузки для Metadata
    }

    fn dump(&self) {
        // Реализация сохранения для Metadata
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
        Metadata { data: new_data, hash_id }
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
    fn load(&mut self) {
        // Реализация загрузки для Vector
    }

    fn dump(&self) {
        // Реализация сохранения для Vector
    }

    fn hash_id(&self) -> u64 {
        self.hash_id
    }


    fn set_hash_id(&mut self, id: u64) {
        self.hash_id = id;
    }
}

//  Collection impl

impl Object for Collection {
    fn load(&mut self) {
        // Реализация загрузки для Vector
    }

    fn dump(&self) {
        // Реализация сохранения для Vector
    }

    fn hash_id(&self) -> u64 {
        self.hash_id
    }

    fn set_hash_id(&mut self, id: u64) {
        self.hash_id = id;
    }
}