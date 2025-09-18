use std::{collections::HashMap, result::Result};
use crate::core::{config::ConfigLoader, objects::{Collection, Object}};

// structs define

pub struct StorageController {
    configs: HashMap<String, String>,
}

pub struct ConnectionController {
    storage_controller: StorageController,
    configs: HashMap<String, String>,
}

pub struct CollectionController {
    storage_controller: StorageController,
    collections: Option<Vec<Collection>>,
}

// Impl block

//  StorageController impl

impl StorageController {
    pub fn new() -> StorageController {
        StorageController { configs: HashMap::new() }
    }

    pub fn load(&mut self, filename: String) {

    }

    pub fn dump<T: Object>(&self, _obj: T) {

    }
}

//  ConnectionController impl

impl ConnectionController {
    pub fn new(storage_controller : StorageController, config_loader : ConfigLoader) -> ConnectionController {
        let names = Vec::new();

        ConnectionController { storage_controller: storage_controller, configs: config_loader.get(names) }
    }

    pub fn connection_handler(&mut self) {

    }

    pub fn query_handler(&self) -> Result<(), &'static str> {
        Ok(())
    }
}

//  CollectionController impl

impl CollectionController {
    pub fn new(storage_controller : StorageController) -> CollectionController {
        CollectionController { storage_controller: storage_controller, collections: None }
    }

    pub fn add_collection(&mut self, name: String) -> Result<(), &'static str> {
        Ok(())
    }

    pub fn delete_collection(&mut self, name: String) -> Result<(), &'static str> {
        Ok(())
    }

    pub fn get_collection(&self, name: String) -> Option<Collection> {
        None
    }

    pub fn add_vector(col: Collection, raw_vec: f64) -> Result<(), &'static str> {
        Ok(())
    }
}