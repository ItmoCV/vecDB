use std::{collections::HashMap};
use crate::core::{interfaces::Object, utils::{calculate_hash, StorageCollection, StorageVector}};
use std::fmt;
use crate::core::controllers::VectorController;
use std::collections::BTreeMap;

// structs define

#[derive(Debug, Clone)]
pub struct Vector {
    pub data: Vec<f32>,
    pub timestamp: i64,
    pub metadata: HashMap<String, String>,
    hash_id: u64,
}

#[derive(Debug)]
pub struct Collection {
    pub name: String,
    pub vectors_controller: VectorController,
    hash_id: u64,
}

// Impl block

//  Vector impl

impl Object for Vector {
    /// Загружает объект Vector из вектора байт (десериализация)
    fn load(&mut self, raw_data: Vec<u8>) {
        let decoded: StorageVector = bincode::deserialize(&raw_data[..])
            .expect("Ошибка");

        self.data = decoded.data;
        self.hash_id = decoded.hash_id;
        self.timestamp = decoded.timestamp;
    }

    /// Сохраняет объект Vector в вектор байт (сериализация)
    fn dump(&self) -> Result<(Vec<u8>, u64), ()> {
        let storage_data = StorageVector { 
            data: self.data.to_vec(),
            timestamp: self.timestamp,
            metadata: self.metadata.clone(),
            hash_id: self.hash_id,
        };

        let encoded = bincode::serialize(&storage_data)
            .expect("Ошибка сериализации Vector");

        Ok((encoded, self.hash_id))
    }

    /// Возвращает hash_id объекта Vector
    fn hash_id(&self) -> u64 {
        self.hash_id
    }

    /// Устанавливает hash_id объекта Vector
    fn set_hash_id(&mut self, id: u64) {
        self.hash_id = id;
    }
}

impl Vector {
    /// Создаёт новый объект Vector с опциональными данными и временной меткой
    pub fn new(data: Option<Vec<f32>>, timestamp: Option<i64>, metadata: Option<HashMap<String, String>>) -> Vector {
        let data_val = data.unwrap_or_default();
        let timestamp_val = timestamp.unwrap_or(0);
        let metadata_val = metadata.unwrap_or_default();

        let hash_id = Vector::calculate_hash(&data_val, timestamp_val, &metadata_val);

        Vector { 
            data: data_val, 
            timestamp: timestamp_val, 
            metadata: metadata_val, 
            hash_id
        }
    }

    fn calculate_hash(data: &Vec<f32>, timestamp: i64, metadata: &HashMap<String, String>) -> u64 {
        let data_bits: Vec<u32> = data.iter().map(|f| f.to_bits()).collect();
        let metadata_btree: BTreeMap<String, String> = metadata.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        calculate_hash(&(data_bits, timestamp, metadata_btree))
    }
}

impl fmt::Display for Vector {
    /// Форматирует объект Vector для вывода
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Vector: {:?}, timestamp: {}, hash: {}, metadata: {:?}",
            self.data,
            self.timestamp,
            self.hash_id,
            self.metadata
        )
    }
}

//  Collection impl

impl Object for Collection {
    /// Загружает объект Collection из вектора байт (десериализация StorageCollection)
    fn load(&mut self, raw_data: Vec<u8>) {
        // Десериализуем не саму Collection, а StorageCollection
        let decoded: StorageCollection = bincode::deserialize(&raw_data[..])
            .expect("Ошибка десериализации StorageCollection");

        self.name = decoded.name;
        self.hash_id = decoded.hash_id;
    }

    /// Сохраняет объект Collection в вектор байт (сериализация StorageCollection)
    fn dump(&self) -> Result<(Vec<u8>, u64), ()> {
        let storage_data = StorageCollection{ 
            name: self.name.clone(),
            hash_id: self.hash_id,
        };

        let encoded = bincode::serialize(&storage_data)
            .expect("Ошибка сериализации Collection");
        
        Ok((encoded, self.hash_id))
    }

    /// Возвращает hash_id объекта Collection
    fn hash_id(&self) -> u64 {
        self.hash_id
    }

    /// Устанавливает hash_id объекта Collection
    fn set_hash_id(&mut self, id: u64) {
        self.hash_id = id;
    }
}

impl Collection {
    /// Создаёт новый объект Collection с опциональным именем
    pub fn new(name: Option<String>) -> Collection {
        let (name, hash_id) = match name {
            Some(n) => {
                let hash = calculate_hash(&n);
                (n, hash)
            },
            None => ("".to_string(), 0),
        };
        let vector_controller = VectorController::new();
        Collection { name, hash_id, vectors_controller: vector_controller }
    }
}