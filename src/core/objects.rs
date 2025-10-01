use std::{collections::HashMap};
use crate::core::{interfaces::Object, utils::{calculate_hash, StorageCollection, StorageMetadata, StorageVector}};
use std::fmt;
use crate::core::controllers::VectorController;
use chrono::{DateTime, Utc};

// structs define

#[derive(Debug, Clone)]
pub struct Metadata {
    pub data: HashMap<String, String>,
    vector_hash_id: Option<u64>,
    hash_id: u64,
}

#[derive(Debug, Clone)]
pub struct Vector {
    pub hash_id: String,
    pub embedding: Vec<f32>,
    pub created_at: DateTime<Utc>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug)]
pub struct Collection {
    pub name: String,
    pub vectors_controller: VectorController,
    hash_id: u64,
}

// Impl block

//  Metadata impl

impl Object for Metadata {
    /// Загружает объект Metadata из вектора байт (десериализация)
    fn load(&mut self, raw_data: Vec<u8>) {
        let decoded: StorageMetadata = bincode::deserialize(&raw_data[..])
            .expect("Ошибка");

        self.data = decoded.data;
        self.hash_id = decoded.hash_id;
        self.vector_hash_id = Some(decoded.vector_hash_id);
    }

    /// Сохраняет объект Metadata в вектор байт (сериализация)
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

    /// Возвращает hash_id объекта Metadata
    fn hash_id(&self) -> u64 {
        self.hash_id
    }

    /// Устанавливает hash_id объекта Metadata
    fn set_hash_id(&mut self, id: u64) {
        self.hash_id = id;
    }
}

impl Metadata {
    /// Вычисляет хеш на основе данных метадаты
    pub fn calculate_hash(data: HashMap<String, String>) -> u64 {
        let mut sorted_data: Vec<(&String, &String)> = data.iter().collect();
        sorted_data.sort_by(|a, b| a.0.cmp(b.0));

        calculate_hash(&sorted_data)
    }

    /// Создаёт новый объект Metadata, опционально с начальными данными
    pub fn new(new_data: Option<HashMap<String, String>>) -> Metadata {
        match new_data {
            Some(data) => {
                let hash_id = Metadata::calculate_hash(data.clone());
                Metadata { data: data , vector_hash_id: None, hash_id: hash_id}
            }
            None => {
                Metadata { data: HashMap::new(), vector_hash_id: None, hash_id: 0}
            }
        }
    }

    /// Привязывает метадату к вектору по его hash_id
    pub fn add_vector(&mut self, parent_vector: Vector) {
        self.vector_hash_id = Some(parent_vector.hash_id);
    }
}

impl fmt::Display for Metadata {
    /// Форматирует объект Metadata для вывода
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
    /// Загружает объект Vector из вектора байт (десериализация)
    fn load(&mut self, raw_data: Vec<u8>) {
        let decoded: StorageVector = bincode::deserialize(&raw_data[..])
            .expect("Ошибка");

        self.embedding = decoded.embedding;
        self.hash_id = decoded.hash_id;
        self.created_at = decoded.timestamp;
    }

    /// Сохраняет объект Vector в вектор байт (сериализация)
    fn dump(&self) -> Result<(Vec<u8>, u64), ()> {
        let meta_hash_id = match self.meta_hash_id {
            Some(meta_hash) => meta_hash,
            None => 0,
        };
        let storage_data = StorageVector { 
            data: self.embedding.to_vec(),
            timestamp: self.timestamp,
            meta_hash_id,
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
    pub fn new(data: Option<Vec<u32>>, timestamp: Option<i64>) -> Vector {
        // TODO calculate_hash
        Vector { 
            data: data.unwrap_or_default(), 
            timestamp: timestamp.unwrap_or(0), 
            meta_hash_id: None, 
            hash_id: 0
        }
    }

    /// Привязывает вектор к метаданным по их hash_id
    pub fn add_metadata(&mut self, child_meta: Metadata) {
        self.meta_hash_id = Some(child_meta.hash_id);
    }
}

impl fmt::Display for Vector {
    /// Форматирует объект Vector для вывода
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
            vector_length: self.vectors_controller.get_length(),
            metrics: self.vectors_controller.get_metrics().to_string(),
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
        let metadata_controller = MetadataController::new();
        Collection { name, hash_id, vectors_controller: vector_controller, metadata_controller }
    }
}