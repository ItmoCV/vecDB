use std::{collections::HashMap};
use crate::core::{interfaces::Object, utils::{calculate_hash, StorageCollection, StorageVector, StorageBucket}};
use std::fmt;
use crate::core::controllers::{VectorController, BucketController};
use crate::core::lsh::LSHMetric;
use std::collections::BTreeMap;

// structs define

#[derive(Debug, Clone)]
pub struct Vector {
    pub data: Vec<f32>,
    pub timestamp: i64,
    pub metadata: HashMap<String, String>,
    hash_id: u64,
}

#[derive(Debug, Clone)]
pub struct Collection {
    pub name: String,
    pub buckets_controller: BucketController,
    pub lsh_metric: LSHMetric,
    pub vector_dimension: usize,
    id: u64,
}

#[derive(Debug, Clone)]
pub struct Bucket {
    pub id: u64,
    pub vectors_controller: VectorController,
    pub created_at: i64,
    pub updated_at: i64,
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
        self.id = decoded.id;
        self.lsh_metric = LSHMetric::from_string(&decoded.lsh_metric)
            .unwrap_or(LSHMetric::Euclidean); // По умолчанию Euclidean для старых коллекций
        self.vector_dimension = decoded.vector_dimension;
    }

    /// Сохраняет объект Collection в вектор байт (сериализация StorageCollection)
    fn dump(&self) -> Result<(Vec<u8>, u64), ()> {
        let storage_data = StorageCollection{ 
            name: self.name.clone(),
            id: self.id,
            lsh_metric: self.lsh_metric.to_string(),
            vector_dimension: self.vector_dimension,
        };

        let encoded = bincode::serialize(&storage_data)
            .expect("Ошибка сериализации Collection");
        
        Ok((encoded, self.id))
    }

    /// Возвращает ID объекта Collection
    fn hash_id(&self) -> u64 {
        self.id
    }

    /// Устанавливает ID объекта Collection
    fn set_hash_id(&mut self, id: u64) {
        self.id = id;
    }
}

impl Collection {
    /// Создаёт новый объект Collection с опциональным именем, метрикой LSH и размерностью векторов
    pub fn new(name: Option<String>, lsh_metric: LSHMetric, vector_dimension: usize) -> Collection {
        let (name, id) = match name {
            Some(n) => {
                let hash = calculate_hash(&n);
                (n, hash)
            },
            None => ("".to_string(), 0),
        };
        let buckets_controller = BucketController::new(vector_dimension, 3, 10.0, lsh_metric.clone(), Some(42));
        Collection { 
            name, 
            id, 
            buckets_controller: buckets_controller,
            lsh_metric,
            vector_dimension
        }
    }

    pub fn find_similar(&self, query: &Vec<f32> , k: usize) -> Result<Vec<(u64, usize, f32)>, Box<dyn std::error::Error>> {
        self.buckets_controller.find_similar(query, k)
    }

    pub fn filter_by_metadata(&self, filters: &HashMap<String, String>) -> Vec<u64> {
        self.buckets_controller.filter_by_metadata(filters)
    }
}

//  Bucket impl

impl Object for Bucket {
    /// Загружает объект Bucket из вектора байт (десериализация)
    fn load(&mut self, raw_data: Vec<u8>) {
        let decoded: StorageBucket = bincode::deserialize(&raw_data[..])
            .expect("Ошибка десериализации Bucket");

        self.id = decoded.id;
        self.created_at = decoded.created_at;
        self.updated_at = decoded.updated_at;
    }

    /// Сохраняет объект Bucket в вектор байт (сериализация)
    fn dump(&self) -> Result<(Vec<u8>, u64), ()> {
        let storage_data = StorageBucket {
            id: self.id,
            created_at: self.created_at,
            updated_at: self.updated_at,
        };

        let encoded = bincode::serialize(&storage_data)
            .expect("Ошибка сериализации Bucket");

        Ok((encoded, self.id))
    }

    /// Возвращает ID объекта Bucket
    fn hash_id(&self) -> u64 {
        self.id
    }

    /// Устанавливает ID объекта Bucket
    fn set_hash_id(&mut self, id: u64) {
        self.id = id;
    }
}

impl Bucket {
    /// Создаёт новый объект Bucket с VectorController
    pub fn new(id: u64) -> Bucket {
        let now = chrono::Utc::now().timestamp();
        let vectors_controller = VectorController::new();

        Bucket {
            id,
            vectors_controller,
            created_at: now,
            updated_at: now,
        }
    }

    /// Добавляет вектор в бакет через VectorController
    pub fn add_vector(&mut self, embedding: Vec<f32>, metadata: HashMap<String, String>) -> Result<u64, Box<dyn std::error::Error>> {
        let vector_id = self.vectors_controller.add_vector(Some(embedding), Some(metadata), None, None)?;
        self.updated_at = chrono::Utc::now().timestamp();
        Ok(vector_id)
    }

    /// Удаляет вектор из бакета
    pub fn remove_vector(&mut self, vector_id: u64) -> Result<(), String> {
        match self.vectors_controller.remove_vector(vector_id) {
            Ok(_) => {
                self.updated_at = chrono::Utc::now().timestamp();
                Ok(())
            }
            Err(e) => Err(e)
        }
    }

    /// Проверяет, содержит ли бакет вектор
    pub fn contains_vector(&self, vector_id: u64) -> bool {
        self.vectors_controller.get_vector_by_id(vector_id).is_some()
    }

    /// Возвращает количество векторов в бакете
    pub fn size(&self) -> usize {
        match &self.vectors_controller.vectors {
            Some(vectors) => vectors.len(),
            None => 0,
        }
    }

    /// Получает вектор по ID
    pub fn get_vector(&self, vector_id: u64) -> Option<&Vector> {
        self.vectors_controller.get_vector_by_id(vector_id)
    }

    /// Поиск похожих векторов в бакете
    pub fn find_similar(&self, query: &Vec<f32>, k: usize) -> Result<Vec<(usize, f32)>, Box<dyn std::error::Error>> {
        self.vectors_controller.find_most_similar(query, k)
    }

    /// Фильтрация векторов по метаданным
    pub fn filter_by_metadata(&self, filters: &HashMap<String, String>) -> Vec<u64> {
        self.vectors_controller.filter_by_metadata(filters)
    }

    /// Обновляет вектор в бакете
    pub fn update_vector(
        &mut self,
        vector_id: u64,
        new_embedding: Option<Vec<f32>>,
        new_metadata: Option<HashMap<String, String>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.vectors_controller.update_vector(vector_id, new_embedding, new_metadata)?;
        self.updated_at = chrono::Utc::now().timestamp();
        Ok(())
    }

    /// Удаляет вектор из бакета и возвращает его
    pub fn remove_and_get_vector(&mut self, vector_id: u64) -> Result<Vector, String> {
        match self.vectors_controller.remove_and_get_vector(vector_id) {
            Ok(vector) => {
                self.updated_at = chrono::Utc::now().timestamp();
                Ok(vector)
            }
            Err(e) => Err(e)
        }
    }

}

impl fmt::Display for Bucket {
    /// Форматирует объект Bucket для вывода
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Bucket: ID {}, vectors: {}, created: {}, updated: {}",
            self.id,
            self.size(),
            self.created_at,
            self.updated_at
        )
    }
}