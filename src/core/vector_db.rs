use std::collections::HashMap;
use std::sync::Arc;

use crate::core::config::ConfigLoader;
use crate::core::controllers::{CollectionController, ConnectionController, StorageController};
use crate::core::lsh::LSHMetric;

pub struct VectorDB {
    storage_controller: Arc<StorageController>,
    collection_controller: CollectionController,
    connection_controller: ConnectionController,
}

impl VectorDB {
    pub fn new(path: String) -> Self {
        let mut config_loader = ConfigLoader::new();
        config_loader.load(path);
        let storage_controller = Arc::new(StorageController::new(config_loader.get("path")));

        // Передаем Arc на storage_controller в CollectionController и ConnectionController
        let collection_controller = CollectionController::new(Arc::clone(&storage_controller));
        let connection_controller = ConnectionController::new(config_loader);

        VectorDB { storage_controller, collection_controller, connection_controller }
    }

    /// Добавляет новую коллекцию
    pub fn add_collection(&mut self, name: String, lsh_metric: LSHMetric, vector_dimension: usize) -> Result<(), &'static str> {
        self.collection_controller.add_collection(name, lsh_metric, vector_dimension)
    }

    /// Удаляет коллекцию
    pub fn delete_collection(&mut self, name: String) -> Result<(), &'static str> {
        self.collection_controller.delete_collection(name)
    }

    /// Получает коллекцию по имени
    pub fn get_collection(&self, name: &str) -> Option<&crate::core::objects::Collection> {
        self.collection_controller.get_collection(name)
    }

    /// Получает список всех коллекций
    pub fn get_all_collections(&self) -> Vec<&crate::core::objects::Collection> {
        self.collection_controller.get_all_collections()
    }

    /// Добавляет вектор в коллекцию
    pub fn add_vector(&mut self, collection_name: &str, embedding: Vec<f32>, metadata: HashMap<String, String>) -> Result<u64, &'static str> {
        self.collection_controller.add_vector(collection_name, embedding, metadata)
    }

    /// Получает вектор по ID из коллекции
    pub fn get_vector(&self, collection_name: &str, vector_id: u64) -> Result<&crate::core::objects::Vector, Box<dyn std::error::Error>> {
        self.collection_controller.get_vector(collection_name, vector_id)
    }

    /// Обновляет вектор в коллекции
    pub fn update_vector(&mut self, collection_name: &str, vector_id: u64, new_embedding: Option<Vec<f32>>, new_metadata: Option<HashMap<String, String>>) -> Result<(), Box<dyn std::error::Error>> {
        self.collection_controller.update_vector(collection_name, vector_id, new_embedding, new_metadata)
    }

    /// Удаляет вектор по ID из коллекции
    pub fn delete_vector(&mut self, collection_name: &str, vector_id: u64) -> Result<(), Box<dyn std::error::Error>> {
        self.collection_controller.delete_vector(collection_name, vector_id)
    }

    /// Сохраняет все коллекции на диск
    pub fn dump(&self) {
        self.collection_controller.dump();
    }

    /// Загружает коллекции с диска
    pub fn load(&mut self) {
        self.collection_controller.load();
    }

    /// Доступ к ConnectionController для кастомной логики соединений
    pub fn connection_controller_mut(&mut self) -> &mut ConnectionController {
        &mut self.connection_controller
    }

    /// Доступ к CollectionController для низкоуровневых операций
    pub fn collection_controller_mut(&mut self) -> &mut CollectionController {
        &mut self.collection_controller
    }

    /// Фильтрует векторы по метаданным в указанной коллекции
    pub fn filter_by_metadata(
        &self,
        collection_name: &str,
        filters: &HashMap<String, String>,
    ) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
        self.collection_controller.filter_by_metadata(collection_name, filters)
    }

    /// Ищет похожие векторы в указанной коллекции
    pub fn find_similar(
        &self,
        collection_name: String,
        query: &Vec<f32>,
        k: usize,
    ) -> Result<Vec<(u64, usize, f32)>, Box<dyn std::error::Error>> {
        self.collection_controller.find_similar(collection_name, query, k)
    }
}