use std::collections::HashMap;
use std::sync::Arc;

use crate::core::config::ConfigLoader;
use crate::core::controllers::{CollectionController, StorageController};
use crate::core::lsh::LSHMetric;
use crate::core::sharding::{ShardManager, ShardCoordinator, ShardConfig, RoutingStrategy};
use crate::core::shard_client::{MultiShardClient, ShardClient};

pub struct VectorDB {
    storage_controller: Arc<StorageController>,
    collection_controller: CollectionController,
    shard_coordinator: Option<ShardCoordinator>,
    multi_shard_client: Option<MultiShardClient>,
    is_sharded: bool,
}

impl VectorDB {
    pub fn new(config_loader: ConfigLoader) -> Self {
        let storage_controller = Arc::new(StorageController::new(config_loader.get("path")));

        // Передаем Arc на storage_controller в CollectionController
        let collection_controller = CollectionController::new(Arc::clone(&storage_controller));

        VectorDB { 
            storage_controller, 
            collection_controller, 
            shard_coordinator: None,
            multi_shard_client: None,
            is_sharded: false,
        }
    }

    /// Создает новую VectorDB с поддержкой шардирования из конфигурации
    pub fn new_from_config(config_loader: ConfigLoader) -> Result<Self, String> {
        let storage_controller = Arc::new(StorageController::new(config_loader.get("storage")));
        let collection_controller = CollectionController::new(Arc::clone(&storage_controller));

        // Определяем роль из конфигурации
        if config_loader.is_coordinator() {
            // Это Coordinator - управляет всеми шардами
            let shard_configs = config_loader.get_shard_configs()?;
            let routing_strategy = config_loader.get_routing_strategy();
            
            Self::new_coordinator(
                storage_controller,
                collection_controller,
                shard_configs,
                routing_strategy
            )
        } else {
            // Это Shard Node - работает только с локальными данными
            Self::new_shard_node(
                storage_controller,
                collection_controller
            )
        }
    }

    /// Создает экземпляр Coordinator (API Gateway)
    fn new_coordinator(
        storage_controller: Arc<StorageController>,
        collection_controller: CollectionController,
        shard_configs: Vec<ShardConfig>,
        routing_strategy: RoutingStrategy
    ) -> Result<Self, String> {
        // Создаем менеджер шардов
        let shard_manager = Arc::new(tokio::sync::RwLock::new(
            ShardManager::new(shard_configs.clone(), routing_strategy)
        ));

        // Создаем клиенты для удаленных шардов
        let mut multi_shard_client = MultiShardClient::new();
        for config in &shard_configs {
            let base_url = format!("http://{}:{}", config.host, config.port);
            let client = ShardClient::new(base_url);
            multi_shard_client.add_shard_client(config.id.clone(), client);
        }

        // Создаем координатор шардов с клиентом для удаленных шардов
        let shard_coordinator = ShardCoordinator::new_with_client(
            Arc::clone(&shard_manager),
            Arc::new(multi_shard_client.clone())
        );

        Ok(VectorDB { 
            storage_controller, 
            collection_controller, 
            shard_coordinator: Some(shard_coordinator),
            multi_shard_client: Some(multi_shard_client),
            is_sharded: true,
        })
    }

    /// Создает экземпляр Shard Node
    fn new_shard_node(
        storage_controller: Arc<StorageController>,
        collection_controller: CollectionController
    ) -> Result<Self, String> {
        // Shard Node работает только с локальными данными
        // Не создает клиентов для других шардов
        Ok(VectorDB { 
            storage_controller, 
            collection_controller, 
            shard_coordinator: None,
            multi_shard_client: None,
            is_sharded: false, // Shard Node не является "шардированным" с точки зрения координации
        })
    }


    /// Добавляет новую коллекцию
    pub async fn add_collection(&mut self, name: String, lsh_metric: LSHMetric, vector_dimension: usize) -> Result<(), &'static str> {
        if self.is_sharded {
            // Для шардированной БД создаем коллекцию через координатор
            if let Some(ref coordinator) = self.shard_coordinator {
                return coordinator.create_collection(name, lsh_metric, vector_dimension).await
                    .map_err(|_| "Ошибка создания коллекции в шардированной БД");
            }
        }
        
        // Для нешардированной БД или fallback
        self.collection_controller.add_collection(name, lsh_metric, vector_dimension)
    }

    /// Удаляет коллекцию
    pub async fn delete_collection(&mut self, name: String) -> Result<(), &'static str> {
        if self.is_sharded {
            // Для шардированной БД удаляем коллекцию через координатор
            if let Some(ref coordinator) = self.shard_coordinator {
                return coordinator.delete_collection(name).await
                    .map_err(|_| "Ошибка удаления коллекции в шардированной БД");
            }
        }
        
        // Для нешардированной БД или fallback
        self.collection_controller.delete_collection(name)
    }

    /// Получает коллекцию по имени
    pub async fn get_collection(&self, name: &str) -> Result<Option<crate::core::objects::Collection>, String> {
        if self.is_sharded {
            // Для шардированной БД получаем коллекцию через координатор
            if let Some(ref coordinator) = self.shard_coordinator {
                return coordinator.get_collection(name.to_string()).await;
            }
        }
        
        // Для нешардированной БД или fallback
        Ok(self.collection_controller.get_collection(name).cloned())
    }

    /// Получает список всех коллекций
    pub async fn get_all_collections(&self) -> Result<Vec<crate::core::objects::Collection>, String> {
        if self.is_sharded {
            // Для шардированной БД получаем коллекции через координатор
            if let Some(ref coordinator) = self.shard_coordinator {
                return coordinator.get_all_collections().await;
            }
        }
        
        // Для нешардированной БД или fallback
        Ok(self.collection_controller.get_all_collections().into_iter().cloned().collect())
    }

    /// Добавляет вектор в коллекцию
    pub async fn add_vector(&mut self, collection_name: &str, embedding: Vec<f32>, metadata: HashMap<String, String>) -> Result<u64, &'static str> {
        if self.is_sharded {
            // Для шардированной БД добавляем вектор через координатор
            if let Some(ref coordinator) = self.shard_coordinator {
                return coordinator.add_vector(collection_name.to_string(), embedding, metadata).await
                    .map_err(|_| "Ошибка добавления вектора в шардированной БД");
            }
        }
        
        // Для нешардированной БД или fallback
        self.collection_controller.add_vector(collection_name, embedding, metadata)
    }

    /// Получает вектор по ID из коллекции
    pub async fn get_vector(&self, collection_name: &str, vector_id: u64) -> Result<crate::core::objects::Vector, Box<dyn std::error::Error>> {
        if self.is_sharded {
            // Для шардированной БД получаем вектор через координатор
            if let Some(ref coordinator) = self.shard_coordinator {
                return coordinator.get_vector(collection_name.to_string(), vector_id).await
                    .map_err(|e| e.into());
            }
        }
        
        // Для нешардированной БД или fallback
        self.collection_controller.get_vector(collection_name, vector_id)
            .map(|v| v.clone())
    }

    /// Обновляет вектор в коллекции
    pub async fn update_vector(&mut self, collection_name: &str, vector_id: u64, new_embedding: Option<Vec<f32>>, new_metadata: Option<HashMap<String, String>>) -> Result<(), Box<dyn std::error::Error>> {
        if self.is_sharded {
            // Для шардированной БД обновляем вектор через координатор
            if let Some(ref coordinator) = self.shard_coordinator {
                return coordinator.update_vector(collection_name.to_string(), vector_id, new_embedding, new_metadata).await
                    .map_err(|e| e.into());
            }
        }
        
        // Для нешардированной БД или fallback
        self.collection_controller.update_vector(collection_name, vector_id, new_embedding, new_metadata)
    }

    /// Удаляет вектор по ID из коллекции
    pub async fn delete_vector(&mut self, collection_name: &str, vector_id: u64) -> Result<(), Box<dyn std::error::Error>> {
        if self.is_sharded {
            // Для шардированной БД удаляем вектор через координатор
            if let Some(ref coordinator) = self.shard_coordinator {
                return coordinator.delete_vector(collection_name.to_string(), vector_id).await
                    .map_err(|e| e.into());
            }
        }
        
        // Для нешардированной БД или fallback
        self.collection_controller.delete_vector(collection_name, vector_id)
    }

    /// Сохраняет все коллекции на диск
    pub async fn dump(&self) {
        if self.is_sharded {
            // Для шардированной БД сохраняем данные через координатор
            if let Some(ref coordinator) = self.shard_coordinator {
                if let Err(e) = coordinator.dump_data().await {
                    eprintln!("Ошибка сохранения данных в шардированной БД: {}", e);
                }
                return;
            }
        }
        
        // Для нешардированной БД или fallback
        self.collection_controller.dump();
    }

    /// Загружает коллекции с диска
    pub async fn load(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.is_sharded {
            // Для шардированной БД загружаем данные через координатор
            if let Some(ref coordinator) = self.shard_coordinator {
                return coordinator.load_data().await
                    .map_err(|e| e.into());
            }
        }
        
        // Для нешардированной БД или fallback
        self.collection_controller.load()
    }


    /// Доступ к StorageController для низкоуровневых операций
    pub fn storage_controller(&self) -> Arc<StorageController> {
        Arc::clone(&self.storage_controller)
    }

    /// Доступ к CollectionController для низкоуровневых операций
    pub fn collection_controller_mut(&mut self) -> &mut CollectionController {
        &mut self.collection_controller
    }

    /// Фильтрует векторы по метаданным в указанной коллекции
    pub async fn filter_by_metadata(
        &self,
        collection_name: &str,
        filters: &HashMap<String, String>,
    ) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
        if self.is_sharded {
            // Для шардированной БД фильтруем через координатор
            if let Some(ref coordinator) = self.shard_coordinator {
                return coordinator.filter_by_metadata(collection_name.to_string(), filters.clone()).await
                    .map_err(|e| e.into());
            }
        }
        
        // Для нешардированной БД или fallback
        self.collection_controller.filter_by_metadata(collection_name, filters)
    }

    /// Ищет похожие векторы в указанной коллекции
    pub async fn find_similar(
        &self,
        collection_name: String,
        query: &Vec<f32>,
        k: usize,
    ) -> Result<Vec<(u64, usize, f32)>, Box<dyn std::error::Error>> {
        if self.is_sharded {
            // Для шардированной БД ищем через координатор
            if let Some(ref coordinator) = self.shard_coordinator {
                return coordinator.find_similar_vectors(collection_name.clone(), query.clone(), k).await
                    .map_err(|e| e.into());
            }
        }
        
        // Для нешардированной БД или fallback
        self.collection_controller.find_similar(collection_name, query, k)
    }

    /// Асинхронный поиск похожих векторов в шардированной БД
    pub async fn find_similar_async(
        &self,
        collection_name: String,
        query: Vec<f32>,
        k: usize,
    ) -> Result<Vec<(u64, usize, f32)>, Box<dyn std::error::Error>> {
        if self.is_sharded {
            // Для шардированной БД используем координатор
            if let Some(ref coordinator) = self.shard_coordinator {
                return coordinator.find_similar_vectors(collection_name, query, k).await
                    .map_err(|e| e.into());
            }
        }
        
        // Fallback к обычному поиску
        self.collection_controller.find_similar(collection_name, &query, k)
    }

    /// Проверяет, является ли БД шардированной
    pub fn is_sharded(&self) -> bool {
        self.is_sharded
    }

    /// Получает статистику по шардам (только для шардированной БД)
    pub async fn get_shard_statistics(&self) -> Result<HashMap<String, serde_json::Value>, String> {
        if !self.is_sharded {
            return Err("База данных не является шардированной".to_string());
        }

        if let Some(ref coordinator) = self.shard_coordinator {
            Ok(coordinator.get_cluster_statistics().await)
        } else {
            Err("Координатор шардов не инициализирован".to_string())
        }
    }

    /// Выполняет балансировку шардов (только для шардированной БД)
    pub async fn rebalance_shards(&self) -> Result<(), String> {
        if !self.is_sharded {
            return Err("База данных не является шардированной".to_string());
        }

        if let Some(ref coordinator) = self.shard_coordinator {
            coordinator.rebalance_shards().await
        } else {
            Err("Координатор шардов не инициализирован".to_string())
        }
    }

    /// Выполняет поиск похожих векторов по всем шардам
    pub async fn find_similar_across_shards(
        &self,
        collection_name: String,
        query: Vec<f32>,
        k: usize,
    ) -> Result<Vec<(u64, usize, f32)>, String> {
        if !self.is_sharded {
            return Err("База данных не является шардированной".to_string());
        }

        if let Some(ref multi_client) = self.multi_shard_client {
            multi_client.find_similar_across_shards(collection_name, query, k).await
        } else {
            Err("Клиент для множественных шардов не инициализирован".to_string())
        }
    }

    /// Проверяет здоровье всех шардов
    pub async fn health_check_shards(&self) -> Result<HashMap<String, bool>, String> {
        if !self.is_sharded {
            return Err("База данных не является шардированной".to_string());
        }

        if let Some(ref multi_client) = self.multi_shard_client {
            Ok(multi_client.health_check_all().await)
        } else {
            Err("Клиент для множественных шардов не инициализирован".to_string())
        }
    }

}