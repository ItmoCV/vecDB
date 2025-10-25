use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};
use crate::core::lsh::LSHMetric;
use crate::core::utils::calculate_hash;

/// Динамическая информация о состоянии шарда
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardInfo {
    pub status: ShardStatus,
    pub capacity: u64,
    pub used_space: u64,
    pub collections: Vec<String>,
}

/// Статус шарда
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ShardStatus {
    Active,
    Inactive,
    Maintenance,
    Failed,
}

/// Конфигурация шарда
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardConfig {
    pub id: String,
    pub host: String,
    pub port: u16,
    pub description: Option<String>,
}

/// Полная информация о шарде (конфигурация + состояние)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Shard {
    pub config: ShardConfig,
    pub info: ShardInfo,
}

/// Менеджер шардов - управляет распределением данных между шардами
pub struct ShardManager {
    shards: HashMap<String, Shard>,
    routing_strategy: RoutingStrategy,
    replication_factor: u8,
}

/// Стратегия роутинга запросов
#[derive(Debug, Clone)]
pub enum RoutingStrategy {
    /// Хеш-роутинг по имени коллекции
    HashBased,
    /// Роутинг по диапазону ID
    RangeBased,
    /// Роутинг по LSH бакетам
    LSHBased,
    /// Роутинг по метаданным
    MetadataBased,
}

/// Результат операции с шардом
#[derive(Debug)]
pub struct ShardOperationResult {
    pub shard_id: String,
    pub success: bool,
    pub data: Option<Vec<u8>>,
    pub error: Option<String>,
}

/// Координатор шардов - управляет операциями между шардами
pub struct ShardCoordinator {
    shard_manager: Arc<RwLock<ShardManager>>,
    multi_shard_client: Option<Arc<crate::core::shard_client::MultiShardClient>>,
}

impl ShardManager {
    /// Создает новый менеджер шардов
    pub fn new(configs: Vec<ShardConfig>, strategy: RoutingStrategy) -> Self {
        let mut shards = HashMap::new();
        
        for config in configs {
            let shard_info = ShardInfo {
                status: ShardStatus::Active,
                capacity: 1000000, // По умолчанию
                used_space: 0,
                collections: Vec::new(),
            };
            let shard = Shard {
                config: config.clone(),
                info: shard_info,
            };
            shards.insert(config.id.clone(), shard);
        }

        ShardManager {
            shards,
            routing_strategy: strategy,
            replication_factor: 2, // По умолчанию
        }
    }

    /// Определяет шард для коллекции
    pub fn get_shard_for_collection(&self, collection_name: &str) -> Result<String, String> {
        match self.routing_strategy {
            RoutingStrategy::HashBased => {
                let hash = calculate_hash(&collection_name);
                let shard_count = self.shards.len();
                let shard_index = (hash % shard_count as u64) as usize;
                
                if let Some(shard_id) = self.shards.keys().nth(shard_index) {
                    Ok(shard_id.clone())
                } else {
                    Err("Нет доступных шардов".to_string())
                }
            }
            RoutingStrategy::RangeBased => {
                // Для диапазонного роутинга нужна дополнительная логика
                // Пока используем хеш-роутинг
                let hash = calculate_hash(&collection_name);
                let shard_count = self.shards.len();
                let shard_index = (hash % shard_count as u64) as usize;
                
                if let Some(shard_id) = self.shards.keys().nth(shard_index) {
                    Ok(shard_id.clone())
                } else {
                    Err("Нет доступных шардов".to_string())
                }
            }
            RoutingStrategy::LSHBased => {
                // Для LSH-роутинга нужен доступ к LSH функции
                // Пока используем хеш-роутинг
                let hash = calculate_hash(&collection_name);
                let shard_count = self.shards.len();
                let shard_index = (hash % shard_count as u64) as usize;
                
                if let Some(shard_id) = self.shards.keys().nth(shard_index) {
                    Ok(shard_id.clone())
                } else {
                    Err("Нет доступных шардов".to_string())
                }
            }
            RoutingStrategy::MetadataBased => {
                // Для роутинга по метаданным нужны дополнительные параметры
                // Пока используем хеш-роутинг
                let hash = calculate_hash(&collection_name);
                let shard_count = self.shards.len();
                let shard_index = (hash % shard_count as u64) as usize;
                
                if let Some(shard_id) = self.shards.keys().nth(shard_index) {
                    Ok(shard_id.clone())
                } else {
                    Err("Нет доступных шардов".to_string())
                }
            }
        }
    }

    /// Определяет шарды для бакета (для репликации)
    pub fn get_shards_for_bucket(&self, bucket_id: u64) -> Result<Vec<String>, String> {
        let primary_shard = self.get_shard_for_bucket(bucket_id)?;
        let mut shards = vec![primary_shard];
        
        // Добавляем реплики
        for _ in 1..self.replication_factor {
            if let Some(replica_shard) = self.get_next_available_shard(&shards) {
                shards.push(replica_shard);
            }
        }
        
        Ok(shards)
    }

    /// Определяет основной шард для бакета
    fn get_shard_for_bucket(&self, bucket_id: u64) -> Result<String, String> {
        let shard_count = self.shards.len();
        let shard_index = (bucket_id % shard_count as u64) as usize;
        
        if let Some(shard_id) = self.shards.keys().nth(shard_index) {
            Ok(shard_id.clone())
        } else {
            Err("Нет доступных шардов".to_string())
        }
    }

    /// Получает следующий доступный шард (для репликации)
    fn get_next_available_shard(&self, exclude: &[String]) -> Option<String> {
        for (shard_id, shard) in &self.shards {
            if !exclude.contains(shard_id) && shard.info.status == ShardStatus::Active {
                return Some(shard_id.clone());
            }
        }
        None
    }

    /// Получает информацию о шарде
    pub fn get_shard_info(&self, shard_id: &str) -> Option<&Shard> {
        self.shards.get(shard_id)
    }

    /// Получает все активные шарды
    pub fn get_active_shards(&self) -> Vec<&Shard> {
        self.shards.values()
            .filter(|shard| shard.info.status == ShardStatus::Active)
            .collect()
    }

    /// Обновляет статус шарда
    pub fn update_shard_status(&mut self, shard_id: &str, status: ShardStatus) -> Result<(), String> {
        if let Some(shard) = self.shards.get_mut(shard_id) {
            shard.info.status = status;
            Ok(())
        } else {
            Err(format!("Шард {} не найден", shard_id))
        }
    }

    /// Добавляет коллекцию к шарду
    pub fn add_collection_to_shard(&mut self, shard_id: &str, collection_name: String) -> Result<(), String> {
        if let Some(shard) = self.shards.get_mut(shard_id) {
            if !shard.info.collections.contains(&collection_name) {
                shard.info.collections.push(collection_name);
            }
            Ok(())
        } else {
            Err(format!("Шард {} не найден", shard_id))
        }
    }

    /// Удаляет коллекцию из шарда
    pub fn remove_collection_from_shard(&mut self, shard_id: &str, collection_name: &str) -> Result<(), String> {
        if let Some(shard) = self.shards.get_mut(shard_id) {
            shard.info.collections.retain(|name| name != collection_name);
            Ok(())
        } else {
            Err(format!("Шард {} не найден", shard_id))
        }
    }

    /// Получает статистику по шардам
    pub fn get_shards_statistics(&self) -> HashMap<String, serde_json::Value> {
        let mut stats = HashMap::new();
        
        for (shard_id, shard) in &self.shards {
            let shard_stats = serde_json::json!({
                "id": shard_id,
                "host": shard.config.host,
                "port": shard.config.port,
                "status": format!("{:?}", shard.info.status),
                "capacity": shard.info.capacity,
                "used_space": shard.info.used_space,
                "collections_count": shard.info.collections.len(),
                "collections": shard.info.collections
            });
            stats.insert(shard_id.clone(), shard_stats);
        }
        
        stats
    }
}

impl ShardCoordinator {
    /// Создает новый координатор шардов
    pub fn new(
        shard_manager: Arc<RwLock<ShardManager>>,
    ) -> Self {
        ShardCoordinator {
            shard_manager,
            multi_shard_client: None,
        }
    }

    /// Создает новый координатор шардов с клиентом для удаленных шардов
    pub fn new_with_client(
        shard_manager: Arc<RwLock<ShardManager>>,
        multi_shard_client: Arc<crate::core::shard_client::MultiShardClient>,
    ) -> Self {
        ShardCoordinator {
            shard_manager,
            multi_shard_client: Some(multi_shard_client),
        }
    }

    /// Создает коллекцию с учетом шардирования
    pub async fn create_collection(
        &self,
        name: String,
        lsh_metric: LSHMetric,
        vector_dimension: usize,
    ) -> Result<(), String> {
        let shard_id = {
            let shard_manager = self.shard_manager.read().await;
            shard_manager.get_shard_for_collection(&name)?
        };

        // Отправляем команду создания коллекции на все шарды
        if let Some(ref multi_client) = self.multi_shard_client {
            // Создаем коллекцию на всех шардах
            let results = multi_client.create_collection_on_all_shards(
                name.clone(), 
                lsh_metric.clone(), 
                vector_dimension
            ).await;
            
            println!("📡 Создание коллекции на {} шардах: {}/{} успешно", 
                     results.results.len(), results.successful_operations, results.results.len());
            
            // Проверяем результаты
            for response in &results.results {
                if !response.success {
                    if let Some(error) = &response.error {
                        eprintln!("⚠️  Ошибка создания коллекции на шарде {}: {}", response.shard_id, error);
                    }
                }
            }
        }

        // Обновляем информацию о шарде
        {
            let mut shard_manager = self.shard_manager.write().await;
            shard_manager.add_collection_to_shard(&shard_id, name)?;
        }

        Ok(())
    }

    /// Удаляет коллекцию с учетом шардирования
    pub async fn delete_collection(&self, name: String) -> Result<(), String> {
        // Отправляем команду удаления коллекции на все шарды
        if let Some(ref multi_client) = self.multi_shard_client {
            let results = multi_client.delete_collection_on_all_shards(name.clone()).await;
            
            println!("📡 Удаление коллекции на {} шардах: {}/{} успешно", 
                     results.results.len(), results.successful_operations, results.results.len());
            
            // Проверяем результаты
            for response in &results.results {
                if !response.success {
                    if let Some(error) = &response.error {
                        eprintln!("⚠️  Ошибка удаления коллекции на шарде {}: {}", response.shard_id, error);
                    }
                }
            }
        }

        // Обновляем информацию о шардах
        {
            let mut shard_manager = self.shard_manager.write().await;
            let shard_ids: Vec<String> = shard_manager.shards.keys().cloned().collect();
            for shard_id in shard_ids {
                let _ = shard_manager.remove_collection_from_shard(&shard_id, &name);
            }
        }

        Ok(())
    }

    /// Добавляет вектор с учетом шардирования
    pub async fn add_vector(
        &self,
        collection_name: String,
        embedding: Vec<f32>,
        metadata: HashMap<String, String>,
    ) -> Result<u64, String> {
        // Определяем шард для коллекции
        let shard_id = {
            let shard_manager = self.shard_manager.read().await;
            shard_manager.get_shard_for_collection(&collection_name)?
        };

        // Отправляем вектор на соответствующий шард
        if let Some(ref multi_client) = self.multi_shard_client {
            match multi_client.add_vector_on_shard(&shard_id, collection_name.clone(), embedding.clone(), metadata.clone()).await {
                Ok(response) => {
                    if response.success {
                        // Получаем ID вектора из ответа
                        let vector_id = response.data
                            .and_then(|data| data.get("id").and_then(|v| v.as_u64()))
                            .unwrap_or(0);
                        println!("📡 Вектор добавлен на шард {}: ID={}", shard_id, vector_id);
                        Ok(vector_id)
                    } else {
                        if let Some(error) = response.error {
                            Err(format!("Ошибка добавления вектора на шард {}: {}", shard_id, error))
                        } else {
                            Err("Неизвестная ошибка добавления вектора".to_string())
                        }
                    }
                }
                Err(e) => {
                    Err(format!("Ошибка связи с шардом {}: {}", shard_id, e))
                }
            }
        } else {
            Err("Клиент для множественных шардов не инициализирован".to_string())
        }
    }

    /// Обновляет вектор с учетом шардирования
    pub async fn update_vector(
        &self,
        collection_name: String,
        vector_id: u64,
        new_embedding: Option<Vec<f32>>,
        new_metadata: Option<HashMap<String, String>>,
    ) -> Result<(), String> {
        // Отправляем обновление на все шарды
        if let Some(ref multi_client) = self.multi_shard_client {
            let mut found = false;
            for (shard_id, client) in multi_client.iter_clients() {
                match client.update_vector(collection_name.clone(), vector_id, new_embedding.clone(), new_metadata.clone()).await {
                    Ok(response) => {
                        if response.success {
                            println!("📡 Вектор обновлен на шарде {}: ID={}", shard_id, vector_id);
                            found = true;
                        } else {
                            if let Some(error) = response.error {
                                eprintln!("⚠️  Ошибка обновления вектора на шарде {}: {}", shard_id, error);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("⚠️  Ошибка связи с шардом {}: {}", shard_id, e);
                    }
                }
            }
            
            if !found {
                return Err(format!("Вектор с id {} не найден ни в одном бакете", vector_id));
            }
        }

        Ok(())
    }

    /// Удаляет вектор с учетом шардирования
    pub async fn delete_vector(
        &self,
        collection_name: String,
        vector_id: u64,
    ) -> Result<(), String> {
        // Отправляем удаление на все шарды
        if let Some(ref multi_client) = self.multi_shard_client {
            let mut found = false;
            for (shard_id, client) in multi_client.iter_clients() {
                match client.delete_vector(collection_name.clone(), vector_id).await {
                    Ok(response) => {
                        if response.success {
                            println!("📡 Вектор удален на шарде {}: ID={}", shard_id, vector_id);
                            found = true;
                        } else {
                            if let Some(error) = response.error {
                                eprintln!("⚠️  Ошибка удаления вектора на шарде {}: {}", shard_id, error);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("⚠️  Ошибка связи с шардом {}: {}", shard_id, e);
                    }
                }
            }
            
            if !found {
                return Err(format!("Вектор с id {} не найден ни в одном бакете", vector_id));
            }
        }

        Ok(())
    }

    /// Получает вектор по ID с учетом шардирования
    pub async fn get_vector(
        &self,
        collection_name: String,
        vector_id: u64,
    ) -> Result<crate::core::objects::Vector, String> {
        // Ищем вектор на всех шардах
        if let Some(ref multi_client) = self.multi_shard_client {
            for (shard_id, client) in multi_client.iter_clients() {
                match client.get_vector(collection_name.clone(), vector_id).await {
                    Ok(response) => {
                        if response.success {
                            if let Some(data) = response.data {
                                // Парсим данные вектора из ответа
                                if let Some(embedding) = data.get("embedding").and_then(|v| v.as_array()) {
                                    let embedding: Vec<f32> = embedding.iter()
                                        .filter_map(|v| v.as_f64().map(|f| f as f32))
                                        .collect();
                                    
                                    let metadata = data.get("metadata")
                                        .and_then(|m| m.as_object())
                                        .map(|obj| {
                                            obj.iter()
                                                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                                                .collect()
                                        })
                                        .unwrap_or_default();

                                    return Ok(crate::core::objects::Vector::new(
                                        Some(embedding),
                                        Some(chrono::Utc::now().timestamp()),
                                        Some(metadata)
                                    ));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("⚠️  Ошибка получения вектора с шарда {}: {}", shard_id, e);
                    }
                }
            }
        }

        Err(format!("Вектор с ID {} не найден в коллекции {}", vector_id, collection_name))
    }

    /// Фильтрует векторы по метаданным с учетом шардирования
    pub async fn filter_by_metadata(
        &self,
        collection_name: String,
        filters: HashMap<String, String>,
    ) -> Result<Vec<u64>, String> {
        let mut all_results = Vec::new();

        // Фильтруем на удаленных шардах
        if let Some(ref multi_client) = self.multi_shard_client {
            let shard_id = {
                let shard_manager = self.shard_manager.read().await;
                shard_manager.get_shard_for_collection(&collection_name)?
            };

            if let Some(client) = multi_client.get_client(&shard_id) {
                match client.filter_by_metadata(collection_name.clone(), filters.clone()).await {
                    Ok(response) => {
                        if response.success {
                            if let Some(data) = response.data {
                                if let Some(vector_ids) = data.get("vector_ids").and_then(|v| v.as_array()) {
                                    for id in vector_ids {
                                        if let Some(vector_id) = id.as_u64() {
                                            all_results.push(vector_id);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("⚠️  Ошибка фильтрации на шарде {}: {}", shard_id, e);
                    }
                }
            }
        }

        // Удаляем дубликаты и сортируем
        all_results.sort();
        all_results.dedup();

        Ok(all_results)
    }

    /// Выполняет поиск похожих векторов с учетом шардирования
    pub async fn find_similar_vectors(
        &self,
        collection_name: String,
        query: Vec<f32>,
        k: usize,
    ) -> Result<Vec<(u64, usize, f32)>, String> {
        let mut all_results = Vec::new();

        // Ищем на удаленных шардах
        if let Some(ref multi_client) = self.multi_shard_client {
            match multi_client.find_similar_across_shards(collection_name, query, k).await {
                Ok(remote_results) => {
                    all_results.extend(remote_results);
                }
                Err(e) => {
                    eprintln!("⚠️  Ошибка поиска на удаленных шардах: {}", e);
                }
            }
        }

        // Сортируем по убыванию схожести и берем топ k
        all_results.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
        all_results.truncate(k);

        Ok(all_results)
    }

    /// Получает статистику по всем шардам
    pub async fn get_cluster_statistics(&self) -> HashMap<String, serde_json::Value> {
        let shard_manager = self.shard_manager.read().await;
        shard_manager.get_shards_statistics()
    }

    /// Сохраняет данные в шардах
    pub async fn dump_data(&self) -> Result<(), String> {
        // Отправляем команды сохранения на удаленные шарды
        if let Some(ref multi_client) = self.multi_shard_client {
            let mut results = Vec::new();
            let mut _successful = 0;
            let mut _failed = 0;

            for (shard_id, client) in multi_client.iter_clients() {
                let request = crate::core::shard_client::ShardRequest {
                    operation: "dump".to_string(),
                    collection: None,
                    vector_id: None,
                    embedding: None,
                    metadata: None,
                    query: None,
                    k: None,
                    filters: None,
                };
                match client.send_request(request).await {
                    Ok(mut response) => {
                        response.shard_id = shard_id.clone();
                        results.push(response);
                        _successful += 1;
                    }
                    Err(error) => {
                        results.push(crate::core::shard_client::ShardResponse {
                            success: false,
                            data: None,
                            error: Some(error),
                            shard_id: shard_id.clone(),
                        });
                        _failed += 1;
                    }
                }
            }

            println!("📡 Сохранение данных на {} шардах: {}/{} успешно", 
                     results.len(), _successful, results.len());
            
            // Проверяем результаты
            for response in &results {
                if !response.success {
                    if let Some(error) = &response.error {
                        eprintln!("⚠️  Ошибка сохранения на шарде {}: {}", response.shard_id, error);
                    }
                }
            }
        }
        
        Ok(())
    }

    /// Загружает данные из шардов
    pub async fn load_data(&self) -> Result<(), String> {
        // Отправляем команды загрузки на удаленные шарды
        if let Some(ref multi_client) = self.multi_shard_client {
            let mut results = Vec::new();
            let mut _successful = 0;
            let mut _failed = 0;

            for (shard_id, client) in multi_client.iter_clients() {
                let request = crate::core::shard_client::ShardRequest {
                    operation: "load".to_string(),
                    collection: None,
                    vector_id: None,
                    embedding: None,
                    metadata: None,
                    query: None,
                    k: None,
                    filters: None,
                };
                match client.send_request(request).await {
                    Ok(mut response) => {
                        response.shard_id = shard_id.clone();
                        results.push(response);
                        _successful += 1;
                    }
                    Err(error) => {
                        results.push(crate::core::shard_client::ShardResponse {
                            success: false,
                            data: None,
                            error: Some(error),
                            shard_id: shard_id.clone(),
                        });
                        _failed += 1;
                    }
                }
            }

            println!("📡 Загрузка данных с {} шардов: {}/{} успешно", 
                     results.len(), _successful, results.len());
            
            // Проверяем результаты
            for response in &results {
                if !response.success {
                    if let Some(error) = &response.error {
                        eprintln!("⚠️  Ошибка загрузки с шарда {}: {}", response.shard_id, error);
                    }
                }
            }
        }
        
        Ok(())
    }

    /// Балансирует нагрузку между шардами
    pub async fn rebalance_shards(&self) -> Result<(), String> {
        let shard_manager = self.shard_manager.read().await;
        
        // Анализируем нагрузку на каждый шард
        let mut shard_loads: Vec<(String, f64)> = Vec::new();
        for (shard_id, shard) in &shard_manager.shards {
            if shard.info.status == ShardStatus::Active {
                let load = shard.info.used_space as f64 / shard.info.capacity as f64;
                shard_loads.push((shard_id.clone(), load));
            }
        }
        
        // Сортируем по нагрузке
        shard_loads.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        
        // Если разница в нагрузке между самыми загруженным и наименее загруженным шардом
        // превышает 20%, выполняем балансировку
        if shard_loads.len() > 1 {
            let max_load = shard_loads.last().unwrap().1;
            let min_load = shard_loads.first().unwrap().1;
            
            if max_load - min_load > 0.2 {
                // Здесь можно добавить логику миграции коллекций
                // между перегруженными и недогруженными шардами
                println!("Выполняется балансировка шардов: max_load={:.2}, min_load={:.2}", max_load, min_load);
            }
        }
        
        Ok(())
    }

    /// Получает коллекцию по имени
    pub async fn get_collection(&self, name: String) -> Result<Option<crate::core::objects::Collection>, String> {
        // Ищем коллекцию на всех шардах
        if let Some(ref multi_client) = self.multi_shard_client {
            for (shard_id, client) in multi_client.iter_clients() {
                let request = crate::core::shard_client::ShardRequest {
                    operation: "get_collection".to_string(),
                    collection: Some(name.clone()),
                    vector_id: None,
                    embedding: None,
                    metadata: None,
                    query: None,
                    k: None,
                    filters: None,
                };
                match client.send_request(request).await {
                    Ok(response) => {
                        if response.success {
                            if let Some(data) = response.data {
                                // Парсим данные коллекции из ответа
                                if let (Some(collection_name), Some(metric_str), Some(dimension)) = (
                                    data.get("name").and_then(|v| v.as_str()),
                                    data.get("metric").and_then(|v| v.as_str()),
                                    data.get("dimension").and_then(|v| v.as_u64()),
                                ) {
                                    // Парсим LSH метрику
                                    let metric = match metric_str {
                                        "Cosine" => crate::core::lsh::LSHMetric::Cosine,
                                        "Euclidean" => crate::core::lsh::LSHMetric::Euclidean,
                                        "Manhattan" => crate::core::lsh::LSHMetric::Manhattan,
                                        _ => continue,
                                    };

                                    return Ok(Some(crate::core::objects::Collection::new(
                                        Some(collection_name.to_string()),
                                        metric,
                                        dimension as usize
                                    )));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("⚠️  Ошибка получения коллекции с шарда {}: {}", shard_id, e);
                    }
                }
            }
        }

        Ok(None)
    }

    /// Получает все коллекции
    pub async fn get_all_collections(&self) -> Result<Vec<crate::core::objects::Collection>, String> {
        let mut all_collections = Vec::new();

        // Получаем коллекции с удаленных шардов
        if let Some(ref multi_client) = self.multi_shard_client {
            let mut results = Vec::new();
            let mut _successful = 0;
            let mut _failed = 0;

            for (shard_id, client) in multi_client.iter_clients() {
                let request = crate::core::shard_client::ShardRequest {
                    operation: "get_all_collections".to_string(),
                    collection: None,
                    vector_id: None,
                    embedding: None,
                    metadata: None,
                    query: None,
                    k: None,
                    filters: None,
                };
                match client.send_request(request).await {
                    Ok(mut response) => {
                        response.shard_id = shard_id.clone();
                        results.push(response);
                        _successful += 1;
                    }
                    Err(error) => {
                        results.push(crate::core::shard_client::ShardResponse {
                            success: false,
                            data: None,
                            error: Some(error),
                            shard_id: shard_id.clone(),
                        });
                        _failed += 1;
                    }
                }
            }

            for response in &results {
                if response.success {
                    if let Some(data) = &response.data {
                        if let Some(collections) = data.get("collections").and_then(|v| v.as_array()) {
                            for collection_data in collections {
                                if let (Some(name), Some(metric_str), Some(dimension)) = (
                                    collection_data.get("name").and_then(|v| v.as_str()),
                                    collection_data.get("metric").and_then(|v| v.as_str()),
                                    collection_data.get("dimension").and_then(|v| v.as_u64()),
                                ) {
                                    let metric = match metric_str {
                                        "Cosine" => crate::core::lsh::LSHMetric::Cosine,
                                        "Euclidean" => crate::core::lsh::LSHMetric::Euclidean,
                                        "Manhattan" => crate::core::lsh::LSHMetric::Manhattan,
                                        _ => continue,
                                    };

                                    all_collections.push(crate::core::objects::Collection::new(
                                        Some(name.to_string()),
                                        metric,
                                        dimension as usize
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        // Удаляем дубликаты по имени
        all_collections.sort_by(|a, b| a.name.cmp(&b.name));
        all_collections.dedup_by(|a, b| a.name == b.name);

        Ok(all_collections)
    }
}