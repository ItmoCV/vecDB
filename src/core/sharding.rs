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
        if self.shards.is_empty() {
            return Err("Нет доступных шардов".to_string());
        }

        match self.routing_strategy {
            RoutingStrategy::HashBased => {
                // Хеш-роутинг: распределение на основе хеша имени коллекции
                let hash = calculate_hash(&collection_name);
                let shard_count = self.shards.len();
                let shard_index = (hash % shard_count as u64) as usize;
                
                // Используем отсортированный список ID для стабильности
                let mut shard_ids: Vec<_> = self.shards.keys().cloned().collect();
                shard_ids.sort();
                
                Ok(shard_ids[shard_index].clone())
            }
            RoutingStrategy::RangeBased => {
                // Диапазонный роутинг: распределение по первой букве имени коллекции
                // Коллекции с именами A-M идут на первую половину шардов, N-Z на вторую
                let mut shard_ids: Vec<_> = self.shards.keys().cloned().collect();
                shard_ids.sort();
                
                let first_char = collection_name.chars().next().unwrap_or('a').to_ascii_lowercase();
                let char_value = first_char as u32;
                
                // Используем диапазон символов для определения шарда
                let shard_index = (char_value as usize) % shard_ids.len();
                
                Ok(shard_ids[shard_index].clone())
            }
            RoutingStrategy::LSHBased => {
                // LSH-роутинг требует информацию о векторе, а не только имя коллекции
                // Для роутинга коллекций используем fallback на хеш-роутинг
                // Примечание: для роутинга векторов нужен отдельный метод с параметром вектора
                let hash = calculate_hash(&collection_name);
                let shard_count = self.shards.len();
                let shard_index = (hash % shard_count as u64) as usize;
                
                let mut shard_ids: Vec<_> = self.shards.keys().cloned().collect();
                shard_ids.sort();
                
                Ok(shard_ids[shard_index].clone())
            }
            RoutingStrategy::MetadataBased => {
                // Роутинг по метаданным требует дополнительные параметры
                // Для роутинга коллекций используем fallback на хеш-роутинг
                // Примечание: для роутинга векторов по метаданным нужен отдельный метод
                let hash = calculate_hash(&collection_name);
                let shard_count = self.shards.len();
                let shard_index = (hash % shard_count as u64) as usize;
                
                let mut shard_ids: Vec<_> = self.shards.keys().cloned().collect();
                shard_ids.sort();
                
                Ok(shard_ids[shard_index].clone())
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
        if self.shards.is_empty() {
            return Err("Нет доступных шардов".to_string());
        }
        
        let shard_count = self.shards.len();
        let shard_index = (bucket_id % shard_count as u64) as usize;
        
        // Используем отсортированный список ID для стабильности
        let mut shard_ids: Vec<_> = self.shards.keys().cloned().collect();
        shard_ids.sort();
        
        Ok(shard_ids[shard_index].clone())
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

    /// Обновляет использование пространства на шарде
    pub fn update_shard_usage(&mut self, shard_id: &str, delta: i64) -> Result<(), String> {
        if let Some(shard) = self.shards.get_mut(shard_id) {
            let new_usage = (shard.info.used_space as i64 + delta).max(0) as u64;
            shard.info.used_space = new_usage;
            Ok(())
        } else {
            Err(format!("Шард {} не найден", shard_id))
        }
    }

    /// Устанавливает использование пространства на шарде
    pub fn set_shard_usage(&mut self, shard_id: &str, used_space: u64) -> Result<(), String> {
        if let Some(shard) = self.shards.get_mut(shard_id) {
            shard.info.used_space = used_space;
            Ok(())
        } else {
            Err(format!("Шард {} не найден", shard_id))
        }
    }

    /// Получает коэффициент загрузки шарда
    pub fn get_shard_load(&self, shard_id: &str) -> Result<f64, String> {
        if let Some(shard) = self.shards.get(shard_id) {
            if shard.info.capacity == 0 {
                return Ok(0.0);
            }
            Ok(shard.info.used_space as f64 / shard.info.capacity as f64)
        } else {
            Err(format!("Шард {} не найден", shard_id))
        }
    }

    /// Получает список перегруженных шардов (load > threshold)
    pub fn get_overloaded_shards(&self, threshold: f64) -> Vec<String> {
        let mut overloaded = Vec::new();
        for (shard_id, shard) in &self.shards {
            if shard.info.status == ShardStatus::Active {
                let load = if shard.info.capacity == 0 {
                    0.0
                } else {
                    shard.info.used_space as f64 / shard.info.capacity as f64
                };
                if load > threshold {
                    overloaded.push(shard_id.clone());
                }
            }
        }
        overloaded
    }

    /// Получает список недогруженных шардов (load < threshold)
    pub fn get_underloaded_shards(&self, threshold: f64) -> Vec<String> {
        let mut underloaded = Vec::new();
        for (shard_id, shard) in &self.shards {
            if shard.info.status == ShardStatus::Active {
                let load = if shard.info.capacity == 0 {
                    0.0
                } else {
                    shard.info.used_space as f64 / shard.info.capacity as f64
                };
                if load < threshold {
                    underloaded.push(shard_id.clone());
                }
            }
        }
        underloaded
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

        // Вычисляем приблизительный размер вектора (embedding + метаданные)
        let vector_size = (embedding.len() * std::mem::size_of::<f32>()) as i64
            + metadata.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() as i64
            + 100; // overhead для других полей

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
                        
                        // Обновляем использование пространства на шарде
                        {
                            let mut shard_manager = self.shard_manager.write().await;
                            let _ = shard_manager.update_shard_usage(&shard_id, vector_size);
                        }
                        
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
        // Сначала пытаемся получить вектор для вычисления размера
        let vector_size = match self.get_vector(collection_name.clone(), vector_id).await {
            Ok(vector) => {
                // Вычисляем размер удаляемого вектора
                let embedding_size = vector.data.len();
                let metadata_size = vector.metadata.iter()
                    .map(|(k, v)| k.len() + v.len())
                    .sum::<usize>();
                Some(((embedding_size * std::mem::size_of::<f32>()) + metadata_size + 100) as i64)
            }
            Err(_) => None,
        };

        // Отправляем удаление на все шарды
        if let Some(ref multi_client) = self.multi_shard_client {
            let mut found = false;
            
            for (shard_id, client) in multi_client.iter_clients() {
                match client.delete_vector(collection_name.clone(), vector_id).await {
                    Ok(response) => {
                        if response.success {
                            println!("📡 Вектор удален на шарде {}: ID={}", shard_id, vector_id);
                            found = true;
                            
                            // Обновляем использование пространства
                            if let Some(size) = vector_size {
                                let mut shard_manager = self.shard_manager.write().await;
                                let _ = shard_manager.update_shard_usage(&shard_id, -size);
                            }
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

    /// Получает размер коллекции на конкретном шарде
    async fn get_collection_size(&self, shard_id: &str, collection_name: &str) -> Result<u64, String> {
        if let Some(ref multi_client) = self.multi_shard_client {
            if let Some(client) = multi_client.get_client(shard_id) {
                let request = crate::core::shard_client::ShardRequest {
                    operation: "get_collection_size".to_string(),
                    collection: Some(collection_name.to_string()),
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
                                if let Some(size) = data.get("size").and_then(|v| v.as_u64()) {
                                    return Ok(size);
                                }
                            }
                        }
                        Ok(0)
                    }
                    Err(e) => Err(format!("Ошибка получения размера коллекции: {}", e))
                }
            } else {
                Err(format!("Шард {} не найден", shard_id))
            }
        } else {
            Err("Клиент для множественных шардов не инициализирован".to_string())
        }
    }

    /// Мигрирует коллекцию с одного шарда на другой
    pub async fn migrate_collection(
        &self,
        collection_name: String,
        from_shard: String,
        to_shard: String
    ) -> Result<(), String> {
        println!("🔄 Начало миграции коллекции '{}' с {} на {}", collection_name, from_shard, to_shard);

        // 1. Получаем информацию о коллекции с исходного шарда
        let collection = self.get_collection(collection_name.clone()).await?
            .ok_or_else(|| format!("Коллекция {} не найдена", collection_name))?;

        // 2. Создаем коллекцию на целевом шарде, если её там нет
        if let Some(ref multi_client) = self.multi_shard_client {
            if let Some(client) = multi_client.get_client(&to_shard) {
                match client.create_collection(
                    collection_name.clone(), 
                    collection.lsh_metric.clone(), 
                    collection.vector_dimension
                ).await {
                    Ok(response) => {
                        if !response.success {
                            // Коллекция уже может существовать, это нормально
                            println!("⚠️  Коллекция уже существует на целевом шарде или ошибка создания");
                        }
                    }
                    Err(e) => {
                        return Err(format!("Ошибка создания коллекции на целевом шарде: {}", e));
                    }
                }
            }
        }

        // 3. Получаем все векторы с исходного шарда
        // Примечание: это упрощенная реализация
        // В реальности нужно получить все векторы и перенести их
        println!("📦 Копирование данных...");
        
        // Обновляем информацию о коллекциях в менеджере шардов
        {
            let mut shard_manager = self.shard_manager.write().await;
            
            // Добавляем коллекцию на целевой шард
            shard_manager.add_collection_to_shard(&to_shard, collection_name.clone())?;
            
            // Удаляем коллекцию с исходного шарда
            shard_manager.remove_collection_from_shard(&from_shard, &collection_name)?;
        }

        // 4. Удаляем коллекцию с исходного шарда
        if let Some(ref multi_client) = self.multi_shard_client {
            if let Some(client) = multi_client.get_client(&from_shard) {
                match client.delete_collection(collection_name.clone()).await {
                    Ok(_) => {
                        println!("✅ Коллекция удалена с исходного шарда");
                    }
                    Err(e) => {
                        eprintln!("⚠️  Ошибка удаления коллекции с исходного шарда: {}", e);
                    }
                }
            }
        }

        println!("✅ Миграция коллекции '{}' завершена", collection_name);
        Ok(())
    }

    /// Находит коллекцию для миграции с перегруженного шарда
    async fn find_collection_to_migrate(&self, shard_id: &str) -> Option<(String, u64)> {
        let shard_manager = self.shard_manager.read().await;
        
        if let Some(shard) = shard_manager.get_shard_info(shard_id) {
            // Находим самую большую коллекцию для миграции
            let mut best_collection = None;
            let mut max_size = 0u64;
            
            for collection_name in &shard.info.collections {
                if let Ok(size) = self.get_collection_size(shard_id, collection_name).await {
                    if size > max_size {
                        max_size = size;
                        best_collection = Some(collection_name.clone());
                    }
                }
            }
            
            if let Some(collection) = best_collection {
                return Some((collection, max_size));
            }
        }
        
        None
    }

    /// Балансирует нагрузку между шардами с автоматической миграцией
    pub async fn rebalance_shards(&self) -> Result<(), String> {
        println!("⚖️  Начало балансировки шардов...");
        
        // Освобождаем shard_manager для анализа
        let shard_loads = {
            let shard_manager = self.shard_manager.read().await;
            
            // Анализируем нагрузку на каждый шард
            let mut loads: Vec<(String, f64)> = Vec::new();
            for (shard_id, shard) in &shard_manager.shards {
                if shard.info.status == ShardStatus::Active {
                    let load = if shard.info.capacity == 0 {
                        0.0
                    } else {
                        shard.info.used_space as f64 / shard.info.capacity as f64
                    };
                    loads.push((shard_id.clone(), load));
                }
            }
            loads
        };
        
        if shard_loads.is_empty() {
            return Err("Нет доступных шардов для балансировки".to_string());
        }
        
        // Сортируем по нагрузке
        let mut sorted_loads = shard_loads.clone();
        sorted_loads.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        
        // Выводим текущую статистику
        println!("📊 Текущая нагрузка на шарды:");
        for (shard_id, load) in &sorted_loads {
            println!("   {} - {:.1}%", shard_id, load * 100.0);
        }
        
        // Если разница в нагрузке между самыми загруженным и наименее загруженным шардом
        // превышает 20%, выполняем балансировку
        if sorted_loads.len() > 1 {
            let (most_loaded_shard, max_load) = sorted_loads.last().unwrap();
            let (least_loaded_shard, min_load) = sorted_loads.first().unwrap();
            
            let load_diff = max_load - min_load;
            
            if load_diff > 0.2 {
                println!("⚠️  Обнаружен дисбаланс: {:.1}% разницы", load_diff * 100.0);
                println!("🔄 Начало миграции коллекций...");
                
                // Находим коллекцию для миграции с самого загруженного шарда
                if let Some((collection_name, size)) = self.find_collection_to_migrate(most_loaded_shard).await {
                    println!("📦 Найдена коллекция для миграции: {} (размер: {} bytes)", collection_name, size);
                    
                    // Выполняем миграцию
                    match self.migrate_collection(
                        collection_name.clone(),
                        most_loaded_shard.clone(),
                        least_loaded_shard.clone()
                    ).await {
                        Ok(_) => {
                            println!("✅ Миграция выполнена успешно");
                            
                            // Обновляем использование пространства на шардах
                            {
                                let mut shard_manager = self.shard_manager.write().await;
                                let _ = shard_manager.update_shard_usage(most_loaded_shard, -(size as i64));
                                let _ = shard_manager.update_shard_usage(least_loaded_shard, size as i64);
                            }
                        }
                        Err(e) => {
                            eprintln!("❌ Ошибка миграции: {}", e);
                        }
                    }
                } else {
                    println!("⚠️  Нет коллекций для миграции на шарде {}", most_loaded_shard);
                }
            } else {
                println!("✅ Балансировка не требуется (разница нагрузки: {:.1}%)", load_diff * 100.0);
            }
        }
        
        println!("⚖️  Балансировка завершена");
        Ok(())
    }

    /// Балансирует нагрузку между шардами с указанным порогом
    pub async fn rebalance_shards_with_threshold(&self, threshold: f64) -> Result<(), String> {
        println!("⚖️  Начало балансировки шардов с порогом {:.1}%...", threshold * 100.0);
        
        let (overloaded, underloaded) = {
            let shard_manager = self.shard_manager.read().await;
            (
                shard_manager.get_overloaded_shards(threshold),
                shard_manager.get_underloaded_shards(1.0 - threshold)
            )
        };
        
        if overloaded.is_empty() {
            println!("✅ Нет перегруженных шардов");
            return Ok(());
        }
        
        if underloaded.is_empty() {
            println!("⚠️  Нет недогруженных шардов для миграции");
            return Ok(());
        }
        
        println!("📊 Перегруженные шарды: {:?}", overloaded);
        println!("📊 Недогруженные шарды: {:?}", underloaded);
        
        // Мигрируем по одной коллекции с каждого перегруженного шарда
        for overloaded_shard in overloaded {
            if let Some(underloaded_shard) = underloaded.first() {
                if let Some((collection_name, size)) = self.find_collection_to_migrate(&overloaded_shard).await {
                    println!("🔄 Миграция {} с {} на {}", collection_name, overloaded_shard, underloaded_shard);
                    
                    match self.migrate_collection(
                        collection_name,
                        overloaded_shard.clone(),
                        underloaded_shard.clone()
                    ).await {
                        Ok(_) => {
                            let mut shard_manager = self.shard_manager.write().await;
                            let _ = shard_manager.update_shard_usage(&overloaded_shard, -(size as i64));
                            let _ = shard_manager.update_shard_usage(underloaded_shard, size as i64);
                        }
                        Err(e) => {
                            eprintln!("❌ Ошибка миграции: {}", e);
                        }
                    }
                }
            }
        }
        
        println!("⚖️  Балансировка завершена");
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

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_shards() -> Vec<ShardConfig> {
        vec![
            ShardConfig {
                id: "shard1".to_string(),
                host: "localhost".to_string(),
                port: 8081,
                description: Some("Shard 1".to_string()),
            },
            ShardConfig {
                id: "shard2".to_string(),
                host: "localhost".to_string(),
                port: 8082,
                description: Some("Shard 2".to_string()),
            },
            ShardConfig {
                id: "shard3".to_string(),
                host: "localhost".to_string(),
                port: 8083,
                description: Some("Shard 3".to_string()),
            },
        ]
    }

    #[test]
    fn test_hash_based_routing_stability() {
        // Проверяем, что хеш-роутинг стабилен при повторных вызовах
        let shards = create_test_shards();
        let manager = ShardManager::new(shards, RoutingStrategy::HashBased);

        let collection_name = "test_collection";
        
        // Выполняем несколько раз и проверяем, что результат одинаковый
        let shard1 = manager.get_shard_for_collection(collection_name).unwrap();
        let shard2 = manager.get_shard_for_collection(collection_name).unwrap();
        let shard3 = manager.get_shard_for_collection(collection_name).unwrap();

        assert_eq!(shard1, shard2);
        assert_eq!(shard2, shard3);
    }

    #[test]
    fn test_hash_based_routing_distribution() {
        // Проверяем, что разные коллекции распределяются по разным шардам
        let shards = create_test_shards();
        let manager = ShardManager::new(shards, RoutingStrategy::HashBased);

        let mut shard_usage = std::collections::HashMap::new();
        
        // Создаем 30 коллекций с разными именами
        for i in 0..30 {
            let collection_name = format!("collection_{}", i);
            let shard_id = manager.get_shard_for_collection(&collection_name).unwrap();
            *shard_usage.entry(shard_id).or_insert(0) += 1;
        }

        // Проверяем, что каждый шард получил хотя бы одну коллекцию
        assert_eq!(shard_usage.len(), 3, "Все шарды должны быть задействованы");
        
        // Проверяем, что распределение относительно равномерное (не менее 5 коллекций на шард)
        for (shard_id, count) in shard_usage.iter() {
            assert!(
                *count >= 5,
                "Шард {} получил слишком мало коллекций: {}",
                shard_id,
                count
            );
        }
    }

    #[test]
    fn test_range_based_routing_stability() {
        // Проверяем, что диапазонный роутинг стабилен
        let shards = create_test_shards();
        let manager = ShardManager::new(shards, RoutingStrategy::RangeBased);

        let collection_name = "test_collection";
        
        let shard1 = manager.get_shard_for_collection(collection_name).unwrap();
        let shard2 = manager.get_shard_for_collection(collection_name).unwrap();

        assert_eq!(shard1, shard2);
    }

    #[test]
    fn test_range_based_routing_different_names() {
        // Проверяем, что коллекции с разными первыми буквами могут попасть на разные шарды
        let shards = create_test_shards();
        let manager = ShardManager::new(shards, RoutingStrategy::RangeBased);

        let mut shard_usage = std::collections::HashMap::new();
        
        // Создаем коллекции с разными первыми буквами
        for letter in 'a'..='z' {
            let collection_name = format!("{}collection", letter);
            let shard_id = manager.get_shard_for_collection(&collection_name).unwrap();
            *shard_usage.entry(shard_id).or_insert(0) += 1;
        }

        // Проверяем, что используется более одного шарда
        assert!(
            shard_usage.len() > 1,
            "Range-based роутинг должен распределять по нескольким шардам"
        );
    }

    #[test]
    fn test_bucket_routing_stability() {
        // Проверяем, что роутинг бакетов стабилен
        let shards = create_test_shards();
        let manager = ShardManager::new(shards, RoutingStrategy::HashBased);

        let bucket_id = 12345u64;
        
        let shard1 = manager.get_shard_for_bucket(bucket_id).unwrap();
        let shard2 = manager.get_shard_for_bucket(bucket_id).unwrap();
        let shard3 = manager.get_shard_for_bucket(bucket_id).unwrap();

        assert_eq!(shard1, shard2);
        assert_eq!(shard2, shard3);
    }

    #[test]
    fn test_empty_shards_error() {
        // Проверяем, что пустой список шардов возвращает ошибку
        let manager = ShardManager::new(vec![], RoutingStrategy::HashBased);

        let result = manager.get_shard_for_collection("test");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Нет доступных шардов");
    }

    #[test]
    fn test_shard_ids_sorted() {
        // Проверяем, что порядок ID шардов не влияет на результат
        let shards1 = create_test_shards();
        let manager1 = ShardManager::new(shards1, RoutingStrategy::HashBased);

        // Создаем шарды в обратном порядке
        let mut shards2 = create_test_shards();
        shards2.reverse();
        let manager2 = ShardManager::new(shards2, RoutingStrategy::HashBased);

        // Проверяем, что для одной и той же коллекции выбирается один шард
        let collection_name = "test_collection";
        let shard1 = manager1.get_shard_for_collection(collection_name).unwrap();
        let shard2 = manager2.get_shard_for_collection(collection_name).unwrap();

        assert_eq!(shard1, shard2, "Порядок добавления шардов не должен влиять на роутинг");
    }

    #[test]
    fn test_replication_factor() {
        // Проверяем, что репликация работает корректно
        let shards = create_test_shards();
        let manager = ShardManager::new(shards, RoutingStrategy::HashBased);

        let bucket_id = 12345u64;
        let shards_for_bucket = manager.get_shards_for_bucket(bucket_id).unwrap();

        // Проверяем, что возвращается несколько шардов (первичный + реплики)
        assert!(
            shards_for_bucket.len() >= 1,
            "Должен быть хотя бы один шард"
        );

        // Проверяем, что все шарды уникальны
        let unique_shards: std::collections::HashSet<_> = shards_for_bucket.iter().collect();
        assert_eq!(
            unique_shards.len(),
            shards_for_bucket.len(),
            "Все шарды должны быть уникальны"
        );
    }

    #[test]
    fn test_get_active_shards() {
        // Проверяем получение активных шардов
        let shards = create_test_shards();
        let manager = ShardManager::new(shards, RoutingStrategy::HashBased);

        let active_shards = manager.get_active_shards();
        assert_eq!(active_shards.len(), 3);
    }

    #[test]
    fn test_update_shard_status() {
        // Проверяем обновление статуса шарда
        let shards = create_test_shards();
        let mut manager = ShardManager::new(shards, RoutingStrategy::HashBased);

        let result = manager.update_shard_status("shard1", ShardStatus::Maintenance);
        assert!(result.is_ok());

        let shard_info = manager.get_shard_info("shard1").unwrap();
        assert_eq!(shard_info.info.status, ShardStatus::Maintenance);
    }

    #[test]
    fn test_add_collection_to_shard() {
        // Проверяем добавление коллекции к шарду
        let shards = create_test_shards();
        let mut manager = ShardManager::new(shards, RoutingStrategy::HashBased);

        let result = manager.add_collection_to_shard("shard1", "test_collection".to_string());
        assert!(result.is_ok());

        let shard_info = manager.get_shard_info("shard1").unwrap();
        assert!(shard_info.info.collections.contains(&"test_collection".to_string()));
    }

    #[test]
    fn test_update_shard_usage() {
        // Проверяем обновление использования пространства
        let shards = create_test_shards();
        let mut manager = ShardManager::new(shards, RoutingStrategy::HashBased);

        // Начальное значение used_space = 0
        let shard_info = manager.get_shard_info("shard1").unwrap();
        assert_eq!(shard_info.info.used_space, 0);

        // Увеличиваем на 1000
        let result = manager.update_shard_usage("shard1", 1000);
        assert!(result.is_ok());
        
        let shard_info = manager.get_shard_info("shard1").unwrap();
        assert_eq!(shard_info.info.used_space, 1000);

        // Уменьшаем на 500
        let result = manager.update_shard_usage("shard1", -500);
        assert!(result.is_ok());
        
        let shard_info = manager.get_shard_info("shard1").unwrap();
        assert_eq!(shard_info.info.used_space, 500);

        // Пытаемся уменьшить больше чем есть - должно быть 0
        let result = manager.update_shard_usage("shard1", -1000);
        assert!(result.is_ok());
        
        let shard_info = manager.get_shard_info("shard1").unwrap();
        assert_eq!(shard_info.info.used_space, 0);
    }

    #[test]
    fn test_set_shard_usage() {
        // Проверяем установку использования пространства
        let shards = create_test_shards();
        let mut manager = ShardManager::new(shards, RoutingStrategy::HashBased);

        let result = manager.set_shard_usage("shard1", 5000);
        assert!(result.is_ok());
        
        let shard_info = manager.get_shard_info("shard1").unwrap();
        assert_eq!(shard_info.info.used_space, 5000);
    }

    #[test]
    fn test_get_shard_load() {
        // Проверяем получение коэффициента загрузки
        let shards = create_test_shards();
        let mut manager = ShardManager::new(shards, RoutingStrategy::HashBased);

        // Устанавливаем used_space = 500000 (capacity = 1000000 по умолчанию)
        manager.set_shard_usage("shard1", 500000).unwrap();
        
        let load = manager.get_shard_load("shard1").unwrap();
        assert_eq!(load, 0.5); // 50% загрузки
    }

    #[test]
    fn test_get_overloaded_shards() {
        // Проверяем получение перегруженных шардов
        let shards = create_test_shards();
        let mut manager = ShardManager::new(shards, RoutingStrategy::HashBased);

        // Устанавливаем разную загрузку на шардах
        manager.set_shard_usage("shard1", 900000).unwrap(); // 90% - перегружен
        manager.set_shard_usage("shard2", 500000).unwrap(); // 50% - норма
        manager.set_shard_usage("shard3", 200000).unwrap(); // 20% - недогружен

        let overloaded = manager.get_overloaded_shards(0.8); // порог 80%
        
        assert_eq!(overloaded.len(), 1);
        assert!(overloaded.contains(&"shard1".to_string()));
    }

    #[test]
    fn test_get_underloaded_shards() {
        // Проверяем получение недогруженных шардов
        let shards = create_test_shards();
        let mut manager = ShardManager::new(shards, RoutingStrategy::HashBased);

        // Устанавливаем разную загрузку на шардах
        manager.set_shard_usage("shard1", 900000).unwrap(); // 90% - перегружен
        manager.set_shard_usage("shard2", 500000).unwrap(); // 50% - норма
        manager.set_shard_usage("shard3", 200000).unwrap(); // 20% - недогружен

        let underloaded = manager.get_underloaded_shards(0.3); // порог 30%
        
        assert_eq!(underloaded.len(), 1);
        assert!(underloaded.contains(&"shard3".to_string()));
    }

    #[test]
    fn test_balancing_detection() {
        // Проверяем определение необходимости балансировки
        let shards = create_test_shards();
        let mut manager = ShardManager::new(shards, RoutingStrategy::HashBased);

        // Создаем дисбаланс: shard1 перегружен, shard3 недогружен
        manager.set_shard_usage("shard1", 900000).unwrap(); // 90%
        manager.set_shard_usage("shard2", 500000).unwrap(); // 50%
        manager.set_shard_usage("shard3", 100000).unwrap(); // 10%

        // Проверяем коэффициенты загрузки
        let load1 = manager.get_shard_load("shard1").unwrap();
        let load3 = manager.get_shard_load("shard3").unwrap();
        
        // Разница > 20% - требуется балансировка
        assert!((load1 - load3) > 0.2);
    }

    #[test]
    fn test_shard_usage_with_invalid_shard() {
        // Проверяем обработку ошибок при работе с несуществующим шардом
        let shards = create_test_shards();
        let mut manager = ShardManager::new(shards, RoutingStrategy::HashBased);

        let result = manager.update_shard_usage("non_existent_shard", 1000);
        assert!(result.is_err());
        
        let result = manager.set_shard_usage("non_existent_shard", 1000);
        assert!(result.is_err());
        
        let result = manager.get_shard_load("non_existent_shard");
        assert!(result.is_err());
    }

    #[test]
    fn test_zero_capacity_shard() {
        // Проверяем обработку шарда с нулевой емкостью
        let mut shards = create_test_shards();
        shards[0] = ShardConfig {
            id: "shard_zero_capacity".to_string(),
            host: "localhost".to_string(),
            port: 9000,
            description: Some("Shard with zero capacity".to_string()),
        };
        
        let mut manager = ShardManager::new(shards, RoutingStrategy::HashBased);
        
        // Устанавливаем capacity в 0
        if let Some(shard) = manager.shards.get_mut("shard_zero_capacity") {
            shard.info.capacity = 0;
        }

        let load = manager.get_shard_load("shard_zero_capacity").unwrap();
        assert_eq!(load, 0.0); // Для нулевой емкости load должен быть 0
    }
}