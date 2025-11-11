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
    /// Роутинг по диапазону ID коллекций
    RangeBased,
    /// Роутинг по LSH бакетам
    LSHBased,
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
    /// Журнал записей, которые должны быть перемещены на восстановленный шард
    pending_repairs: Arc<RwLock<HashMap<String, Vec<RepairEntry>>>>,
}
/// Запись о необходимости миграции данных на целевой шард после его восстановления
#[derive(Debug, Clone, Serialize, Deserialize)]
struct RepairEntry {
    collection: String,
    vector_id: u64,
    from_shard: String,
}

impl ShardManager {
    /// Создает новый менеджер шардов
    pub fn new(configs: Vec<ShardConfig>, strategy: RoutingStrategy, replication_factor: u8) -> Self {
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
            replication_factor,
        }
    }

    /// Определяет режим шардирования на основе стратегии роутинга
    pub fn get_sharding_mode(&self) -> &'static str {
        match self.routing_strategy {
            RoutingStrategy::HashBased | RoutingStrategy::RangeBased => "CollectionBased",
            RoutingStrategy::LSHBased => "BucketBased",
        }
    }

    /// Определяет шард для коллекции
    /// Используется только для HashBased и RangeBased стратегий
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
                
                let selected_shard = &shard_ids[shard_index];
                println!("🔍 Hash-routing: collection='{}', hash={}, shards={}, index={}, selected='{}'", 
                         collection_name, hash, shard_count, shard_index, selected_shard);
                
                Ok(selected_shard.clone())
            }
            RoutingStrategy::RangeBased => {
                // Диапазонный роутинг: распределение по первой букве имени коллекции
                let mut shard_ids: Vec<_> = self.shards.keys().cloned().collect();
                shard_ids.sort();
                
                let first_char = collection_name.chars().next().unwrap_or('a').to_ascii_lowercase();
                let char_value = first_char as u32;
                
                // Используем диапазон символов для определения шарда
                let shard_index = (char_value as usize) % shard_ids.len();
                
                println!("🔍 Range-routing: collection='{}', first_char='{}', char_value={}, shard_index={}, selected='{}'", 
                         collection_name, first_char, char_value, shard_index, shard_ids[shard_index]);
                
                Ok(shard_ids[shard_index].clone())
            }
            RoutingStrategy::LSHBased => {
                // LSH-роутинг не используется для коллекций, только для бакетов
                return Err("LSHBased роутинг не поддерживается для коллекций. Используйте get_shard_for_bucket".to_string());
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
    /// Используется для LSHBased стратегии и репликации
    fn get_shard_for_bucket(&self, bucket_id: u64) -> Result<String, String> {
        if self.shards.is_empty() {
            return Err("Нет доступных шардов".to_string());
        }
        
        let shard_count = self.shards.len();
        let shard_index = (bucket_id % shard_count as u64) as usize;
        
        // Используем отсортированный список ID для стабильности
        let mut shard_ids: Vec<_> = self.shards.keys().cloned().collect();
        shard_ids.sort();
        
        let selected_shard = &shard_ids[shard_index];
        println!("🔍 Bucket-routing: bucket_id={}, shards={}, index={}, selected='{}'", 
                 bucket_id, shard_count, shard_index, selected_shard);
        
        Ok(selected_shard.clone())
    }

    /// Определяет шард для вектора на основе LSH бакета
    /// Используется только для LSHBased стратегии
    pub fn get_shard_for_vector(&self, embedding: &[f32], lsh: &crate::core::lsh::LSH) -> Result<String, String> {
        if self.shards.is_empty() {
            return Err("Нет доступных шардов".to_string());
        }

        match self.routing_strategy {
            RoutingStrategy::LSHBased => {
                // Вычисляем LSH хеш бакета для вектора
                let bucket_hash = lsh.hash(embedding);
                
                println!("🔍 LSH routing: embedding_len={}, bucket_hash={}", embedding.len(), bucket_hash);
                
                // Используем bucket_hash для определения шарда
                self.get_shard_for_bucket(bucket_hash)
            }
            _ => {
                Err("LSHBased роутинг поддерживается только для LSHBased стратегии".to_string())
            }
        }
    }

    /// Получает следующий доступный шард (для репликации)
    /// Использует детерминированный выбор на основе отсортированного списка шардов
    fn get_next_available_shard(&self, exclude: &[String]) -> Option<String> {
        // Создаем отсортированный список для детерминированного выбора
        let mut available_shards: Vec<String> = self.shards.iter()
            .filter(|(shard_id, shard)| {
                !exclude.contains(shard_id) && shard.info.status == ShardStatus::Active
            })
            .map(|(shard_id, _)| shard_id.clone())
            .collect();
        
        available_shards.sort();
        
        // Возвращаем первый доступный шард
        available_shards.first().cloned()
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
            pending_repairs: Arc::new(RwLock::new(HashMap::new())),
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
            pending_repairs: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Разрешает целевой шард для записи: если предпочтительный недоступен, выбирает ближайший доступный по кольцу
    async fn resolve_writable_shard(&self, preferred_shard: &str) -> Result<String, String> {
        // Получаем упорядоченный список шардов (для стабильного обхода по кольцу)
        let (shard_ids_sorted, preferred_index) = {
            let shard_manager = self.shard_manager.read().await;
            let mut ids: Vec<String> = shard_manager.shards.keys().cloned().collect();
            if ids.is_empty() {
                return Err("Нет доступных шардов".to_string());
            }
            ids.sort();
            let idx = ids
                .iter()
                .position(|id| id == preferred_shard)
                .unwrap_or(0);
            (ids, idx)
        };

        // Если есть HTTP клиенты, проверяем реальную доступность через health-check
        if let Some(ref multi_client) = self.multi_shard_client {
            let n = shard_ids_sorted.len();
            for offset in 0..n {
                let i = (preferred_index + offset) % n;
                let candidate = &shard_ids_sorted[i];
                if let Some(client) = multi_client.get_client(candidate) {
                    match client.health_check().await {
                        Ok(true) => return Ok(candidate.clone()),
                        _ => continue,
                    }
                }
            }
            return Err("Нет доступных (здоровых) шардов".to_string());
        }

        // Фолбэк: если клиентов нет, ориентируемся на статус в менеджере шардов
        let shard_manager = self.shard_manager.read().await;
        for offset in 0..shard_ids_sorted.len() {
            let i = (preferred_index + offset) % shard_ids_sorted.len();
            let candidate = &shard_ids_sorted[i];
            if let Some(shard) = shard_manager.get_shard_info(candidate) {
                if shard.info.status == ShardStatus::Active {
                    return Ok(candidate.clone());
                }
            }
        }
        Err("Нет активных шардов для записи".to_string())
    }

    /// Создает коллекцию с учетом режима шардирования
    pub async fn create_collection(
        &self,
        name: String,
        lsh_metric: LSHMetric,
        vector_dimension: usize,
    ) -> Result<(), String> {
        let sharding_mode = {
            let shard_manager = self.shard_manager.read().await;
            shard_manager.get_sharding_mode()
        };

        match sharding_mode {
            "CollectionBased" => {
                // Collection-based: создаем коллекцию только на одном шарде
                let shard_id = {
                    let shard_manager = self.shard_manager.read().await;
                    let target_shard = shard_manager.get_shard_for_collection(&name)?;
                    println!("🎯 Collection-based: роутинг коллекции '{}' на шард: {}", name, target_shard);
                    target_shard
                };

                // Проверяем доступность и при необходимости выбираем ближайший доступный
                let writable_shard_id = self.resolve_writable_shard(&shard_id).await?;

                if let Some(ref multi_client) = self.multi_shard_client {
                    if let Some(client) = multi_client.get_client(&writable_shard_id) {
                        match client.create_collection(name.clone(), lsh_metric, vector_dimension).await {
                            Ok(response) => {
                                if !response.success {
                                    return Err(format!("Ошибка создания коллекции на шарде {}: {:?}", 
                                                     writable_shard_id, response.error));
                                }
                                println!("✅ Коллекция '{}' создана на шарде {} (collection-based)", name, writable_shard_id);
                            }
                            Err(e) => {
                                return Err(format!("Ошибка связи с шардом {}: {}", writable_shard_id, e));
                            }
                        }
                    } else {
                        return Err(format!("Клиент для шарда {} не найден", writable_shard_id));
                    }
                } else {
                    return Err("Клиент для множественных шардов не инициализирован".to_string());
                }

                // Регистрируем коллекцию в метаданных
                {
                    let mut shard_manager = self.shard_manager.write().await;
                    shard_manager.add_collection_to_shard(&writable_shard_id, name)?;
                }

                Ok(())
            }
            "BucketBased" => {
                // Bucket-based: создаем коллекцию на ВСЕХ шардах
                println!("📦 Bucket-based: создание коллекции '{}' на всех шардах", name);

                if let Some(ref multi_client) = self.multi_shard_client {
                    let results = multi_client.create_collection_on_all_shards(
                        name.clone(), 
                        lsh_metric.clone(), 
                        vector_dimension
                    ).await;
                    
                    println!("📡 Создание коллекции на {} шардах: {}/{} успешно", 
                             results.results.len(), results.successful_operations, results.results.len());
                    
                    let mut has_errors = false;
                    for response in &results.results {
                        if !response.success {
                            if let Some(error) = &response.error {
                                eprintln!("⚠️  Ошибка создания коллекции на шарде {}: {}", response.shard_id, error);
                                has_errors = true;
                            }
                        } else {
                            println!("✅ Коллекция '{}' создана на шарде {}", name, response.shard_id);
                        }
                    }
                    
                    if has_errors && results.successful_operations == 0 {
                        return Err("Не удалось создать коллекцию ни на одном шарде".to_string());
                    }
                } else {
                    return Err("Клиент для множественных шардов не инициализирован".to_string());
                }

                // Регистрируем коллекцию на всех шардах в метаданных
                {
                    let mut shard_manager = self.shard_manager.write().await;
                    let shard_ids: Vec<String> = shard_manager.shards.keys().cloned().collect();
                    for shard_id in shard_ids {
                        let _ = shard_manager.add_collection_to_shard(&shard_id, name.clone());
                    }
                }

                println!("✅ Коллекция '{}' готова к bucket-based шардированию", name);
                Ok(())
            }
            _ => {
                Err(format!("Неизвестный режим шардирования: {}", sharding_mode))
            }
        }
    }

    /// Удаляет коллекцию с учетом режима шардирования
    pub async fn delete_collection(&self, name: String) -> Result<(), String> {
        let sharding_mode = {
            let shard_manager = self.shard_manager.read().await;
            shard_manager.get_sharding_mode()
        };

        match sharding_mode {
            "CollectionBased" => {
                // Collection-based: удаляем коллекцию только с одного шарда
                let shard_id = {
                    let shard_manager = self.shard_manager.read().await;
                    shard_manager.get_shard_for_collection(&name)?
                };

                // Проверяем доступность и при необходимости выбираем ближайший доступный
                let writable_shard_id = self.resolve_writable_shard(&shard_id).await?;

                println!("🗑️  Collection-based: удаление коллекции '{}' с шарда {} (целевой='{}')", name, writable_shard_id, shard_id);

                if let Some(ref multi_client) = self.multi_shard_client {
                    if let Some(client) = multi_client.get_client(&writable_shard_id) {
                        match client.delete_collection(name.clone()).await {
                            Ok(response) => {
                                if !response.success {
                                    if let Some(error) = response.error {
                                        eprintln!("⚠️  Ошибка удаления коллекции на шарде {}: {}", writable_shard_id, error);
                                    }
                                } else {
                                    println!("✅ Коллекция '{}' удалена с шарда {}", name, writable_shard_id);
                                }
                            }
                            Err(e) => {
                                eprintln!("⚠️  Ошибка связи с шардом {}: {}", writable_shard_id, e);
                            }
                        }
                    }
                }

                // Удаляем коллекцию из метаданных
                {
                    let mut shard_manager = self.shard_manager.write().await;
                    shard_manager.remove_collection_from_shard(&writable_shard_id, &name)?;
                }

                Ok(())
            }
            "BucketBased" => {
                // Bucket-based: удаляем коллекцию со ВСЕХ шардов
                println!("🗑️  Bucket-based: удаление коллекции '{}' со всех шардов", name);

                if let Some(ref multi_client) = self.multi_shard_client {
                    let results = multi_client.delete_collection_on_all_shards(name.clone()).await;
                    
                    println!("📡 Удаление коллекции с {} шардов: {}/{} успешно", 
                             results.results.len(), results.successful_operations, results.results.len());
                    
                    for response in &results.results {
                        if !response.success {
                            if let Some(error) = &response.error {
                                eprintln!("⚠️  Ошибка удаления коллекции на шарде {}: {}", response.shard_id, error);
                            }
                        } else {
                            println!("✅ Коллекция '{}' удалена с шарда {}", name, response.shard_id);
                        }
                    }
                }

                // Удаляем коллекцию из метаданных всех шардов
                {
                    let mut shard_manager = self.shard_manager.write().await;
                    let shard_ids: Vec<String> = shard_manager.shards.keys().cloned().collect();
                    for shard_id in shard_ids {
                        let _ = shard_manager.remove_collection_from_shard(&shard_id, &name);
                    }
                }

                Ok(())
            }
            _ => {
                Err(format!("Неизвестный режим шардирования: {}", sharding_mode))
            }
        }
    }

    /// Добавляет вектор с учетом режима шардирования
    pub async fn add_vector(
        &self,
        collection_name: String,
        embedding: Vec<f32>,
        metadata: HashMap<String, String>,
    ) -> Result<u64, String> {
        let sharding_mode = {
            let shard_manager = self.shard_manager.read().await;
            shard_manager.get_sharding_mode()
        };

        // Вычисляем приблизительный размер вектора (embedding + метаданные)
        let vector_size = (embedding.len() * std::mem::size_of::<f32>()) as i64
            + metadata.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>() as i64
            + 100; // overhead для других полей

        match sharding_mode {
            "CollectionBased" => {
                // Collection-based: роутинг по имени коллекции (HashBased или RangeBased)
                let primary_shard_id = {
                    let shard_manager = self.shard_manager.read().await;
                    shard_manager.get_shard_for_collection(&collection_name)?
                };

                // Получаем список шардов для репликации
                let shard_ids_for_replication = {
                    let shard_manager = self.shard_manager.read().await;
                    let mut shards = vec![primary_shard_id.clone()];
                    
                    // Добавляем реплики
                    for _ in 1..shard_manager.replication_factor {
                        if let Some(replica_shard) = shard_manager.get_next_available_shard(&shards) {
                            shards.push(replica_shard);
                        }
                    }
                    shards
                };

                // Проверяем доступность и выбираем доступные шарды
                let mut writable_shard_ids = Vec::new();
                for shard_id in &shard_ids_for_replication {
                    if let Ok(writable) = self.resolve_writable_shard(shard_id).await {
                        writable_shard_ids.push(writable);
                    }
                }

                if writable_shard_ids.is_empty() {
                    return Err("Нет доступных шардов для записи".to_string());
                }

                println!("🎯 Collection-based: вектор в коллекции '{}' -> шарды {:?} (основной='{}')",
                         collection_name, writable_shard_ids, primary_shard_id);

                if let Some(ref multi_client) = self.multi_shard_client {
                    // Записываем на все реплики
                    let replication_result = multi_client.add_vector_on_shards(
                        &writable_shard_ids,
                        collection_name.clone(),
                        embedding.clone(),
                        metadata.clone()
                    ).await;

                    // Проверяем, что хотя бы одна запись успешна
                    if replication_result.successful_operations == 0 {
                        return Err("Не удалось записать вектор ни на один шард".to_string());
                    }

                    // Получаем vector_id из первого успешного ответа
                    let vector_id = replication_result.results.iter()
                        .find(|r| r.success)
                        .and_then(|r| r.data.as_ref())
                        .and_then(|data| data.get("id"))
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);

                    println!("📡 Вектор добавлен на {} шардов (collection-based): ID={}, успешно: {}/{}", 
                             writable_shard_ids.len(), vector_id, 
                             replication_result.successful_operations, replication_result.results.len());
                    
                    // Обновляем использование пространства на всех шардах, где запись успешна
                    {
                        let mut shard_manager = self.shard_manager.write().await;
                        for response in &replication_result.results {
                            if response.success {
                                let _ = shard_manager.update_shard_usage(&response.shard_id, vector_size);
                            }
                        }
                    }

                    // Если основной шард недоступен, добавляем задачу на последующую миграцию
                    if !writable_shard_ids.contains(&primary_shard_id) {
                        let mut repairs = self.pending_repairs.write().await;
                        let entries = repairs.entry(primary_shard_id.clone()).or_insert_with(Vec::new);
                        entries.push(RepairEntry { 
                            collection: collection_name.clone(), 
                            vector_id, 
                            from_shard: writable_shard_ids[0].clone() 
                        });
                    }
                    
                    Ok(vector_id)
                } else {
                    Err("Клиент для множественных шардов не инициализирован".to_string())
                }
            }
            "BucketBased" => {
                // Bucket-based: роутинг по LSH бакету embedding
                // Создаем временный LSH для роутинга (используем параметры по умолчанию)
                let temp_lsh = crate::core::lsh::LSH::new(
                    embedding.len(),
                    3, // num_hashes
                    10.0, // bucket_width
                    crate::core::lsh::LSHMetric::Euclidean, // default metric
                    Some(42) // seed для воспроизводимости
                );
                
                // Получаем список шардов для репликации на основе bucket_id
                let bucket_id = temp_lsh.hash(&embedding);

                let shard_ids_for_replication = {
                    let shard_manager = self.shard_manager.read().await;
                    shard_manager.get_shards_for_bucket(bucket_id)?
                };

                // Проверяем доступность и выбираем доступные шарды
                let mut writable_shard_ids = Vec::new();
                for shard_id in &shard_ids_for_replication {
                    if let Ok(writable) = self.resolve_writable_shard(shard_id).await {
                        writable_shard_ids.push(writable);
                    }
                }

                if writable_shard_ids.is_empty() {
                    return Err("Нет доступных шардов для записи".to_string());
                }

                let primary_shard_id = shard_ids_for_replication[0].clone();

                println!("🎯 Bucket-based: collection='{}', embedding_len={}, bucket_id={}, шарды {:?} (основной='{}')", 
                         collection_name, embedding.len(), bucket_id, writable_shard_ids, primary_shard_id);

                if let Some(ref multi_client) = self.multi_shard_client {
                    // Записываем на все реплики
                    let replication_result = multi_client.add_vector_on_shards(
                        &writable_shard_ids,
                        collection_name.clone(),
                        embedding.clone(),
                        metadata.clone()
                    ).await;

                    // Проверяем, что хотя бы одна запись успешна
                    if replication_result.successful_operations == 0 {
                        return Err("Не удалось записать вектор ни на один шард".to_string());
                    }

                    // Получаем vector_id из первого успешного ответа
                    let vector_id = replication_result.results.iter()
                        .find(|r| r.success)
                        .and_then(|r| r.data.as_ref())
                        .and_then(|data| data.get("id"))
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);

                    println!("📡 Вектор добавлен на {} шардов (bucket-based): ID={}, успешно: {}/{}", 
                             writable_shard_ids.len(), vector_id,
                             replication_result.successful_operations, replication_result.results.len());
                    
                    // Обновляем использование пространства на всех шардах, где запись успешна
                    {
                        let mut shard_manager = self.shard_manager.write().await;
                        for response in &replication_result.results {
                            if response.success {
                                let _ = shard_manager.update_shard_usage(&response.shard_id, vector_size);
                            }
                        }
                    }

                    // Если основной шард недоступен, добавляем задачу на последующую миграцию
                    if !writable_shard_ids.contains(&primary_shard_id) {
                        let mut repairs = self.pending_repairs.write().await;
                        let entries = repairs.entry(primary_shard_id.clone()).or_insert_with(Vec::new);
                        entries.push(RepairEntry { 
                            collection: collection_name.clone(), 
                            vector_id, 
                            from_shard: writable_shard_ids[0].clone() 
                        });
                    }
                    
                    Ok(vector_id)
                } else {
                    Err("Клиент для множественных шардов не инициализирован".to_string())
                }
            }
            _ => {
                Err(format!("Неизвестный режим шардирования: {}", sharding_mode))
            }
        }
    }

    /// Запускает консолидацию данных на восстановленный шард: переносит накопленные записи
    pub async fn reconcile_shard(&self, target_shard: String) -> Result<(), String> {
        let entries_opt = {
            let mut repairs = self.pending_repairs.write().await;
            repairs.remove(&target_shard)
        };

        if entries_opt.is_none() {
            return Ok(());
        }

        let entries = entries_opt.unwrap();
        println!("🔁 Реконсолидация на шард {}: {} элементов", target_shard, entries.len());

        if entries.is_empty() {
            return Ok(());
        }

        let multi_client = match &self.multi_shard_client {
            Some(c) => c.clone(),
            None => return Err("Клиент для множественных шардов не инициализирован".to_string()),
        };

        // Для надежности еще раз проверим, что целевой шард доступен
        let resolved_target = self.resolve_writable_shard(&target_shard).await.unwrap_or(target_shard.clone());

        // Убедимся, что нужные коллекции существуют на целевом шарде (создадим при необходимости)
        {
            use std::collections::HashSet;
            let mut collections: HashSet<String> = HashSet::new();
            for e in &entries { collections.insert(e.collection.clone()); }

            for cname in collections.into_iter() {
                if let Some(client) = multi_client.get_client(&resolved_target) {
                    // Проверяем наличие коллекции на целевом шарде через внутренний RPC get_collection
                    let exists = match {
                        let req = crate::core::shard_client::ShardRequest {
                            operation: "get_collection".to_string(),
                            collection: Some(cname.clone()),
                            vector_id: None,
                            embedding: None,
                            metadata: None,
                            query: None,
                            k: None,
                            filters: None,
                        };
                        client.send_request(req).await
                    } {
                        Ok(resp) => resp.success,
                        Err(_) => false,
                    };

                    if !exists {
                        // Получаем параметры коллекции из кластера
                        if let Ok(Some(coll)) = self.get_collection(cname.clone()).await {
                            let _ = client.create_collection(
                                cname.clone(),
                                coll.lsh_metric.clone(),
                                coll.vector_dimension
                            ).await;
                        }
                    }
                }
            }
        }

        for entry in entries {
            // 1) Считать вектор с фактического шарда
            let vector_resp = if let Some(client) = multi_client.get_client(&entry.from_shard) {
                client.get_vector(entry.collection.clone(), entry.vector_id).await
            } else {
                Err(format!("Клиент для шарда {} не найден", entry.from_shard))
            };

            let mut embedding: Option<Vec<f32>> = None;
            let mut metadata: HashMap<String, String> = HashMap::new();
            if let Ok(resp) = vector_resp {
                if resp.success {
                    if let Some(data) = resp.data {
                        if let Some(arr) = data.get("embedding").and_then(|v| v.as_array()) {
                            embedding = Some(arr.iter().filter_map(|v| v.as_f64().map(|f| f as f32)).collect());
                        }
                        if let Some(meta) = data.get("metadata").and_then(|m| m.as_object()) {
                            metadata = meta.iter().filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string()))).collect();
                        }
                    }
                }
            }

            // Если не удалось получить данные — пропускаем запись (оставляем на следующую попытку)
            if embedding.is_none() {
                // Возвращаем запись обратно в список
                let mut repairs = self.pending_repairs.write().await;
                let entries_back = repairs.entry(target_shard.clone()).or_insert_with(Vec::new);
                entries_back.push(entry.clone());
                continue;
            }

            // 2) Добавить вектор на целевой шард (коллекция к этому моменту должна существовать)
            if let Some(client) = multi_client.get_client(&resolved_target) {
                let add_res = client.add_vector(entry.collection.clone(), embedding.unwrap(), metadata.clone()).await;
                match add_res {
                    Ok(r) if r.success => {
                        // 3) Удалить вектор со временного шарда
                        if let Some(src_client) = multi_client.get_client(&entry.from_shard) {
                            let _ = src_client.delete_vector(entry.collection.clone(), entry.vector_id).await;
                        }
                    }
                    _ => {
                        // Сбой — вернем запись назад
                        let mut repairs = self.pending_repairs.write().await;
                        let entries_back = repairs.entry(target_shard.clone()).or_insert_with(Vec::new);
                        entries_back.push(entry.clone());
                    }
                }
            }
        }

        Ok(())
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
                // Пропускаем шард, если он не здоров
                match client.health_check().await {
                    Ok(true) => {}
                    _ => { continue; }
                }
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
                // Пропускаем шард, если он не здоров
                match client.health_check().await {
                    Ok(true) => {}
                    _ => { continue; }
                }
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
                // Пропускаем шард, если он не здоров
                match client.health_check().await {
                    Ok(true) => {}
                    _ => { continue; }
                }
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

        let sharding_mode = {
            let shard_manager = self.shard_manager.read().await;
            shard_manager.get_sharding_mode()
        };

        if let Some(ref multi_client) = self.multi_shard_client {
            match sharding_mode {
                "CollectionBased" => {
                    // Collection-based: фильтруем только на одном шарде
                    let shard_id = {
                        let shard_manager = self.shard_manager.read().await;
                        shard_manager.get_shard_for_collection(&collection_name)?
                    };

                    if let Some(client) = multi_client.get_client(&shard_id) {
                        // Если шард не здоров — просто возвращаем текущий результат (пустой/частичный)
                        if !matches!(client.health_check().await, Ok(true)) {
                            return Ok(all_results);
                        }
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
                "BucketBased" => {
                    // Bucket-based: запрашиваем все шарды
                    let shard_ids = {
                        let shard_manager = self.shard_manager.read().await;
                        shard_manager.get_active_shards()
                            .iter()
                            .map(|shard| shard.config.id.clone())
                            .collect::<Vec<String>>()
                    };

                    for shard_id in shard_ids {
                        if let Some(client) = multi_client.get_client(&shard_id) {
                            // Пропускаем шард, если он не здоров
                            match client.health_check().await {
                                Ok(true) => {}
                                _ => { continue; }
                            }
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
                }
                _ => {
                    eprintln!("⚠️  Неизвестный режим шардирования: {}", sharding_mode);
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
            // Внутри клиента идёт обращение ко всем шардам; здесь дополнительных проверок не требуется
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
                // Пропускаем шард, если он не здоров
                match client.health_check().await {
                    Ok(true) => {}
                    _ => {
                        results.push(crate::core::shard_client::ShardResponse {
                            success: false,
                            data: None,
                            error: Some("Шард недоступен".to_string()),
                            shard_id: shard_id.clone(),
                        });
                        _failed += 1;
                        continue;
                    }
                }
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
                // Пропускаем шард, если он не здоров
                match client.health_check().await {
                    Ok(true) => {}
                    _ => {
                        results.push(crate::core::shard_client::ShardResponse {
                            success: false,
                            data: None,
                            error: Some("Шард недоступен".to_string()),
                            shard_id: shard_id.clone(),
                        });
                        _failed += 1;
                        continue;
                    }
                }
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
            // Разрешаем ближайший доступный шард для чтения/запроса
            let readable_shard = self.resolve_writable_shard(shard_id).await.unwrap_or(shard_id.to_string());
            if let Some(client) = multi_client.get_client(&readable_shard) {
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
                Err(format!("Шард {} не найден", readable_shard))
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

        // 2. Создаем коллекцию на целевом шарде, если её там нет (с учетом доступности)
        let writable_to_shard = self.resolve_writable_shard(&to_shard).await?;
        if let Some(ref multi_client) = self.multi_shard_client {
            if let Some(client) = multi_client.get_client(&writable_to_shard) {
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

        // 3. Получаем все векторы с исходного шарда и переносим их
        println!("📦 Копирование данных...");
        
        if let Some(ref multi_client) = self.multi_shard_client {
            // Получаем все векторы с исходного шарда
            let vectors_to_migrate = self.get_all_vectors_from_collection(
                collection_name.clone(), 
                from_shard.clone()
            ).await?;
            
            println!("📊 Найдено {} векторов для миграции", vectors_to_migrate.len());
            
            // Переносим каждый вектор на целевой шард
            let mut migrated_count = 0;
            let mut failed_count = 0;
            
            for (vector_id, vector_data) in vectors_to_migrate {
                if let Some(client) = multi_client.get_client(&writable_to_shard) {
                    // Парсим данные вектора
                    if let (Some(embedding), Some(metadata)) = (
                        vector_data.get("embedding").and_then(|v| v.as_array()),
                        vector_data.get("metadata").and_then(|m| m.as_object())
                    ) {
                        let embedding: Vec<f32> = embedding.iter()
                            .filter_map(|v| v.as_f64().map(|f| f as f32))
                            .collect();
                        
                        let metadata: HashMap<String, String> = metadata.iter()
                            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                            .collect();
                        
                        // Добавляем вектор на целевой шард
                        match client.add_vector(
                            collection_name.clone(),
                            embedding,
                            metadata
                        ).await {
                            Ok(response) => {
                                if response.success {
                                    migrated_count += 1;
                                    if migrated_count % 100 == 0 {
                                        println!("📈 Перенесено {} векторов...", migrated_count);
                                    }
                                } else {
                                    failed_count += 1;
                                    eprintln!("⚠️  Ошибка добавления вектора {}: {:?}", vector_id, response.error);
                                }
                            }
                            Err(e) => {
                                failed_count += 1;
                                eprintln!("⚠️  Ошибка связи при добавлении вектора {}: {}", vector_id, e);
                            }
                        }
                    } else {
                        failed_count += 1;
                        eprintln!("⚠️  Неверный формат данных вектора {}", vector_id);
                    }
                } else {
                    return Err(format!("Клиент для целевого шарда {} не найден", writable_to_shard));
                }
            }
            
            println!("✅ Миграция завершена: {} успешно, {} ошибок", migrated_count, failed_count);
            
            if failed_count > 0 {
                eprintln!("⚠️  {} векторов не удалось перенести", failed_count);
                // Если слишком много ошибок, отменяем миграцию
                if failed_count > migrated_count / 2 {
                    return Err(format!("Слишком много ошибок при миграции: {} из {} векторов", 
                                     failed_count, migrated_count + failed_count));
                }
            }
        }
        
        // 4. Проверяем целостность данных после миграции
        println!("🔍 Проверка целостности данных...");
        
        if let Some(ref _multi_client) = self.multi_shard_client {
            // Получаем количество векторов на исходном шарде
            let original_count = self.get_collection_vector_count(collection_name.clone(), from_shard.clone()).await.unwrap_or(0);
            
            // Получаем количество векторов на целевом шарде
            let migrated_count = self.get_collection_vector_count(collection_name.clone(), to_shard.clone()).await.unwrap_or(0);
            
            println!("📊 Исходный шард: {} векторов, Целевой шард: {} векторов", original_count, migrated_count);
            
            if migrated_count < original_count {
                eprintln!("⚠️  Не все векторы перенесены! Исходный: {}, Целевой: {}", original_count, migrated_count);
                // Не удаляем коллекцию с исходного шарда, если данные не полностью перенесены
                return Err("Миграция не завершена из-за потери данных".to_string());
            }
        }
        
        // 5. Обновляем информацию о коллекциях в менеджере шардов
        {
            let mut shard_manager = self.shard_manager.write().await;
            
            // Добавляем коллекцию на целевой шард
            shard_manager.add_collection_to_shard(&writable_to_shard, collection_name.clone())?;
            
            // Удаляем коллекцию с исходного шарда
            shard_manager.remove_collection_from_shard(&from_shard, &collection_name)?;
        }

        // 6. Удаляем коллекцию с исходного шарда (с учетом доступности исходного)
        let writable_from_shard = self.resolve_writable_shard(&from_shard).await.unwrap_or(from_shard);
        if let Some(ref multi_client) = self.multi_shard_client {
            if let Some(client) = multi_client.get_client(&writable_from_shard) {
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

    /// Получает все векторы из коллекции на указанном шарде
    async fn get_all_vectors_from_collection(
        &self,
        collection_name: String,
        shard_id: String,
    ) -> Result<Vec<(u64, serde_json::Value)>, String> {
        if let Some(ref multi_client) = self.multi_shard_client {
            // Разрешаем ближайший доступный шард для чтения/запроса
            let readable_shard = self.resolve_writable_shard(&shard_id).await.unwrap_or(shard_id.clone());
            if let Some(client) = multi_client.get_client(&readable_shard) {
                let request = crate::core::shard_client::ShardRequest {
                    operation: "get_all_vectors".to_string(),
                    collection: Some(collection_name.clone()),
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
                                if let Some(vectors) = data.get("vectors").and_then(|v| v.as_array()) {
                                    let mut result = Vec::new();
                                    
                                    for vector_data in vectors {
                                        if let (Some(id), Some(vector_info)) = (
                                            vector_data.get("id").and_then(|v| v.as_u64()),
                                            vector_data.get("data")
                                        ) {
                                            result.push((id, vector_info.clone()));
                                        }
                                    }
                                    
                                    println!("📊 Получено {} векторов с шарда {}", result.len(), readable_shard);
                                    Ok(result)
                                } else {
                                    Ok(Vec::new())
                                }
                            } else {
                                Ok(Vec::new())
                            }
                        } else {
                            Err(format!("Ошибка получения векторов с шарда {}: {:?}", 
                                       readable_shard, response.error))
                        }
                    }
                    Err(e) => {
                        Err(format!("Ошибка связи с шардом {}: {}", readable_shard, e))
                    }
                }
            } else {
                Err(format!("Клиент для шарда {} не найден", readable_shard))
            }
        } else {
            Err("Клиент для множественных шардов не инициализирован".to_string())
        }
    }

    /// Получает количество векторов в коллекции на указанном шарде
    async fn get_collection_vector_count(
        &self,
        collection_name: String,
        shard_id: String,
    ) -> Result<u64, String> {
        if let Some(ref multi_client) = self.multi_shard_client {
            // Разрешаем ближайший доступный шард для чтения/запроса
            let readable_shard = self.resolve_writable_shard(&shard_id).await.unwrap_or(shard_id.clone());
            if let Some(client) = multi_client.get_client(&readable_shard) {
                let request = crate::core::shard_client::ShardRequest {
                    operation: "get_collection_size".to_string(),
                    collection: Some(collection_name),
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
                                    Ok(size)
                                } else {
                                    Ok(0)
                                }
                            } else {
                                Ok(0)
                            }
                        } else {
                            Err(format!("Ошибка получения размера коллекции с шарда {}: {:?}", 
                                       readable_shard, response.error))
                        }
                    }
                    Err(e) => {
                        Err(format!("Ошибка связи с шардом {}: {}", readable_shard, e))
                    }
                }
            } else {
                Err(format!("Клиент для шарда {} не найден", readable_shard))
            }
        } else {
            Err("Клиент для множественных шардов не инициализирован".to_string())
        }
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

    /// Получает коллекцию по имени (bucket-based шардирование)
    /// Ищет на любом шарде, так как коллекция существует на всех шардах
    pub async fn get_collection(&self, name: String) -> Result<Option<crate::core::objects::Collection>, String> {
        let sharding_mode = {
            let shard_manager = self.shard_manager.read().await;
            shard_manager.get_sharding_mode()
        };

        if let Some(ref multi_client) = self.multi_shard_client {
            let shard_ids_to_check: Vec<String> = match sharding_mode {
                "CollectionBased" => {
                    // Collection-based: запрашиваем только один целевой шард
                    let shard_id = {
                        let shard_manager = self.shard_manager.read().await;
                        match shard_manager.get_shard_for_collection(&name) {
                            Ok(id) => vec![id],
                            Err(_) => vec![], // Если не удалось определить шард, возвращаем пустой вектор
                        }
                    };
                    shard_id
                }
                "BucketBased" => {
                    // Bucket-based: коллекция может быть на любом шарде
                    multi_client.iter_clients().map(|(id, _)| id.clone()).collect()
                }
                _ => {
                    eprintln!("⚠️  Неизвестный режим шардирования: {}", sharding_mode);
                    vec![]
                }
            };

            for shard_id in shard_ids_to_check {
                if let Some(client) = multi_client.get_client(&shard_id) {
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