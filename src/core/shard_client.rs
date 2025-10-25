use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use serde_json;
use reqwest::Client;
use tokio::time::{Duration, timeout};
use crate::core::lsh::LSHMetric;

/// HTTP клиент для взаимодействия с удаленными шардами
#[derive(Clone)]
pub struct ShardClient {
    client: Client,
    base_url: String,
    timeout_duration: Duration,
}

/// Запрос к шарду
#[derive(Debug, Serialize, Deserialize)]
pub struct ShardRequest {
    pub operation: String,
    pub collection: Option<String>,
    pub vector_id: Option<u64>,
    pub embedding: Option<Vec<f32>>,
    pub metadata: Option<HashMap<String, String>>,
    pub query: Option<Vec<f32>>,
    pub k: Option<usize>,
    pub filters: Option<HashMap<String, String>>,
}

/// Ответ от шарда
#[derive(Debug, Serialize, Deserialize)]
pub struct ShardResponse {
    pub success: bool,
    pub data: Option<serde_json::Value>,
    pub error: Option<String>,
    pub shard_id: String,
}

/// Результат операции с несколькими шардами
#[derive(Debug)]
pub struct MultiShardResult {
    pub results: Vec<ShardResponse>,
    pub successful_operations: usize,
    pub failed_operations: usize,
}

impl ShardClient {
    /// Создает новый клиент для шарда
    pub fn new(base_url: String) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Не удалось создать HTTP клиент");

        ShardClient {
            client,
            base_url,
            timeout_duration: Duration::from_secs(30),
        }
    }

    /// Создает коллекцию на удаленном шарде
    pub async fn create_collection(
        &self,
        name: String,
        metric: LSHMetric,
        dimension: usize,
    ) -> Result<ShardResponse, String> {
        let request = ShardRequest {
            operation: "create_collection".to_string(),
            collection: Some(name),
            vector_id: None,
            embedding: None,
            metadata: Some(HashMap::from([
                ("metric".to_string(), format!("{:?}", metric)),
                ("dimension".to_string(), dimension.to_string()),
            ])),
            query: None,
            k: None,
            filters: None,
        };

        self.send_request(request).await
    }

    /// Удаляет коллекцию на удаленном шарде
    pub async fn delete_collection(&self, name: String) -> Result<ShardResponse, String> {
        let request = ShardRequest {
            operation: "delete_collection".to_string(),
            collection: Some(name),
            vector_id: None,
            embedding: None,
            metadata: None,
            query: None,
            k: None,
            filters: None,
        };

        self.send_request(request).await
    }

    /// Добавляет вектор на удаленный шард
    pub async fn add_vector(
        &self,
        collection: String,
        embedding: Vec<f32>,
        metadata: HashMap<String, String>,
    ) -> Result<ShardResponse, String> {
        let request = ShardRequest {
            operation: "add_vector".to_string(),
            collection: Some(collection),
            vector_id: None,
            embedding: Some(embedding),
            metadata: Some(metadata),
            query: None,
            k: None,
            filters: None,
        };

        self.send_request(request).await
    }

    /// Обновляет вектор на удаленном шарде
    pub async fn update_vector(
        &self,
        collection: String,
        vector_id: u64,
        embedding: Option<Vec<f32>>,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<ShardResponse, String> {
        let request = ShardRequest {
            operation: "update_vector".to_string(),
            collection: Some(collection),
            vector_id: Some(vector_id),
            embedding,
            metadata,
            query: None,
            k: None,
            filters: None,
        };

        self.send_request(request).await
    }

    /// Удаляет вектор на удаленном шарде
    pub async fn delete_vector(
        &self,
        collection: String,
        vector_id: u64,
    ) -> Result<ShardResponse, String> {
        let request = ShardRequest {
            operation: "delete_vector".to_string(),
            collection: Some(collection),
            vector_id: Some(vector_id),
            embedding: None,
            metadata: None,
            query: None,
            k: None,
            filters: None,
        };

        self.send_request(request).await
    }

    /// Получает вектор с удаленного шарда
    pub async fn get_vector(
        &self,
        collection: String,
        vector_id: u64,
    ) -> Result<ShardResponse, String> {
        let request = ShardRequest {
            operation: "get_vector".to_string(),
            collection: Some(collection),
            vector_id: Some(vector_id),
            embedding: None,
            metadata: None,
            query: None,
            k: None,
            filters: None,
        };

        self.send_request(request).await
    }

    /// Выполняет поиск похожих векторов на удаленном шарде
    pub async fn find_similar(
        &self,
        collection: String,
        query: Vec<f32>,
        k: usize,
    ) -> Result<ShardResponse, String> {
        let request = ShardRequest {
            operation: "find_similar".to_string(),
            collection: Some(collection),
            vector_id: None,
            embedding: None,
            metadata: None,
            query: Some(query),
            k: Some(k),
            filters: None,
        };

        self.send_request(request).await
    }

    /// Фильтрует векторы по метаданным на удаленном шарде
    pub async fn filter_by_metadata(
        &self,
        collection: String,
        filters: HashMap<String, String>,
    ) -> Result<ShardResponse, String> {
        let request = ShardRequest {
            operation: "filter_by_metadata".to_string(),
            collection: Some(collection),
            vector_id: None,
            embedding: None,
            metadata: None,
            query: None,
            k: None,
            filters: Some(filters),
        };

        self.send_request(request).await
    }

    /// Получает статистику удаленного шарда
    pub async fn get_statistics(&self) -> Result<ShardResponse, String> {
        let request = ShardRequest {
            operation: "get_statistics".to_string(),
            collection: None,
            vector_id: None,
            embedding: None,
            metadata: None,
            query: None,
            k: None,
            filters: None,
        };

        self.send_request(request).await
    }

    /// Отправляет команду остановки на шард
    pub async fn stop_shard(&self) -> Result<ShardResponse, String> {
        let request = ShardRequest {
            operation: "stop".to_string(),
            collection: None,
            vector_id: None,
            embedding: None,
            metadata: None,
            query: None,
            k: None,
            filters: None,
        };

        self.send_request(request).await
    }

    /// Отправляет запрос к шарду
    pub async fn send_request(&self, request: ShardRequest) -> Result<ShardResponse, String> {
        let url = format!("{}/shard", self.base_url);
        
        let response = timeout(
            self.timeout_duration,
            self.client.post(&url).json(&request).send()
        ).await
        .map_err(|_| "Таймаут запроса к шарду".to_string())?
        .map_err(|e| format!("Ошибка HTTP запроса: {}", e))?;

        if !response.status().is_success() {
            return Err(format!("HTTP ошибка: {}", response.status()));
        }

        let shard_response: ShardResponse = response
            .json()
            .await
            .map_err(|e| format!("Ошибка парсинга ответа: {}", e))?;

        Ok(shard_response)
    }

    /// Проверяет доступность шарда
    pub async fn health_check(&self) -> Result<bool, String> {
        let url = format!("{}/health", self.base_url);
        
        match timeout(
            Duration::from_secs(5),
            self.client.get(&url).send()
        ).await {
            Ok(Ok(response)) => Ok(response.status().is_success()),
            Ok(Err(e)) => Err(format!("Ошибка HTTP: {}", e)),
            Err(_) => Err("Таймаут проверки здоровья".to_string()),
        }
    }
}

/// Менеджер клиентов для множественных шардов
#[derive(Clone)]
pub struct MultiShardClient {
    clients: HashMap<String, ShardClient>,
}

impl MultiShardClient {
    /// Создает новый менеджер клиентов
    pub fn new() -> Self {
        MultiShardClient {
            clients: HashMap::new(),
        }
    }

    /// Добавляет клиент для шарда
    pub fn add_shard_client(&mut self, shard_id: String, client: ShardClient) {
        self.clients.insert(shard_id, client);
    }

    /// Получает клиент для шарда
    pub fn get_client(&self, shard_id: &str) -> Option<&ShardClient> {
        self.clients.get(shard_id)
    }

    /// Получает итератор по всем клиентам
    pub fn iter_clients(&self) -> impl Iterator<Item = (&String, &ShardClient)> {
        self.clients.iter()
    }

    /// Выполняет операцию на всех шардах
    pub async fn execute_on_all_shards<F, Fut>(&self, operation: F) -> MultiShardResult
    where
        F: Fn(&ShardClient) -> Fut + Clone,
        Fut: std::future::Future<Output = Result<ShardResponse, String>>,
    {
        let mut results = Vec::new();
        let mut successful = 0;
        let mut failed = 0;

        for (shard_id, client) in &self.clients {
            match operation(client).await {
                Ok(mut response) => {
                    response.shard_id = shard_id.clone();
                    results.push(response);
                    successful += 1;
                }
                Err(error) => {
                    results.push(ShardResponse {
                        success: false,
                        data: None,
                        error: Some(error),
                        shard_id: shard_id.clone(),
                    });
                    failed += 1;
                }
            }
        }

        MultiShardResult {
            results,
            successful_operations: successful,
            failed_operations: failed,
        }
    }

    /// Создает коллекцию на всех шардах
    pub async fn create_collection_on_all_shards(
        &self,
        name: String,
        lsh_metric: LSHMetric,
        vector_dimension: usize,
    ) -> MultiShardResult {
        let mut results = Vec::new();
        let mut successful = 0;
        let mut failed = 0;

        for (shard_id, client) in &self.clients {
            match client.create_collection(name.clone(), lsh_metric.clone(), vector_dimension).await {
                Ok(mut response) => {
                    response.shard_id = shard_id.clone();
                    results.push(response);
                    successful += 1;
                }
                Err(error) => {
                    results.push(ShardResponse {
                        success: false,
                        data: None,
                        error: Some(error),
                        shard_id: shard_id.clone(),
                    });
                    failed += 1;
                }
            }
        }

        MultiShardResult {
            results,
            successful_operations: successful,
            failed_operations: failed,
        }
    }

    /// Удаляет коллекцию на всех шардах
    pub async fn delete_collection_on_all_shards(&self, name: String) -> MultiShardResult {
        let mut results = Vec::new();
        let mut successful = 0;
        let mut failed = 0;

        for (shard_id, client) in &self.clients {
            match client.delete_collection(name.clone()).await {
                Ok(mut response) => {
                    response.shard_id = shard_id.clone();
                    results.push(response);
                    successful += 1;
                }
                Err(error) => {
                    results.push(ShardResponse {
                        success: false,
                        data: None,
                        error: Some(error),
                        shard_id: shard_id.clone(),
                    });
                    failed += 1;
                }
            }
        }

        MultiShardResult {
            results,
            successful_operations: successful,
            failed_operations: failed,
        }
    }

    /// Добавляет вектор на конкретном шарде
    pub async fn add_vector_on_shard(
        &self,
        shard_id: &str,
        collection_name: String,
        embedding: Vec<f32>,
        metadata: HashMap<String, String>,
    ) -> Result<ShardResponse, String> {
        if let Some(client) = self.clients.get(shard_id) {
            client.add_vector(collection_name, embedding, metadata).await
        } else {
            Err(format!("Шард {} не найден", shard_id))
        }
    }

    /// Выполняет операцию на конкретном шарде
    pub async fn execute_on_shard<F, Fut>(
        &self,
        shard_id: &str,
        operation: F,
    ) -> Result<ShardResponse, String>
    where
        F: FnOnce(&ShardClient) -> Fut,
        Fut: std::future::Future<Output = Result<ShardResponse, String>>,
    {
        let client = self.clients.get(shard_id)
            .ok_or_else(|| format!("Шард {} не найден", shard_id))?;

        let mut response = operation(client).await?;
        response.shard_id = shard_id.to_string();
        Ok(response)
    }

    /// Выполняет поиск похожих векторов на всех шардах и объединяет результаты
    pub async fn find_similar_across_shards(
        &self,
        collection: String,
        query: Vec<f32>,
        k: usize,
    ) -> Result<Vec<(u64, usize, f32)>, String> {
        let mut all_results = Vec::new();

        for (shard_id, client) in &self.clients {
            match client.find_similar(collection.clone(), query.clone(), k).await {
                Ok(response) => {
                    if response.success {
                        if let Some(data) = response.data {
                            if let Some(results) = data.get("results").and_then(|r| r.as_array()) {
                                for result in results {
                                    if let (Some(bucket_id), Some(vector_index), Some(score)) = (
                                        result.get("bucket_id").and_then(|v| v.as_u64()),
                                        result.get("vector_index").and_then(|v| v.as_u64()),
                                        result.get("score").and_then(|v| v.as_f64()),
                                    ) {
                                        all_results.push((bucket_id, vector_index as usize, score as f32));
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Ошибка поиска на шарде {}: {}", shard_id, e);
                }
            }
        }

        // Сортируем по убыванию схожести и берем топ k
        all_results.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
        all_results.truncate(k);

        Ok(all_results)
    }

    /// Проверяет здоровье всех шардов
    pub async fn health_check_all(&self) -> HashMap<String, bool> {
        let mut health_status = HashMap::new();

        for (shard_id, client) in &self.clients {
            match client.health_check().await {
                Ok(true) => {
                    health_status.insert(shard_id.clone(), true);
                }
                _ => {
                    health_status.insert(shard_id.clone(), false);
                }
            }
        }

        health_status
    }

    /// Получает статистику всех шардов
    pub async fn get_all_statistics(&self) -> HashMap<String, serde_json::Value> {
        let mut statistics = HashMap::new();

        for (shard_id, client) in &self.clients {
            match client.get_statistics().await {
                Ok(response) => {
                    if response.success {
                        if let Some(data) = response.data {
                            statistics.insert(shard_id.clone(), data);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Ошибка получения статистики с шарда {}: {}", shard_id, e);
                }
            }
        }

        statistics
    }

    /// Останавливает все шарды
    pub async fn stop_all_shards(&self) -> MultiShardResult {
        let mut results = Vec::new();
        let mut successful = 0;
        let mut failed = 0;

        for (shard_id, client) in &self.clients {
            match client.stop_shard().await {
                Ok(mut response) => {
                    response.shard_id = shard_id.clone();
                    results.push(response);
                    successful += 1;
                }
                Err(error) => {
                    results.push(ShardResponse {
                        success: false,
                        data: None,
                        error: Some(error),
                        shard_id: shard_id.clone(),
                    });
                    failed += 1;
                }
            }
        }

        MultiShardResult {
            results,
            successful_operations: successful,
            failed_operations: failed,
        }
    }
}
