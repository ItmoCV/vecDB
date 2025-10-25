use super::{controllers::{VectorController, BucketController}, lsh::{LSH, LSHMetric}, objects::Collection};

#[test]
fn test_vector_controller_creation() {
    let controller = VectorController::new();
    assert!(controller.vectors.is_some());
    assert_eq!(controller.vectors.as_ref().unwrap().len(), 0);
}

#[test]
fn test_bucket_controller_creation() {
    let controller = BucketController::new(4, 3, 1.0, LSHMetric::Euclidean, Some(42));
    assert!(controller.buckets.is_some());
    assert_eq!(controller.dimension, Some(4));
    assert!(controller.lsh.is_some());
}


// Тесты для LSH функциональности

#[test]
fn test_lsh_basic_functionality() {
    let lsh = LSH::new(4, 3, 1.0, LSHMetric::Euclidean, Some(42));
    
    let vector1 = vec![1.0, 2.0, 3.0, 4.0];
    let vector2 = vec![1.1, 2.1, 3.1, 4.1];
    let vector3 = vec![10.0, 20.0, 30.0, 40.0];
    
    let hash1 = lsh.hash(&vector1);
    let hash2 = lsh.hash(&vector2);
    let hash3 = lsh.hash(&vector3);
    
    // Проверяем, что хэши детерминированы
    assert_eq!(lsh.hash(&vector1), hash1);
    assert_eq!(lsh.hash(&vector2), hash2);
    assert_eq!(lsh.hash(&vector3), hash3);
}

#[test]
fn test_bucket_controller_basic_operations() {
    let bucket_controller = BucketController::new(4, 3, 1.0, LSHMetric::Euclidean, Some(42));
    
    // Проверяем начальное состояние
    assert_eq!(bucket_controller.dimension, Some(4));
    assert!(bucket_controller.lsh.is_some());
    assert!(bucket_controller.buckets.is_some());
}

#[test]
fn test_collection_with_different_metrics() {
    // Тестируем создание коллекций с разными метриками
    let collection_euclidean = Collection::new(Some("test_euclidean".to_string()), LSHMetric::Euclidean, 384);
    let collection_cosine = Collection::new(Some("test_cosine".to_string()), LSHMetric::Cosine, 384);
    let collection_manhattan = Collection::new(Some("test_manhattan".to_string()), LSHMetric::Manhattan, 384);
    
    // Проверяем, что метрики установлены правильно
    assert_eq!(collection_euclidean.lsh_metric, LSHMetric::Euclidean);
    assert_eq!(collection_cosine.lsh_metric, LSHMetric::Cosine);
    assert_eq!(collection_manhattan.lsh_metric, LSHMetric::Manhattan);
    
    // Проверяем, что размерности векторов установлены правильно
    assert_eq!(collection_euclidean.vector_dimension, 384);
    assert_eq!(collection_cosine.vector_dimension, 384);
    assert_eq!(collection_manhattan.vector_dimension, 384);
    
    // Проверяем, что имена коллекций установлены правильно
    assert_eq!(collection_euclidean.name, "test_euclidean");
    assert_eq!(collection_cosine.name, "test_cosine");
    assert_eq!(collection_manhattan.name, "test_manhattan");
}

#[test]
fn test_lsh_metric_serialization() {
    // Тестируем сериализацию и десериализацию метрик
    let metrics = vec![LSHMetric::Euclidean, LSHMetric::Cosine, LSHMetric::Manhattan];
    
    for metric in metrics {
        let metric_string = metric.to_string();
        let deserialized_metric = LSHMetric::from_string(&metric_string).unwrap();
        assert_eq!(metric, deserialized_metric);
    }
    
    // Тестируем обработку неизвестной метрики
    let result = LSHMetric::from_string("Unknown");
    assert!(result.is_err());
}

// ===== ТЕСТЫ ШАРДИРОВАНИЯ =====

#[test]
fn test_shard_manager_creation() {
    use crate::core::sharding::{ShardManager, ShardConfig, RoutingStrategy};
    
    let configs = vec![
        ShardConfig {
            id: "shard1".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8080,
            description: Some("Тестовый шард 1".to_string()),
        },
        ShardConfig {
            id: "shard2".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8081,
            description: Some("Тестовый шард 2".to_string()),
        },
    ];

    let manager = ShardManager::new(configs, RoutingStrategy::HashBased);
    
    // Проверяем, что шарды созданы
    assert!(manager.get_shard_info("shard1").is_some());
    assert!(manager.get_shard_info("shard2").is_some());
    
    // Проверяем, что все шарды активны
    let active_shards = manager.get_active_shards();
    assert_eq!(active_shards.len(), 2);
}

#[test]
fn test_collection_routing() {
    use crate::core::sharding::{ShardManager, ShardConfig, RoutingStrategy};
    
    let configs = vec![
        ShardConfig {
            id: "shard1".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8080,
            description: Some("Тестовый шард 1".to_string()),
        },
        ShardConfig {
            id: "shard2".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8081,
            description: Some("Тестовый шард 2".to_string()),
        },
    ];

    let manager = ShardManager::new(configs, RoutingStrategy::HashBased);
    
    // Тестируем роутинг коллекций
    let shard1 = manager.get_shard_for_collection("collection1").unwrap();
    let shard2 = manager.get_shard_for_collection("collection2").unwrap();
    
    assert!(shard1 == "shard1" || shard1 == "shard2");
    assert!(shard2 == "shard1" || shard2 == "shard2");
    
    // Одинаковые имена коллекций должны попадать в один шард
    let shard1_again = manager.get_shard_for_collection("collection1").unwrap();
    assert_eq!(shard1, shard1_again);
}

#[test]
fn test_multi_shard_client_creation() {
    use crate::core::shard_client::{MultiShardClient, ShardClient};
    
    let mut client = MultiShardClient::new();
    
    // Добавляем клиенты для шардов
    let client1 = ShardClient::new("http://127.0.0.1:8080".to_string());
    let client2 = ShardClient::new("http://127.0.0.1:8081".to_string());
    
    client.add_shard_client("shard1".to_string(), client1);
    client.add_shard_client("shard2".to_string(), client2);
    
    // Проверяем, что клиенты добавлены
    assert!(client.get_client("shard1").is_some());
    assert!(client.get_client("shard2").is_some());
    assert!(client.get_client("shard3").is_none());
}

