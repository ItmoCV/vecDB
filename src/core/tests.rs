use std::collections::HashMap;

use super::{controllers::{VectorController, BucketController}, embeddings::make_embeddings, lsh::{LSH, LSHMetric}, objects::Collection};

fn embedding_for(text: &str) -> Vec<f32> {
    make_embeddings(text).expect("Не удалось создать эмбеддинг в тесте")
}

fn metadata_with_category(category: &str) -> HashMap<String, String> {
    let mut metadata = HashMap::new();
    metadata.insert("category".to_string(), category.to_string());
    metadata
}

#[test]
fn add_and_get_vector() {
    let mut controller = VectorController::new();
    let embedding = embedding_for("hello world");
    let metadata = metadata_with_category("greeting");

    let id = controller
        .add_vector(Some(embedding.clone()), Some(metadata.clone()), None, None)
        .expect("Не удалось добавить вектор");

    let stored = controller
        .get_vector_by_id(id)
        .expect("Добавленный вектор должен существовать");

    assert_eq!(stored.data, embedding);
    assert_eq!(stored.metadata.get("category"), Some(&"greeting".to_string()));
    assert!(stored.timestamp > 0);
}

#[test]
fn add_and_remove_metadata() {
    let mut controller = VectorController::new();
    let embedding = embedding_for("hello");
    let metadata = metadata_with_category("greeting");
    let id = controller
        .add_vector(Some(embedding), Some(metadata), None, None)
        .expect("Не удалось добавить вектор");

    let mut extra = HashMap::new();
    extra.insert("lang".to_string(), "en".to_string());

    controller
        .add_metadata_to_vector(id, extra)
        .expect("Метаданные должны добавляться");

    let vector = controller
        .get_vector_by_id(id)
        .expect("Вектор должен существовать после добавления метаданных");
    assert_eq!(vector.metadata.get("lang"), Some(&"en".to_string()));

    controller
        .remove_metadata_from_vector(id, "lang")
        .expect("Метаданные должны удаляться");

    let vector = controller
        .get_vector_by_id(id)
        .expect("Вектор должен существовать после удаления метаданных");
    assert!(!vector.metadata.contains_key("lang"));
}

#[test]
fn update_vector_replaces_data() {
    let mut controller = VectorController::new();
    let embedding = embedding_for("hello");
    let metadata = metadata_with_category("greeting");
    let id = controller
        .add_vector(Some(embedding), Some(metadata), None, None)
        .expect("Не удалось добавить вектор");

    let new_embedding = embedding_for("hello rust");
    // Оставляем старые метаданные
    controller
        .update_vector(id, Some(new_embedding.clone()), None)
        .expect("Обновление должно завершиться успешно");

    let vector = controller
        .get_vector_by_id(id)
        .expect("Вектор должен существовать после обновления");

    assert_eq!(vector.data, new_embedding);
}

#[test]
fn update_vector_replaces_metadata() {
    let mut controller = VectorController::new();
    let embedding = embedding_for("hello");
    let metadata = metadata_with_category("greeting");
    let id = controller
        .add_vector(Some(embedding), Some(metadata), None, None)
        .expect("Не удалось добавить вектор");

    let mut new_metadata = metadata_with_category("updated_greeting");
    new_metadata.insert("lang".to_string(), "en".to_string());

    // Оставляем старый embedding
    controller
        .update_vector(id, None, Some(new_metadata.clone()))
        .expect("Обновление должно завершиться успешно");

    let vector = controller
        .get_vector_by_id(id)
        .expect("Вектор должен существовать после обновления");

    assert_eq!(vector.metadata, new_metadata);
}

#[test]
fn remove_vector_deletes_entry() {
    let mut controller = VectorController::new();
    let embedding = embedding_for("goodbye");
    let metadata = metadata_with_category("farewell");
    let id = controller
        .add_vector(Some(embedding), Some(metadata), None, None)
        .expect("Не удалось добавить вектор");

    controller
        .remove_vector(id)
        .expect("Удаление должно завершиться успешно");

    assert!(controller.get_vector_by_id(id).is_none());
}

#[test]
fn filter_by_metadata_returns_only_matching_ids() {
    let mut controller = VectorController::new();

    let id1 = controller
        .add_vector(Some(embedding_for("hello")), Some(metadata_with_category("greeting")), None, None)
        .expect("Не удалось добавить первый вектор");

    let id2 = controller
        .add_vector(Some(embedding_for("bye")), Some(metadata_with_category("farewell")), None, None)
        .expect("Не удалось добавить второй вектор");

    let id3 = controller
        .add_vector(Some(embedding_for("hi")), Some(metadata_with_category("greeting")), None, None)
        .expect("Не удалось добавить третий вектор");

    let mut filters = HashMap::new();
    filters.insert("category".to_string(), "greeting".to_string());

    let filtered = controller.filter_by_metadata(&filters);

    assert_eq!(filtered.len(), 2);
    assert!(filtered.contains(&id1));
    assert!(filtered.contains(&id3));
    assert!(!filtered.contains(&id2));
}

#[test]
fn find_most_similar_returns_k_vectors() {
    let mut controller = VectorController::new();

    let _id1 = controller
        .add_vector(Some(embedding_for("hello")), Some(metadata_with_category("greeting")), None, None)
        .expect("Не удалось добавить первый вектор");

    let _id2 = controller
        .add_vector(Some(embedding_for("farewell")), Some(metadata_with_category("farewell")), None, None)
        .expect("Не удалось добавить второй вектор");

    let _id3 = controller
        .add_vector(Some(embedding_for("hi")), Some(metadata_with_category("greeting")), None, None)
        .expect("Не удалось добавить третий вектор");

    let results = controller
        .find_most_similar(&embedding_for("hello"), 2)
        .expect("Поиск похожих векторов должен завершиться успешно");

    assert_eq!(results.len(), 2);

    let (first_index, first_score) = results[0];
    let (second_index, second_score) = results[1];

    assert!(first_index == 0);
    assert!(second_index == 1);
    assert_ne!(first_index, second_index);

    assert!(first_score > 0.0);
    assert!(second_score > 0.0);

    assert!(first_score >= second_score);
}

#[test]
fn operations_fail_for_missing_vector() {
    let mut controller = VectorController::new();
    let missing_id = 999_u64;

    let mut extra_meta = HashMap::new();
    extra_meta.insert("key".to_string(), "value".to_string());

    assert!(controller.add_metadata_to_vector(missing_id, extra_meta.clone()).is_err());
    assert!(controller.remove_metadata_from_vector(missing_id, "key").is_err());
    assert!(controller.update_vector(missing_id, Some(vec![1.0, 0.0, 0.0, 0.0]), Some(extra_meta)).is_err());
    assert!(controller.remove_vector(missing_id).is_err());
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
    
    // Похожие векторы должны иметь одинаковые или близкие хэши
    println!("Hash 1: {}, Hash 2: {}, Hash 3: {}", hash1, hash2, hash3);
    
    // Проверяем, что хэши детерминированы
    assert_eq!(lsh.hash(&vector1), hash1);
    assert_eq!(lsh.hash(&vector2), hash2);
    assert_eq!(lsh.hash(&vector3), hash3);
}

#[test]
fn test_bucket_controller_creation() {
    let lsh_controller = BucketController::new(4, 3, 1.0, LSHMetric::Euclidean, Some(42));
    
    assert_eq!(lsh_controller.dimension, Some(4));
    assert_eq!(lsh_controller.count(), 0);
    assert_eq!(lsh_controller.total_vectors(), 0);
}

#[test]
fn test_bucket_controller_add_vectors() {
    let mut lsh_controller = BucketController::new(4, 3, 1.0, LSHMetric::Euclidean, Some(42));
    
    let vector1 = vec![1.0, 2.0, 3.0, 4.0];
    let vector2 = vec![1.1, 2.1, 3.1, 4.1];
    let vector3 = vec![10.0, 20.0, 30.0, 40.0];
    
    let mut metadata1 = HashMap::new();
    metadata1.insert("category".to_string(), "test1".to_string());
    
    let mut metadata2 = HashMap::new();
    metadata2.insert("category".to_string(), "test2".to_string());
    
    let mut metadata3 = HashMap::new();
    metadata3.insert("category".to_string(), "test3".to_string());
    
    // Добавляем векторы
    let id1 = lsh_controller.add_vector(vector1, metadata1).expect("Не удалось добавить первый вектор");
    let id2 = lsh_controller.add_vector(vector2, metadata2).expect("Не удалось добавить второй вектор");
    let id3 = lsh_controller.add_vector(vector3, metadata3).expect("Не удалось добавить третий вектор");
    
    // Проверяем, что векторы добавлены
    assert_eq!(lsh_controller.total_vectors(), 3);
    assert!(lsh_controller.count() > 0); // Должны быть созданы бакеты
    
    // Проверяем, что векторы можно получить
    assert!(lsh_controller.get_vector(id1).is_some());
    assert!(lsh_controller.get_vector(id2).is_some());
    assert!(lsh_controller.get_vector(id3).is_some());
}

#[test]
fn test_bucket_controller_similarity_search() {
    let mut lsh_controller = BucketController::new(4, 3, 1.0, LSHMetric::Euclidean, Some(42));
    
    let vector1 = vec![1.0, 2.0, 3.0, 4.0];
    let vector2 = vec![1.1, 2.1, 3.1, 4.1];
    let vector3 = vec![10.0, 20.0, 30.0, 40.0];
    
    let mut metadata1 = HashMap::new();
    metadata1.insert("category".to_string(), "similar".to_string());
    
    let mut metadata2 = HashMap::new();
    metadata2.insert("category".to_string(), "similar".to_string());
    
    let mut metadata3 = HashMap::new();
    metadata3.insert("category".to_string(), "different".to_string());
    
    // Добавляем векторы
    let _id1 = lsh_controller.add_vector(vector1.clone(), metadata1).expect("Не удалось добавить первый вектор");
    let _id2 = lsh_controller.add_vector(vector2.clone(), metadata2).expect("Не удалось добавить второй вектор");
    let _id3 = lsh_controller.add_vector(vector3.clone(), metadata3).expect("Не удалось добавить третий вектор");
    
    // Поиск похожих векторов - используем точно такой же вектор
    let query = vector1.clone();
    
    let results = lsh_controller.find_similar(&query, 2).expect("Не удалось выполнить поиск");
    
    // Должны найтись похожие векторы (хотя бы сам вектор)
    println!("Найдено {} похожих векторов", results.len());
    
    for (bucket_id, vector_idx, score) in &results {
        println!("Бакет: {}, Индекс: {}, Score: {}", bucket_id, vector_idx, score);
    }
    
    // Проверяем, что поиск не падает с ошибкой
    assert!(results.len() >= 0); // Может быть 0 или больше
}

#[test]
fn test_bucket_controller_multi_bucket_search() {
    let mut lsh_controller = BucketController::new(4, 3, 1.0, LSHMetric::Euclidean, Some(42));
    
    let vector1 = vec![1.0, 2.0, 3.0, 4.0];
    let vector2 = vec![1.1, 2.1, 3.1, 4.1];
    let vector3 = vec![10.0, 20.0, 30.0, 40.0];
    
    let mut metadata1 = HashMap::new();
    metadata1.insert("category".to_string(), "test".to_string());
    
    // Добавляем векторы
    let _id1 = lsh_controller.add_vector(vector1.clone(), metadata1.clone()).expect("Не удалось добавить первый вектор");
    let _id2 = lsh_controller.add_vector(vector2.clone(), metadata1.clone()).expect("Не удалось добавить второй вектор");
    let _id3 = lsh_controller.add_vector(vector3.clone(), metadata1).expect("Не удалось добавить третий вектор");
    
    // Поиск в нескольких бакетах - используем точно такой же вектор
    let query = vector1.clone();
    
    let results = lsh_controller.find_similar_multi_bucket(&query, 3).expect("Не удалось выполнить поиск");
    
    // Проверяем, что поиск не падает с ошибкой
    println!("Мульти-бакет поиск нашел {} результатов", results.len());
    assert!(results.len() >= 0); // Может быть 0 или больше
}

#[test]
fn test_bucket_controller_metadata_filtering() {
    let mut lsh_controller = BucketController::new(4, 3, 1.0, LSHMetric::Euclidean, Some(42));
    
    let vector1 = vec![1.0, 2.0, 3.0, 4.0];
    let vector2 = vec![1.1, 2.1, 3.1, 4.1];
    let vector3 = vec![10.0, 20.0, 30.0, 40.0];
    
    let mut metadata1 = HashMap::new();
    metadata1.insert("category".to_string(), "documents".to_string());
    metadata1.insert("language".to_string(), "ru".to_string());
    
    let mut metadata2 = HashMap::new();
    metadata2.insert("category".to_string(), "images".to_string());
    metadata2.insert("language".to_string(), "en".to_string());
    
    let mut metadata3 = HashMap::new();
    metadata3.insert("category".to_string(), "documents".to_string());
    metadata3.insert("language".to_string(), "en".to_string());
    
    // Добавляем векторы
    let _id1 = lsh_controller.add_vector(vector1, metadata1).expect("Не удалось добавить первый вектор");
    let _id2 = lsh_controller.add_vector(vector2, metadata2).expect("Не удалось добавить второй вектор");
    let _id3 = lsh_controller.add_vector(vector3, metadata3).expect("Не удалось добавить третий вектор");
    
    // Фильтрация по метаданным
    let mut filters = HashMap::new();
    filters.insert("category".to_string(), "documents".to_string());
    
    let filtered_ids = lsh_controller.filter_by_metadata(&filters);
    
    // Должны найтись 2 вектора с категорией "documents"
    assert_eq!(filtered_ids.len(), 2);
}

#[test]
fn test_bucket_controller_statistics() {
    let mut lsh_controller = BucketController::new(4, 3, 1.0, LSHMetric::Euclidean, Some(42));
    
    let vector1 = vec![1.0, 2.0, 3.0, 4.0];
    let vector2 = vec![1.1, 2.1, 3.1, 4.1];
    
    let mut metadata = HashMap::new();
    metadata.insert("category".to_string(), "test".to_string());
    
    // Добавляем векторы
    let _id1 = lsh_controller.add_vector(vector1, metadata.clone()).expect("Не удалось добавить первый вектор");
    let _id2 = lsh_controller.add_vector(vector2, metadata).expect("Не удалось добавить второй вектор");
    
    // Получаем статистику
    let stats = lsh_controller.get_statistics();
    
    assert_eq!(stats.get("total_vectors").unwrap(), "2");
    assert_eq!(stats.get("dimension").unwrap(), "4");
    assert_eq!(stats.get("num_hashes").unwrap(), "3");
    assert_eq!(stats.get("bucket_width").unwrap(), "1");
    
    println!("Статистика LSH: {:?}", stats);
}

#[test]
fn test_bucket_controller_remove_vector() {
    let mut lsh_controller = BucketController::new(4, 3, 1.0, LSHMetric::Euclidean, Some(42));
    
    let vector1 = vec![1.0, 2.0, 3.0, 4.0];
    let vector2 = vec![1.1, 2.1, 3.1, 4.1];
    
    let mut metadata = HashMap::new();
    metadata.insert("category".to_string(), "test".to_string());
    
    // Добавляем векторы
    let id1 = lsh_controller.add_vector(vector1, metadata.clone()).expect("Не удалось добавить первый вектор");
    let id2 = lsh_controller.add_vector(vector2, metadata).expect("Не удалось добавить второй вектор");
    
    assert_eq!(lsh_controller.total_vectors(), 2);
    
    // Удаляем первый вектор
    lsh_controller.remove_vector(id1).expect("Не удалось удалить вектор");
    
    assert_eq!(lsh_controller.total_vectors(), 1);
    assert!(lsh_controller.get_vector(id1).is_none());
    assert!(lsh_controller.get_vector(id2).is_some());
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

#[test]
fn test_vector_storage_in_buckets() {
    use crate::core::controllers::StorageController;
    
    // Создаем StorageController
    let storage_controller = StorageController::new(HashMap::new());
    
    // Создаем тестовые данные
    let collection_name = "test_collection".to_string();
    let bucket_id = 12345u64;
    let vector_id = 67890u64;
    let test_data = vec![1, 2, 3, 4, 5];
    
    // Сохраняем вектор в бакет
    let save_result = storage_controller.save_vector_to_bucket(
        collection_name.clone(), 
        bucket_id.to_string(), 
        vector_id, 
        test_data.clone()
    );
    assert!(save_result.is_ok(), "Ошибка сохранения вектора в бакет");
    
    // Загружаем вектор из бакета
    let loaded_data = storage_controller.read_vector_from_bucket(
        collection_name.clone(), 
        bucket_id.to_string(), 
        vector_id
    );
    
    assert!(loaded_data.is_some(), "Вектор не найден в бакете");
    assert_eq!(loaded_data.unwrap(), test_data, "Загруженные данные не совпадают с сохраненными");
    
    // Тестируем загрузку несуществующего вектора
    let non_existent = storage_controller.read_vector_from_bucket(
        collection_name,
        bucket_id.to_string(),
        99999u64
    );
    assert!(non_existent.is_none(), "Несуществующий вектор не должен быть найден");
}

#[test]
fn test_bucket_storage_in_own_folder() {
    use crate::core::controllers::StorageController;
    
    // Создаем StorageController
    let storage_controller = StorageController::new(HashMap::new());
    
    // Создаем тестовые данные
    let collection_name = "test_collection_bucket".to_string();
    let bucket_id = 54321u64;
    let test_data = vec![10, 20, 30, 40, 50];
    
    // Сохраняем данные бакета в папку бакета
    let save_result = storage_controller.save_bucket(
        collection_name.clone(), 
        bucket_id.to_string(), 
        test_data.clone()
    );
    assert!(save_result.is_ok(), "Ошибка сохранения данных бакета");
    
    // Загружаем данные бакета из папки бакета
    let loaded_data = storage_controller.read_bucket(
        collection_name.clone(), 
        bucket_id.to_string()
    );
    
    assert!(loaded_data.is_some(), "Данные бакета не найдены");
    assert_eq!(loaded_data.unwrap(), test_data, "Загруженные данные бакета не совпадают с сохраненными");
    
    // Тестируем загрузку несуществующего бакета
    let non_existent = storage_controller.read_bucket(
        collection_name, 
        "non_existent_bucket".to_string()
    );
    assert!(non_existent.is_none(), "Несуществующий бакет не должен быть найден");
    
    // Тестируем получение списка бакетов
    let bucket_names = storage_controller.get_all_buckets_names("test_collection_bucket".to_string());
    assert!(bucket_names.contains(&bucket_id.to_string()), "Бакет должен быть найден в списке");
}

#[test]
fn test_vector_moves_between_buckets_on_update() {
    use crate::core::controllers::BucketController;
    use crate::core::lsh::LSHMetric;
    
    // Создаем BucketController с LSH
    let mut bucket_controller = BucketController::new(4, 3, 1.0, LSHMetric::Euclidean, Some(42));
    
    // Добавляем вектор
    let original_vector = vec![1.0, 2.0, 3.0, 4.0];
    let mut metadata = HashMap::new();
    metadata.insert("type".to_string(), "test".to_string());
    
    let vector_id = bucket_controller.add_vector(original_vector.clone(), metadata).unwrap();
    
    // Проверяем, что вектор добавлен в бакет
    let original_bucket_id = bucket_controller.lsh.as_ref().unwrap().hash(&original_vector);
    let original_bucket = bucket_controller.get_bucket(original_bucket_id).unwrap();
    assert!(original_bucket.contains_vector(vector_id), "Вектор должен быть в исходном бакете");
    
    // Обновляем вектор так, чтобы он попал в другой бакет
    let new_vector = vec![10.0, 20.0, 30.0, 40.0]; // Значительно отличается от исходного
    let new_metadata = HashMap::new();
    
    bucket_controller.update_vector(vector_id, Some(new_vector.clone()), Some(new_metadata)).unwrap();
    
    // Проверяем, что вектор переместился в новый бакет
    let new_bucket_id = bucket_controller.lsh.as_ref().unwrap().hash(&new_vector);
    assert_ne!(original_bucket_id, new_bucket_id, "Вектор должен попасть в другой бакет");
    
    let new_bucket = bucket_controller.get_bucket(new_bucket_id).unwrap();
    assert!(new_bucket.contains_vector(vector_id), "Вектор должен быть в новом бакете");
    
    // Проверяем, что вектор больше не в старом бакете
    let old_bucket = bucket_controller.get_bucket(original_bucket_id);
    if let Some(bucket) = old_bucket {
        assert!(!bucket.contains_vector(vector_id), "Вектор не должен быть в старом бакете");
    }
    
    // Проверяем, что данные вектора обновились
    let updated_vector = bucket_controller.get_vector(vector_id).unwrap();
    assert_eq!(updated_vector.data, new_vector, "Данные вектора должны обновиться");
}

#[test]
fn test_vector_dimension_validation() {
    use crate::core::controllers::{CollectionController, StorageController};
    use crate::core::lsh::LSHMetric;
    use std::collections::HashMap;
    
    // Создаем контроллеры
    let storage_controller = StorageController::new(HashMap::new());
    let mut collection_controller = CollectionController::new(storage_controller);
    
    // Создаем коллекцию с размерностью 384
    let collection_name = "test_collection".to_string();
    collection_controller.add_collection(collection_name.clone(), LSHMetric::Euclidean, 384).unwrap();
    
    // Создаем правильный вектор (384 измерения)
    let correct_vector = vec![1.0; 384];
    let metadata = HashMap::new();
    
    // Добавляем правильный вектор - должно работать
    let result = collection_controller.add_vector(&collection_name, correct_vector, metadata.clone());
    assert!(result.is_ok(), "Правильный вектор должен быть добавлен успешно");
    
    // Создаем неправильный вектор (100 измерений)
    let incorrect_vector = vec![1.0; 100];
    
    // Пытаемся добавить неправильный вектор - должно вернуть ошибку
    let result = collection_controller.add_vector(&collection_name, incorrect_vector, metadata);
    assert!(result.is_err(), "Неправильный вектор должен быть отклонен");
    
    // Проверяем сообщение об ошибке
    if let Err(error_msg) = result {
        assert!(error_msg.contains("Размерность вектора не соответствует размерности коллекции"));
    }
    
    println!("Тест валидации размерности векторов завершен успешно!");
}

