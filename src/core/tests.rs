use std::collections::HashMap;

use super::{controllers::VectorController, embeddings::make_embeddings};

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
        .add_vector(embedding.clone(), metadata.clone())
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
        .add_vector(embedding, metadata)
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
        .add_vector(embedding, metadata)
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
        .add_vector(embedding, metadata)
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
        .add_vector(embedding, metadata)
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
        .add_vector(embedding_for("hello"), metadata_with_category("greeting"))
        .expect("Не удалось добавить первый вектор");

    let id2 = controller
        .add_vector(embedding_for("bye"), metadata_with_category("farewell"))
        .expect("Не удалось добавить второй вектор");

    let id3 = controller
        .add_vector(embedding_for("hi"), metadata_with_category("greeting"))
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
        .add_vector(embedding_for("hello"), metadata_with_category("greeting"))
        .expect("Не удалось добавить первый вектор");

    let _id2 = controller
        .add_vector(embedding_for("farewell"), metadata_with_category("farewell"))
        .expect("Не удалось добавить второй вектор");

    let _id3 = controller
        .add_vector(embedding_for("hi"), metadata_with_category("greeting"))
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

