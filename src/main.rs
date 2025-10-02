// src/main.rs
use std::collections::HashMap;

use crate::core::embeddings::make_embeddings;
use crate::core::controllers::{CollectionController, StorageController};
use crate::core::lsh::LSHMetric;

pub mod core;

// Функция для создания метаданных
fn create_metadata(category: &str, additional: Option<HashMap<String, String>>) -> HashMap<String, String> {
    let mut meta = HashMap::new();
    meta.insert("category".to_string(), category.to_string());

    if let Some(additional_meta) = additional {
        meta.extend(additional_meta);
    }

    meta
}

fn main() {
    println!("=== Демонстрация работы с CollectionController ===");
    
    // Создаем StorageController и CollectionController
    let storage_controller = StorageController::new(HashMap::new());
    let mut collection_controller = CollectionController::new(storage_controller);
    
    // Создаем коллекцию с метрикой Euclidean и размерностью векторов 384
    let collection_name = "my_documents".to_string();
    let vector_dimension = 384; // Размерность эмбеддингов
    collection_controller.add_collection(collection_name.clone(), LSHMetric::Euclidean, vector_dimension).unwrap();
    println!("Создана коллекция: {} с размерностью векторов: {}", collection_name, vector_dimension);
    
    // Подготавливаем текстовые векторы
    let texts = vec![
        "Привет, мир!",
        "Добро пожаловать в векторную базу данных",
        "Это демонстрация работы с коллекциями"
    ];
    
    let mut vector_ids = Vec::new();
    
    // Добавляем векторы в коллекцию
    for (i, text) in texts.iter().enumerate() {
        let embedding = make_embeddings(text).expect("Не удалось создать эмбеддинг");
        let metadata = create_metadata("document", None);
        
        let id = collection_controller.add_vector(&collection_name, embedding, metadata).unwrap();
        vector_ids.push(id);
        println!("Добавлен вектор {} с ID: {}", i + 1, id);
    }
    
    println!("Всего добавлено {} векторов в коллекцию '{}'", vector_ids.len(), collection_name);
    
    // Сохраняем коллекцию
    if let Some(collection) = collection_controller.get_collection(&collection_name) {
        collection_controller.dump_one(collection);
        println!("Коллекция '{}' успешно сохранена!", collection_name);
    } else {
        println!("Ошибка: коллекция '{}' не найдена", collection_name);
    }
    
    println!("\nДемонстрация завершена успешно!");
}