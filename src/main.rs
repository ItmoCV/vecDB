// src/main.rs
use std::collections::HashMap;

use crate::core::embeddings::make_embeddings;
use crate::core::vector_db::VectorDB;
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
    println!("=== Демонстрация работы с VectorDB ===");
    
    // Создаем VectorDB
    let mut db = VectorDB::new(None);
    
    // Создаем коллекцию с метрикой Euclidean и размерностью векторов 384
    let collection_name = "my_documents".to_string();
    let vector_dimension = 384; // Размерность эмбеддингов
    db.add_collection(collection_name.clone(), LSHMetric::Euclidean, vector_dimension).unwrap();
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
        
        let id = db.add_vector(&collection_name, embedding, metadata).unwrap();
        vector_ids.push(id);
        println!("Добавлен вектор {} с ID: {}", i + 1, id);
    }
    
    println!("Всего добавлено {} векторов в коллекцию '{}'", vector_ids.len(), collection_name);
    
    // Сохраняем все коллекции
    db.dump();
    println!("Коллекции успешно сохранены!");
    
    println!("\nДемонстрация завершена успешно!");
}