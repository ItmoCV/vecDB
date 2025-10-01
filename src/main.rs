// src/main.rs
use std::collections::HashMap;

use crate::core::embeddings::make_embeddings;
use crate::core::controllers::VectorController;

pub mod core;

// Функция для кодирования текста в эмбеддинг
fn encode_text(text: &str) -> Result<Vec<f32>, Box<dyn std::error::Error>> {
    make_embeddings(text)
}

// Функция для создания метаданных
fn create_metadata(category: &str, additional: Option<HashMap<String, String>>) -> HashMap<String, String> {
    let mut meta = HashMap::new();
    meta.insert("category".to_string(), category.to_string());
    
    if let Some(additional_meta) = additional {
        meta.extend(additional_meta);
    }
    
    meta
}

// Функция для подготовки вектора (кодирование + метаданные)
fn prepare_vector_data(text: &str, category: &str, additional_meta: Option<HashMap<String, String>>) -> Result<(Vec<f32>, HashMap<String, String>), Box<dyn std::error::Error>> {
    let embedding = encode_text(text)?;
    let metadata = create_metadata(category, additional_meta);
    Ok((embedding, metadata))
}

fn main() {
    let mut controller = VectorController::new();

    // Подготовка данных для векторов
    let (embedding1, meta1) = match prepare_vector_data("Hello, world!", "greeting", None) {
        Ok(data) => data,
        Err(e) => {
            eprintln!("Error preparing vector1 data: {}", e);
            return;
        }
    };

    let (embedding2, meta2) = match prepare_vector_data("Goodbye, see you later!", "farewell", None) {
        Ok(data) => data,
        Err(e) => {
            eprintln!("Error preparing vector2 data: {}", e);
            return;
        }
    };

    let (embedding3, meta3) = match prepare_vector_data("Hi there!", "greeting", None) {
        Ok(data) => data,
        Err(e) => {
            eprintln!("Error preparing vector3 data: {}", e);
            return;
        }
    };

    // Добавляем векторы через контроллер, передавая уже закодированные эмбеддинги
    let id1 = match controller.add_vector_from_embedding(embedding1, meta1) {
        Ok(id) => id,
        Err(e) => {
            eprintln!("Error adding vector1: {}", e);
            return;
        }
    };

    let id2 = match controller.add_vector_from_embedding(embedding2, meta2) {
        Ok(id) => id,
        Err(e) => {
            eprintln!("Error adding vector2: {}", e);
            return;
        }
    };

    let id3 = match controller.add_vector_from_embedding(embedding3, meta3) {
        Ok(id) => id,
        Err(e) => {
            eprintln!("Error adding vector3: {}", e);
            return;
        }
    };

    println!("Added vectors with IDs: {}, {}, {}", id1, id2, id3);

    // Добавляем новую метадату к вектору
    let mut additional_meta = HashMap::new();
    additional_meta.insert("lang".to_string(), "en".to_string());

    if controller.add_metadata_to_vector(&id1, additional_meta) {
        println!("Metadata added to vector: {}", id1);
    } else {
        println!("Vector not found.");
    }

    // Выводим обновлённые метаданные
    if let Some(vector) = controller.get_vector_by_id(&id1) {
        println!("Updated metadata: {:?}", vector.metadata);
    }

    // Удаляем поле "lang" из метаданных
    if controller.remove_metadata_from_vector(&id1, "lang") {
        println!("Metadata 'lang' removed from vector: {}", id1);
    } else {
        println!("Vector or key not found.");
    }

    // Выводим метаданные после удаления
    if let Some(vector) = controller.get_vector_by_id(&id1) {
        println!("Metadata after removal: {:?}", vector.metadata);
    }

    // Поиск наиболее похожего вектора
    match controller.find_most_similar("hello") {
        Ok((index, score)) => {
            println!("Most similar vector at index {}: score = {}", index, score);
        }
        Err(e) => eprintln!("Error: {}", e),
    }

    // Удаление вектора
    controller.remove_vector(&id1);
    println!("Removed vector with ID: {}", id1);

    // Вывод вектора по id
    if let Some(vector) = controller.get_vector_by_id(&id2) {
        println!("Vector ID: {}", vector.hash_id);
        println!("Metadata: {:?}", vector.metadata);
        println!("Created at: {}", vector.created_at);
    } else {
        println!("Vector with ID {} not found.", id2);
    }

    // Обновление вектора - кодируем новый текст заранее
    let (new_embedding, new_meta) = match prepare_vector_data("Hello, Rust!", "updated_greeting", None) {
        Ok(data) => data,
        Err(e) => {
            eprintln!("Error preparing new vector data: {}", e);
            return;
        }
    };

    match controller.update_vector_by_embedding(&id2, new_embedding, new_meta) {
        Ok(true) => println!("Vector with ID {} updated successfully.", id2),
        Ok(false) => println!("Vector with ID {} not found for update.", id2),
        Err(e) => eprintln!("Error updating vector: {}", e),
    }

    // Проверим обновлённый вектор
    if let Some(vector) = controller.get_vector_by_id(&id2) {
        println!("Updated metadata: {:?}", vector.metadata);
    }

    // Фильтрация по метадате
    let mut filters = HashMap::new();
    filters.insert("category".to_string(), "greeting".to_string());
    let filtered_ids = controller.filter_by_metadata(&filters);
    println!("Vectors with category 'greeting': {:?}", filtered_ids);
}