use std::collections::HashMap;

use crate::vectors::embeddings::{VectorController, create_vector_with_embedding};

pub mod core;
pub mod vectors;

fn main() {
    let mut meta1 = HashMap::new();
    meta1.insert("category".to_string(), "greeting".to_string());
    let vector1 = match create_vector_with_embedding("Hello, world!", meta1) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error creating vector1: {}", e);
            return;
        }
    };

    let mut meta2 = HashMap::new();
    meta2.insert("category".to_string(), "farewell".to_string());
    let vector2 = match create_vector_with_embedding("Goodbye, see you later!", meta2) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error creating vector2: {}", e);
            return;
        }
    };

    let mut meta3 = HashMap::new();
    meta3.insert("category".to_string(), "greeting".to_string());
    let vector3 = match create_vector_with_embedding("Hi there!", meta3) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error creating vector3: {}", e);
            return;
        }
    };

    let mut controller = VectorController::new();

    // Добавление вектора
    controller.add_vector(vector1);
    controller.add_vector(vector2);
    controller.add_vector(vector3);

    // Поиск наиболее похожего вектора
    match controller.find_most_similar("hello") {
        Ok((index, score)) => {
            println!("Most similar vector at index {}: score = {}", index, score);
        }
        Err(e) => eprintln!("Error: {}", e),
    }

    // Удаление вектора
    let id_to_remove = controller.get_vector(0).unwrap().id.clone();
    controller.remove_vector(&id_to_remove);
    println!("Removed vector with ID: {}", id_to_remove);

    // Вывод вектора по id
    let target_id = &controller.get_vector(0).unwrap().id.clone();
    if let Some(vector) = controller.get_vector_by_id(target_id) {
        println!("Vector ID: {}", vector.id);
        println!("Embedding: {:?}", vector.embedding);
        println!("Metadata: {:?}", vector.metadata);
        println!("Created at: {}", vector.created_at);
    } else {
        println!("Vector with ID {} not found.", target_id);
    }

    // Обновление вектора
    let new_meta = {
        let mut m = HashMap::new();
        m.insert("category".to_string(), "updated_greeting".to_string());
        m
    };

    match controller.update_vector_by_text(&target_id, "Hello, Rust!", new_meta) {
        Ok(true) => println!("Vector with ID {} updated successfully.", target_id),
        Ok(false) => println!("Vector with ID {} not found for update.", target_id),
        Err(e) => eprintln!("Error updating vector: {}", e),
    }

    // Проверим обновлённый вектор
    if let Some(vector) = controller.get_vector_by_id(&target_id) {
        println!("Updated embedding: {:?}", vector.embedding);
        println!("Updated meta: {:?}", vector.metadata);
    }
    

}