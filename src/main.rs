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

    let mut controller = VectorController::new();
    controller.add_vector(vector1);
    controller.add_vector(vector2);

    // Ищем наиболее похожий
    match controller.find_most_similar("hello") {
        Ok((index, score)) => {
            println!("Most similar vector at index {}: score = {}", index, score);
        }
        Err(e) => eprintln!("Error: {}", e),
    }

    // Удаляем вектор
    let id_to_remove = controller.get_vector(0).unwrap().id.clone();
    controller.remove_vector(&id_to_remove);
    println!("Removed vector with ID: {}", id_to_remove);

}