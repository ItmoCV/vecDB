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
fn prepare_vector_data(
    text: &str,
    category: &str,
    additional_meta: Option<HashMap<String, String>>,
) -> Result<(Vec<f32>, HashMap<String, String>), Box<dyn std::error::Error>> {
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
            eprintln!("Ошибка при подготовке данных для вектора 1: {}", e);
            return;
        }
    };

    let (embedding2, meta2) = match prepare_vector_data("Goodbye, see you later!", "farewell", None) {
        Ok(data) => data,
        Err(e) => {
            eprintln!("Ошибка при подготовке данных для вектора 2: {}", e);
            return;
        }
    };

    let (embedding3, meta3) = match prepare_vector_data("Hi there!", "greeting", None) {
        Ok(data) => data,
        Err(e) => {
            eprintln!("Ошибка при подготовке данных для вектора 3: {}", e);
            return;
        }
    };

    // Добавляем векторы через контроллер, передавая уже закодированные эмбеддинги
    let id1 = match controller.add_vector(embedding1, meta1) {
        Ok(id) => id,
        Err(e) => {
            eprintln!("Ошибка при добавлении вектора 1: {}", e);
            return;
        }
    };

    let id2 = match controller.add_vector(embedding2, meta2) {
        Ok(id) => id,
        Err(e) => {
            eprintln!("Ошибка при добавлении вектора 2: {}", e);
            return;
        }
    };

    let id3 = match controller.add_vector(embedding3, meta3) {
        Ok(id) => id,
        Err(e) => {
            eprintln!("Ошибка при добавлении вектора 3: {}", e);
            return;
        }
    };

    println!("Добавлены векторы с ID: {}, {}, {}", id1, id2, id3);

    // Добавляем новую метадату к вектору
    let mut additional_meta = HashMap::new();
    additional_meta.insert("lang".to_string(), "en".to_string());

    match controller.add_metadata_to_vector(id1, additional_meta) {
        Ok(_) => println!("Метаданные добавлены к вектору: {}", id1),
        Err(_) => println!("Вектор не найден."),
    }

    // Выводим обновлённые метаданные
    if let Some(vector) = controller.get_vector_by_id(id1) {
        println!("Обновлённые метаданные: {:?}", vector.metadata);
    }

    // Удаляем поле "lang" из метаданных
    match controller.remove_metadata_from_vector(id1, "lang") {
        Ok(_) => println!("Метаданные 'lang' удалены из вектора: {}", id1),
        Err(_) => println!("Вектор или ключ не найден."),
    }

    // Выводим метаданные после удаления
    if let Some(vector) = controller.get_vector_by_id(id1) {
        println!("Метаданные после удаления: {:?}", vector.metadata);
    }

    // Поиск наиболее похожего вектора
    match controller.find_most_similar("hello") {
        Ok((index, score)) => {
            println!("Наиболее похожий вектор по индексу {}: score = {}", index, score);
        }
        Err(e) => eprintln!("Ошибка: {}", e),
    }

    // Удаление вектора
    match controller.remove_vector(id1) {
        Ok(_) => println!("Вектор с ID {} удалён.", id1),
        Err(e) => println!("Ошибка при удалении вектора: {}", e),
    }

    // Вывод вектора по id
    if let Some(vector) = controller.get_vector_by_id(id2) {
        println!("Вектор: {:?}", vector);
    } else {
        println!("Вектор с ID {} не найден.", id2);
    }

    // Обновление вектора - кодируем новый текст заранее
    let (new_embedding, new_meta) = match prepare_vector_data("Hello, Rust!", "updated_greeting", None) {
        Ok(data) => data,
        Err(e) => {
            eprintln!("Ошибка при подготовке новых данных для вектора: {}", e);
            return;
        }
    };

    match controller.update_vector(id2, new_embedding, new_meta) {
        Ok(_) => println!("Вектор с ID {} успешно обновлён.", id2),
        Err(_) => println!("Вектор с ID {} не найден для обновления.", id2),
    }

    // Проверим обновлённый вектор
    if let Some(vector) = controller.get_vector_by_id(id2) {
        println!("Обновлённые метаданные: {:?}", vector.metadata);
    }

    // Фильтрация по метадате
    let mut filters = HashMap::new();
    filters.insert("category".to_string(), "greeting".to_string());
    let filtered_ids = controller.filter_by_metadata(&filters);
    println!("Векторы с категорией 'greeting': {:?}", filtered_ids);
}