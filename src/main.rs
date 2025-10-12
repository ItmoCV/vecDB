// src/main.rs
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::core::embeddings::make_embeddings;
use crate::core::vector_db::VectorDB;
use crate::core::lsh::LSHMetric;
use crate::core::controllers::{CollectionController, ConnectionController, StorageController};
use crate::core::config::ConfigLoader;

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

#[tokio::main]
async fn main() {
    println!("=== Демонстрация работы с VectorDB ===\n");

    // Извлекаем путь до конфига из аргументов командной строки
    let args: Vec<String> = env::args().collect();
    let config_path = if args.len() > 1 {
        args[1].clone()
    } else {
        println!("Путь до конфига должен быть передан как первый аргумент командной строки.");
        println!("Пример запуска: cargo run -- config.json");
        std::process::exit(1);
    };

    // Создаем VectorDB, передав путь до конфиг файла
    let mut db = VectorDB::new(config_path.clone());

    // Пробуем загрузить существующие коллекции
    println!("📂 Попытка загрузить существующие коллекции...");
    db.load();

    // Создаем коллекцию с метрикой Euclidean и размерностью векторов 384
    let collection_name = "my_documents".to_string();
    let vector_dimension = 384; // Размерность эмбеддингов
    
    match db.add_collection(collection_name.clone(), LSHMetric::Euclidean, vector_dimension) {
        Ok(_) => {
            println!("✅ Создана новая коллекция: {} с размерностью векторов: {}", collection_name, vector_dimension);
            
            // Подготавливаем текстовые векторы
            let texts = vec![
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
                println!("  ➕ Добавлен вектор {} с ID: {}", i + 1, id);
            }

            println!("📝 Всего добавлено {} векторов в коллекцию '{}'", vector_ids.len(), collection_name);

            // Сохраняем все коллекции
            db.dump();
            println!("💾 Коллекции успешно сохранены на диск!\n");
        }
        Err(_) => {
            println!("ℹ️  Коллекция '{}' уже существует, используем существующую\n", collection_name);
        }
    }

    // ========== ЗАПУСК HTTP СЕРВЕРА ==========
    println!("🚀 Подготовка к запуску HTTP сервера...");
    
    // Подготовка контроллеров для HTTP сервера
    let mut config_loader = ConfigLoader::new();
    config_loader.load(config_path);
    
    let storage_controller = Arc::new(
        StorageController::new(config_loader.get("path"))
    );
    
    // Извлекаем collection_controller из db и оборачиваем в Arc<RwLock<>>
    let collection_controller = Arc::new(RwLock::new(
        std::mem::replace(
            db.collection_controller_mut(),
            CollectionController::new(Arc::clone(&storage_controller))
        )
    ));
    
    // Получаем адрес и порт из конфига ПЕРЕД созданием connection_controller
    let connection_config = config_loader.get("connection");
    let host = connection_config.get("host")
        .map(|s| s.as_str())
        .unwrap_or("0.0.0.0");
    let port = connection_config.get("port")
        .map(|s| s.as_str())
        .unwrap_or("8080");
    
    // Создаем connection_controller для управления HTTP соединениями
    let mut connection_controller = ConnectionController::new(
        Arc::clone(&storage_controller),
        config_loader
    );
    
    let addr_str = format!("{}:{}", host, port);
    let addr = addr_str.parse().expect("Неверный адрес сервера из конфига");
    
    println!("\n✅ Сервер готов к запуску");
    println!("🌐 Адрес сервера: http://{}", addr);
    println!("📖 Swagger UI: http://{}/swagger-ui", addr);
    println!("📄 OpenAPI спецификация: http://{}/api-docs/openapi.json", addr);
    println!("\n🛑 Для остановки сервера отправьте POST запрос на /stop");
    println!("═══════════════════════════════════════════════════════\n");
    
    // Запускаем HTTP сервер (блокирует выполнение до остановки)
    match connection_controller
        .connection_handler(collection_controller, addr)
        .await
    {
        Ok(returned_controller) => {
            println!("\n🛑 Получен сигнал остановки сервера");
            println!("💾 Сохранение всех коллекций на диск...");
            
            // Получаем контроллер обратно и выполняем dump
            let ctrl = returned_controller.read().await;
            ctrl.dump();
            
            println!("✅ Все коллекции успешно сохранены!");
            println!("👋 Завершение работы...");
        }
        Err(e) => {
            eprintln!("\n❌ Ошибка запуска сервера: {:?}", e);
            std::process::exit(1);
        }
    }
}