// src/main.rs
use std::env;
use crate::core::vector_db::VectorDB;
use crate::core::config::ConfigLoader;
use crate::core::controllers::ConnectionController;

pub mod core;


#[tokio::main]
async fn main() {
    println!("=== Запуск VectorDB ===\n");

    // Извлекаем путь до конфига из аргументов командной строки
    let args: Vec<String> = env::args().collect();
    let config_path = if args.len() > 1 {
        args[1].clone()
    } else {
        println!("Путь до конфига должен быть передан как первый аргумент командной строки.");
        println!("Пример запуска: cargo run -- config.json");
        std::process::exit(1);
    };

    // Создаем единый ConfigLoader
    let mut config_loader = ConfigLoader::new();
    config_loader.load(config_path.clone());
    
    println!("📋 Настройки из конфигурации:");
    let role = config_loader.get_role();
    println!("🎯 Роль: {}", role.to_uppercase());
    
    let is_coordinator = config_loader.is_coordinator();

    if is_coordinator {
        println!("📡 Координатор - управляет шардами");
        match config_loader.get_shard_configs() {
            Ok(shard_configs) => {
                println!("📊 Найдено {} шардов в конфигурации", shard_configs.len());
                for (i, config) in shard_configs.iter().enumerate() {
                    println!("  Шард {}: {}:{}", i + 1, config.host, config.port);
                }
            }
            Err(e) => {
                println!("❌ Ошибка чтения конфигурации шардов: {}", e);
            }
        }
    } else {
        println!("💾 Шард - работает только с локальными данными");
    }

    // Создаем VectorDB из конфигурации
    let mut db = match VectorDB::new_from_config(config_loader.clone()) {
        Ok(db) => {
            if role == "coordinator" {
                println!("✅ Создан координатор VectorDB");
            } else {
                println!("✅ Создан шард VectorDB");
            }
            db
        }
        Err(e) => {
            println!("❌ Ошибка создания VectorDB: {}", e);
            println!("🔄 Пробуем создать обычную VectorDB...");
            // Создаем новый ConfigLoader для fallback
            let mut fallback_config = ConfigLoader::new();
            fallback_config.load(config_path.clone());
            VectorDB::new(fallback_config)
        }
    };

    // Пробуем загрузить существующие коллекции с диска
    println!("📂 Попытка загрузить существующие коллекции...");
    match db.load().await {
        Ok(_) => {
            println!("✅ Загружено коллекций с диска");
        }
        Err(e) => {
            println!("⚠️  Не удалось загрузить данные с диска: {}", e);
            println!("🆕 Будет создана новая пустая база данных");
        }
    }

    // ========== ЗАПУСК HTTP СЕРВЕРА ==========
    println!("🚀 Подготовка к запуску HTTP сервера...");
    
    // Создаем ConnectionController для HTTP сервера
    let mut connection_controller = ConnectionController::new(config_loader);
    
    // Получаем адрес сервера из ConnectionController
    let connection_config = connection_controller.get_connection_config();
    let host = connection_config.get("host")
        .map(|s| s.as_str())
        .unwrap_or("0.0.0.0");
    let port = connection_config.get("port")
        .map(|s| s.as_str())
        .unwrap_or("8080");
    
    let addr_str = format!("{}:{}", host, port);
    let addr = match addr_str.parse::<std::net::SocketAddr>() {
        Ok(addr) => addr,
        Err(e) => {
            eprintln!("❌ Ошибка парсинга адреса сервера: {}", e);
            std::process::exit(1);
        }
    };
    
    println!("\n✅ VectorDB сервер готов к работе");
    println!("🌐 Адрес сервера: http://{}", addr);
    if is_coordinator {
        println!("📖 Swagger UI: http://{}/swagger-ui", addr);
        println!("📄 OpenAPI спецификация: http://{}/api-docs/openapi.json", addr);
    }
    println!("🔍 Health check: http://{}/health", addr);
    println!("\n🛑 Для остановки сервера отправьте POST запрос на /stop");
    println!("═══════════════════════════════════════════════════════\n");
    
    // Запускаем HTTP сервер через ConnectionController (блокирует выполнение до остановки)
    match connection_controller.start_server(db, addr).await {
        Ok(returned_db) => {
            println!("\n🛑 Получен сигнал остановки сервера");
            println!("💾 Сохранение всех коллекций на диск...");
            
            // Выполняем dump через возвращенный VectorDB
            returned_db.dump().await;
            
            println!("✅ Все коллекции успешно сохранены!");
            println!("👋 Завершение работы...");
        }
        Err(e) => {
            eprintln!("\n❌ Ошибка запуска сервера: {:?}", e);
            std::process::exit(1);
        }
    }
}