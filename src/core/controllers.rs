use std::{collections::HashMap, result::Result};
use axum::{routing::{get, post}, Router, extract::State, Json};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tokio::net::TcpListener;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::core::{objects::{Collection, Vector, Bucket}, interfaces::{CollectionObjectController, Object}, embeddings::{find_most_similar}, lsh::{LSH, LSHMetric}};
use std::fs;
use std::path::Path;
use std::io::ErrorKind;
use chrono::Utc;

// structs define

pub struct StorageController {
    configs: HashMap<String, String>,
}

pub struct ConnectionController {
    // storage_controller: StorageController,
    configs: HashMap<String, String>,
}

pub struct CollectionController {
    storage_controller: StorageController,
    collections: Option<Vec<Collection>>,
}

#[derive(Debug, Clone)]
pub struct VectorController {
    pub vectors: Option<Vec<Vector>>,
}

#[derive(Debug)]
pub struct BucketController {
    pub buckets: Option<Vec<Bucket>>,
    pub lsh: Option<LSH>,
    pub dimension: Option<usize>,
}

// Impl block

//  StorageController impl

impl StorageController {
    /// Создаёт новый контроллер хранилища, инициализирует папку storage, если её нет
    pub fn new(configs: HashMap<String, String>) -> StorageController {
        fs::create_dir_all(format!("{}/storage", configs.get(&"path".to_string()).unwrap_or(&".".to_string())))
            .expect("Не удалось создать папку storage");
        StorageController { configs }
    }

    /// Универсальный метод для сохранения данных в файл
    fn save_to_file<P: AsRef<Path>>(&self, dir_path: P, file_name: u64, raw_data: Vec<u8>) -> Result<(), std::io::Error> {
        fs::create_dir_all(&dir_path)?;
        let file_path = dir_path.as_ref().join(format!("{}.bin", file_name));
        fs::write(file_path, raw_data)
    }

    /// Сохраняет сырые данные коллекции по hash_id
    pub fn save_collection(&self, collection_name: String, raw_data: Vec<u8>, hash_id: u64) -> Result<(), std::io::Error> {
        self.save_to_file(format!("{}/storage/{}", self.configs.get(&"path".to_string()).unwrap_or(&".".to_string()), collection_name), hash_id, raw_data)
    }

    /// Сохраняет сырые данные вектора по hash_id
    pub fn save_vector(&self, collection_name: String, raw_data: Vec<u8>, hash_id: u64) -> Result<(), std::io::Error> {
        self.save_to_file(format!("{}/storage/{}/vectors", self.configs.get(&"path".to_string()).unwrap_or(&".".to_string()), collection_name), hash_id, raw_data)
    }

    /// Сохраняет сырые данные бакета в папку бакета по пути /storage/collection_name/bucket_name/bucket.bin
    pub fn save_bucket(&self, collection_name: String, bucket_name: String, raw_data: Vec<u8>) -> Result<(), std::io::Error> {
        self.save_to_file(format!("{}/storage/{}/{}", self.configs.get(&"path".to_string()).unwrap_or(&".".to_string()), collection_name, bucket_name), 0, raw_data) // Используем 0 как имя файла bucket.bin
    }

    /// Сохраняет вектор в папку бакета по пути /storage/collection_name/bucket_name/vectors/vector_name.bin
    pub fn save_vector_to_bucket(&self, collection_name: String, bucket_name: String, vector_id: u64, raw_data: Vec<u8>) -> Result<(), std::io::Error> {
        self.save_to_file(format!("{}/storage/{}/{}/vectors", self.configs.get(&"path".to_string()).unwrap_or(&".".to_string()), collection_name, bucket_name), vector_id, raw_data)
    }

    /// Загружает вектор из папки бакета
    pub fn read_vector_from_bucket(&self, collection_name: String, bucket_name: String, vector_id: u64) -> Option<Vec<u8>> {
        let vector_path_bin = format!("{}/storage/{}/{}/vectors/{}.bin", self.configs.get(&"path".to_string()).unwrap_or(&".".to_string()), collection_name, bucket_name, vector_id);
        match fs::read(&vector_path_bin) {
            Ok(data) => Some(data),
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    None
                } else {
                    panic!("Ошибка чтения файла вектора из бакета: {:?}", e);
                }
            }
        }
    }

    /// Возвращает список имён всех коллекций (папок) в storage
    pub fn get_all_collections_name(&self) -> Vec<String> {
        let storage_path = format!("{}/storage", self.configs.get(&"path".to_string()).unwrap_or(&".".to_string()));
        let path = Path::new(&storage_path);
        match fs::read_dir(path) {
            Ok(entries) => entries.filter_map(|entry| {
                if let Ok(e) = entry {
                    let entry_path = e.path();
                    if entry_path.is_dir() {
                        if let Some(fname) = entry_path.file_name() {
                            if let Some(name) = fname.to_str() {
                                return Some(name.to_string());
                            }
                        }
                    }
                }
                None
            }).collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Читает сырые данные коллекции (первый найденный файл в папке коллекции)
    pub fn read_collection(&self, collection_name: String) -> Option<Vec<u8>> {
        let col_path = format!("{}/storage/{}", self.configs.get(&"path".to_string()).unwrap_or(&".".to_string()), collection_name);
        let path = Path::new(&col_path);

        match fs::read_dir(path) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    let entry_path = entry.path();
                    if entry_path.is_file() {
                        if let Ok(data) = fs::read(&entry_path) {
                            return Some(data);
                        }
                    }
                }
                None
            }
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    None
                } else {
                    panic!("Ошибка чтения директории: {:?}", e);
                }
            }
        }
    }

    /// Читает все векторы (файлы) из папки vectors коллекции и возвращает их содержимое в виде HashMap, где ключ — hash (u64), значение — Vec<u8>
    pub fn read_all_vector(&self, collection_name: String) -> HashMap<u64, Vec<u8>> {
        let vector_path = format!("{}/storage/{}/vectors", self.configs.get(&"path".to_string()).unwrap_or(&".".to_string()), collection_name);
        let path = Path::new(&vector_path);
        let mut result = HashMap::new();

        match fs::read_dir(path) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    let entry_path = entry.path();
                    if entry_path.is_file() {
                        if let Some(file_name) = entry_path.file_name().and_then(|n| n.to_str()) {
                            // Извлекаем hash из имени файла (например, "123456.bin" -> 123456)
                            let hash_str = file_name.strip_suffix(".bin").unwrap_or(file_name);
                            if let Ok(hash) = hash_str.parse::<u64>() {
                                if let Ok(data) = fs::read(&entry_path) {
                                    result.insert(hash, data);
                                }
                            }
                        }
                    }
                }
                result
            }
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    result
                } else {
                    panic!("Ошибка чтения директории: {:?}", e);
                }
            }
        }
    }

    /// Возвращает вектор хэшей (u64) файлов векторов по названию коллекции (имя файла соответствует хэшу)
    pub fn get_all_vectors_names(&self, collection_name: String) -> Vec<u64> {
        let vector_path = format!("{}/storage/{}/vectors", self.configs.get(&"path".to_string()).unwrap_or(&".".to_string()), collection_name);
        let path = Path::new(&vector_path);

        match fs::read_dir(path) {
            Ok(entries) => entries.filter_map(|entry| {
                entry.ok().and_then(|e| {
                    let entry_path = e.path();
                    if entry_path.is_file() {
                        entry_path.file_name()
                            .and_then(|n| n.to_str())
                            .and_then(|s| s.strip_suffix(".bin").or(Some(s)))
                            .and_then(|s| s.parse::<u64>().ok())
                    } else {
                        None
                    }
                })
            }).collect(),
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    Vec::new()
                } else {
                    panic!("Ошибка чтения директории: {:?}", e);
                }
            }
        }
    }

    /// Читает конкретный вектор по имени коллекции и имени (или хэшу) вектора
    pub fn read_vector(&self, collection_name: String, vector_hash: u64) -> Option<Vec<u8>> {
        let vector_path_bin = format!("{}/storage/{}/vectors/{}.bin", self.configs.get(&"path".to_string()).unwrap_or(&".".to_string()), collection_name, vector_hash);
        match fs::read(&vector_path_bin) {
            Ok(data) => Some(data),
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    None
                } else {
                    panic!("Ошибка чтения файла вектора: {:?}", e);
                }
            }
        }
    }

    /// Читает все файлы метадаты из папки metadata внутри коллекции и возвращает их содержимое в виде HashMap<u64, Vec<u8>>, где ключ - hash (имя файла без расширения)
    pub fn read_all_metadata(&self, collection_name: String) -> HashMap<u64, Vec<u8>> {
        let metadata_path = format!("{}/storage/{}/metadata", self.configs.get(&"path".to_string()).unwrap_or(&".".to_string()), collection_name);
        let path = Path::new(&metadata_path);

        match fs::read_dir(path) {
            Ok(entries) => {
                let mut result = HashMap::new();
                for entry in entries.flatten() {
                    let entry_path = entry.path();
                    if entry_path.is_file() {
                        if let Some(file_name) = entry_path.file_name().and_then(|n| n.to_str()) {
                            // Получаем hash из имени файла (без .bin)
                            let hash_str = file_name.strip_suffix(".bin").unwrap_or(file_name);
                            if let Ok(hash) = hash_str.parse::<u64>() {
                                if let Ok(data) = fs::read(&entry_path) {
                                    result.insert(hash, data);
                                }
                            }
                        }
                    }
                }
                result
            },
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    HashMap::new()
                } else {
                    panic!("Ошибка чтения директории метадаты: {:?}", e);
                }
            }
        }
    }

    /// Возвращает вектор имён файлов метадаты по названию коллекции (без расширения .bin) в виде Vec<u64>
    pub fn get_all_metadata_names(&self, collection_name: String) -> Vec<u64> {
        let metadata_path = format!("{}/storage/{}/metadata", self.configs.get(&"path".to_string()).unwrap_or(&".".to_string()), collection_name);
        let path = Path::new(&metadata_path);

        match fs::read_dir(path) {
            Ok(entries) => entries.filter_map(|entry| {
                entry.ok().and_then(|e| {
                    let entry_path = e.path();
                    if entry_path.is_file() {
                        entry_path.file_name()
                            .and_then(|n| n.to_str())
                            .and_then(|s| s.strip_suffix(".bin").or(Some(s)))
                            .and_then(|s| s.parse::<u64>().ok())
                    } else {
                        None
                    }
                })
            }).collect(),
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    Vec::new()
                } else {
                    panic!("Ошибка чтения директории метадаты: {:?}", e);
                }
            }
        }
    }

    /// Читает конкретный файл метадаты по имени коллекции и имени файла метадаты (без расширения)
    pub fn read_metadata(&self, collection_name: String, metadata_hash: u64) -> Option<Vec<u8>> {
        let metadata_path_bin = format!("{}/storage/{}/metadata/{}.bin", self.configs.get(&"path".to_string()).unwrap_or(&".".to_string()), collection_name, metadata_hash);
        match fs::read(&metadata_path_bin) {
            Ok(data) => Some(data),
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    None
                } else {
                    panic!("Ошибка чтения файла метадаты: {:?}", e);
                }
            }
        }
    }

    /// Читает все бакеты (файлы) из папки buckets коллекции и возвращает их содержимое в виде HashMap, где ключ — hash (u64), значение — Vec<u8>
    pub fn read_all_buckets(&self, collection_name: String) -> HashMap<String, Vec<u8>> {
        let collection_path = format!("{}/storage/{}", self.configs.get(&"path".to_string()).unwrap_or(&".".to_string()), collection_name);
        let path = Path::new(&collection_path);
        let mut result = HashMap::new();

        match fs::read_dir(path) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    let entry_path = entry.path();
                    if entry_path.is_dir() {
                        if let Some(bucket_name) = entry_path.file_name().and_then(|n| n.to_str()) {
                            if bucket_name == "vectors" {
                                continue;
                            }
                            
                            let bucket_file_path = entry_path.join("0.bin");
                            if let Ok(data) = fs::read(&bucket_file_path) {
                                result.insert(bucket_name.to_string(), data);
                            }
                        }
                    }
                }
                result
            }
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    result
                } else {
                    panic!("Ошибка чтения директории коллекции: {:?}", e);
                }
            }
        }
    }

    /// Возвращает вектор ID бакетов (String) по названию коллекции
    pub fn get_all_buckets_names(&self, collection_name: String) -> Vec<String> {
        let collection_path = format!("{}/storage/{}", self.configs.get(&"path".to_string()).unwrap_or(&".".to_string()), collection_name);
        let path = Path::new(&collection_path);

        match fs::read_dir(path) {
            Ok(entries) => entries.filter_map(|entry| {
                entry.ok().and_then(|e| {
                    let entry_path = e.path();
                    if entry_path.is_dir() {
                        if let Some(bucket_name) = entry_path.file_name().and_then(|n| n.to_str()) {
                            // Пропускаем папку vectors, если она есть на верхнем уровне
                            if bucket_name == "vectors" {
                                None
                            } else {
                                // Проверяем, что в папке есть файл 0.bin (bucket.bin)
                                let bucket_file_path = entry_path.join("0.bin");
                                if bucket_file_path.exists() {
                                    // Проверяем, что имя папки является числом (ID бакета)
                                    if bucket_name.parse::<u64>().is_ok() {
                                        Some(bucket_name.to_string())
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            }
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
            }).collect(),
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    Vec::new()
                } else {
                    panic!("Ошибка чтения директории коллекции: {:?}", e);
                }
            }
        }
    }

    /// Читает конкретный бакет по имени коллекции и имени (или хэшу) бакета
    pub fn read_bucket(&self, collection_name: String, bucket_name: String) -> Option<Vec<u8>> {
        let bucket_path_bin = format!("{}/storage/{}/{}/0.bin", self.configs.get(&"path".to_string()).unwrap_or(&".".to_string()), collection_name, bucket_name);
        match fs::read(&bucket_path_bin) {
            Ok(data) => Some(data),
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    None
                } else {
                    panic!("Ошибка чтения файла бакета: {:?}", e);
                }
            }
        }
    }
}

//  ConnectionController impl

impl ConnectionController {
    /// Создаёт новый ConnectionController с заданным StorageController и ConfigLoader
    pub fn new(configs: HashMap<String, String>) -> ConnectionController {
        ConnectionController { configs: configs }
    }

    /// Запускает HTTP RPC-сервер на указанном адресе. Нужен общий доступ к CollectionController.
    pub async fn connection_handler(&mut self, controller: Arc<RwLock<CollectionController>>, addr: SocketAddr) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let app_state = AppState { controller, configs: self.configs.clone() };

        let app = Router::new()
            .route("/health", get(health))
            .route("/query", post(rpc_query))
            .with_state(app_state);

        let listener = TcpListener::bind(addr).await?;
        axum::serve(listener, app).await?;
        Ok(())
    }

    /// Синхронный вызов для локного использования без HTTP
    pub fn query_handler(&self) -> Result<(), &'static str> { Ok(()) }
}

#[derive(Clone)]
struct AppState {
    controller: Arc<RwLock<CollectionController>>,
    configs: HashMap<String, String>,
}

#[derive(Deserialize)]
struct RpcQuery {
    action: String,
    payload: Option<serde_json::Value>,
}

#[derive(Serialize)]
struct RpcResponse<T> {
    status: String,
    data: Option<T>,
    message: Option<String>,
}

async fn health() -> Json<RpcResponse<String>> {
    Json(RpcResponse { status: "ok".to_string(), data: Some("healthy".to_string()), message: None })
}

#[derive(Deserialize)]
struct AddCollectionParams {
    name: String,
    metric: String,
    dimension: usize,
}

#[derive(Deserialize)]
struct AddVectorParams {
    collection: String,
    embedding: Vec<f32>,
    metadata: Option<HashMap<String, String>>,
}

async fn rpc_query(State(state): State<AppState>, Json(req): Json<RpcQuery>) -> Json<RpcResponse<serde_json::Value>> {
    match req.action.as_str() {
        "add_collection" => {
            let parsed: Result<AddCollectionParams, _> = serde_json::from_value(req.payload.unwrap_or_default());
            match parsed {
                Ok(p) => {
                    let metric = LSHMetric::from_string(&p.metric).unwrap_or(LSHMetric::Euclidean);
                    let mut ctrl = state.controller.write().await;
                    match ctrl.add_collection(p.name, metric, p.dimension) {
                        Ok(_) => Json(RpcResponse { status: "ok".to_string(), data: Some(serde_json::json!({"added": true})), message: None }),
                        Err(e) => Json(RpcResponse { status: "error".to_string(), data: None, message: Some(e.to_string()) }),
                    }
                }
                Err(e) => Json(RpcResponse { status: "error".to_string(), data: None, message: Some(format!("bad payload: {}", e)) }),
            }
        }
        "add_vector" => {
            let parsed: Result<AddVectorParams, _> = serde_json::from_value(req.payload.unwrap_or_default());
            match parsed {
                Ok(p) => {
                    let mut ctrl = state.controller.write().await;
                    match ctrl.add_vector(&p.collection, p.embedding, p.metadata.unwrap_or_default()) {
                        Ok(id) => Json(RpcResponse { status: "ok".to_string(), data: Some(serde_json::json!({"id": id})), message: None }),
                        Err(e) => Json(RpcResponse { status: "error".to_string(), data: None, message: Some(e.to_string()) }),
                    }
                }
                Err(e) => Json(RpcResponse { status: "error".to_string(), data: None, message: Some(format!("bad payload: {}", e)) }),
            }
        }
        "dump" => {
            let ctrl = state.controller.read().await;
            ctrl.dump();
            Json(RpcResponse { status: "ok".to_string(), data: Some(serde_json::json!({"dumped": true})), message: None })
        }
        "load" => {
            let mut ctrl = state.controller.write().await;
            ctrl.load();
            Json(RpcResponse { status: "ok".to_string(), data: Some(serde_json::json!({"loaded": true})), message: None })
        }
        other => {
            Json(RpcResponse { status: "error".to_string(), data: None, message: Some(format!("unknown action: {}", other)) })
        }
    }
}

//  CollectionController impl

impl CollectionController {
    /// Создаёт новый CollectionController с заданным StorageController
    pub fn new(storage_controller: StorageController) -> CollectionController {
        CollectionController { storage_controller, collections: None }
    }

    /// Добавляет новую коллекцию с указанным именем
    pub fn add_collection(&mut self, name: String, lsh_metric: LSHMetric, vector_dimension: usize) -> Result<(), &'static str> {
        let collections = self.collections.get_or_insert_with(Vec::new);
        collections.push(Collection::new(Some(name), lsh_metric, vector_dimension));
        Ok(())
    }

    /// Удаляет коллекцию по имени
    pub fn delete_collection(&mut self, name: String) -> Result<(), &'static str> {
        match self.collections.as_mut() {
            Some(collections) => {
                if let Some(pos) = collections.iter().position(|c| c.name == name) {
                    collections.remove(pos);
                    Ok(())
                } else {
                    Err("Коллекция с таким именем не найдена")
                }
            }
            None => Err("Коллекции не инициализированы"),
        }
    }

    /// Получает ссылку на коллекцию по имени
    pub fn get_collection(&self, name: &str) -> Option<&Collection> {
        self.collections.as_ref()?.iter().find(|c| c.name == name)
    }

    /// Получает мутабельную ссылку на коллекцию по имени
    pub fn get_collection_mut(&mut self, name: &str) -> Option<&mut Collection> {
        self.collections.as_mut()?.iter_mut().find(|c| c.name == name)
    }

    /// Добавляет вектор в коллекцию по имени коллекции
    pub fn add_vector(
        &mut self,
        collection_name: &str,
        embedding: Vec<f32>,
        metadata: HashMap<String, String>,
    ) -> Result<u64, &'static str> {
        // Проверяем, инициализированы ли коллекции
        let collections = match self.collections.as_mut() {
            Some(c) => c,
            None => return Err("Коллекции не инициализированы"),
        };

        // Ищем коллекцию по имени
        let collection = match collections.iter_mut().find(|col| col.name == collection_name) {
            Some(col) => col,
            None => return Err("Коллекция с указанным именем не найдена"),
        };

        // Проверяем размерность вектора
        if embedding.len() != collection.vector_dimension {
            return Err("Размерность вектора не соответствует размерности коллекции");
        }

        match collection.buckets_controller.add_vector(embedding, metadata) {
            Ok(id) => Ok(id),
            Err(_) => Err("Ошибка при добавлении вектора в LSH бакет"),
        }
    }

    /// Сохраняет одну коллекцию и все её векторы и метаданные
    pub fn dump_one(&self, collection: &Collection) {
        let collection_name = &collection.name;
        match collection.dump() {
            Ok((raw_data, hash_id)) => {
                if let Err(e) = self.storage_controller.save_collection(collection_name.clone(), raw_data, hash_id) {
                    eprintln!("Ошибка сохранения коллекции '{}': {:?}", collection_name, e);
                    return;
                }
                println!("Коллекция '{}' успешно сохранена (hash_id: {}).", collection_name, hash_id);
            }
            Err(_) => {
                eprintln!("Ошибка сериализации коллекции '{}'.", collection_name);
                return;
            }
        }

        // Сохраняем бакеты
        if let Some(ref buckets) = collection.buckets_controller.buckets {
            for bucket in buckets {
                match bucket.dump() {
                    Ok((bucket_raw_data, _hash_id)) => {
                        match self.storage_controller.save_bucket(collection_name.clone(), bucket.id.to_string(), bucket_raw_data) {
                            Ok(_) => println!("Бакет {} успешно сохранён в коллекции '{}'.", bucket.id, collection_name),
                            Err(e) => eprintln!("Ошибка сохранения бакета {} в коллекции '{}': {:?}", bucket.id, collection_name, e),
                        }
                    }
                    Err(_) => {
                        eprintln!("Ошибка сериализации бакета {}.", bucket.id);
                    }
                }
            }
        }

        // Сохраняем векторы в соответствующие бакеты
        for (bucket_id, vector_id, vector_raw_data) in collection.buckets_controller.dump_vectors() {
            match self.storage_controller.save_vector_to_bucket(collection_name.clone(), bucket_id.to_string(), vector_id, vector_raw_data) {
                Ok(_) => println!("Вектор с ID {} успешно сохранён в бакете {} коллекции '{}'.", vector_id, bucket_id, collection_name),
                Err(e) => eprintln!("Ошибка сохранения вектора с ID {} в бакете {} коллекции '{}': {:?}", vector_id, bucket_id, collection_name, e),
            }
        }
    }

    /// Сохраняет все коллекции
    pub fn dump(&self) {
        match &self.collections {
            Some(collections) if !collections.is_empty() => {
                for collection in collections {
                    self.dump_one(collection);
                }
            }
            _ => println!("Нет коллекций для сохранения."),
        }
    }

    /// Загружает одну коллекцию по имени из storage
    pub fn load_one(&mut self, name: String) {
        if let Some(raw_collection) = self.storage_controller.read_collection(name.clone()) {
            let mut collection = Collection::new(None, LSHMetric::Euclidean, 384); // Временные значения, будут загружены из файла
            collection.load(raw_collection);

            // Загружаем бакеты
            let raw_buckets = self.storage_controller.read_all_buckets(name.clone());
            // Конвертируем HashMap<String, Vec<u8>> в HashMap<u64, Vec<u8>> для совместимости
            let mut buckets_data: HashMap<u64, Vec<u8>> = HashMap::new();
            for (bucket_name, data) in raw_buckets {
                // Парсим ID бакета из имени
                if let Ok(bucket_id) = bucket_name.parse::<u64>() {
                    buckets_data.insert(bucket_id, data);
                }
            }
            collection.buckets_controller.load(buckets_data);

            // Загружаем векторы из бакетов
            collection.buckets_controller.load_vectors_from_buckets(&self.storage_controller, name.clone());

            match &mut self.collections {
                Some(collections) => {
                    collections.push(collection);
                }
                None => {
                    self.collections = Some(vec![collection]);
                }
            }
        }
    }

    /// Загружает все коллекции из storage
    pub fn load(&mut self) {
        let collection_names = self.storage_controller.get_all_collections_name();
        let mut count = 0;

        for name in collection_names {
            let before = self.collections.as_ref().map(|c| c.len()).unwrap_or(0);
            self.load_one(name);
            let after = self.collections.as_ref().map(|c| c.len()).unwrap_or(0);
            if after > before {
                count += 1;
            }
        }

        if count > 0 {
            println!("Загружено {} коллекций.", count);
        } else {
            println!("Коллекции не найдены в storage.");
        }
    }

    /// Получает бакет по ID
    pub fn get_bucket(&self, collection_name: &str, bucket_id: u64) -> Option<&Bucket> {
        let collection = self.get_collection(collection_name)?;
        collection.buckets_controller.get_bucket(bucket_id)
    }

    /// Получает все бакеты в коллекции
    pub fn get_all_buckets(&self, collection_name: &str) -> Option<Vec<&Bucket>> {
        let collection = self.get_collection(collection_name)?;
        Some(collection.buckets_controller.get_all_buckets())
    }

    /// Обновляет вектор в коллекции, при необходимости перемещая его в другой бакет
    pub fn update_vector(
        &mut self,
        collection_name: &str,
        vector_id: u64,
        new_embedding: Option<Vec<f32>>,
        new_metadata: Option<HashMap<String, String>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let collection = self.get_collection_mut(collection_name)
            .ok_or_else(|| format!("Коллекция '{}' не найдена", collection_name))?;
        
        // Проверяем размерность нового вектора, если он предоставлен
        if let Some(ref embedding) = new_embedding {
            if embedding.len() != collection.vector_dimension {
                return Err(format!("Размерность вектора {} не соответствует размерности коллекции {}", 
                    embedding.len(), collection.vector_dimension).into());
            }
        }
        
        collection.buckets_controller.update_vector(vector_id, new_embedding, new_metadata)
    }

    pub fn find_similar(
        &self, 
        collection_name: String, 
        query: &Vec<f32>, 
        k: usize
    ) -> Result<Vec<(u64, usize, f32)>, Box<dyn std::error::Error>> {
        let collection = self.get_collection(&collection_name);
        match collection {
            Some(current) => {
                // Проверяем размерность запроса
                current.find_similar(query, k)
            }
            None => Err(format!("Коллекция '{}' не найдена", collection_name).into())
        }
    }
}

//  VectorController impl

impl VectorController {
    pub fn new() -> Self {
        VectorController { vectors: None }
    }

    /// добавляет объект вектора к базе
    /// 
    /// Параметры:
    /// - embedding: вектор эмбеддинга (обязательный для создания нового вектора)
    /// - metadata: метаданные вектора (обязательные для создания нового вектора)
    /// - vector_id: ID вектора (опциональный, если None - создается автоматически)
    /// - vector: готовый объект вектора (опциональный, если Some - используется вместо создания нового)
    /// 
    /// Примеры использования:
    /// - add_vector(Some(embedding), Some(metadata), None, None) - создать новый вектор
    /// - add_vector(None, None, Some(id), Some(vector)) - добавить готовый вектор
    /// - add_vector(Some(embedding), Some(metadata), Some(id), None) - создать вектор с заданным ID
    pub fn add_vector(
        &mut self,
        embedding: Option<Vec<f32>>,
        metadata: Option<HashMap<String, String>>,
        vector_id: Option<u64>,
        vector: Option<Vector>,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let final_vector = if let Some(v) = vector {
            // Используем готовый объект вектора
            v
        } else {
            // Создаем новый вектор
            let timestamp = Utc::now().timestamp();
            let mut new_vector = Vector::new(embedding, Some(timestamp), metadata);
            
            // Устанавливаем ID если указан
            if let Some(id) = vector_id {
                new_vector.set_hash_id(id);
            }
            
            new_vector
        };
        
        let id = final_vector.hash_id();
        match &mut self.vectors {
            Some(vecs) => vecs.push(final_vector),
            None => self.vectors = Some(vec![final_vector]),
        }
        Ok(id)
    }

    /// Удаляет вектор по id
    pub fn remove_vector(&mut self, id: u64) -> Result<(), String> {
        if let Some(ref mut vectors) = self.vectors {
            if let Some(pos) = vectors.iter().position(|v| v.hash_id() == id) {
                vectors.remove(pos);
                Ok(())
            } else {
                Err(format!("Вектор с id {} не найден.", id))
            }
        } else {
            Err("Список векторов пуст.".to_string())
        }
    }

    /// Удаляет вектор по id и возвращает его
    pub fn remove_and_get_vector(&mut self, id: u64) -> Result<Vector, String> {
        if let Some(ref mut vectors) = self.vectors {
            if let Some(pos) = vectors.iter().position(|v| v.hash_id() == id) {
                Ok(vectors.remove(pos))
            } else {
                Err(format!("Вектор с id {} не найден.", id))
            }
        } else {
            Err("Список векторов пуст.".to_string())
        }
    }
    
    /// Обновляет эмбеддинг и метаданные по id
    pub fn update_vector(
        &mut self,
        id: u64,
        new_embedding: Option<Vec<f32>>,
        new_metadata: Option<HashMap<String, String>>,
    ) -> Result<(), String> {
        if let Some(ref mut vectors) = self.vectors {
            if let Some(v) = vectors.iter_mut().find(|v| v.hash_id() == id) {
                if let Some(embedding) = new_embedding {
                    v.data = embedding;
                }
                if let Some(metadata) = new_metadata {
                    v.metadata = metadata;
                }
                return Ok(());
            }
        }
        Err(format!("Вектор с id {} не найден.", id))
    }

    /// Добавляет метаданные к вектору по ID (объединяет с существующими)
    pub fn add_metadata_to_vector(&mut self, id: u64, new_metadata: HashMap<String, String>) -> Result<(), String> {
        if let Some(ref mut vectors) = self.vectors {
            if let Some(v) = vectors.iter_mut().find(|v| v.hash_id() == id) {
                v.metadata.extend(new_metadata);
                Ok(())
            } else {
                Err(format!("Вектор с id {} не найден.", id))
            }
        } else {
            Err("Список векторов пуст.".to_string())
        }
    }

    /// Удаляет метаданные по ключу у вектора по ID
    pub fn remove_metadata_from_vector(&mut self, id: u64, key: &str) -> Result<(), String> {
        if let Some(ref mut vectors) = self.vectors {
            if let Some(v) = vectors.iter_mut().find(|v| v.hash_id() == id) {
                v.metadata.remove(key);
                Ok(())
            } else {
                Err(format!("Вектор с id {} не найден.", id))
            }
        } else {
            Err("Список векторов пуст.".to_string())
        }
    }

    /// поиск наиболее похожего вектора
    pub fn find_most_similar(&self, query: &Vec<f32>, k: usize) -> Result<Vec<(usize, f32)>, Box<dyn std::error::Error>> {
        match &self.vectors {
            Some(vectors) => find_most_similar(query, vectors, k),
            None => Err("Список векторов пуст.".into()),
        }
    }

    /// Получение вектора по порядковому индексу
    pub fn get_vector(&self, index: usize) -> Option<&Vector> {
        match &self.vectors {
            Some(vectors) => vectors.get(index),
            None => None,
        }
    }

    /// Получение вектора по hash_id (u64)
    pub fn get_vector_by_id(&self, id: u64) -> Option<&Vector> {
        match &self.vectors {
            Some(vectors) => vectors.iter().find(|v| v.hash_id() == id),
            None => None,
        }
    }

    // фильтрация по метаданным
    pub fn filter_by_metadata(&self, filters: &HashMap<String, String>) -> Vec<u64> {
        let mut result = Vec::new();
        if let Some(ref vectors) = self.vectors {
            for vector in vectors {
                let mut matches = true;
                for (key, value) in filters {
                    if let Some(v) = vector.metadata.get(key) {
                        if v != value {
                            matches = false;
                            break;
                        }
                    } else {
                        matches = false;
                        break;
                    }
                }
                if matches {
                    result.push(vector.hash_id());
                }
            }
        }
        result
    }
}

impl CollectionObjectController for VectorController {
    /// Загружает векторы из HashMap<u64, Vec<u8>> (hash_id -> данные)
    fn load(&mut self, raw_data: HashMap<u64, Vec<u8>>) {
        let mut vectors = Vec::new();
        for (hash_id, data) in raw_data {
            let mut vector = Vector::new(None, None, None);
            vector.load(data);
            vector.set_hash_id(hash_id);
            vectors.push(vector);
        }
        self.vectors = Some(vectors);
    }

    /// Сохраняет векторы в HashMap<u64, Vec<u8>> (hash_id -> данные)
    fn dump(&self) -> HashMap<u64, Vec<u8>> {
        let mut ready_storage_data: HashMap<u64, Vec<u8>> = HashMap::new();
        if let Some(ref vectors) = self.vectors {
            for vector in vectors {
                match vector.dump() {
                    Ok((raw_vector, hash_id)) => {
                        ready_storage_data.insert(hash_id, raw_vector);
                    }
                    Err(_) => {
                        eprintln!("Ошибка сериализации вектора.");
                    }
                }
            }
        }

        ready_storage_data
    }
}

//  BucketController impl

impl BucketController {

    /// Создаёт новый BucketController с LSH для автоматического создания бакетов
    pub fn new(dimension: usize, num_hashes: usize, bucket_width: f32, metric: LSHMetric, seed: Option<u64>) -> Self {
        let lsh = LSH::new(dimension, num_hashes, bucket_width, metric, seed);
        BucketController {
            buckets: None,
            lsh: Some(lsh),
            dimension: Some(dimension),
        }
    }

    /// Получает бакет по ID
    pub fn get_bucket(&self, id: u64) -> Option<&Bucket> {
        match &self.buckets {
            Some(buckets) => buckets.iter().find(|b| b.id == id),
            None => None,
        }
    }

    /// Получает мутабельную ссылку на бакет по ID
    pub fn get_bucket_mut(&mut self, id: u64) -> Option<&mut Bucket> {
        match &mut self.buckets {
            Some(buckets) => buckets.iter_mut().find(|b| b.id == id),
            None => None,
        }
    }

    /// Добавляет вектор с автоматическим созданием бакета на основе LSH
    pub fn add_vector(
        &mut self,
        embedding: Vec<f32>,
        metadata: HashMap<String, String>,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let lsh = self.lsh.as_ref().ok_or("LSH не инициализирован. Используйте new для создания контроллера с LSH.")?;
        let dimension = self.dimension.ok_or("Размерность не установлена")?;

        if embedding.len() != dimension {
            return Err(format!("Размерность вектора {} не соответствует ожидаемой {}", embedding.len(), dimension).into());
        }

        let bucket_hash = lsh.hash(&embedding);

        let bucket = self.get_or_create_bucket(bucket_hash)?;

        bucket.add_vector(embedding, metadata)
    }

    /// Получает или создает бакет
    fn get_or_create_bucket(
        &mut self,
        bucket_id: u64,
    ) -> Result<&mut Bucket, Box<dyn std::error::Error>> {
        // Проверяем, существует ли бакет
        let bucket_exists = if let Some(ref buckets) = self.buckets {
            buckets.iter().any(|b| b.id == bucket_id)
        } else {
            false
        };

        if !bucket_exists {
            // Бакет не существует, создаем новый
            let bucket = Bucket::new(bucket_id);
            match &mut self.buckets {
                Some(buckets) => {
                    buckets.push(bucket);
                }
                None => {
                    self.buckets = Some(vec![bucket]);
                }
            }
        }

        // Теперь возвращаем ссылку на бакет
        if let Some(ref mut buckets) = self.buckets {
            Ok(buckets.iter_mut().find(|b| b.id == bucket_id).unwrap())
        } else {
            unreachable!()
        }
    }

    /// Получает все бакеты
    pub fn get_all_buckets(&self) -> Vec<&Bucket> {
        match &self.buckets {
            Some(buckets) => buckets.iter().collect(),
            None => Vec::new(),
        }
    }

    /// Получает количество бакетов
    pub fn count(&self) -> usize {
        match &self.buckets {
            Some(buckets) => buckets.len(),
            None => 0,
        }
    }

    /// Поиск похожих векторов с использованием LSH
    pub fn find_similar(
        &self,
        query: &Vec<f32>,
        k: usize,
    ) -> Result<Vec<(u64, usize, f32)>, Box<dyn std::error::Error>> {
        let lsh = self.lsh.as_ref().ok_or("LSH не инициализирован")?;
        let dimension = self.dimension.ok_or("Размерность не установлена")?;

        if query.len() != dimension {
            return Err(format!("Размерность вектора {} не соответствует ожидаемой {}", query.len(), dimension).into());
        }

        let query_hash = lsh.hash(query);
        
        if let Some(ref buckets) = self.buckets {
            if let Some(bucket) = buckets.iter().find(|b| b.hash_id() == query_hash) {
                let results = bucket.find_similar(query, k)?;
                return Ok(results.into_iter().map(|(idx, score)| (bucket.hash_id(), idx, score)).collect());
            }
        }

        Ok(Vec::new())
    }

    /// Поиск похожих векторов в нескольких бакетах
    pub fn find_similar_multi_bucket(
        &self,
        query: &Vec<f32>,
        k: usize,
    ) -> Result<Vec<(u64, usize, f32)>, Box<dyn std::error::Error>> {
        let lsh = self.lsh.as_ref().ok_or("LSH не инициализирован")?;
        let dimension = self.dimension.ok_or("Размерность не установлена")?;

        if query.len() != dimension {
            return Err(format!("Размерность вектора {} не соответствует ожидаемой {}", query.len(), dimension).into());
        }

        let mut all_results = Vec::new();

        let query_hashes = lsh.multi_hash(query, 3);

        if let Some(ref buckets) = self.buckets {
            for hash in query_hashes {
                if let Some(bucket) = buckets.iter().find(|b| b.hash_id() == hash) {
                    let results = bucket.find_similar(query, k)?;
                    for (idx, score) in results {
                        all_results.push((bucket.hash_id(), idx, score));
                    }
                }
            }
        }

        all_results.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
        all_results.truncate(k);

        Ok(all_results)
    }

    /// Получает общее количество векторов во всех бакетах
    pub fn total_vectors(&self) -> usize {
        match &self.buckets {
            Some(buckets) => buckets.iter().map(|b| b.size()).sum(),
            None => 0,
        }
    }

    /// Удаляет вектор из соответствующего бакета
    pub fn remove_vector(&mut self, vector_id: u64) -> Result<(), String> {
        if let Some(ref mut buckets) = self.buckets {
            for (index, bucket) in buckets.iter_mut().enumerate() {
                if bucket.contains_vector(vector_id) {
                    let result = bucket.remove_vector(vector_id);
                    
                    // Если вектор успешно удален, проверяем, не стал ли бакет пустым
                    if result.is_ok() && bucket.size() == 0 {
                        buckets.remove(index);
                    }
                    
                    return result;
                }
            }
        }
        Err(format!("Вектор с id {} не найден ни в одном бакете", vector_id))
    }

    /// Получает вектор по ID из любого бакета
    pub fn get_vector(&self, vector_id: u64) -> Option<&Vector> {
        if let Some(ref buckets) = self.buckets {
            for bucket in buckets {
                if let Some(vector) = bucket.get_vector(vector_id) {
                    return Some(vector);
                }
            }
        }
        None
    }

    /// Фильтрация векторов по метаданным во всех бакетах
    pub fn filter_by_metadata(&self, filters: &HashMap<String, String>) -> Vec<u64> {
        let mut result = Vec::new();
        if let Some(ref buckets) = self.buckets {
            for bucket in buckets {
                result.extend(bucket.filter_by_metadata(filters));
            }
        }
        result
    }

    /// Получает статистику по бакетам
    pub fn get_statistics(&self) -> HashMap<String, String> {
        let mut stats = HashMap::new();
        stats.insert("total_buckets".to_string(), self.count().to_string());
        stats.insert("total_vectors".to_string(), self.total_vectors().to_string());
        
        if let Some(dimension) = self.dimension {
            stats.insert("dimension".to_string(), dimension.to_string());
        }
        
        if let Some(ref lsh) = self.lsh {
            stats.insert("num_hashes".to_string(), lsh.num_hashes.to_string());
            stats.insert("bucket_width".to_string(), lsh.bucket_width.to_string());
        }
        
        if let Some(ref buckets) = self.buckets {
            let avg_vectors = if buckets.is_empty() { 0.0 } else { self.total_vectors() as f32 / buckets.len() as f32 };
            stats.insert("avg_vectors_per_bucket".to_string(), format!("{:.2}", avg_vectors));
        }
        
        stats
    }

    /// Обновляет вектор, при необходимости перемещая его в другой бакет
    pub fn update_vector(
        &mut self,
        vector_id: u64,
        new_embedding: Option<Vec<f32>>,
        new_metadata: Option<HashMap<String, String>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let lsh = self.lsh.as_ref().ok_or("LSH не инициализирован")?;
        let dimension = self.dimension.ok_or("Размерность не установлена")?;

        // Находим вектор в текущем бакете и извлекаем его
        let mut vector_to_move: Option<Vector> = None;
        let mut source_bucket_id: Option<u64> = None;

        if let Some(ref mut buckets) = self.buckets {
            for bucket in buckets.iter_mut() {
                if let Some(vector) = bucket.get_vector(vector_id) {
                    // Создаем временную копию для проверки нового хэша
                    let mut temp_vector = vector.clone();
                    
                    // Обновляем данные временного вектора
                    if let Some(embedding) = new_embedding.clone() {
                        if embedding.len() != dimension {
                            return Err(format!("Размерность вектора {} не соответствует ожидаемой {}", embedding.len(), dimension).into());
                        }
                        temp_vector.data = embedding;
                    }
                    if let Some(metadata) = new_metadata.clone() {
                        temp_vector.metadata = metadata;
                    }

                    // Вычисляем новый LSH хэш
                    let new_bucket_id = lsh.hash(&temp_vector.data);
                    
                    // Если хэш изменился, нужно переместить вектор
                    if bucket.id != new_bucket_id {
                        // Извлекаем вектор из старого бакета
                        vector_to_move = Some(bucket.remove_and_get_vector(vector_id)?);
                        source_bucket_id = Some(bucket.id);
                        
                        // Обновляем данные извлеченного вектора
                        if let Some(ref mut vector) = vector_to_move {
                            if let Some(embedding) = new_embedding {
                                vector.data = embedding;
                            }
                            if let Some(metadata) = new_metadata {
                                vector.metadata = metadata;
                            }
                        }
                    } else {
                        // Хэш не изменился, просто обновляем вектор в текущем бакете
                        return bucket.update_vector(vector_id, new_embedding, new_metadata);
                    }
                    break;
                }
            }
        }

        // Если нужно переместить вектор
        if let (Some(vector), Some(source_id)) = (vector_to_move, source_bucket_id) {
            // Добавляем вектор в новый бакет
            let new_bucket_id = lsh.hash(&vector.data);
            let target_bucket = self.get_or_create_bucket(new_bucket_id)?;
            
            // Добавляем вектор напрямую в новый бакет
            target_bucket.vectors_controller.add_vector(None, None, None, Some(vector))?;
            
            // Удаляем пустой бакет, если он остался без векторов
            self.remove_empty_bucket(source_id);
        }

        Ok(())
    }

    /// Удаляет пустой бакет по ID
    fn remove_empty_bucket(&mut self, bucket_id: u64) {
        if let Some(ref mut buckets) = self.buckets {
            if let Some(pos) = buckets.iter().position(|b| b.id == bucket_id) {
                let bucket = &buckets[pos];
                if bucket.size() == 0 {
                    buckets.remove(pos);
                }
            }
        }
    }

    /// Возвращает все векторы из всех бакетов для сохранения в файловую систему
    pub fn dump_vectors(&self) -> Vec<(u64, u64, Vec<u8>)> {
        let mut vectors_data = Vec::new();
        if let Some(ref buckets) = self.buckets {
            for bucket in buckets {
                if let Some(ref vectors) = bucket.vectors_controller.vectors {
                    for vector in vectors {
                        match vector.dump() {
                            Ok((raw_data, vector_id)) => {
                                vectors_data.push((bucket.id, vector_id, raw_data));
                            }
                            Err(_) => {
                                eprintln!("Ошибка сериализации вектора с ID {}.", vector.hash_id());
                            }
                        }
                    }
                }
            }
        }
        vectors_data
    }

    /// Загружает векторы из бакетов из файловой системы
    pub fn load_vectors_from_buckets(&mut self, storage_controller: &StorageController, collection_name: String) {
        if let Some(ref mut buckets) = self.buckets {
            for bucket in buckets.iter_mut() {
                if let Some(ref vectors) = bucket.vectors_controller.vectors {
                    for vector in vectors {
                        let vector_id = vector.hash_id();
                        if let Some(_raw_data) = storage_controller.read_vector_from_bucket(collection_name.clone(), bucket.id.to_string(), vector_id) {
                            println!("Вектор с ID {} загружен из бакета {} коллекции '{}'.", vector_id, bucket.id, collection_name);
                        }
                    }
                }
            }
        }
    }
}

impl CollectionObjectController for BucketController {
    /// Загружает бакеты из HashMap<u64, Vec<u8>> (bucket_id -> данные)
    fn load(&mut self, raw_data: HashMap<u64, Vec<u8>>) {
        let mut buckets = Vec::new();
        for (bucket_id, data) in raw_data {
            let mut bucket = Bucket::new(bucket_id);
            bucket.load(data);
            buckets.push(bucket);
        }
        self.buckets = Some(buckets);
    }

    /// Сохраняет бакеты в HashMap<u64, Vec<u8>> (hash_id -> данные)
    fn dump(&self) -> HashMap<u64, Vec<u8>> {
        let mut ready_storage_data: HashMap<u64, Vec<u8>> = HashMap::new();
        if let Some(ref buckets) = self.buckets {
            for bucket in buckets {
                match bucket.dump() {
                    Ok((raw_bucket, hash_id)) => {
                        ready_storage_data.insert(hash_id, raw_bucket);
                    }
                    Err(_) => {
                        eprintln!("Ошибка сериализации бакета.");
                    }
                }
            }
        }

        ready_storage_data
    }
}