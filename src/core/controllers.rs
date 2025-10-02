use std::{collections::HashMap, result::Result};
use crate::core::{config::ConfigLoader, objects::{Collection, Vector}, interfaces::{CollectionObjectController, Object}, embeddings::{find_most_similar}};
use std::fs;
use std::path::Path;
use std::io::ErrorKind;
use chrono::{Utc};

// structs define

pub struct StorageController {
    configs: HashMap<String, String>,
}

pub struct ConnectionController {
    storage_controller: StorageController,
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

impl VectorController {
    pub fn new() -> Self {
        VectorController { vectors: None }
    }

    /// добавляет объект вектора к базе
    pub fn add_vector(
        &mut self,
        embedding: Vec<f32>,
        metadata: HashMap<String, String>,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let timestamp = Utc::now().timestamp();
        let vector = Vector::new(Some(embedding), Some(timestamp), Some(metadata));
        let id = vector.hash_id();
        match &mut self.vectors {
            Some(vecs) => vecs.push(vector),
            None => self.vectors = Some(vec![vector]),
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


// Impl block

//  StorageController impl

impl StorageController {
    /// Создаёт новый контроллер хранилища, инициализирует папку storage, если её нет
    pub fn new(configs: HashMap<String, String>) -> StorageController {
        fs::create_dir_all("storage")
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
        self.save_to_file(format!("./storage/{}", collection_name), hash_id, raw_data)
    }

    /// Сохраняет сырые данные вектора по hash_id
    pub fn save_vector(&self, collection_name: String, raw_data: Vec<u8>, hash_id: u64) -> Result<(), std::io::Error> {
        self.save_to_file(format!("./storage/{}/vectors", collection_name), hash_id, raw_data)
    }

    /// Сохраняет сырые данные метадаты по hash_id
    pub fn save_metadata(&self, collection_name: String, raw_data: Vec<u8>, hash_id: u64) -> Result<(), std::io::Error> {
        self.save_to_file(format!("./storage/{}/metadata", collection_name), hash_id, raw_data)
    }

    /// Возвращает список имён всех коллекций (папок) в storage
    pub fn get_all_collections_name(&self) -> Vec<String> {
        let path = Path::new("./storage");
        match fs::read_dir(path) {
            Ok(entries) => entries.filter_map(|entry| {
                entry.ok().and_then(|e| {
                    let path = e.path();
                    if path.is_dir() {
                        path.file_name().and_then(|n| n.to_str().map(|s| s.to_string()))
                    } else {
                        None
                    }
                })
            }).collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Читает сырые данные коллекции (первый найденный файл в папке коллекции)
    pub fn read_collection(&self, collection_name: String) -> Option<Vec<u8>> {
        let col_path = format!("./storage/{}", collection_name);
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
        let vector_path = format!("./storage/{}/vectors", collection_name);
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
        let vector_path = format!("./storage/{}/vectors", collection_name);
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
        let vector_path_bin = format!("./storage/{}/vectors/{}.bin", collection_name, vector_hash);
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
        let metadata_path = format!("./storage/{}/metadata", collection_name);
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
        let metadata_path = format!("./storage/{}/metadata", collection_name);
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
        let metadata_path_bin = format!("./storage/{}/metadata/{}.bin", collection_name, metadata_hash);
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
}

//  ConnectionController impl

impl ConnectionController {
    /// Создаёт новый ConnectionController с заданным StorageController и ConfigLoader
    pub fn new(storage_controller : StorageController, config_loader : ConfigLoader) -> ConnectionController {
        let names = Vec::new();

        ConnectionController { storage_controller: storage_controller, configs: config_loader.get(names) }
    }

    /// Обработчик соединения (заглушка)
    pub fn connection_handler(&mut self) {

    }

    /// Обработчик запросов (заглушка)
    pub fn query_handler(&self) -> Result<(), &'static str> {
        Ok(())
    }
}

//  CollectionController impl

impl CollectionController {
    /// Создаёт новый CollectionController с заданным StorageController
    pub fn new(storage_controller: StorageController) -> CollectionController {
        CollectionController { storage_controller, collections: None }
    }

    /// Добавляет новую коллекцию с указанным именем
    pub fn add_collection(&mut self, name: String) -> Result<(), &'static str> {
        let collections = self.collections.get_or_insert_with(Vec::new);
        collections.push(Collection::new(Some(name)));
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

        match collection.vectors_controller.add_vector(embedding, metadata) {
            Ok(id) => Ok(id),
            Err(_) => Err("Ошибка при добавлении вектора"),
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

        for (vec_id, vec_raw_data) in collection.vectors_controller.dump() {
            match self.storage_controller.save_vector(collection_name.clone(), vec_raw_data, vec_id) {
                Ok(_) => println!("Вектор с hash_id {} успешно сохранён в коллекции '{}'.", vec_id, collection_name),
                Err(e) => eprintln!("Ошибка сохранения вектора с hash_id {} в коллекции '{}': {:?}", vec_id, collection_name, e),
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
            let mut collection = Collection::new(None);
            collection.load(raw_collection);

            let raw_vector = self.storage_controller.read_all_vector(name.clone());
            collection.vectors_controller.load(raw_vector);

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
}

//  VectorController impl

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