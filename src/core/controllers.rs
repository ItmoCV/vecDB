use std::{collections::HashMap, result::Result};
use crate::core::{config::ConfigLoader, objects::{Collection, Object}};
use std::fs;
use std::path::Path;
use std::io::ErrorKind;

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

// Impl block

//  StorageController impl

impl StorageController {
    /// Создаёт новый контроллер хранилища, инициализирует папку storage, если её нет
    pub fn new(configs : HashMap<String, String>) -> StorageController {
        fs::create_dir_all("storage")
            .expect("Storage folder not create");

        StorageController { configs: configs }
    }

    /// Возвращает список имён всех коллекций (папок) в storage
    pub fn get_all_collection(&self) -> Vec<String> {
        let path = Path::new("./storage");
        let mut collection_names: Vec<String> = Vec::new();

        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let entry_path = entry.path();
            if entry.metadata().unwrap().is_dir() {
                let collection_path = entry_path.to_str();
                match collection_path {
                    Some(path) => {
                        if let Some(last) = path.to_string().split('/').last() {
                            collection_names.push(last.to_string());
                        }
                    }
                    None => {}
                }
            }
        }

        collection_names
    }

    /// Читает сырые данные коллекции (первый найденный файл в папке коллекции)
    pub fn read_collection(&self, collection_name: String) -> Option<Vec<u8>> {
        let col_path = format!("./storage/{}", collection_name);
        let path = Path::new(&col_path);

        let read_dir = fs::read_dir(path);
        let mut col_raw_data: Vec<u8> = Vec::new();
        match read_dir {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry.unwrap();
                    let entry_path = entry.path();
                    if entry.metadata().unwrap().is_file() {
                        let col_raw_path = entry_path.to_str();
                        match col_raw_path {
                            Some(col_file) => {
                                col_raw_data = fs::read(col_file).unwrap();
                            }
                            None => {}
                        }
                    }
                }
                Some(col_raw_data)
            }
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    return None;
                } else {
                    panic!("Ошибка чтения директории: {:?}", e);
                }
            }
        }
    }

    /// Читает все векторы (файлы) из папки vectors коллекции и возвращает их содержимое
    pub fn read_all_vector(&self, collection_name: String) -> Vec<Vec<u8>> {
        let vector_path = format!("./storage/{}/vectors", collection_name);
        let path = Path::new(&vector_path);

        let mut raw_data_list: Vec<Vec<u8>> = Vec::new();

        let read_dir = fs::read_dir(path);
        match read_dir {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry.unwrap();
                    let entry_path = entry.path();
                    if entry.metadata().unwrap().is_file() {
                        let vector_path = entry_path.to_str();
                        match vector_path {
                            Some(vector) => {
                                let raw_vec = fs::read(vector).unwrap();
                                raw_data_list.push(raw_vec);
                            }
                            None => {}
                        }
                    }
                }
            }
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    // Если папка не найдена, возвращаем пустой список
                    return Vec::new();
                } else {
                    panic!("Ошибка чтения директории: {:?}", e);
                }
            }
        }

        raw_data_list
    }

    /// Возвращает вектор имён файлов векторов по названию коллекции (без расширения .bin)
    pub fn get_all_vectors(&self, collection_name: String) -> Vec<String> {
        let vector_path = format!("./storage/{}/vectors", collection_name);
        let path = Path::new(&vector_path);

        let mut vector_file_names: Vec<String> = Vec::new();

        let read_dir = fs::read_dir(path);
        match read_dir {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry.unwrap();
                    let entry_path = entry.path();
                    if entry.metadata().unwrap().is_file() {
                        if let Some(file_name) = entry_path.file_name() {
                            if let Some(file_name_str) = file_name.to_str() {
                                // Удаляем приписку ".bin" если она есть
                                if let Some(stripped) = file_name_str.strip_suffix(".bin") {
                                    vector_file_names.push(stripped.to_string());
                                } else {
                                    vector_file_names.push(file_name_str.to_string());
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    // Если папка не найдена, возвращаем пустой список
                    return Vec::new();
                } else {
                    panic!("Ошибка чтения директории: {:?}", e);
                }
            }
        }

        vector_file_names
    }

    /// Читает конкретный вектор по имени коллекции и имени (или хэшу) вектора
    pub fn read_vector(&self, collection_name: String, vector_name: String) -> Option<Vec<u8>> {
        // Формируем путь к файлу вектора
        let vector_path_bin = format!("./storage/{}/vectors/{}.bin", collection_name, vector_name);
        let vector_path = Path::new(&vector_path_bin);

        match fs::read(vector_path) {
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

    /// Читает все файлы метадаты из папки metadata внутри коллекции и возвращает их содержимое в виде Vec<Vec<u8>>
    pub fn read_all_metadata(&self, collection_name: String) -> Vec<Vec<u8>> {
        let metadata_path = format!("./storage/{}/metadata", collection_name);
        let path = Path::new(&metadata_path);

        let mut metadata_list: Vec<Vec<u8>> = Vec::new();

        let read_dir = fs::read_dir(path);
        match read_dir {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry.unwrap();
                    let entry_path = entry.path();
                    if entry.metadata().unwrap().is_file() {
                        let meta_file_path = entry_path.to_str();
                        match meta_file_path {
                            Some(meta_file) => {
                                let meta_data = fs::read(meta_file).unwrap();
                                metadata_list.push(meta_data);
                            }
                            None => {}
                        }
                    }
                }
            }
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    // Если папка не найдена, возвращаем пустой список
                    return Vec::new();
                } else {
                    panic!("Ошибка чтения директории метадаты: {:?}", e);
                }
            }
        }

        metadata_list
    }

    /// Возвращает вектор имён файлов метадаты по названию коллекции (без расширения .bin)
    pub fn get_all_metadata_names(&self, collection_name: String) -> Vec<String> {
        let metadata_path = format!("./storage/{}/metadata", collection_name);
        let path = Path::new(&metadata_path);

        let mut metadata_file_names: Vec<String> = Vec::new();

        let read_dir = fs::read_dir(path);
        match read_dir {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry.unwrap();
                    let entry_path = entry.path();
                    if entry.metadata().unwrap().is_file() {
                        if let Some(file_name) = entry_path.file_name() {
                            if let Some(file_name_str) = file_name.to_str() {
                                // Удаляем приписку ".bin" если она есть
                                if let Some(stripped) = file_name_str.strip_suffix(".bin") {
                                    metadata_file_names.push(stripped.to_string());
                                } else {
                                    metadata_file_names.push(file_name_str.to_string());
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                if e.kind() == ErrorKind::NotFound {
                    // Если папка не найдена, возвращаем пустой список
                    return Vec::new();
                } else {
                    panic!("Ошибка чтения директории метадаты: {:?}", e);
                }
            }
        }

        metadata_file_names
    }

    /// Читает конкретный файл метадаты по имени коллекции и имени файла метадаты (без расширения)
    pub fn read_metadata(&self, collection_name: String, metadata_name: String) -> Option<Vec<u8>> {
        let metadata_path_bin = format!("./storage/{}/metadata/{}.bin", collection_name, metadata_name);
        let metadata_path = Path::new(&metadata_path_bin);

        match fs::read(metadata_path) {
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

    /// Загружает коллекцию по имени (пока не реализовано)
    pub fn load(&mut self, colelction_name: String) {

    }

    /// Сохраняет объект, реализующий трейт Object (пока не реализовано)
    pub fn dump<T: Object>(&self, _obj: T) {

    }
}

//  ConnectionController impl

impl ConnectionController {
    pub fn new(storage_controller : StorageController, config_loader : ConfigLoader) -> ConnectionController {
        let names = Vec::new();

        ConnectionController { storage_controller: storage_controller, configs: config_loader.get(names) }
    }

    pub fn connection_handler(&mut self) {

    }

    pub fn query_handler(&self) -> Result<(), &'static str> {
        Ok(())
    }
}

//  CollectionController impl

impl CollectionController {
    pub fn new(storage_controller : StorageController) -> CollectionController {
        CollectionController { storage_controller: storage_controller, collections: None }
    }

    pub fn add_collection(&mut self, name: String) -> Result<(), &'static str> {
        Ok(())
    }

    pub fn delete_collection(&mut self, name: String) -> Result<(), &'static str> {
        Ok(())
    }

    pub fn get_collection(&self, name: String) -> Option<Collection> {
        None
    }

    pub fn add_vector(col: Collection, raw_vec: f64) -> Result<(), &'static str> {
        Ok(())
    }
}