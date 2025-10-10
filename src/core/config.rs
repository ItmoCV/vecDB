use std::{collections::HashMap};
use std::fs;
use serde_json::Value;

// structs define

pub struct ConfigLoader {
    configs: Option<HashMap<String, String>>,
}

// Impl block

impl ConfigLoader {
    pub fn new() -> ConfigLoader {
        ConfigLoader { configs: None }
    }

    // Возвращает плоский хэшмап с ключами без префикса, соответствующими секции <names[0]>.
    // Например, если names = ["connection"], то выберет "connection.host" -> "0.0.0.0" и "connection.port" -> "8080"
    // и вернёт HashMap {"host": "0.0.0.0", "port": "8080"}
    pub fn get(&self, name: &str) -> HashMap<String, String> {
        match &self.configs {
            Some(configs) => {
                let mut result = HashMap::new();
                let prefix_dot = format!("{}.", name);
                for (key, value) in configs {
                    if key.starts_with(&prefix_dot) {
                        let simple_key = key.strip_prefix(&prefix_dot).unwrap_or(key).to_string();
                        result.insert(simple_key, value.clone());
                    }
                }
                result
            },
            None => HashMap::new(),
        }
    }

    pub fn load(&mut self, path: String) {
        let content = match fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to read config file '{}': {:?}", path, e);
                self.configs = None;
                return;
            }
        };
        let json: Value = match serde_json::from_str(&content) {
            Ok(j) => j,
            Err(e) => {
                eprintln!("Failed to parse config file '{}': {:?}", path, e);
                self.configs = None;
                return;
            }
        };

        let mut flat = HashMap::new();
        if let Value::Object(map) = json {
            for (k, v) in map.iter() {
                if v.is_object() {
                    // flatten one level
                    if let Value::Object(inner) = v {
                        for (ik, iv) in inner.iter() {
                            flat.insert(format!("{}.{}", k, ik), iv.to_string().trim_matches('"').to_string());
                        }
                    }
                } else {
                    flat.insert(k.clone(), v.to_string().trim_matches('"').to_string());
                }
            }
        }
        self.configs = Some(flat);
    }
}