use std::{collections::HashMap};
use std::fs;
use serde_json::Value;
use crate::core::sharding::{ShardConfig, RoutingStrategy};

// structs define

#[derive(Clone)]
pub struct ConfigLoader {
    configs: Option<HashMap<String, String>>,
    config_path: Option<String>,
}

// Impl block

impl ConfigLoader {
    pub fn new() -> ConfigLoader {
        ConfigLoader { 
            configs: None,
            config_path: None,
        }
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
        self.config_path = Some(path.clone());
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

    /// Получает конфигурацию шардов из JSON
    pub fn get_shard_configs(&self) -> Result<Vec<ShardConfig>, String> {
        let sharding_config = self.get("sharding");
        if sharding_config.is_empty() {
            return Err("Секция sharding не найдена в конфигурации".to_string());
        }

        // Используем правильный путь к конфигурации вместо хардкода
        let config_path = self.config_path.as_ref()
            .ok_or("Конфигурация не загружена")?;
        
        let content = match fs::read_to_string(config_path) {
            Ok(c) => c,
            Err(e) => return Err(format!("Не удалось прочитать {}: {}", config_path, e)),
        };

        let json: Value = match serde_json::from_str(&content) {
            Ok(j) => j,
            Err(e) => return Err(format!("Ошибка парсинга {}: {}", config_path, e)),
        };

        if let Some(sharding) = json.get("sharding") {
            if let Some(shards_array) = sharding.get("shards") {
                if let Some(shards) = shards_array.as_array() {
                    let mut configs = Vec::new();
                    for shard in shards {
                        if let Some(shard_obj) = shard.as_object() {
                            let config = ShardConfig {
                                id: shard_obj.get("id")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("unknown")
                                    .to_string(),
                                host: shard_obj.get("host")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("127.0.0.1")
                                    .to_string(),
                                port: shard_obj.get("port")
                                    .and_then(|v| v.as_u64())
                                    .unwrap_or(8080) as u16,
                                description: shard_obj.get("description")
                                    .and_then(|v| v.as_str())
                                    .map(|s| s.to_string()),
                            };
                            configs.push(config);
                        }
                    }
                    return Ok(configs);
                }
            }
        }

        Err("Не удалось найти конфигурацию шардов".to_string())
    }

    /// Получает роль экземпляра из конфигурации
    pub fn get_role(&self) -> String {
        self.get("server").get("role").unwrap_or(&"shard".to_string()).clone()
    }

    /// Проверяет, является ли экземпляр координатором
    pub fn is_coordinator(&self) -> bool {
        self.get_role() == "coordinator"
    }

    /// Проверяет, является ли экземпляр шардом
    pub fn is_shard(&self) -> bool {
        self.get_role() == "shard"
    }

    /// Получает стратегию роутинга из конфигурации
    pub fn get_routing_strategy(&self) -> RoutingStrategy {
        let sharding_config = self.get("sharding");
        let default_strategy = "hash_based".to_string();
        let strategy = sharding_config.get("strategy")
            .unwrap_or(&default_strategy);
        
        match strategy.as_str() {
            "hash_based" => RoutingStrategy::HashBased,
            "range_based" => RoutingStrategy::RangeBased,
            "lsh_based" => RoutingStrategy::LSHBased,
            "metadata_based" => RoutingStrategy::MetadataBased,
            _ => RoutingStrategy::HashBased,
        }
    }

    /// Проверяет, включено ли шардирование
    pub fn is_sharding_enabled(&self) -> bool {
        let sharding_config = self.get("sharding");
        sharding_config.get("enabled")
            .map(|v| v == "true")
            .unwrap_or(false)
    }

}