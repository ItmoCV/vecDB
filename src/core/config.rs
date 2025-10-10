use std::{collections::HashMap};

// structs define

pub struct ConfigLoader {
    configs: Option<HashMap<String, String>>,
}

// Impl block

impl ConfigLoader {
    pub fn new() -> ConfigLoader {
        ConfigLoader { configs: None }
    }

    pub fn get(&self, names: Vec<String>) -> HashMap<String, String> {
        match &self.configs {
            Some(configs) => {
                let mut result = HashMap::new();
                for name in names {
                    if let Some(value) = configs.get(&name) {
                        result.insert(name, value.clone());
                    }
                }
                result
            },
            None => HashMap::new(),
        }
    }

    pub fn load(&mut self, path: String) {
        // TODO: load from json
        // self.configs = load from json
    }
}