use std::collections::HashMap;

pub trait CollectionObjectController {
    fn load(&mut self, raw_data: HashMap<u64, Vec<u8>>);
    fn dump(&self) -> HashMap<u64, Vec<u8>>;
}

#[allow(dead_code)]
pub trait Object {
    fn load(&mut self, raw_data: Vec<u8>);
    fn dump(&self) -> Result<(Vec<u8>, u64), ()>;
    fn hash_id(&self) -> u64;
    fn set_hash_id(&mut self, id: u64);
}