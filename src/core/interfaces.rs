use std::collections::HashMap;

/// Трейт для контроллеров объектов коллекции (например, векторов или метаданных)
pub trait CollectionObjectController {
    /// Загружает объекты из HashMap<u64, Vec<u8>> (hash_id -> данные)
    fn load(&mut self, raw_data: HashMap<u64, Vec<u8>>);

    /// Сохраняет объекты в HashMap<u64, Vec<u8>> (hash_id -> данные)
    fn dump(&self) -> HashMap<u64, Vec<u8>>;
}

#[allow(dead_code)]
/// Трейт для объектов, которые могут быть сериализованы и десериализованы
pub trait Object {
    /// Загружает объект из вектора байт
    fn load(&mut self, raw_data: Vec<u8>);

    /// Сохраняет объект в вектор байт, возвращает также hash_id
    fn dump(&self) -> Result<(Vec<u8>, u64), ()>;

    /// Возвращает hash_id объекта
    fn hash_id(&self) -> u64;

    /// Устанавливает hash_id объекта
    fn set_hash_id(&mut self, id: u64);
}