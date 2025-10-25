use serde::{Deserialize, Serialize};
use utoipa::{ToSchema, OpenApi};

/// Параметры для создания коллекции
#[derive(Serialize, Deserialize, ToSchema)]
pub struct AddCollectionParams {
    /// Название коллекции
    pub name: String,
    /// Метрика для LSH
    pub metric: String,
    /// Размерность векторов
    pub dimension: usize,
}

/// Параметры для удаления коллекции
#[derive(Serialize, Deserialize, ToSchema)]
pub struct DeleteCollectionParams {
    /// Название коллекции для удаления
    pub name: String,
}

/// Параметры для добавления вектора
#[derive(Serialize, Deserialize, ToSchema)]
pub struct AddVectorParams {
    /// Название коллекции
    pub collection: String,
    /// Вектор эмбеддинга
    pub embedding: Vec<f32>,
    /// Метаданные вектора
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<std::collections::HashMap<String, String>>,
}

/// Параметры для обновления вектора
#[derive(Serialize, Deserialize, ToSchema)]
pub struct UpdateVectorParams {
    /// Название коллекции
    pub collection: String,
    /// ID вектора
    pub vector_id: u64,
    /// Новый вектор эмбеддинга
    #[serde(skip_serializing_if = "Option::is_none")]
    pub embedding: Option<Vec<f32>>,
    /// Новые метаданные
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<std::collections::HashMap<String, String>>,
}

/// Параметры для получения вектора
#[derive(Serialize, Deserialize, ToSchema)]
pub struct GetVectorParams {
    /// Название коллекции
    pub collection: String,
    /// ID вектора
    pub vector_id: u64,
}

/// Параметры для удаления вектора
#[derive(Serialize, Deserialize, ToSchema)]
pub struct DeleteVectorParams {
    /// Название коллекции
    pub collection: String,
    /// ID вектора
    pub vector_id: u64,
}

/// Параметры для фильтрации по метаданным
#[derive(Serialize, Deserialize, ToSchema)]
pub struct FilterByMetadataParams {
    /// Название коллекции
    pub collection: String,
    /// Фильтры метаданных
    pub filters: std::collections::HashMap<String, String>,
}

/// Параметры для поиска похожих векторов
#[derive(Serialize, Deserialize, ToSchema)]
pub struct FindSimilarParams {
    /// Название коллекции
    pub collection: String,
    /// Запросный вектор
    pub query: Vec<f32>,
    /// Количество похожих векторов
    pub k: usize,
}

/// Стандартный RPC ответ
#[derive(Serialize, Deserialize, ToSchema)]
pub struct RpcResponse {
    /// Статус ответа
    pub status: String,
    /// Данные ответа
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    /// Сообщение об ошибке или дополнительная информация
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Результат поиска похожих векторов
#[derive(Serialize, Deserialize, ToSchema)]
pub struct SimilarVectorResult {
    pub bucket_id: u64,
    pub vector_index: usize,
    pub score: f32,
}

/// OpenAPI спецификация для VectorDB API
#[derive(OpenApi)]
#[openapi(
    paths(
        crate::core::handlers::add_collection,
        crate::core::handlers::delete_collection,
        crate::core::handlers::add_vector,
        crate::core::handlers::update_vector,
        crate::core::handlers::get_vector,
        crate::core::handlers::delete_vector,
        crate::core::handlers::filter_by_metadata,
        crate::core::handlers::find_similar,
        crate::core::handlers::stop
    ),
    components(
        schemas(
            AddCollectionParams,
            DeleteCollectionParams,
            AddVectorParams,
            UpdateVectorParams,
            GetVectorParams,
            DeleteVectorParams,
            FilterByMetadataParams,
            FindSimilarParams,
            RpcResponse,
            SimilarVectorResult
        )
    ),
    tags(
        (name = "Collections", description = "Операции с коллекциями"),
        (name = "Vectors", description = "Операции с векторами"),
        (name = "System", description = "Системные операции")
    ),
    info(
        title = "VectorDB API",
        version = "1.0.0",
        description = "API для работы с векторной базой данных",
        contact(
            name = "VectorDB Team"
        )
    ),
    servers(
        (url = "http://localhost:8080", description = "Локальный сервер разработки")
    )
)]
pub struct ApiDoc;

/// Загружает OpenAPI спецификацию из макросов utoipa
pub fn load_openapi_spec() -> utoipa::openapi::OpenApi {
    ApiDoc::openapi()
}