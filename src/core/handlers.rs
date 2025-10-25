use axum::{extract::State, Json};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::sync::broadcast;
use std::collections::HashMap;
use serde_json;

use crate::core::{
    lsh::LSHMetric,
    interfaces::Object,
    openapi::{
        AddCollectionParams, DeleteCollectionParams, AddVectorParams, UpdateVectorParams,
        GetVectorParams, DeleteVectorParams, FilterByMetadataParams, FindSimilarParams,
        RpcResponse, SimilarVectorResult
    }
};

/// Состояние приложения для HTTP обработчиков
#[derive(Clone)]
pub struct AppState {
    pub controller: Arc<RwLock<CollectionController>>,
    pub configs: HashMap<String, String>,
    pub shutdown_tx: broadcast::Sender<()>,
}

// Временный импорт для CollectionController
// TODO: Вынести в отдельный модуль или реорганизовать
use crate::core::controllers::CollectionController;

/// Создание коллекции
#[utoipa::path(
    post,
    path = "/collection",
    request_body = AddCollectionParams,
    responses(
        (status = 200, description = "Коллекция успешно создана", body = RpcResponse),
        (status = 400, description = "Ошибка в запросе", body = RpcResponse)
    ),
    tag = "Collections"
)]
pub async fn add_collection(State(state): State<AppState>, Json(payload): Json<AddCollectionParams>) -> Json<RpcResponse> {
    let metric = LSHMetric::from_string(&payload.metric).unwrap_or(LSHMetric::Euclidean);
    let mut ctrl = state.controller.write().await;
    match ctrl.add_collection(payload.name, metric, payload.dimension) {
        Ok(_) => Json(RpcResponse { 
            status: "ok".to_string(), 
            data: Some(serde_json::json!({"added": true})), 
            message: None 
        }),
        Err(e) => Json(RpcResponse { 
            status: "error".to_string(), 
            data: None, 
            message: Some(e.to_string()) 
        }),
    }
}

/// Удаление коллекции
#[utoipa::path(
    post,
    path = "/collection/delete",
    request_body = DeleteCollectionParams,
    responses(
        (status = 200, description = "Коллекция успешно удалена", body = RpcResponse),
        (status = 400, description = "Ошибка в запросе", body = RpcResponse)
    ),
    tag = "Collections"
)]
pub async fn delete_collection(State(state): State<AppState>, Json(payload): Json<DeleteCollectionParams>) -> Json<RpcResponse> {
    let mut ctrl = state.controller.write().await;
    match ctrl.delete_collection(payload.name) {
        Ok(_) => Json(RpcResponse { 
            status: "ok".to_string(), 
            data: Some(serde_json::json!({"deleted": true})), 
            message: None 
        }),
        Err(e) => Json(RpcResponse { 
            status: "error".to_string(), 
            data: None, 
            message: Some(e.to_string()) 
        }),
    }
}

/// Получение всех коллекций
pub async fn get_all_collections(State(state): State<AppState>, Json(_payload): Json<serde_json::Value>) -> Json<RpcResponse> {
    let ctrl = state.controller.read().await;
    let collections = ctrl.get_all_collections();
    
    let collections_info: Vec<serde_json::Value> = collections.iter().map(|c| {
        serde_json::json!({
            "name": c.name,
            "vector_dimension": c.vector_dimension,
            "metric": format!("{:?}", c.lsh_metric),
            "total_vectors": c.buckets_controller.total_vectors(),
            "total_buckets": c.buckets_controller.count()
        })
    }).collect();
    
    Json(RpcResponse { 
        status: "ok".to_string(), 
        data: Some(serde_json::json!({
            "collections": collections_info,
            "total": collections_info.len()
        })), 
        message: None 
    })
}

/// Добавление вектора
#[utoipa::path(
    post,
    path = "/vector",
    request_body = AddVectorParams,
    responses(
        (status = 200, description = "Вектор успешно добавлен", body = RpcResponse),
        (status = 400, description = "Ошибка в запросе", body = RpcResponse)
    ),
    tag = "Vectors"
)]
pub async fn add_vector(State(state): State<AppState>, Json(payload): Json<AddVectorParams>) -> Json<RpcResponse> {
    let mut ctrl = state.controller.write().await;
    match ctrl.add_vector(&payload.collection, payload.embedding, payload.metadata.unwrap_or_default()) {
        Ok(id) => Json(RpcResponse { 
            status: "ok".to_string(), 
            data: Some(serde_json::json!({"id": id})), 
            message: None 
        }),
        Err(e) => Json(RpcResponse { 
            status: "error".to_string(), 
            data: None, 
            message: Some(e.to_string()) 
        }),
    }
}

/// Обновление вектора
#[utoipa::path(
    put,
    path = "/vector/update",
    request_body = UpdateVectorParams,
    responses(
        (status = 200, description = "Вектор успешно обновлен", body = RpcResponse),
        (status = 400, description = "Ошибка в запросе", body = RpcResponse)
    ),
    tag = "Vectors"
)]
pub async fn update_vector(State(state): State<AppState>, Json(payload): Json<UpdateVectorParams>) -> Json<RpcResponse> {
    let mut ctrl = state.controller.write().await;
    match ctrl.update_vector(&payload.collection, payload.vector_id, payload.embedding, payload.metadata) {
        Ok(_) => Json(RpcResponse { 
            status: "ok".to_string(), 
            data: Some(serde_json::json!({"updated": true})), 
            message: None 
        }),
        Err(e) => Json(RpcResponse { 
            status: "error".to_string(), 
            data: None, 
            message: Some(e.to_string()) 
        }),
    }
}

/// Получение вектора
#[utoipa::path(
    post,
    path = "/vector/get",
    request_body = GetVectorParams,
    responses(
        (status = 200, description = "Вектор успешно получен", body = RpcResponse),
        (status = 400, description = "Ошибка в запросе", body = RpcResponse)
    ),
    tag = "Vectors"
)]
pub async fn get_vector(State(state): State<AppState>, Json(payload): Json<GetVectorParams>) -> Json<RpcResponse> {
    let ctrl = state.controller.read().await;
    match ctrl.get_collection(&payload.collection) {
        Some(collection) => {
            match collection.buckets_controller.get_vector(payload.vector_id) {
                Some(vector) => Json(RpcResponse { 
                    status: "ok".to_string(), 
                    data: Some(serde_json::json!({
                        "id": vector.hash_id(),
                        "embedding": vector.data,
                        "metadata": vector.metadata
                    })), 
                    message: None 
                }),
                None => Json(RpcResponse { 
                    status: "error".to_string(), 
                    data: None, 
                    message: Some("Вектор не найден".to_string()) 
                }),
            }
        }
        None => Json(RpcResponse { 
            status: "error".to_string(), 
            data: None, 
            message: Some("Коллекция не найдена".to_string()) 
        }),
    }
}

/// Удаление вектора
#[utoipa::path(
    post,
    path = "/vector/delete",
    request_body = DeleteVectorParams,
    responses(
        (status = 200, description = "Вектор успешно удален", body = RpcResponse),
        (status = 400, description = "Ошибка в запросе", body = RpcResponse)
    ),
    tag = "Vectors"
)]
pub async fn delete_vector(State(state): State<AppState>, Json(payload): Json<DeleteVectorParams>) -> Json<RpcResponse> {
    let mut ctrl = state.controller.write().await;
    match ctrl.get_collection_mut(&payload.collection) {
        Some(collection) => {
            match collection.buckets_controller.remove_vector(payload.vector_id) {
                Ok(_) => Json(RpcResponse { 
                    status: "ok".to_string(), 
                    data: Some(serde_json::json!({"deleted": true})), 
                    message: None 
                }),
                Err(e) => Json(RpcResponse { 
                    status: "error".to_string(), 
                    data: None, 
                    message: Some(e) 
                }),
            }
        }
        None => Json(RpcResponse { 
            status: "error".to_string(), 
            data: None, 
            message: Some("Коллекция не найдена".to_string()) 
        }),
    }
}

/// Фильтрация векторов по метаданным
#[utoipa::path(
    post,
    path = "/vector/filter",
    request_body = FilterByMetadataParams,
    responses(
        (status = 200, description = "Векторы успешно отфильтрованы", body = RpcResponse),
        (status = 400, description = "Ошибка в запросе", body = RpcResponse)
    ),
    tag = "Vectors"
)]
pub async fn filter_by_metadata(State(state): State<AppState>, Json(payload): Json<FilterByMetadataParams>) -> Json<RpcResponse> {
    let ctrl = state.controller.read().await;
    match ctrl.filter_by_metadata(&payload.collection, &payload.filters) {
        Ok(vector_ids) => Json(RpcResponse { 
            status: "ok".to_string(), 
            data: Some(serde_json::json!({"vector_ids": vector_ids})), 
            message: None 
        }),
        Err(e) => Json(RpcResponse { 
            status: "error".to_string(), 
            data: None, 
            message: Some(e.to_string()) 
        }),
    }
}

/// Поиск похожих векторов
#[utoipa::path(
    post,
    path = "/vector/similar",
    request_body = FindSimilarParams,
    responses(
        (status = 200, description = "Похожие векторы найдены", body = RpcResponse),
        (status = 400, description = "Ошибка в запросе", body = RpcResponse)
    ),
    tag = "Vectors"
)]
pub async fn find_similar(State(state): State<AppState>, Json(payload): Json<FindSimilarParams>) -> Json<RpcResponse> {
    let ctrl = state.controller.read().await;
    match ctrl.find_similar(payload.collection, &payload.query, payload.k) {
        Ok(results) => {
            // Преобразуем кортежи в структуры для красивого JSON
            let formatted_results: Vec<SimilarVectorResult> = results
                .into_iter()
                .map(|(bucket_id, vector_index, score)| SimilarVectorResult {
                    bucket_id,
                    vector_index,
                    score,
                })
                .collect();
            
            Json(RpcResponse { 
                status: "ok".to_string(), 
                data: Some(serde_json::json!({"results": formatted_results})), 
                message: None 
            })
        },
        Err(e) => Json(RpcResponse { 
            status: "error".to_string(), 
            data: None, 
            message: Some(e.to_string()) 
        }),
    }
}

/// Остановка сервера
#[utoipa::path(
    post,
    path = "/stop",
    responses(
        (status = 200, description = "Сервер остановлен", body = RpcResponse)
    ),
    tag = "System"
)]
pub async fn stop(State(state): State<AppState>) -> Json<RpcResponse> {
    // Отправляем сигнал остановки
    let _ = state.shutdown_tx.send(());
    
    Json(RpcResponse { 
        status: "ok".to_string(), 
        data: Some(serde_json::json!("Server stopping...")), 
        message: None 
    })
}
