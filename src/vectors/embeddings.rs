// src/lib.rs
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::error::Error;

pub fn make_embeddings(
    sentence: &str,
) -> Result<Vec<f32>, Box<dyn std::error::Error>> {
    // Инициализация модели (автоматически скачается при первом запуске)
    let mut model = TextEmbedding::try_new(
    InitOptions::new(EmbeddingModel::AllMiniLML6V2),
    )?;

    let documents = vec![sentence];

    // Получаем эмбеддинг (вектор f32)
    let embeddings = model.embed(documents, None)?;

    // Теперь ты можешь сохранить `embedding` в свой список/векторную БД
    Ok(embeddings[0].clone())
}


#[derive(Debug, Clone)]
pub struct Vector {
    pub id: String,
    pub embedding: Vec<f32>,
    pub created_at: DateTime<Utc>,
    pub metadata: HashMap<String, String>,
}

pub fn create_vector_with_embedding(
    sentence: &str,
    metadata: HashMap<String, String>,
) -> Result<Vector, Box<dyn std::error::Error>> {
    let embedding = make_embeddings(sentence)?;
    let id = uuid::Uuid::new_v4().to_string();
    let created_at = Utc::now();

    Ok(Vector {
        id,
        embedding,
        created_at,
        metadata,
    })
}

pub struct VectorController {
    pub vectors: Vec<Vector>,
}

impl VectorController {
    pub fn new() -> Self {
        VectorController { vectors: Vec::new() }
    }

    pub fn add_vector(&mut self, vector: Vector) {
        self.vectors.push(vector);
    }

    pub fn remove_vector(&mut self, id: &str) -> bool {
        if let Some(pos) = self.vectors.iter().position(|v| v.id == id) {
            self.vectors.remove(pos);
            true
        } else {
            false
        }
    }

    pub fn update_vector_by_text(&mut self, id: &str, new_text: &str, new_metadata: HashMap<String, String>) -> Result<bool, Box<dyn Error>> {
        if let Some(v) = self.vectors.iter_mut().find(|v| v.id == id) {
            let new_embedding = make_embeddings(new_text)?;
            v.embedding = new_embedding;
            v.metadata = new_metadata;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Добавляет метаданные к вектору по ID (объединяет с существующими)
    pub fn add_metadata_to_vector(&mut self, id: &str, new_metadata: HashMap<String, String>) -> bool {
        if let Some(v) = self.vectors.iter_mut().find(|v| v.id == id) {
            v.metadata.extend(new_metadata);
            true
        } else {
            false
        }
    }

    /// Удаляет метаданные по ключу у вектора по ID
    pub fn remove_metadata_from_vector(&mut self, id: &str, key: &str) -> bool {
        if let Some(v) = self.vectors.iter_mut().find(|v| v.id == id) {
            v.metadata.remove(key);
            true
        } else {
            false
        }
    }

    pub fn find_most_similar(&self, query: &str) -> Result<(usize, f32), Box<dyn std::error::Error>> {
        find_most_similar(query, &self.vectors)
    }

    pub fn get_vector(&self, index: usize) -> Option<&Vector> {
        self.vectors.get(index)
    }

    pub fn get_vector_by_id(&self, id: &str) -> Option<&Vector> {
        self.vectors.iter().find(|v| v.id == id)
    }

    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }

    // Новый метод: фильтрация по метаданным
    pub fn filter_by_metadata(&self, filters: &HashMap<String, String>) -> Vec<String> {
        let mut result = Vec::new();
        for vector in &self.vectors {
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
                result.push(vector.id.clone());
            }
        }
        result
    }
}

pub fn find_most_similar(
    query: &str,
    vectors: &[Vector],
) -> Result<(usize, f32), Box<dyn Error>> {
    if vectors.is_empty() {
        return Err("Vector list is empty".into());
    }

    let query_embedding = make_embeddings(query)?;

    let mut best_index = 0;
    let mut best_score = f32::NEG_INFINITY;

    for (i, vector) in vectors.iter().enumerate() {
        let score = cosine_similarity(&query_embedding, &vector.embedding);
        if score > best_score {
            best_score = score;
            best_index = i;
        }
    }

    Ok((best_index, best_score))
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "Vectors must have the same dimension");

    let dot_product: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        0.0
    } else {
        dot_product / (norm_a * norm_b)
    }
}