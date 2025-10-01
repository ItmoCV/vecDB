use std::error::Error;

use crate::core::{objects::{Vector}};

#[cfg(not(test))]
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};

#[cfg(not(test))]
pub fn make_embeddings(
    sentence: &str,
) -> Result<Vec<f32>, Box<dyn std::error::Error>> {
    // Инициализация модели 
    let mut model = TextEmbedding::try_new(
    InitOptions::new(EmbeddingModel::AllMiniLML6V2),
    )?;

    let documents = vec![sentence];

    // Получаем эмбеддинг (вектор f32)
    let embeddings = model.embed(documents, None)?;

    Ok(embeddings[0].clone())
}

#[cfg(test)]
pub fn make_embeddings(
    sentence: &str,
) -> Result<Vec<f32>, Box<dyn std::error::Error>> {
    let mut embedding = vec![0.0_f32; 4];

    for (index, byte) in sentence.as_bytes().iter().take(3).enumerate() {
        embedding[index] = *byte as f32;
    }

    embedding[3] = sentence.chars().count() as f32;

    Ok(embedding)
}

pub fn find_most_similar(
    query: &str,
    vectors: &[Vector],
) -> Result<(usize, f32), Box<dyn Error>> {
    if vectors.is_empty() {
        return Err("Vector list is empty".into());
    }

    // Кодируем запрос заранее
    let query_embedding = make_embeddings(query)?;

    let mut best_index = 0;
    let mut best_score = f32::NEG_INFINITY;

    for (i, vector) in vectors.iter().enumerate() {
        let score = cosine_similarity(&query_embedding, &vector.data);
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