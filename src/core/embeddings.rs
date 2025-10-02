use std::error::Error;
use std::cmp::Ordering::Equal;

use crate::core::objects::Vector;

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
    query: &Vec<f32>,
    vectors: &[Vector],
    k: usize,
) -> Result<Vec<(usize, f32)>, Box<dyn Error>> {
    if vectors.is_empty() {
        return Err("Список векторов пуст".into());
    }

    let mut scored: Vec<(usize, f32)> = vectors
        .iter()
        .enumerate()
        .map(|(i, vector)| (i, cosine_similarity(query, &vector.data)))
        .collect();

    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Equal));

    let top_k = scored.into_iter().take(k).collect();

    Ok(top_k)
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