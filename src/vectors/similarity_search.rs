use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use std::error::Error;

/// Находит наиболее близкое предложение из списка к заданному запросу.
///
/// Возвращает кортеж: (индекс наиболее близкого предложения, значение косинусного сходства).
fn find_most_similar(
    query: &str,
    candidates: &[&str],
) -> Result<(usize, f32), Box<dyn Error>> {

    let mut model = TextEmbedding::try_new(
    InitOptions::new(EmbeddingModel::AllMiniLML6V2).with_show_download_progress(false),
    )?;

    if candidates.is_empty() {
        return Err("Candidate list is empty".into());
    }

    // Эмбеддинг запроса
    let query_embedding = model.embed(vec![query], None)?[0].clone();

    // Эмбеддинги кандидатов
    let candidate_embeddings = model.embed(candidates.iter().map(|s| *s).collect(), None)?;

    // Находим кандидата с максимальным косинусным сходством
    let mut best_index = 0;
    let mut best_score = f32::NEG_INFINITY;

    for (i, candidate_emb) in candidate_embeddings.iter().enumerate() {
        let score = cosine_similarity(&query_embedding, candidate_emb);
        if score > best_score {
            best_score = score;
            best_index = i;
        }
    }

    Ok((best_index, best_score))
}

/// Вычисляет косинусное сходство между двумя векторами.
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    assert_eq!(a.len(), b.len(), "Vectors must have the same dimension");

    let dot_product: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        0.0 // или можно вернуть ошибку, но для эмбеддингов это редко случается
    } else {
        dot_product / (norm_a * norm_b)
    }
}

// Пример использования
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let query = "How do I bake a cake?";
    let candidates = vec![
        "The weather is nice today.",
        "Baking a cake requires flour, sugar, and eggs.",
        "Rust is a systems programming language.",
        "I love eating chocolate cake.",
    ];

    let (best_idx, score) = find_most_similar(query, &candidates)?;
    println!("Query: {}", query);
    println!("Most similar: {}", candidates[best_idx]);
    println!("Cosine similarity: {:.4}", score);

    Ok(())
}