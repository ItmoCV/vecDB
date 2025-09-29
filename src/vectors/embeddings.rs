use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};

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