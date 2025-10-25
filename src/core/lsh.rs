use std::collections::HashMap;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

// structs define

/// Метрики расстояния для LSH
#[derive(Debug, Clone, PartialEq)]
pub enum LSHMetric {
    /// Евклидово расстояние
    Euclidean,
    /// Косинусное расстояние
    Cosine,
    /// Манхэттенское расстояние
    Manhattan,
}

#[derive(Debug, Clone)]
pub struct LSH {
    /// Количество хэш-функций
    pub num_hashes: usize,
    /// Размерность векторов
    pub dimension: usize,
    /// Случайные проекции для хэш-функций
    pub projections: Vec<Vec<f32>>,
    /// Случайные смещения для хэш-функций
    pub offsets: Vec<f32>,
    /// Ширина бакетов
    pub bucket_width: f32,
    /// Метрика расстояния
    pub metric: LSHMetric,
}

// Impl block

//  LSHMetric impl

impl LSHMetric {
    pub fn to_string(&self) -> String {
        match self {
            LSHMetric::Euclidean => "Euclidean".to_string(),
            LSHMetric::Cosine => "Cosine".to_string(),
            LSHMetric::Manhattan => "Manhattan".to_string(),
        }
    }

    pub fn from_string(s: &str) -> Result<Self, String> {
        match s {
            "Euclidean" => Ok(LSHMetric::Euclidean),
            "Cosine" => Ok(LSHMetric::Cosine),
            "Manhattan" => Ok(LSHMetric::Manhattan),
            _ => Err(format!("Неизвестная метрика: {}", s)),
        }
    }
}

//  LSH impl

impl LSH {
    /// Создает новый LSH с заданными параметрами
    pub fn new(dimension: usize, num_hashes: usize, bucket_width: f32, metric: LSHMetric, seed: Option<u64>) -> Self {
        let mut rng = if let Some(seed) = seed {
            StdRng::seed_from_u64(seed)
        } else {
            StdRng::from_entropy()
        };

        // Генерируем случайные проекции
        let mut projections = Vec::with_capacity(num_hashes);
        for _ in 0..num_hashes {
            let mut projection = Vec::with_capacity(dimension);
            for _ in 0..dimension {
                projection.push(rng.gen_range(-1.0..1.0));
            }
            projections.push(projection);
        }

        // Генерируем случайные смещения
        let mut offsets = Vec::with_capacity(num_hashes);
        for _ in 0..num_hashes {
            offsets.push(rng.gen_range(0.0..bucket_width));
        }

        LSH {
            num_hashes,
            dimension,
            projections,
            offsets,
            bucket_width,
            metric,
        }
    }

    /// Вычисляет расстояние между двумя векторами в зависимости от метрики
    fn compute_distance(&self, vector: &[f32], projection: &[f32]) -> f32 {
        match self.metric {
            LSHMetric::Euclidean => self.euclidean_distance(vector, projection),
            LSHMetric::Cosine => self.cosine_distance(vector, projection),
            LSHMetric::Manhattan => self.manhattan_distance(vector, projection),
        }
    }

    /// Евклидово расстояние (скалярное произведение)
    fn euclidean_distance(&self, vector: &[f32], projection: &[f32]) -> f32 {
        let mut dot_product = 0.0;
        for i in 0..self.dimension {
            dot_product += vector[i] * projection[i];
        }
        dot_product
    }

    /// Косинусное расстояние
    fn cosine_distance(&self, vector: &[f32], projection: &[f32]) -> f32 {
        let mut dot_product = 0.0;
        let mut norm_a = 0.0;
        let mut norm_b = 0.0;
        
        for i in 0..self.dimension {
            dot_product += vector[i] * projection[i];
            norm_a += vector[i] * vector[i];
            norm_b += projection[i] * projection[i];
        }
        
        if norm_a == 0.0 || norm_b == 0.0 {
            0.0
        } else {
            dot_product / (norm_a.sqrt() * norm_b.sqrt())
        }
    }

    /// Манхэттенское расстояние
    fn manhattan_distance(&self, vector: &[f32], projection: &[f32]) -> f32 {
        let mut distance = 0.0;
        for i in 0..self.dimension {
            distance += (vector[i] - projection[i]).abs();
        }
        distance
    }

    /// Вычисляет хэш для вектора
    pub fn hash(&self, vector: &[f32]) -> u64 {
        if vector.len() != self.dimension {
            panic!("Размерность вектора {} не соответствует ожидаемой {}", vector.len(), self.dimension);
        }

        let mut hash_value = 0u64;
        let mut multiplier = 1u64;

        for i in 0..self.num_hashes {
            // Вычисляем расстояние в зависимости от метрики
            let distance = self.compute_distance(vector, &self.projections[i]);

            // Добавляем смещение и делим на ширину бакета
            let hash_bucket = ((distance + self.offsets[i]) / self.bucket_width).floor() as i64;
            
            // Преобразуем в положительное число и добавляем к общему хэшу
            hash_value = hash_value.wrapping_add((hash_bucket as u64).wrapping_mul(multiplier));
            multiplier = multiplier.wrapping_mul(31); // Простое число для лучшего распределения
        }

        hash_value
    }

    /// Вычисляет несколько хэшей для вектора (для более точного поиска)
    pub fn multi_hash(&self, vector: &[f32], num_hashes: usize) -> Vec<u64> {
        let mut hashes = Vec::with_capacity(num_hashes);
        
        for i in 0..num_hashes {
            if i >= self.num_hashes {
                break;
            }

            let distance = self.compute_distance(vector, &self.projections[i]);
            let hash_bucket = ((distance + self.offsets[i]) / self.bucket_width).floor() as i64;
            hashes.push(hash_bucket as u64);
        }

        hashes
    }

    /// Находит похожие векторы в LSH бакетах
    pub fn find_similar_buckets(&self, query_vector: &[f32], all_buckets: &HashMap<u64, Vec<u64>>) -> Vec<u64> {
        let query_hash = self.hash(query_vector);
        let mut similar_vectors = Vec::new();

        // Ищем в том же бакете
        if let Some(vectors) = all_buckets.get(&query_hash) {
            similar_vectors.extend(vectors.iter());
        }

        similar_vectors
    }
}