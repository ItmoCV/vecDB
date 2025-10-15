# vecDB

Высокопроизводительная векторная база данных на Rust с поддержкой Locality-Sensitive Hashing (LSH) для эффективного поиска похожих векторов.

## 🚀 Особенности

- **LSH-оптимизированный поиск**: Автоматическое группирование векторов в бакеты на основе Locality-Sensitive Hashing
- **Множественные метрики**: Поддержка Euclidean, Cosine и Manhattan расстояний
- **Гибкая размерность**: Настраиваемая размерность векторов на уровне коллекции
- **Валидация данных**: Автоматическая проверка размерности векторов
- **Персистентность**: Сохранение данных на диск с возможностью загрузки
- **Метаданные**: Поддержка произвольных метаданных для векторов
- **Автоматическая очистка**: Удаление пустых бакетов при обновлении или удалении векторов
- **Высокая производительность**: Оптимизированные алгоритмы на Rust

## 📋 Требования

- Rust 1.70+
- Cargo

## 🛠 Установка

```bash
git clone https://github.com/ItmoCV/vecDB.git
cd vecDB
cargo build
```

## 🏗 Архитектура

### Основные компоненты

- **Collection**: Коллекция векторов с настраиваемой метрикой LSH и размерностью
- **Bucket**: Группа похожих векторов, созданная алгоритмом LSH
- **Vector**: Отдельный вектор с эмбеддингом, метаданными и временными метками
- **LSH**: Алгоритм для группировки похожих векторов

### Структура данных

```
Collection
├── name: String
├── vector_dimension: usize
├── lsh_metric: LSHMetric
└── buckets_controller: BucketController
    └── buckets: Vec<Bucket>
        └── vectors_controller: VectorController
            └── vectors: Vec<Vector>
```

## 📖 Использование

### Создание коллекции

```rust
use vecDB::core::controllers::{CollectionController, StorageController};
use vecDB::core::lsh::LSHMetric;
use std::collections::HashMap;

// Создание контроллеров
let storage_controller = StorageController::new(HashMap::new());
let mut collection_controller = CollectionController::new(storage_controller);

// Создание коллекции с размерностью 384 и метрикой Euclidean
let collection_name = "my_documents".to_string();
let vector_dimension = 384;
collection_controller.add_collection(
    collection_name.clone(), 
    LSHMetric::Euclidean, 
    vector_dimension
).unwrap();
```

### Добавление векторов

```rust
use std::collections::HashMap;

// Создание вектора и метаданных
let embedding = vec![0.1, 0.2, 0.3, /* ... 384 элемента */];
let mut metadata = HashMap::new();
metadata.insert("category".to_string(), "document".to_string());
metadata.insert("author".to_string(), "John Doe".to_string());

// Добавление вектора в коллекцию
let vector_id = collection_controller.add_vector(
    &collection_name, 
    embedding, 
    metadata
).unwrap();
```

### Поиск похожих векторов

```rust
// Поиск 5 наиболее похожих векторов
let query_vector = vec![0.1, 0.2, 0.3, /* ... */];
let results = collection_controller.search_similar(
    &collection_name,
    &query_vector,
    5
).unwrap();

for (vector_id, similarity_score) in results {
    println!("Vector ID: {}, Similarity: {}", vector_id, similarity_score);
}
```

### Обновление векторов

```rust
// Обновление эмбеддинга вектора
let new_embedding = vec![0.4, 0.5, 0.6, /* ... */];
let new_metadata = HashMap::new();
new_metadata.insert("updated".to_string(), "true".to_string());

collection_controller.update_vector(
    &collection_name,
    vector_id,
    Some(new_embedding),
    Some(new_metadata)
).unwrap();
```

### Сохранение и загрузка

```rust
// Сохранение коллекции
if let Some(collection) = collection_controller.get_collection(&collection_name) {
    collection_controller.dump_one(collection);
    println!("Коллекция сохранена!");
}

// Загрузка коллекции
collection_controller.load_one(collection_name.clone());
```

## 🔧 Конфигурация LSH

### Доступные метрики

- **Euclidean**: Евклидово расстояние
- **Cosine**: Косинусное сходство
- **Manhattan**: Манхэттенское расстояние
```

## 📁 Структура хранения

Данные сохраняются в следующей структуре:

```
storage/
├── collection_name/
│   ├── 0.bin                    # Метаданные коллекции
│   ├── bucket_id_1/
│   │   ├── 0.bin               # Метаданные бакета
│   │   └── vectors/
│   │       ├── vector_id_1.bin # Вектор 1
│   │       └── vector_id_2.bin # Вектор 2
│   └── bucket_id_2/
│       ├── 0.bin
│       └── vectors/
│           └── vector_id_3.bin
```

## 🧪 Тестирование

```bash
# Запуск всех тестов
cargo test

# Запуск конкретного теста
cargo test test_vector_dimension_validation

# Запуск с выводом
cargo test -- --nocapture
```

### Основные тесты

#### Тесты VectorController

- **`add_and_get_vector`**: Проверяет базовое добавление вектора в контроллер и его получение по ID. Убеждается, что данные, метаданные и временная метка сохраняются корректно.

- **`add_and_remove_metadata`**: Тестирует добавление и удаление метаданных у существующего вектора. Проверяет, что метаданные корректно добавляются и удаляются без потери других данных.

- **`update_vector_replaces_data`**: Проверяет обновление embedding вектора с сохранением существующих метаданных. Убеждается, что новые данные полностью заменяют старые.

- **`update_vector_replaces_metadata`**: Тестирует обновление метаданных вектора с сохранением существующего embedding. Проверяет полную замену метаданных.

- **`remove_vector_deletes_entry`**: Проверяет корректное удаление вектора из контроллера. Убеждается, что после удаления вектор больше недоступен.

- **`filter_by_metadata_returns_only_matching_ids`**: Тестирует фильтрацию векторов по метаданным. Проверяет, что возвращаются только векторы, соответствующие заданным критериям.

- **`find_most_similar_returns_k_vectors`**: Проверяет поиск k наиболее похожих векторов. Убеждается, что результаты отсортированы по убыванию схожести и возвращается правильное количество векторов.

- **`operations_fail_for_missing_vector`**: Проверяет, что все операции (добавление/удаление метаданных, обновление, удаление) корректно обрабатывают несуществующие векторы и возвращают ошибки.

#### Тесты LSH функциональности

- **`test_lsh_basic_functionality`**: Проверяет базовую работу LSH — создание, хэширование векторов, детерминированность хэшей для одинаковых векторов. Убеждается, что похожие векторы получают близкие хэши.

- **`test_bucket_controller_creation`**: Проверяет корректное создание контроллера бакетов, инициализацию размерности и отсутствие бакетов/векторов в начале.

- **`test_bucket_controller_add_vectors`**: Проверяет добавление векторов в бакеты, корректность увеличения количества векторов и бакетов, а также возможность получения векторов по ID.

- **`test_bucket_controller_similarity_search`**: Проверяет поиск похожих векторов в бакетах с помощью LSH, корректность поиска по схожести.

- **`test_bucket_controller_multi_bucket_search`**: Тестирует поиск похожих векторов в нескольких бакетах одновременно. Проверяет работу мульти-бакетного поиска.

- **`test_bucket_controller_metadata_filtering`**: Проверяет фильтрацию векторов по метаданным в контексте LSH. Убеждается, что поиск учитывает метаданные.

- **`test_bucket_controller_statistics`**: Тестирует получение статистики по LSH контроллеру. Проверяет корректность подсчета векторов, бакетов и параметров LSH.

- **`test_bucket_controller_remove_vector`**: Проверяет удаление векторов из LSH контроллера. Убеждается, что векторы корректно удаляются из соответствующих бакетов.

#### Тесты коллекций и метрик

- **`test_collection_with_different_metrics`**: Проверяет работу коллекций с разными метриками LSH (евклидова, косинусная, манхэттенская), корректность инициализации каждой метрики.

- **`test_lsh_metric_serialization`**: Тестирует сериализацию и десериализацию метрик LSH. Проверяет корректность преобразования метрик в строки и обратно.

#### Тесты хранения данных

- **`test_vector_storage_in_buckets`**: Проверяет сохранение и загрузку векторов в/из бакетов. Убеждается, что данные корректно сохраняются на диск и загружаются обратно.

- **`test_bucket_storage_in_own_folder`**: Тестирует сохранение и загрузку данных бакетов в отдельных папках. Проверяет структуру хранения и получение списка бакетов.

#### Тесты обновления и валидации

- **`test_vector_moves_between_buckets_on_update`**: Проверяет, что при обновлении вектора (изменении embedding) он перемещается в другой бакет, а в старом больше не содержится.

- **`test_vector_dimension_validation`**: Проверяет, что добавление вектора с неправильной размерностью вызывает ошибку, а с правильной — проходит успешно. Убеждается в корректности валидации размерности.

- **`test_empty_bucket_removal_on_vector_update`**: Проверяет, что при обновлении вектора пустые бакеты автоматически удаляются из системы.

- **`test_empty_bucket_removal_on_vector_deletion`**: Проверяет, что при удалении вектора пустые бакеты автоматически удаляются из системы.

## 🔍 API Reference

### CollectionController

#### Основные методы

- `add_collection(name, lsh_metric, vector_dimension)` - Создание коллекции
- `add_vector(collection_name, embedding, metadata)` - Добавление вектора
- `search_similar(collection_name, query, k)` - Поиск похожих векторов
- `update_vector(collection_name, vector_id, embedding, metadata)` - Обновление вектора
- `delete_vector(collection_name, vector_id)` - Удаление вектора

#### Управление коллекциями

- `get_collection(name)` - Получение коллекции
- `get_all_collections()` - Получение всех коллекций
- `delete_collection(name)` - Удаление коллекции
- `dump_one(collection)` - Сохранение коллекции
- `load_one(name)` - Загрузка коллекции

### BucketController

- `add_vector(embedding, metadata)` - Добавление вектора в бакет
- `get_bucket(id)` - Получение бакета по ID
- `get_all_buckets()` - Получение всех бакетов
- `find_similar(query, k)` - Поиск в одном бакете (используется если размер бакета >= k)
- `find_similar_multi_bucket(query, k)` - Поиск в нескольких бакетах (используется если размер бакета < k)
- `update_vector(vector_id, embedding, metadata)` - Обновление вектора

#### Умный поиск похожих векторов
Метод `find_similar` в коллекции автоматически выбирает оптимальную стратегию:
- **Если размер бакета >= k**: использует `find_similar` (быстрый поиск в одном бакете)
- **Если размер бакета < k**: использует `find_similar_multi_bucket` (поиск в 3 ближайших бакетах)

### VectorController

- `add_vector(embedding, metadata, vector_id, vector)` - Универсальное добавление
- `get_vector_by_id(id)` - Получение вектора по ID
- `remove_vector(id)` - Удаление вектора
- `find_most_similar(query, k)` - Поиск похожих векторов

---

*Файл `README.md` сгенерирован с помощью AI, поэтому не серчайте 🙂*