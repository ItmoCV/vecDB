#!/usr/bin/env python3
"""
Тест распределения коллекций по шардам
"""

import hashlib

def calculate_hash(collection_name):
    """Имитация Rust DefaultHasher (примерная)"""
    # В Rust используется DefaultHasher, который зависит от платформы
    # Используем Python hashlib для демонстрации
    hash_value = int(hashlib.sha256(collection_name.encode()).hexdigest(), 16)
    return hash_value

def get_shard_index(collection_name, num_shards=2):
    """Определяет индекс шарда для коллекции"""
    hash_val = calculate_hash(collection_name)
    shard_index = hash_val % num_shards
    return shard_index, hash_val

# Тестируем распределение разных имен коллекций
test_collections = [
    "my_documents",
    "users",
    "products",
    "orders",
    "images",
    "videos",
    "posts",
    "comments",
    "messages",
    "notifications",
    "analytics",
    "logs",
    "collection_0",
    "collection_1",
    "collection_2",
    "collection_3",
    "test",
    "demo",
]

print("=" * 80)
print("ТЕСТ РАСПРЕДЕЛЕНИЯ КОЛЛЕКЦИЙ ПО ШАРДАМ")
print("=" * 80)
print()

shard_distribution = {0: [], 1: []}

for collection_name in test_collections:
    shard_idx, hash_val = get_shard_index(collection_name)
    shard_id = f"shard{shard_idx + 1}"
    shard_distribution[shard_idx].append(collection_name)
    print(f"Collection: {collection_name:20} -> Hash: {hash_val:20} % 2 = {shard_idx} -> {shard_id}")

print()
print("=" * 80)
print("РЕЗУЛЬТАТЫ РАСПРЕДЕЛЕНИЯ:")
print("=" * 80)
print(f"Shard1 ({len(shard_distribution[0])} коллекций): {', '.join(shard_distribution[0])}")
print(f"Shard2 ({len(shard_distribution[1])} коллекций): {', '.join(shard_distribution[1])}")
print()
print(f"Баланс: {len(shard_distribution[0])} vs {len(shard_distribution[1])}")

