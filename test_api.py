"""
Скрипт для тестирования HTTP API VectorDB
Проверяет все методы ConnectionController после создания базы в main.rs
"""

import requests
import json
import numpy as np
from typing import List, Dict, Optional
import time


class VectorDBClient:
    """Клиент для работы с VectorDB HTTP API"""
    
    def __init__(self, base_url: str = "http://127.0.0.1:8080"):
        self.base_url = base_url
        self.session = requests.Session()
        
    def _make_request(self, endpoint: str, data: dict, method: str = "POST") -> dict:
        """Выполняет HTTP запрос к API"""
        url = f"{self.base_url}{endpoint}"
        try:
            if method == "POST":
                response = self.session.post(url, json=data, timeout=10)
            elif method == "GET":
                response = self.session.get(url, timeout=10)
            else:
                raise ValueError(f"Неподдерживаемый метод: {method}")
            
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"❌ Ошибка запроса к {endpoint}: {e}")
            return {"status": "error", "message": str(e)}
    
    def add_collection(self, name: str, metric: str = "euclidean", dimension: int = 384) -> dict:
        """Создает новую коллекцию"""
        data = {
            "name": name,
            "metric": metric,
            "dimension": dimension
        }
        return self._make_request("/collection", data)
    
    def delete_collection(self, name: str) -> dict:
        """Удаляет коллекцию"""
        data = {"name": name}
        return self._make_request("/collection/delete", data)
    
    def add_vector(self, collection: str, embedding: List[float], 
                   metadata: Optional[Dict[str, str]] = None) -> dict:
        """Добавляет вектор в коллекцию"""
        data = {
            "collection": collection,
            "embedding": embedding,
            "metadata": metadata or {}
        }
        return self._make_request("/vector", data)
    
    def update_vector(self, collection: str, vector_id: int,
                     embedding: Optional[List[float]] = None,
                     metadata: Optional[Dict[str, str]] = None) -> dict:
        """Обновляет вектор"""
        data = {
            "collection": collection,
            "vector_id": vector_id,
            "embedding": embedding,
            "metadata": metadata
        }
        return self._make_request("/vector/update", data)
    
    def get_vector(self, collection: str, vector_id: int) -> dict:
        """Получает вектор по ID"""
        data = {
            "collection": collection,
            "vector_id": vector_id
        }
        return self._make_request("/vector/get", data)
    
    def delete_vector(self, collection: str, vector_id: int) -> dict:
        """Удаляет вектор"""
        data = {
            "collection": collection,
            "vector_id": vector_id
        }
        return self._make_request("/vector/delete", data)
    
    def filter_by_metadata(self, collection: str, filters: Dict[str, str]) -> dict:
        """Фильтрует векторы по метаданным"""
        data = {
            "collection": collection,
            "filters": filters
        }
        return self._make_request("/vector/filter", data)
    
    def find_similar(self, collection: str, query: List[float], k: int = 5) -> dict:
        """Ищет похожие векторы"""
        data = {
            "collection": collection,
            "query": query,
            "k": k
        }
        return self._make_request("/vector/similar", data)
    
    def stop_server(self) -> dict:
        """Останавливает сервер"""
        return self._make_request("/stop", {})


def generate_random_embedding(dimension: int = 384) -> List[float]:
    """Генерирует случайный вектор заданной размерности"""
    return np.random.randn(dimension).tolist()


def print_response(operation: str, response: dict):
    """Красиво выводит ответ от API"""
    status = response.get("status", "unknown")
    if status == "ok":
        print(f"✅ {operation}: Успешно")
        if response.get("data"):
            print(f"   Данные: {json.dumps(response['data'], ensure_ascii=False, indent=2)}")
    else:
        print(f"❌ {operation}: Ошибка")
        if response.get("message"):
            print(f"   Сообщение: {response['message']}")


def test_api():
    """Тестирует все методы API"""
    print("=" * 60)
    print("🚀 Тестирование VectorDB HTTP API")
    print("=" * 60)
    
    client = VectorDBClient()
    
    # Проверяем, что сервер запущен
    print("\n📡 Проверка подключения к серверу...")
    time.sleep(1)
    
    # 1. Работа с коллекциями, созданными в main.rs
    print("\n" + "=" * 60)
    print("📂 Проверка существующей коллекции 'my_documents'")
    print("=" * 60)
    
    collection_name = "my_documents"
    
    # Попытка получить вектор из существующей коллекции
    # (предполагается, что main.rs уже создал коллекцию и добавил векторы)
    print("\n🔍 Попытка получить информацию о существующих векторах...")
    
    # 2. Создание новой тестовой коллекции
    print("\n" + "=" * 60)
    print("📦 Создание новой тестовой коллекции")
    print("=" * 60)
    
    test_collection = "test_collection"
    response = client.add_collection(test_collection, "euclidean", 128)
    print_response("Создание коллекции", response)
    
    # 3. Добавление векторов
    print("\n" + "=" * 60)
    print("➕ Добавление векторов")
    print("=" * 60)
    
    vector_ids = []
    
    # Добавляем несколько векторов с разными метаданными
    test_data = [
        {
            "embedding": generate_random_embedding(128),
            "metadata": {"category": "test", "type": "document", "index": "0"}
        },
        {
            "embedding": generate_random_embedding(128),
            "metadata": {"category": "test", "type": "image", "index": "1"}
        },
        {
            "embedding": generate_random_embedding(128),
            "metadata": {"category": "production", "type": "document", "index": "2"}
        }
    ]
    
    for i, data in enumerate(test_data):
        response = client.add_vector(test_collection, data["embedding"], data["metadata"])
        print_response(f"Добавление вектора {i+1}", response)
        if response.get("status") == "ok" and response.get("data"):
            vector_ids.append(response["data"].get("id"))
    
    print(f"\n📝 Добавлено векторов: {len(vector_ids)}")
    print(f"   ID векторов: {vector_ids}")
    
    # 4. Получение вектора по ID
    if vector_ids:
        print("\n" + "=" * 60)
        print("🔍 Получение вектора по ID")
        print("=" * 60)
        
        response = client.get_vector(test_collection, vector_ids[0])
        print_response(f"Получение вектора ID={vector_ids[0]}", response)
    
    # 5. Фильтрация по метаданным
    print("\n" + "=" * 60)
    print("🔎 Фильтрация векторов по метаданным")
    print("=" * 60)
    
    # Фильтруем по category=test
    response = client.filter_by_metadata(test_collection, {"category": "test"})
    print_response("Фильтрация по category=test", response)
    
    # Фильтруем по type=document
    response = client.filter_by_metadata(test_collection, {"type": "document"})
    print_response("Фильтрация по type=document", response)
    
    # 6. Поиск похожих векторов
    print("\n" + "=" * 60)
    print("🎯 Поиск похожих векторов")
    print("=" * 60)
    
    query_vector = generate_random_embedding(128)
    response = client.find_similar(test_collection, query_vector, k=3)
    print_response("Поиск 3 похожих векторов", response)
    
    # 7. Обновление вектора
    if vector_ids:
        print("\n" + "=" * 60)
        print("✏️ Обновление вектора")
        print("=" * 60)
        
        # Обновляем только метаданные
        new_metadata = {"category": "updated", "type": "modified", "timestamp": str(time.time())}
        response = client.update_vector(test_collection, vector_ids[0], metadata=new_metadata)
        print_response(f"Обновление метаданных вектора ID={vector_ids[0]}", response)
        
        # Проверяем обновление
        response = client.get_vector(test_collection, vector_ids[0])
        print_response(f"Проверка обновленного вектора ID={vector_ids[0]}", response)
    
    # 8. Удаление вектора
    if vector_ids and len(vector_ids) > 1:
        print("\n" + "=" * 60)
        print("🗑️ Удаление вектора")
        print("=" * 60)
        
        response = client.delete_vector(test_collection, vector_ids[-1])
        print_response(f"Удаление вектора ID={vector_ids[-1]}", response)
    
    # 9. Удаление коллекции
    print("\n" + "=" * 60)
    print("🗑️ Удаление тестовой коллекции")
    print("=" * 60)
    
    response = client.delete_collection(test_collection)
    print_response("Удаление коллекции", response)
    
    # 10. Работа с существующей коллекцией my_documents (из main.rs)
    print("\n" + "=" * 60)
    print("📚 Проверка коллекции из main.rs")
    print("=" * 60)
    
    # Добавляем новый вектор в существующую коллекцию
    print("\n➕ Добавление вектора в коллекцию 'my_documents'...")
    new_embedding = generate_random_embedding(384)  # Размерность из main.rs
    new_metadata = {"category": "document", "source": "python_test", "timestamp": str(time.time())}
    response = client.add_vector("my_documents", new_embedding, new_metadata)
    print_response("Добавление вектора в my_documents", response)
    
    if response.get("status") == "ok" and response.get("data"):
        new_id = response["data"].get("id")
        
        # Ищем похожие векторы
        print("\n🎯 Поиск похожих векторов в my_documents...")
        response = client.find_similar("my_documents", new_embedding, k=3)
        print_response("Поиск похожих векторов", response)
        
        # Фильтруем по метаданным
        print("\n🔎 Фильтрация векторов в my_documents...")
        response = client.filter_by_metadata("my_documents", {"category": "document"})
        print_response("Фильтрация по category=document", response)
    
    print("\n" + "=" * 60)
    print("✅ Тестирование завершено!")
    print("=" * 60)
    
    # Останавливаем сервер
    print("\n🛑 Отправка команды остановки сервера...")
    response = client.stop_server()
    print_response("Остановка сервера", response)
    
    print("\n💤 Ожидание корректного завершения сервера...")
    time.sleep(2)


def main():
    """Главная функция"""
    try:
        test_api()
    except KeyboardInterrupt:
        print("\n\n⚠️ Тестирование прервано пользователем")
    except Exception as e:
        print(f"\n\n❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()