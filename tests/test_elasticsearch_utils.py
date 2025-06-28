import pytest
import sqlite3
import json
from unittest.mock import patch, MagicMock, ANY
from elasticsearch_utils import ElasticsearchManager


@pytest.fixture
def manager():
    """
    Фикстура для создания экземпляра ElasticsearchManager.

    Мокирует внешние зависимости (Elasticsearch, OpenAI) в конструкторе,
    чтобы избежать реальных сетевых вызовов при инициализации объекта.
    """
    with patch('elasticsearch_utils.Elasticsearch') as mock_es_class, \
         patch('elasticsearch_utils.OpenAI') as mock_openai_class:
        
        mock_es_instance = mock_es_class.return_value
        mock_es_instance.ping.return_value = True

        manager_instance = ElasticsearchManager(db_path='fake.db')

        manager_instance.es = mock_es_instance
        manager_instance.client = mock_openai_class.return_value
        manager_instance.index_name = "test_index"

    return manager_instance


def test_initialize_elasticsearch_creates_index_if_not_exists(manager):
    """
    Проверяет, что индекс создается, если он еще не существует.
    """
    manager.es.indices.exists.return_value = False
    manager.initialize_elasticsearch()
    manager.es.indices.create.assert_called_once_with(index="test_index", body=ANY)


def test_initialize_elasticsearch_does_not_create_if_exists(manager):
    """
    Проверяет, что индекс НЕ создается, если он уже существует.
    """
    manager.es.indices.exists.return_value = True
    manager.initialize_elasticsearch()
    manager.es.indices.create.assert_not_called()


def test_sync_db_to_elasticsearch_with_in_memory_db(manager):
    """
    Проверяет синхронизацию с БД, используя временную БД в памяти.
    """
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()
    cursor.execute('CREATE TABLE memes (id INTEGER PRIMARY KEY, embedding TEXT);')
    
    test_data = [
        (1, json.dumps([0.1] * 1536)),  
        (2, json.dumps([0.2] * 1536)),  
        (3, 'невалидный-json'),           
        (4, None)                       
    ]
    cursor.executemany("INSERT INTO memes (id, embedding) VALUES (?, ?)", test_data)
    conn.commit()

    with patch('elasticsearch_utils.sqlite3.connect', return_value=conn):
        manager.es.exists.side_effect = [False, True] 
        manager.db_path = ':memory:' 
        manager.sync_db_to_elasticsearch()

    assert manager.es.exists.call_count == 2
    manager.es.index.assert_called_once_with(
        index="test_index",
        id=1,
        body={'db_id': '1', 'embedding': [0.1] * 1536},
        refresh=True
    )
    conn.close()


# @patch('sqlite3.connect')
# def test_search_with_hybrid_happy_path(mock_sqlite_connect, manager):
#     """
#     Проверяет полный успешный сценарий поиска: от OpenAI до ответа.
#     """
#     mock_embedding = [0.9] * 1536
#     manager.get_embedding = MagicMock(return_value=mock_embedding)

#     manager.es.search.return_value = {
#         'hits': {'hits': [{'_source': {'db_id': '101'}, '_score': 0.95}]}
#     }

#     mock_cursor = mock_sqlite_connect.return_value.cursor.return_value
#     mock_cursor.fetchone.return_value = ('Meme 101', 'img.png', 'Desc', 'tags')

#     results = manager.search_with_hybrid("тестовый запрос", k=1)

#     manager.es.search.assert_called_once()
#     assert manager.es.search.call_args[1]['body']['knn']['query_vector'] == mock_embedding

#     mock_cursor.execute.assert_called_once_with(
#         "SELECT name, image, description, tags FROM memes WHERE id = ?",
#         ('101',)
#     )
#     assert len(results) == 1
#     assert results[0]['id'] == '101'
#     assert results[0]['name'] == 'Meme 101'


# def test_search_with_hybrid_no_embedding(manager):
#     """
#     Проверяет, что поиск прерывается, если не удалось получить эмбеддинг.
#     """
#     manager.get_embedding = MagicMock(return_value=None)
#     results = manager.search_with_hybrid("пустой запрос")

#     manager.es.search.assert_not_called()
#     assert results == []
