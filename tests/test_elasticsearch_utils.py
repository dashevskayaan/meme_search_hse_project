import pytest
from unittest.mock import patch, MagicMock, ANY

from elasticsearch_utils import ElasticsearchManager


@pytest.fixture
def manager():
    """
    Фикстура, создающая экземпляр ElasticsearchManager для тестов.

    Заменяет внешние зависимости (Elasticsearch, OpenAI) и внутренние
    методы поиска (_search_text_fields, _search_knn) на моки (MagicMock)
    для полной изоляции тестируемой логики.
    """
    with patch('elasticsearch_utils.Elasticsearch') as mock_es_class, \
         patch('elasticsearch_utils.OpenAI') as mock_openai_class:
        
        mock_es_instance = mock_es_class.return_value
        mock_es_instance.ping.return_value = True

        manager_instance = ElasticsearchManager(db_path='fake.db')

        manager_instance.es = mock_es_instance
        manager_instance.client = mock_openai_class.return_value
        manager_instance.index_name = "test_index"
    
    manager_instance._search_text_fields = MagicMock()
    manager_instance._search_knn = MagicMock()

    return manager_instance


def test_initialize_elasticsearch_creates_index_if_not_exists(manager):
    """Проверяет, что индекс создается, если он не существует."""
    manager.es.indices.exists.return_value = False
    manager.initialize_elasticsearch()
    manager.es.indices.create.assert_called_once_with(index="test_index", body=ANY)


def test_initialize_elasticsearch_does_not_create_if_exists(manager):
    """Проверяет, что индекс НЕ создается, если он уже существует."""
    manager.es.indices.exists.return_value = True
    manager.initialize_elasticsearch()
    manager.es.indices.create.assert_not_called()


def test_hybrid_search_text_only_when_enough_results(manager):
    """
    Тестирует сценарий, когда текстовый поиск находит достаточно 
    результатов, и KNN-поиск не вызывается.
    """
    k = 3
    manager._search_text_fields.return_value = [
        {'id': 1, 'name': 'text_meme_1'},
        {'id': 2, 'name': 'text_meme_2'},
        {'id': 3, 'name': 'text_meme_3'},
        {'id': 4, 'name': 'text_meme_4'}
    ]

    results = manager.search_with_hybrid("тестовый запрос", k=k)

    manager._search_text_fields.assert_called_once_with("тестовый запрос", k * 2)
    manager._search_knn.assert_not_called()
    assert len(results) == k
    assert results[0]['id'] == 1
    assert results[2]['id'] == 3


def test_hybrid_search_supplements_with_knn_when_not_enough(manager):
    """
    Тестирует сценарий, когда текстовый поиск находит мало результатов,
    и они дополняются уникальными результатами из KNN-поиска.
    """
    k = 5
    manager._search_text_fields.return_value = [
        {'id': 101, 'name': 'text_meme_A'},
        {'id': 102, 'name': 'text_meme_B'}
    ]
    manager._search_knn.return_value = [
        {'id': 101, 'name': 'duplicate_meme'},
        {'id': 201, 'name': 'knn_meme_C'},
        {'id': 202, 'name': 'knn_meme_D'},
        {'id': 203, 'name': 'knn_meme_E'}
    ]
    
    results = manager.search_with_hybrid("другой запрос", k=k)

    manager._search_text_fields.assert_called_once_with("другой запрос", k * 2)
    manager._search_knn.assert_called_once_with("другой запрос", k)
    
    assert len(results) == k
    
    result_ids = [r['id'] for r in results]
    expected_ids = [101, 102, 201, 202, 203]
    assert result_ids == expected_ids


def test_hybrid_search_falls_back_to_knn_on_no_text_results(manager):
    """
    Тестирует сценарий, когда текстовый поиск ничего не находит,
    и поиск полностью переключается на KNN.
    """
    k = 3
    manager._search_text_fields.return_value = []
    manager._search_knn.return_value = [
        {'id': 301, 'name': 'knn_fallback_1'},
        {'id': 302, 'name': 'knn_fallback_2'},
        {'id': 303, 'name': 'knn_fallback_3'}
    ]

    results = manager.search_with_hybrid("несуществующий запрос", k=k)

    manager._search_text_fields.assert_called_once_with("несуществующий запрос", k * 2)
    manager._search_knn.assert_called_once_with("несуществующий запрос", k)
    
    assert len(results) == k
    assert results[0]['id'] == 301
