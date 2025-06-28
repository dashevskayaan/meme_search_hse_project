import sqlite3
import json
import logging
import emoji
from typing import List, Dict, Any
from elasticsearch import Elasticsearch, helpers
from openai import OpenAI
from config_openai import OPENAI_API_KEY
from config import ES_HOST, ES_PORT

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ElasticsearchManager:
    """
    Управление поиском мемов.

    Содержит методы для:
      - Приоритетного текстового поиска по тегам и описанию.
      - KNN-поиска по эмбеддингам 
      - Перевода смайликов в текст
    """

    def __init__(
        self,
        db_path: str = 'memes.db',
        index_name: str = 'memes_index',
        embedding_model: str = 'text-embedding-3-small',
        embedding_dim: int = 1536
    ):
        """
        Инициализирует соединение с Elasticsearch и клиент OpenAI.

        Args:
            db_path (str): Путь до SQLite базы данных с мемами (файл .db).
            index_name (str): Имя индекса в Elasticsearch для хранения мемов.
            embedding_model (str): Название модели OpenAI для создания эмбеддингов текста.
            embedding_dim (int): Размерность векторов эмбеддинга, используемая в индексе.

        Raises:
            ConnectionError: Если не удалось подключиться к Elasticsearch.
        """
        self.db_path = db_path
        self.index_name = index_name
        self.embedding_model = embedding_model
        self.embedding_dim = embedding_dim

        self.es = Elasticsearch(
            hosts=[f"http://{ES_HOST}:{ES_PORT}"],
            request_timeout=30,
            max_retries=3,
            retry_on_timeout=True
        )
        if not self.es.ping():
            raise ConnectionError("Не удалось подключиться к Elasticsearch")
        logger.info("Подключение к Elasticsearch успешно")

        # Клиент OpenAI для эмбеддингов
        self.client = OpenAI(api_key=OPENAI_API_KEY)

    def search_with_hybrid(self, query: str, k: int = 20, alpha: float = 0.2) -> List[Dict[str, Any]]:
        """
        Выполняет гибридный поиск: сначала текстовый, а при нехватке результатов — KNN-поиск.

        Логика:
          1. Запрос текстового поиска с расширенным размером (2*k).
          2. Если найдено >= k результатов — возвращает первые k.
          3. Если найдено < k — дополняет их результатами KNN-поиска, исключая дубли.
          4. Если текстовых результатов нет — возвращает чистый KNN-поиск.

        Args:
            query (str): Пользовательский запрос (текст или emoji).
            k (int): Максимальное число возвращаемых результатов.
            alpha (float): Вес KNN-результатов при смешивании (не используется в текущей логике).

        Returns:
            List[Dict[str, Any]]: Список документов с полями:
                - id: идентификатор из SQLite.
                - score: оценка релевантности.
                - name, image, description, tags: метаданные мема.
        """
        text_results = self._search_text_fields(query, k * 2)
        if text_results:
            if len(text_results) >= k:
                return text_results[:k]

            knn_results = self._search_knn(query, k)
            text_ids = {str(doc['id']) for doc in text_results}
            filtered = [doc for doc in knn_results if str(doc['id']) not in text_ids]
            return text_results + filtered[: k - len(text_results)]
        else:
            return self._search_knn(query, k)

    def _is_emoji_only(self, s: str) -> bool:
        """
        Проверяет, состоит ли строка исключительно из emoji.

        Args:
            s (str): Входная строка.

        Returns:
            bool: True, если все символы строки находятся в словаре emoji.EMOJI_DATA и строка не пуста.
        """
        return bool(s.strip()) and all(ch in emoji.EMOJI_DATA for ch in s.strip())

    def _translate_emoji_to_text(self, s: str) -> str:
        """
        Переводит emoji-строку в текстовую форму для эмбеддинга.

        Пример: "😊🎉" -> "smiling face party popper"

        Args:
            s (str): Строка, содержащая только emoji.

        Returns:
            str: Демодизированная строка без двоеточий и подчёркиваний.
        """
        return emoji.demojize(s).replace(':', '').replace('_', ' ')

    def initialize_elasticsearch(self) -> None:
        """
        Создаёт индекс в Elasticsearch, если он ещё не существует.

        Mappings:
          - db_id: integer
          - name, description, tags: text
          - image: keyword
          - image_embedding: dense_vector для KNN-поиска

        Returns:
            None
        """
        if not self.es.indices.exists(index=self.index_name):
            mapping = {
                "mappings": {
                    "properties": {
                        "db_id": {"type": "integer"},
                        "name": {"type": "text"},
                        "description": {"type": "text"},
                        "tags": {"type": "text"},
                        "image": {"type": "keyword"},
                        "image_embedding": {
                            "type": "dense_vector",
                            "dims": self.embedding_dim,
                            "index": True,
                            "similarity": "cosine"
                        }
                    }
                }
            }
            self.es.indices.create(index=self.index_name, body=mapping)

    def _search_text_fields(self, query: str, k: int) -> List[Dict[str, Any]]:
        """
        Выполняет текстовый поиск по полям tags, description и name.

        Args:
            query (str): Строка запроса.
            k (int): Количество возвращаемых результатов.

        Returns:
            List[Dict[str, Any]]: Список найденных документов с оценкой и метаданными.

        Исключения:
            При любой ошибке логирует ошибку и возвращает пустой список.
        """
        try:
            resp = self.es.search(
                index=self.index_name,
                body={
                    "size": k,
                    "query": {
                        "multi_match": {
                            "query": query,
                            "fields": ["tags", "description", "name"],
                            "fuzziness": 1 if len(query) > 3 else 0
                        }
                    }
                }
            )
            return [
                {"id": h['_source'].get('db_id') or h['_source'].get('id'),
                 "score": h['_score'],
                 **{f: h['_source'].get(f) for f in ['name', 'image', 'description', 'tags']}
                }
                for h in resp['hits']['hits']
            ]
        except Exception as e:
            logger.error(f"Ошибка текстового поиска: {e}")
            return []

    def _search_knn(self, query: str, k: int) -> List[Dict[str, Any]]:
        """
        Выполняет KNN-поиск по эмбеддингам.

        Логика:
          1. Если запрос только emoji — переводит в текст.
          2. Создает эмбеддинг с помощью OpenAI.
          3. Запускает KNN-запрос в Elasticsearch по полю image_embedding.

        Args:
            query (str): Запрос (текст или emoji).
            k (int): Количество возвращаемых кандидатов.

        Returns:
            List[Dict[str, Any]]: Список документов с оценкой и метаданными.
        """
        if self._is_emoji_only(query):
            query = self._translate_emoji_to_text(query)
            logger.info(f"Translate emoji for embedding: '{query}'")
        emb = self.client.embeddings.create(model=self.embedding_model, input=query).data[0].embedding
        print('Длина эмбеддинга для KNN:', len(emb))
        try:
            resp = self.es.search(
                index=self.index_name,
                body={
                    "size": k,
                    "query": {
                        "knn": {
                            "field": "image_embedding",
                            "query_vector": emb,
                            "num_candidates": 100
                        }
                    }
                }
            )
            return [
                {"id": h['_source'].get('db_id') or h['_source'].get('id'),
                 "score": h['_score'],
                 **{f: h['_source'].get(f) for f in ['name', 'image', 'description', 'tags']}
                }
                for h in resp['hits']['hits']
            ]
        except Exception as e:
            logger.error(f"Ошибка KNN-поиска: {e}")
            return []

    def search(self, query: str, k: int = 5) -> List[Dict[str, Any]]:
        """
        Основной метод поиска: текстовый или KNN в зависимости от типа запроса.

        Логика:
          - Если запрос состоит только из emoji — выполняет KNN-поиск.
          - Иначе: сначала текстовый поиск, при отсутствии результатов — KNN.

        Args:
            query (str): Запрос пользователя.
            k (int): Максимальное число результатов.

        Returns:
            List[Dict[str, Any]]: Релевантные мемы.
        """
        if self._is_emoji_only(query):
            translated = self._translate_emoji_to_text(query)
            logger.info(f"Emoji-запрос: '{query}' -> '{translated}'")
            return self._search_knn(translated, k)

        text_results = self._search_text_fields(query, k)
        if text_results:
            return text_results
        return self._search_knn(query, k)

    def sync_db_to_elasticsearch(self) -> None:
        """
        Синхронизирует данные из локальной SQLite БД в индекс Elasticsearch.

        Чтение:
          - Подключается к базе memes.db.
          - Выбирает записи с заполненным JSON-эмбеддингом.
          - Пропускает некорректные эмбеддинги.

        Загрузка:
          - Формирует bulk-операцию для Elasticsearch.
          - Загружает документы через helpers.bulk.

        Returns:
            None
        """
        actions = []
        print('Открываем базу:', self.db_path)
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            count = 0
            for row in conn.execute(
                "SELECT id, name, description, tags, image, embedding FROM memes WHERE embedding IS NOT NULL"
            ):
                try:
                    emb = json.loads(row['embedding'])
                except Exception as e:
                    print('Ошибка при чтении embedding:', e)
                    continue
                doc = {
                    "db_id": row['id'],
                    "name": row['name'],
                    "description": row['description'],
                    "tags": row['tags'],
                    "image": row['image'],
                    "image_embedding": emb
                }
                actions.append({"_index": self.index_name, "_id": row['id'], "_source": doc})
                count += 1
            print('Всего мемов для заливки:', count)
        if actions:
            print('Начинаем bulk...')
            helpers.bulk(self.es, actions)
            print(f"Загружено {len(actions)} документов")
        else:
            print('Нет документов для загрузки!')
