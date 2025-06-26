import sqlite3
import json
import logging
import emoji
from typing import List, Optional
from elasticsearch import Elasticsearch
from openai import OpenAI
from config_openai import OPENAI_API_KEY
from config import ES_HOST, ES_PORT

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ElasticsearchManager:
    """Класс для управления поиском мемов через Elasticsearch и OpenAI."""
        
    def __init__(self, db_path: str = 'memes.db'):
        """
        Инициализация параметров.
        """
        self.db_path = db_path
        self.es = Elasticsearch(
            f"http://{ES_HOST}:{ES_PORT}",
            timeout=30,
            max_retries=3,
            retry_on_timeout=True
        )
        self.client = OpenAI(api_key=OPENAI_API_KEY)
        self.embedding_model = "text-embedding-3-small"
        self.index_name = "memes_index"
        
        try:
            if not self.es.ping():
                raise ConnectionError("Не удалось подключиться к Elasticsearch")
            logger.info("Подключение к Elasticsearch успешно")
        except Exception as e:
            logger.error(f"Ошибка подключения к Elasticsearch: {e}")
            raise

    def _is_emoji_only(self, text: str) -> bool:
        """
        Проверяет, состоит ли строка только из эмоджи.
        """
        return all(char in emoji.EMOJI_DATA for char in text.strip())

    def _translate_emoji_to_text(self, emoji_text: str) -> str:
        """
        Конвертирует эмоджи в текстовые описания на английском.
        """
        return emoji.demojize(emoji_text).replace("_", " ").replace(":", "")

    def initialize_elasticsearch(self) -> None:
        """Создает индекс в Elasticsearch, если его не существует."""
        try:
            if not self.es.indices.exists(index=self.index_name):
                mapping = {
                    "mappings": {
                        "properties": {
                            "db_id": {"type": "keyword"},
                            "embedding": {
                                "type": "dense_vector",
                                "dims": 1536,
                                "index": True,
                                "similarity": "cosine"
                            }
                        }
                    }
                }
                self.es.indices.create(index=self.index_name, body=mapping)
                logger.info(f"Индекс {self.index_name} создан")
        except Exception as e:
            logger.error(f"Ошибка при создании индекса: {e}")
            raise
    
    def sync_db_to_elasticsearch(self) -> None:
        """Синхронизирует эмбеддинги из SQLite в Elasticsearch."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                cursor.execute("""
                    SELECT id, embedding 
                    FROM memes 
                    WHERE embedding IS NOT NULL
                """)
                
                for row in cursor:
                    try:
                        meme_id = row['id']
                        embedding_json = row['embedding']
                        
                        if not embedding_json:
                            logger.warning(f"Пустой embedding для мема {meme_id}")
                            continue
                            
                        embedding = json.loads(embedding_json)
                        if not isinstance(embedding, list) or len(embedding) != 1536:
                            logger.warning(f"Неверный формат эмбеддинга для мема {meme_id}")
                            continue
                        
                        doc = {
                            "db_id": str(meme_id),
                            "embedding": embedding
                        }
                        
                        if not self.es.exists(index=self.index_name, id=meme_id):
                            self.es.index(
                                index=self.index_name,
                                id=meme_id,
                                body=doc,
                                refresh=True
                            )
                            logger.debug(f"Добавлен мем {meme_id} в Elasticsearch")
                            
                    except json.JSONDecodeError:
                        logger.warning(f"Невалидный JSON в embedding для мема {meme_id}")
                        continue
                    except Exception as e:
                        logger.error(f"Ошибка обработки мема {meme_id}: {e}")
                        continue
                        
        except sqlite3.Error as e:
            logger.error(f"Ошибка SQLite: {e}")
            raise
        except Exception as e:
            logger.error(f"Неожиданная ошибка при синхронизации: {e}")
            raise
    
    def search_with_hybrid(self, query: str, k: int = 5) -> List[dict]:
        '''Гибридный поиск'''
        if self._is_emoji_only(query):
            processed_query = self._translate_emoji_to_text(query)
            logger.info(f"Эмодзи преобразованы: '{query}' -> '{processed_query}'")
        else:
            processed_query = query

        text_results = self._search_text_fields(processed_query, k)
        
        vector_results = self._search_knn(processed_query, k)
        
        combined = self._combine_results(text_results, vector_results, k)
        
        return combined

def _search_text_fields(self, query: str, k: int) -> List[dict]:
    """Текстовый поиск по description и tags"""
    try:
        response = self.es.search(
            index=self.index_name,
            body={
                "query": {
                    "multi_match": {
                        "query": query,
                        "fields": ["description", "tags"],
                        "fuzziness": "AUTO"
                    }
                },
                "size": k
            }
        )
        return self._format_es_results(response)
    except Exception as e:
        logger.error(f"Ошибка текстового поиска: {e}")
        return []

def _search_knn(self, query: str, k: int) -> List[dict]:
    """Векторный поиск через KNN"""
    embedding = self.get_embedding(query)
    if not embedding:
        return []

    try:
        response = self.es.search(
            index=self.index_name,
            body={
                "knn": {
                    "field": "embedding",
                    "query_vector": embedding,
                    "k": k,
                    "num_candidates": 100
                }
            }
        )
        return self._format_es_results(response)
    except Exception as e:
        logger.error(f"Ошибка KNN-поиска: {e}")
        return []

def _format_es_results(self, es_response) -> List[dict]:
    """Форматирует результаты из Elasticsearch."""
    conn = sqlite3.connect(self.db_path)
    cursor = conn.cursor()
    results = []
    
    for hit in es_response['hits']['hits']:
        meme_id = hit['_source']['db_id']
        cursor.execute(
            "SELECT name, image, description, tags FROM memes WHERE id = ?",
            (meme_id,)
        )
        row = cursor.fetchone()
        if row:
            results.append({
                'id': meme_id,
                'score': hit.get('_score', 0),
                'name': row[0] or "",
                'image': row[1] or "",
                'description': row[2] or "",
                'tags': row[3] or ""
            })
    
    conn.close()
    return results

def _combine_results(self, text_results: List[dict], vector_results: List[dict], k: int) -> List[dict]:
    """
    Объединяет и ранжирует результаты из двух методов поиска.

       - Уникальные результаты объединяются
       - Дубликаты сохраняются с максимальным score
       - Сортировка по убыванию score 
    """
    combined = {}
    
    for item in text_results:
        combined[item['id']] = item

    for item in vector_results:
        if item['id'] in combined:
            combined[item['id']]['score'] = max(
                combined[item['id']]['score'],
                item['score']
            )
        else:
            combined[item['id']] = item
    
    return sorted(combined.values(), key=lambda x: x['score'], reverse=True)[:k]
    def get_embedding(self, text: str) -> Optional[List[float]]:
        """
        Генерирует эмбеддинг для текста через OpenAI API.
        """
        if not text.strip():
            return None
            
        try:
            response = self.client.embeddings.create(
                model=self.embedding_model,
                input=text,
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"Ошибка векторизации текста '{text[:50]}...': {e}")
            return None
