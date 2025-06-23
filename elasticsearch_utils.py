import sqlite3
import json
import logging
from typing import List, Dict, Optional
from elasticsearch import Elasticsearch
from openai import OpenAI
from config_openai import OPENAI_API_KEY
from config import ES_HOST, ES_PORT

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ElasticsearchManager:
    def __init__(self, db_path: str = 'memes.db'):
        self.db_path = db_path
        self.es = Elasticsearch(
            [{'host': ES_HOST, 'port': ES_PORT}],
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
        except Exception as e:
            logger.error(f"Ошибка подключения к Elasticsearch: {e}")
            raise

    def initialize_elasticsearch(self) -> None:
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
    
    def search_with_hybrid(self, query: str, k: int = 5):
        query_embedding = self.get_embedding(query)
        if not query_embedding:
            return []

        try:
            es_response = self.es.search(
                index=self.index_name,
                body={
                    "knn": {
                        "field": "embedding",
                        "query_vector": query_embedding,
                        "k": k,
                        "num_candidates": 100
                    },
                    "_source": ["db_id"]
                }
            )
            
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
                    name, image, description, tags = row
                    results.append({
                        'id': meme_id,
                        'score': hit['_score'],
                        'name': name or "",
                        'image': image or "",
                        'description': description or "",
                        'tags': tags or ""
                    })
            
            conn.close()
            return sorted(results, key=lambda x: x['score'], reverse=True)[:k]
        
        except Exception as e:
            logger.error(f"Ошибка при поиске: {e}")
            return []
    
    def get_embedding(self, text: str) -> Optional[List[float]]:
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
