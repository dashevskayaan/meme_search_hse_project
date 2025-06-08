import sqlite3
from elasticsearch import Elasticsearch
from openai import OpenAI
import json
from config import OPENAI_API_KEY, ES_HOST, ES_PORT

class ElasticsearchManager:
    def __init__(self, db_path='memes.db'):
        self.db_path = db_path
        self.es = Elasticsearch([{'host': ES_HOST, 'port': ES_PORT}])
        self.client = OpenAI(api_key=OPENAI_API_KEY)
        self.embedding_model = "text-embedding-3-small"
        self.index_name = "memes_index"
        
    def initialize_elasticsearch(self):
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
    
    def sync_db_to_elasticsearch(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT id, embedding FROM memes WHERE embedding IS NOT NULL")
        rows = cursor.fetchall()
        
        for meme_id, embedding_json in rows:
            if not self.es.exists(index=self.index_name, id=meme_id):
                embedding = json.loads(embedding_json)
                doc = {
                    "db_id": str(meme_id),
                    "embedding": embedding
                }
                self.es.index(index=self.index_name, id=meme_id, body=doc)
        
        conn.close()
    
    def search_with_hybrid(self, query: str, k: int = 5):
        query_embedding = self.get_embedding(query)
        if not query_embedding:
            return []
        
        knn_query = {
            "knn": {
                "field": "embedding",
                "query_vector": query_embedding,
                "k": k,
                "num_candidates": 100
            },
            "_source": ["db_id"]
        }
        
        try:
            es_response = self.es.search(index=self.index_name, body=knn_query)
            hits = es_response['hits']['hits']
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            results = []
            for hit in hits:
                meme_id = hit['_source']['db_id']
                cursor.execute(
                    "SELECT description, file_id FROM memes WHERE id = ?",
                    (meme_id,)
                )
                row = cursor.fetchone()
                if row:
                    description, file_id = row
                    results.append({
                        'id': meme_id,
                        'score': hit['_score'],
                        'description': description,
                        'file_id': file_id
                    })
            
            conn.close()
            return results
            
        except Exception as e:
            print(f"Ошибка при поиске в бд: {e}")
            return []
    
    def get_embedding(self, text: str):
        try:
            response = self.client.embeddings.create(
                model=self.embedding_model,
                input=text,
            )
            return response.data[0].embedding
        except Exception as e:
            print(f"Ошибка векторизации: {e}")
            return None
