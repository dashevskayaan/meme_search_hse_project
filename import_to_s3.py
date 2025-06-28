from minio import Minio
import requests
import os
from urllib.parse import urlparse
import json
from dotenv import load_dotenv

load_dotenv()

minio_client = Minio(
    "s3.ru1.storage.beget.cloud",
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=True
)

def upload_with_minio(url: str, bucket_name: str, object_name: str) -> bool:
    """
    Загружает файл по URL в хранилище
    Скачивает файл по указанному URL и загружает его в указанный бакет
    с заданным именем объекта. Возвращает статус операции (True/False)
    """

    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        
        minio_client.put_object(
            bucket_name,
            object_name,
            response.raw,
            length=int(response.headers.get('content-length', 0)),
            content_type=response.headers.get('content-type')
        )
        return True
    except Exception as e:
        print(f"Ошибка при загрузке {url}: {e}")
        return False

def process_image_urls(file_path: str, bucket_name: str) -> None:
    """
    Обрабатывает файл с URL изображений и загружает их в хранилище
    Читает JSON-файл, содержащий URL изображений, извлекает ссылки на изображения
    и загружает каждое изображение в указанный бакет. Имена объектов
    формируются автоматически на основе порядка изображений в файле.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            image_urls = []
            for line in f:
                line = line.strip()
                if line and line.startswith('{"id":'):
                    try:
                        data = json.loads(line)
                        if 'images' in data and data['images']:
                            image_urls.append(data['images'])
                    except json.JSONDecodeError:
                        continue
        
        for index, url in enumerate(image_urls):
            parsed = urlparse(url)
            filename = os.path.basename(parsed.path)
            ext = os.path.splitext(filename)[1] or '.jpg'
            
            object_name = f"{index}{ext}"
            upload_with_minio(url, bucket_name, object_name)

    except Exception as e:
        print(f"Ошибка при обработке файла: {e}")

process_image_urls(
    file_path=os.path.join("data_base", "memes_base.json"),
    bucket_name="25e18a90d3c1-memes-base"
)
