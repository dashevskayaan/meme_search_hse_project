from minio import Minio
import json
import os

minio_client = Minio(
    "s3.ru1.storage.beget.cloud",
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=True
)

def get_object_urls(bucket_name: str) -> dict:
    """
    Получает URL объектов из MinIO хранилища и возвращает их в виде словаря.
    Получает список всех объектов в указанном бакете, сортирует их по имени
    и генерирует публичные URL для доступа к каждому объекту.
    """
    try:
        objects = minio_client.list_objects(bucket_name, recursive=True)
        sorted_objects = sorted(objects, key=lambda x: x.object_name)
        
        storage_links = {}
        for obj in sorted_objects:
            url = minio_client.presigned_get_object(bucket_name, obj.object_name)
            file_number = obj.object_name.split('.')[0]
            storage_links[file_number] = url
        
        return storage_links
    
    except Exception as e:
        print(f"Ошибка при получении URL из хранилища: {e}")
        return {}

def update_json_with_links(json_file_path: str, storage_links: dict) -> bool:
    """
    Обновляет JSON файл, добавляя ссылки на изображения из MinIO.
    Читает JSON файл построчно, находит записи с соответствующими ID
    и добавляет/обновляет поле 'images' с presigned URL из storage_links.
    """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        

        updated_lines = []
        for line in lines:
            if line.startswith('{"id":'):
                data = json.loads(line)
                meme_id = data['id']
                if meme_id in storage_links:
                    data['images'] = storage_links[meme_id]
                updated_lines.append(json.dumps(data, ensure_ascii=False))
            else:
                updated_lines.append(line.strip())
        
        with open(json_file_path, 'w', encoding='utf-8') as f:
            for line in updated_lines:
                f.write(line + '\n')
        
        return True
    
    except Exception as e:
        print(f"Ошибка при обновлении JSON: {e}")
        return False

def main() -> None:
    """
    Основная функция для выполнения скрипта.
    Получает ссылки на объекты из хранилища и обновляет JSON файл
    этими ссылками. Выводит результат выполнения операций.
    """
    storage_links = get_object_urls(bucket_name="25e18a90d3c1-memes-base")
    
    if storage_links:
        update_json_with_links(json_file_path=os.path.join("data_base", "memes_base.json"), storage_links=storage_links)

if __name__ == "__main__":
    main()