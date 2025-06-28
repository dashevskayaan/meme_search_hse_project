import sqlite3
import json

def update_database_from_jsonl(db_path: str, jsonl_path: str):
    """
    Читает JSONL файл и обновляет базу данных SQLite.

    Создает таблицу 'memes', если она не существует, а затем
    вставляет или обновляет записи на основе данных из файла.

    Args:
        db_path (str): Путь к файлу базы данных SQLite.
        jsonl_path (str): Путь к файлу с данными в формате JSON Lines.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS memes (
        id INTEGER PRIMARY KEY,
        name TEXT,
        image TEXT,
        description TEXT,
        tags TEXT,
        embedding TEXT
    );
    ''')

    try:
        with open(jsonl_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"[!] Ошибка: Файл не найден по пути {jsonl_path}")
        conn.close()
        return

    for i, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue
        try:
            meme = json.loads(line)
            image = meme.get('images', '-')

            cursor.execute('''
                INSERT INTO memes (id, name, image, description, tags)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    image = excluded.image,
                    description = excluded.description,
                    tags = excluded.tags
            ''', (
                int(meme['id']),
                meme.get('name', ''),
                image,
                meme.get('description', ''),
                meme.get('tags', '')
            ))

        except (json.JSONDecodeError, KeyError) as e:
            print(f"Ошибка парсинга или отсутствия ключа на строке {i + 1}: {e}")
        except Exception as e:
            print(f"Неизвестная ошибка на строке {i + 1}: {e}")

    conn.commit()
    conn.close()
    print("Готово: база обновлена")

if __name__ == "__main__":
    DB_PATH = 'memes.db'
    JSON_PATH = './data_base/memes_base.json'
    update_database_from_jsonl(db_path=DB_PATH, jsonl_path=JSON_PATH)
