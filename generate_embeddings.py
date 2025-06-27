from openai import OpenAI
import sqlite3
import json
from config_openai import OPENAI_API_KEY

client = OpenAI(api_key=OPENAI_API_KEY)

DB_PATH = 'memes.db'
EMBEDDING_MODEL = "text-embedding-3-small"
LIMIT = 5

def get_embedding(text: str) -> list:
    try:
        response = client.embeddings.create(
            model=EMBEDDING_MODEL,
            input=text,
        )
        return response.data[0].embedding
    except Exception as e:
        print(f"[!] Ошибка при получении эмбеддинга: {e}")
        return None

def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute("ALTER TABLE memes ADD COLUMN embedding TEXT")
        conn.commit()
        print("[+] Колонка 'embedding' добавлена.")
    except sqlite3.OperationalError:
        print("[i] Колонка 'embedding' уже существует.")

    cursor.execute("""
        SELECT id, description FROM memes 
        WHERE embedding IS NULL AND description IS NOT NULL 
        LIMIT ?
    """, (LIMIT,))
    rows = cursor.fetchall()

    print(f"[i] Найдено {len(rows)} мемов для обработки.")

    for meme_id, desc in rows:
        text = desc.strip()
        if not text:
            continue

        embedding = get_embedding(text)
        if embedding:
            cursor.execute(
                "UPDATE memes SET embedding = ? WHERE id = ?",
                (json.dumps(embedding), meme_id)
            )
            conn.commit()
            print(f"[+] Мем {meme_id} обновлён.")

    conn.close()
    print("[✓] Готово.")

if __name__ == "__main__":
    main()
