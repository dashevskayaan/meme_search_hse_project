import sqlite3
import json
import time
import requests
from PIL import Image
from io import BytesIO
import torch
import open_clip

model, _, preprocess = open_clip.create_model_and_transforms(
    model_name='ViT-B-32',
    pretrained='laion2b_s34b_b79k'
)
model.eval()

DB_PATH = "memes.db"
BATCH_PAUSE = 50  
PAUSE_SECONDS = 15 

def get_clip_image_embedding(url: str) -> list:
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        image = Image.open(BytesIO(response.content)).convert("RGB")
        image_tensor = preprocess(image).unsqueeze(0)

        with torch.no_grad():
            embedding = model.encode_image(image_tensor).squeeze().tolist()

        return embedding
    except Exception as e:
        print(f"[!] Ошибка обработки {url}: {e}")
        return None

def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        cursor.execute("ALTER TABLE memes ADD COLUMN image_embedding TEXT")
        conn.commit()
        print("[+] Добавлена колонка image_embedding.")
    except sqlite3.OperationalError:
        print("[i] Колонка image_embedding уже существует.")

    cursor.execute("SELECT id, image FROM memes WHERE image IS NOT NULL AND image_embedding IS NULL")
    memes = cursor.fetchall()
    print(f"[i] Обработка {len(memes)} мемов...")

    for i, (meme_id, url) in enumerate(memes, 1):
        print(f"→ Мем {meme_id} [{url}]")

        embedding = get_clip_image_embedding(url)
        if embedding:
            cursor.execute(
                "UPDATE memes SET image_embedding = ? WHERE id = ?",
                (json.dumps(embedding), meme_id)
            )
            conn.commit()
            print(f"[✓] Мем {meme_id} обновлён.")
        else:
            print(f"[✗] Пропуск мема {meme_id}.")

        if i % BATCH_PAUSE == 0:
            print(f"[⏸] Обработано {i} мемов — пауза {PAUSE_SECONDS} секунд...\n")
            time.sleep(PAUSE_SECONDS)

        time.sleep(0.5)  

    conn.close()
    print("[✓] Всё готово.")

if __name__ == "__main__":
    main()
