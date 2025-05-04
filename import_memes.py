import sqlite3
import json
import os

current_dir = os.path.dirname(__file__)
json_path = os.path.join(current_dir, 'data_base', 'memes_base.json')#это рассчитано на структуру, в котрой хранятся сейчас файлы на ГХ 
                                                                    #(json-ы хранятся в папке data_base)            
conn = sqlite3.connect('memes.db')
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS memes (
    id INTEGER PRIMARY KEY,
    name TEXT,
    image TEXT,
    description TEXT,
    tags TEXT
);
''')

with open(json_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

for i in range(1, len(lines)):
    line = lines[i].strip()
    if not line:
        continue
    try:
        meme = json.loads(line)
        image = meme.get('images', '-')
        #ON CONFLICT(id) DO UPDATE SET более мягкое, чем insert or replace (должно быть по крайней мере...)

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
            meme['name'],
            image,
            meme['description'],
            meme['tags']
        ))

    except Exception as e:
        print(f"Ошибка на строке {i}: {e}")

conn.commit()
conn.close()
print("Готово: база обновлена")
