import pytest
from unittest.mock import patch, mock_open, MagicMock, call

from import_memes import update_database_from_jsonl

@patch('sqlite3.connect')
@patch('builtins.open', new_callable=mock_open)
def test_update_database_happy_path(mock_file_open, mock_db_connect):
    """
    Проверяет основной успешный сценарий обновления БД из JSONL файла.

    Убеждается, что для каждой валидной строки в файле вызывается
    корректный SQL-запрос на вставку/обновление.
    """
    json_line1 = '{"id": 1, "name": "Meme 1", "description": "Desc 1", "tags": "tag1"}'
    json_line2 = '{"id": 2, "name": "Meme 2", "images": "img2.png", "description": "Desc 2", "tags": "tag2"}'
    mock_file_content = f"{json_line1}\n{json_line2}"
    mock_file_open.return_value.readlines.return_value = mock_file_content.splitlines()

    mock_conn = mock_db_connect.return_value

    update_database_from_jsonl('fake_db.db', 'fake_data.jsonl')

    create_table_sql = '''
    CREATE TABLE IF NOT EXISTS memes (
        id INTEGER PRIMARY KEY,
        name TEXT,
        image TEXT,
        description TEXT,
        tags TEXT,
        embedding TEXT
    );
    '''
    
    insert_sql_template = '''
                INSERT INTO memes (id, name, image, description, tags)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    image = excluded.image,
                    description = excluded.description,
                    tags = excluded.tags
            '''
            
    expected_calls = [
        call(create_table_sql), 
        call(insert_sql_template, (1, 'Meme 1', '-', 'Desc 1', 'tag1')),
        call(insert_sql_template, (2, 'Meme 2', 'img2.png', 'Desc 2', 'tag2'))
    ]
    
    mock_conn.cursor().execute.assert_has_calls(expected_calls, any_order=False)
    
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()


@patch('sqlite3.connect')
@patch('builtins.open', new_callable=mock_open)
def test_update_database_with_bad_data(mock_file_open, mock_db_connect):
    """
    Проверяет, что скрипт устойчив к ошибкам в данных.

    Если в файле есть пустые строки или некорректный JSON,
    скрипт должен пропустить их и обработать только валидные данные.
    """
    valid_line = '{"id": 10, "name": "Valid Meme", "description": "Good one", "tags": "ok"}'
    malformed_line = '{"id": 11, "name": "Broken JSON"' 
    empty_line = ''
    mock_file_content = f"{valid_line}\n{malformed_line}\n{empty_line}"
    mock_file_open.return_value.readlines.return_value = mock_file_content.splitlines()

    mock_conn = mock_db_connect.return_value
    mock_cursor = mock_conn.cursor.return_value

    update_database_from_jsonl('fake_db.db', 'fake_data.jsonl')

    insert_sql = '''
                INSERT INTO memes (id, name, image, description, tags)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name = excluded.name,
                    image = excluded.image,
                    description = excluded.description,
                    tags = excluded.tags
            '''
    
    insert_calls = [c for c in mock_cursor.execute.call_args_list if "INSERT" in c[0][0]]
    assert len(insert_calls) == 1
    
    assert insert_calls[0] == call(insert_sql, (10, 'Valid Meme', '-', 'Good one', 'ok'))
    
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()