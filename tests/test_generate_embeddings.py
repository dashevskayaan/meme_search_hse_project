import sqlite3
import json
from unittest.mock import patch, MagicMock, call

from generate_embeddings import get_embedding, main


@patch('generate_embeddings.client')
def test_get_embedding_success(mock_client):
    """
    Проверяет успешное получение эмбеддинга от API.

    Убеждается, что функция возвращает корректный вектор,
    когда API отвечает успешно.
    """
    mock_response = MagicMock()
    mock_response.data[0].embedding = [0.1, 0.2, 0.3]
    mock_client.embeddings.create.return_value = mock_response

    text = "тестовый текст"
    embedding = get_embedding(text)

    assert embedding == [0.1, 0.2, 0.3]
    mock_client.embeddings.create.assert_called_once_with(
        model="text-embedding-3-small",
        input=text
    )


@patch('generate_embeddings.client')
def test_get_embedding_api_error(mock_client):
    """
    Проверяет обработку ошибки при вызове API.

    Убеждается, что функция возвращает None, если клиент OpenAI
    вызывает исключение.
    """
    mock_client.embeddings.create.side_effect = Exception("API Error")

    embedding = get_embedding("любой текст")

    assert embedding is None


@patch('generate_embeddings.get_embedding')
@patch('sqlite3.connect')
def test_main_happy_path(mock_connect, mock_get_embedding):
    """
    Имитирует ситуацию, когда колонка 'embedding' успешно добавляется,
    находятся мемы для обработки, и для них успешно генерируются
    и сохраняются эмбеддинги.
    """
    mock_conn = mock_connect.return_value
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.return_value = [(1, 'мем 1'), (2, 'мем 2')]
    mock_get_embedding.return_value = [0.5, 0.6]

    main()

    assert mock_cursor.execute.call_count > 2  

    mock_cursor.execute.assert_any_call("ALTER TABLE memes ADD COLUMN embedding TEXT")

    mock_cursor.execute.assert_any_call(
        """
        SELECT id, description FROM memes 
        WHERE embedding IS NULL AND description IS NOT NULL 
        LIMIT ?
    """, (5,))

    expected_update_calls = [
        call("UPDATE memes SET embedding = ? WHERE id = ?", (json.dumps([0.5, 0.6]), 1)),
        call("UPDATE memes SET embedding = ? WHERE id = ?", (json.dumps([0.5, 0.6]), 2)),
    ]
    mock_cursor.execute.assert_has_calls(expected_update_calls, any_order=True)

    assert mock_conn.commit.call_count == 3  #тут 1 для ALTER, 2 для UPDATE
    mock_conn.close.assert_called_once()


@patch('generate_embeddings.get_embedding')
@patch('sqlite3.connect')
def test_main_column_already_exists(mock_connect, mock_get_embedding):
    """
    Проверяет сценарий, когда колонка 'embedding' уже существует.

    Убеждается, что скрипт корректно обрабатывает `sqlite3.OperationalError`
    и продолжает выполнение.

    Args:
        mock_connect (MagicMock): Мок для `sqlite3.connect`.
        mock_get_embedding (MagicMock): Мок для функции `get_embedding`.
    """
    mock_conn = mock_connect.return_value
    mock_cursor = mock_conn.cursor.return_value
    
    mock_cursor.execute.side_effect = [sqlite3.OperationalError, None]
    mock_cursor.fetchall.return_value = [] 

    main()
    mock_cursor.execute.assert_any_call("ALTER TABLE memes ADD COLUMN embedding TEXT")
    mock_cursor.execute.assert_any_call(
        """
        SELECT id, description FROM memes 
        WHERE embedding IS NULL AND description IS NOT NULL 
        LIMIT ?
    """, (5,))


@patch('generate_embeddings.get_embedding')
@patch('sqlite3.connect')
def test_main_embedding_fails_for_one_meme(mock_connect, mock_get_embedding):
    """
    Проверяет, что UPDATE не вызывается для мема, у которого не удалось
    получить эмбеддинг.
    
    """
    mock_conn = mock_connect.return_value
    mock_cursor = mock_conn.cursor.return_value
    mock_cursor.fetchall.return_value = [(1, 'хороший мем'), (2, 'плохой мем')]

    mock_get_embedding.side_effect = [[0.5, 0.6], None]

    main()
    successful_update_call = call(
        "UPDATE memes SET embedding = ? WHERE id = ?",
        (json.dumps([0.5, 0.6]), 1)
    )
    all_execute_calls = mock_cursor.execute.call_args_list
    
    assert successful_update_call in all_execute_calls

    update_calls_count = 0
    for c in all_execute_calls: #смотрим сколько раз вызывается update
        if 'UPDATE' in c[0][0]:
            update_calls_count += 1
            
    assert update_calls_count == 1
    
    assert mock_conn.commit.call_count == 2