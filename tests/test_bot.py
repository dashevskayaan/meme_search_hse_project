import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from aiogram.types import ReplyKeyboardRemove
from bot import (
    MemeStates,
    process_action,
    process_count,
    process_topic,
    send_meme_by_id,
    send_meme_with_description,
    Texts,
)
pytest_plugins = ("pytester",)
@pytest.mark.asyncio
async def test_send_meme_with_description_success(mock_bot):
    """
    Проверяет успешную отправку мема с фото и текстом описания.

    Убеждается, что функция возвращает True и вызывает методы `send_photo`
    и `send_message` с корректными параметрами.
    """
    test_meme = (1, 'image_data', 'Test Meme', 'Test Description')

    with patch('bot.bot', mock_bot):
        result = await send_meme_with_description(123, test_meme)
        assert result is True
        mock_bot.send_photo.assert_awaited_once_with(
            chat_id=123,
            photo='image_data',
            caption='Test Meme'
        )
        mock_bot.send_message.assert_awaited_once_with(
            chat_id=123,
            text='Описание: Test Description',
            parse_mode='HTML'
        )


@pytest.mark.asyncio
async def test_send_meme_by_id(mock_bot):
    """
    Проверяет отправку мема по его идентификатору.

    Мокирует базу данных, чтобы убедиться, что функция корректно
    извлекает данные и вызывает отправку фото.
    """
    test_meme_data = (1, 'image_url', 'Test Meme', 'Description')

    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = test_meme_data
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch('bot.bot', mock_bot), \
         patch('bot.sqlite3.connect') as mock_connect:
        mock_connect.return_value.__enter__.return_value = mock_conn
        await send_meme_by_id(123, 1)
        mock_bot.send_photo.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_topic(mock_message, mock_state):
    """
    Проверяет обработку ввода темы мемов пользователем.

    Убеждается, что после ввода темы состояние FSM корректно
    обновляется и бот переходит к ожиданию количества мемов.
    """
    mock_message.text = "новые мемы"

    await process_topic(mock_message, mock_state)

    mock_state.update_data.assert_awaited_once_with(topic="новые мемы")
    mock_state.set_state.assert_awaited_once_with(MemeStates.waiting_for_count)
    mock_message.answer.assert_awaited_once()


@pytest.mark.asyncio
async def test_process_count_with_mock_db(mock_message, mock_state):
    """
    Проверяет комплексный сценарий поиска и отправки мемов.
    """
    mock_message.text = "5"
    mock_state.get_data.return_value = {'topic': 'тест'}

    es_mock_instance = MagicMock()
    es_mock_instance.search_with_hybrid.return_value = [{'id': str(i)} for i in range(1, 11)]

    db_memes = [(i, f'img_{i}', f'name_{i}', f'desc_{i}') for i in range(1, 11)]
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = db_memes
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    with patch('bot.ElasticsearchManager', return_value=es_mock_instance) as mock_es_class, \
         patch('bot.sqlite3.connect') as mock_connect, \
         patch('bot.random.sample', lambda population, k: population[:k]), \
         patch('bot.bot', AsyncMock()):
        
        mock_connect.return_value.__enter__.return_value = mock_conn
        
        await process_count(mock_message, mock_state)

        mock_es_class.assert_called_once()
        es_mock_instance.search_with_hybrid.assert_called_once_with('тест', k=100)
        assert mock_message.answer.call_count > 0


@pytest.mark.asyncio
async def test_process_action_more_memes(mock_message, mock_state):
    """
    Проверяет обработку запроса "ещё мемов" от пользователя.

    Убеждается, что бот правильно реагирует на кнопку, запрашивая
    новое количество мемов и переходя в состояние waiting_for_count.
    """
    mock_message.text = Texts.more_memes
    mock_state.get_data.return_value = {
        'last_topic': 'тест',
        'shown_memes': [1, 2],
        'total_memes': 5
    }

    await process_action(mock_message, mock_state)

    mock_message.answer.assert_awaited_once()
    mock_state.set_state.assert_awaited_once_with(MemeStates.waiting_for_count)


@pytest.mark.asyncio
async def test_process_action_new_theme(mock_message, mock_state):
    """
    Проверяет обработку запроса "новая тема" от пользователя.

    Убеждается, что бот сбрасывает показанные мемы, запрашивает
    новую тему и переходит в состояние waiting_for_topic.
    """
    mock_message.text = Texts.new_theme

    await process_action(mock_message, mock_state)

    mock_state.update_data.assert_awaited_once_with(shown_memes=[])
    mock_message.answer.assert_awaited_once_with(
        Texts.enter_topic,
        reply_markup=ReplyKeyboardRemove()
    )
    mock_state.set_state.assert_awaited_once_with(MemeStates.waiting_for_topic)