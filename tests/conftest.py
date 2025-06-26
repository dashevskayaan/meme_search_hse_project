import pytest
import os
from unittest.mock import AsyncMock, patch

os.environ['BOT_TOKEN'] = '123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11'

os.environ['OPENAI_API_KEY'] = 'sk-fake-key-for-testing-purposes' 


@pytest.fixture
def mock_bot():
    """Фикстура для мока объекта Bot."""
    return AsyncMock()

@pytest.fixture
def mock_message():
    """Фикстура для мока объекта Message."""
    message = AsyncMock()
    message.text = "test"
    message.chat.id = 123
    message.answer = AsyncMock()
    return message

@pytest.fixture
def mock_state():
    """Фикстура для мока FSMContext."""
    state = AsyncMock()
    state.set_state = AsyncMock()
    state.get_state = AsyncMock(return_value=None)
    state.clear = AsyncMock()
    state.update_data = AsyncMock()
    state.get_data = AsyncMock(return_value={})
    return state