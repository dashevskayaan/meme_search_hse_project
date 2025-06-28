import logging
import os
import random
import sqlite3
import asyncio 
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, ReplyKeyboardRemove
from aiogram.utils.keyboard import ReplyKeyboardBuilder
from config import Texts
from elasticsearch_utils import ElasticsearchManager

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_TOKEN = os.getenv('BOT_TOKEN')
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

class MemeStates(StatesGroup):
    """
    Класс состояний конечного автомата (FSM) для управления этапами диалога с пользователем:
      - waiting_for_topic: бот ожидает, что пользователь введёт тему для поиска мемов
      - waiting_for_count: бот ожидает число, сколько мемов показать по теме
      - waiting_for_action: бот ожидает, какое действие выбрать дальше (ещё мемы, новая тема и т.д.)
      - waiting_for_meme_number: бот ожидает ввод номера мема для показа по id
    """
    waiting_for_topic = State()
    waiting_for_count = State()
    waiting_for_action = State()
    waiting_for_meme_number = State()

async def send_meme_with_description(chat_id: int, meme_data: tuple) -> bool:
    """
    Отправляет пользователю фотографию мема и его описание.

    Args:
        chat_id (int): Идентификатор чата Telegram, куда отправлять мем.
        meme_data (tuple): Кортеж из четырёх элементов:
            - meme_id (int): id мема в базе (не используется напрямую)
            - image (str): ссылка на изображение мема (URL)
            - name (str): название мема (отправляется в подписи к фото)
            - description (str): текстовое описание мема (отправляется отдельным сообщением)

    Returns:
        bool: True — если мем был успешно отправлен (фото и описание),
              False — если при отправке возникла ошибка (например, невалидная ссылка).

    Функция нужна для компактной и единой логики отправки мемов, чтобы не дублировать код по всему проекту.
    """
    meme_id, image, name, description = meme_data
    try:
        await bot.send_photo(
            chat_id=chat_id,
            photo=image,
            caption=name[:1000]
        )
        if description:
            await bot.send_message(
                chat_id=chat_id,
                text=f"Описание: {description[:1000]}",
                parse_mode='HTML'
            )
        return True
    except Exception as e:
        logger.error(f"Ошибка отправки мема {meme_id}: {e}")
        return False

async def send_meme_by_id(chat_id: int, meme_id: int):
    """
    Находит и отправляет мем по его номеру из базы, с автоматической коррекцией некорректных номеров.

    Args:
        chat_id (int): id чата Telegram, куда отправлять мем.
        meme_id (int): номер мема, запрошенный пользователем (может выходить за диапазон).

    Returns:
        None

    Алгоритм:
        1. Корректирует meme_id, чтобы он всегда был от 1 до 1122 (если пользователь ввёл 0 или слишком большое число).
        2. Ищет мем с этим id в базе данных SQLite ('memes.db').
        3. Если мем найден — вызывает send_meme_with_description, иначе пишет пользователю "Мем не найден!".

    Эта функция нужна для удобной выдачи мемов по номеру и обработки ошибок пользователя.
    """
    actual_id = meme_id if 1 <= meme_id <= 1122 else meme_id % 1122 or 1122
    with sqlite3.connect('memes.db') as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, image, name, description FROM memes WHERE id = ?",
            (actual_id,)
        )
        meme = cursor.fetchone()
    if meme:
        await send_meme_with_description(chat_id, meme)
    else:
        await bot.send_message(chat_id, "Мем не найден!")

@dp.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    """
    Обрабатывает команду /start — сбрасывает состояние пользователя, отправляет приветственное сообщение и основное меню.

    Args:
        message (Message): Telegram сообщение, содержащее команду /start.
        state (FSMContext): Контекст состояния FSM пользователя (для сброса).

    Returns:
        None

    Функция нужна для "чистого старта" бота и показа начальных кнопок.
    """
    await state.clear()
    builder = ReplyKeyboardBuilder()
    builder.button(text=Texts.random_meme_button)
    builder.button(text=Texts.start_search)
    await message.answer(
        Texts.start_message,
        reply_markup=builder.as_markup(resize_keyboard=True),
        parse_mode='HTML'
    )

@dp.message(F.text == Texts.start_search)
async def handle_start_search(message: Message, state: FSMContext):
    """
    Обрабатывает нажатие кнопки "Поиск по теме".
    Переводит пользователя в состояние ввода темы для поиска.

    Args:
        message (Message): Telegram сообщение пользователя.
        state (FSMContext): Контекст состояния FSM пользователя.

    Returns:
        None

    Зачем: чтобы дальше получать от пользователя тему поиска мемов.
    """
    await message.answer(Texts.enter_topic, reply_markup=ReplyKeyboardRemove())
    await state.set_state(MemeStates.waiting_for_topic)

@dp.message(F.text == Texts.random_meme_button)
async def handle_random_meme_button(message: types.Message, state: FSMContext):
    """
    Обрабатывает нажатие кнопки "Случайный мем".
    Просит пользователя ввести номер мема для показа по id.

    Args:
        message (types.Message): Telegram сообщение пользователя.
        state (FSMContext): Контекст состояния FSM пользователя.

    Returns:
        None

    Эта функция нужна для запуска ветки случайного выбора мема по номеру.
    """
    await message.answer(Texts.enter_number_for_meme, reply_markup=ReplyKeyboardRemove())
    await state.set_state(MemeStates.waiting_for_meme_number)

@dp.message(F.text.isdigit(), MemeStates.waiting_for_meme_number)
async def handle_meme_number_input(message: types.Message, state: FSMContext):
    """
    Обрабатывает ввод пользователем номера мема.
    Отправляет пользователю соответствующий мем и переходит к выбору дальнейшего действия.

    Args:
        message (types.Message): Сообщение пользователя, содержащее число.
        state (FSMContext): Контекст FSM.

    Returns:
        None

    Зачем: основной обработчик случайного выбора мема по номеру.
    """
    number = int(message.text)
    await send_meme_by_id(message.chat.id, number)
    await ask_for_action(message, state)

@dp.message(Command("random_meme"))
async def cmd_random_meme(message: types.Message, state: FSMContext):
    """
    Обрабатывает команду /random_meme.
    Запрашивает у пользователя номер мема.

    Args:
        message (types.Message): Telegram сообщение пользователя.
        state (FSMContext): Контекст состояния FSM пользователя.

    Returns:
        None

    Зачем: поддержка команды для теста или быстрого доступа к случайному номеру.
    """
    await state.set_state(MemeStates.waiting_for_meme_number)
    await message.answer("Введите число:", reply_markup=ReplyKeyboardRemove())

@dp.message(Command("help"))
async def cmd_help(message: types.Message, state: FSMContext):
    """
    Обрабатывает команду /help.
    Показывает справку по возможностям бота и действиям пользователя.

    Args:
        message (types.Message): Telegram сообщение пользователя.
        state (FSMContext): Контекст состояния FSM пользователя.

    Returns:
        None

    Зачем: помогает разобраться новым пользователям с командами.
    """
    await message.answer(Texts.help_message, parse_mode='HTML')
    await ask_for_action(message, state)

async def ask_for_action(message: types.Message, state: FSMContext):
    """
    Отправляет пользователю меню дальнейших действий после показа мемов.

    Args:
        message (types.Message): Сообщение Telegram, после которого нужно предложить выбор действия.
        state (FSMContext): Контекст состояния FSM пользователя.

    Returns:
        None

    Меню даёт кнопки: "Показать ещё", "Новая тема", "Завершить поиск". Используется во всех ключевых точках сценария.
    """
    builder = ReplyKeyboardBuilder()
    builder.button(text=Texts.more_memes)
    builder.button(text=Texts.new_theme)
    builder.button(text=Texts.end_search)
    await message.answer(
        Texts.choose_action,
        reply_markup=builder.as_markup(resize_keyboard=True)
    )
    await state.set_state(MemeStates.waiting_for_action)

@dp.message(MemeStates.waiting_for_topic)
async def process_topic(message: types.Message, state: FSMContext):
    """
    Обрабатывает ввод темы поиска мемов от пользователя.
    Если тема пуста — просит ввести снова.
    Если тема валидная — переходит к ожиданию ввода количества мемов.

    Args:
        message (types.Message): Сообщение с текстом темы поиска.
        state (FSMContext): Контекст состояния FSM пользователя.

    Returns:
        None

    Важно: на этом этапе тема только сохраняется и пользователь видит напоминание о дальнейших шагах.
    """
    topic = message.text.strip()
    if not topic:
        await message.answer(Texts.enter_topic)
        return
    await state.update_data(topic=topic)
    await message.answer(
        f"<b>Тема:</b> {topic}\n{Texts.enter_count}",
        parse_mode='HTML',
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(MemeStates.waiting_for_count)

@dp.message(MemeStates.waiting_for_count)
async def process_count(message: types.Message, state: FSMContext):
    """
    Обрабатывает ввод количества мемов по выбранной теме.
    Выполняет поиск через ElasticsearchManager и отправляет найденные мемы пользователю.

    Args:
        message (types.Message): Сообщение пользователя (ожидается число).
        state (FSMContext): Контекст состояния FSM пользователя.

    Returns:
        None

    Детали:
        - Если введено не число, число < 1, либо > 20 — просит ввести корректное число.
        - Запускает гибридный поиск (сначала text, потом knn) по теме.
        - Находит реальные мемы в базе и фильтрует уже показанные пользователю.
        - Если ничего не найдено — уведомляет пользователя.
        - Если мемов меньше, чем просили — показывает сколько удалось найти.
        - После отправки мемов — предлагает дальнейшие действия.

    Использует: 
        - es_manager.search_with_hybrid
        - shown_memes — чтобы не повторять уже показанные пользователю мемы.
    """
    try:
        requested_count = int(message.text)
        if requested_count <= 0:
            await message.answer(Texts.positive_number)
            return
        if requested_count > 20:
            requested_count = 20
            await message.answer(Texts.max_memes_limit)
    except ValueError:
        await message.answer(Texts.enter_number)
        return

    user_data = await state.get_data()
    topic = user_data['topic']

    es_manager = ElasticsearchManager()
    search_results = es_manager.search_with_hybrid(topic, k=100, alpha=0.5)
    meme_ids = [int(r['id']) for r in search_results]

    if not meme_ids:
        await message.answer(Texts.no_memes_found)
        await state.set_state(MemeStates.waiting_for_topic)
        return
    
    with sqlite3.connect('memes.db') as conn:
        cursor = conn.cursor()
        placeholders = ','.join(['?'] * len(meme_ids))
        cursor.execute(f"""
            SELECT id, image, name, description FROM memes 
            WHERE id IN ({placeholders})
        """, meme_ids)
        all_matching_memes = cursor.fetchall()

    if not all_matching_memes:
        await message.answer(Texts.no_memes_found)
        await state.set_state(MemeStates.waiting_for_topic)
        return

    shown_memes = user_data.get('shown_memes', [])
    available_memes = [m for m in all_matching_memes if m[0] not in shown_memes]

    if not available_memes and shown_memes:
        await message.answer(Texts.all_memes_viewed)
        await ask_for_action(message, state)
        return

    if not shown_memes:
        available_memes = all_matching_memes

    actual_count = min(requested_count, len(available_memes))
    selected_memes = random.sample(available_memes, actual_count)

    shown_memes.extend([m[0] for m in selected_memes])
    await state.update_data(shown_memes=shown_memes)

    sent_count = 0
    for meme in selected_memes:
        success = await send_meme_with_description(message.chat.id, meme)
        if success:
            sent_count += 1

    if sent_count < requested_count:
        if sent_count == 0:
            await message.answer(Texts.no_memes_found)
            await state.set_state(MemeStates.waiting_for_topic)
            return
        await message.answer(
            f"{Texts.memes_found.format(sent_count)} (всего доступно: {len(all_matching_memes)})."
        )
    else:
        await message.answer(Texts.memes_found.format(sent_count))

    await state.update_data(
        last_topic=topic,
        last_count=sent_count,
        total_memes=len(all_matching_memes)
    )
    await ask_for_action(message, state)

@dp.message(MemeStates.waiting_for_action)
async def process_action(message: types.Message, state: FSMContext):
    """
    Обрабатывает выбор пользователя после выдачи мемов: показать ещё, начать новый поиск, завершить работу.

    Args:
        message (types.Message): Сообщение пользователя (выбор действия или кнопка).
        state (FSMContext): Контекст состояния FSM пользователя.

    Returns:
        None

    Логика:
        - "Показать ещё" — считает уже просмотренные мемы и предлагает ввести число для показа новых.
        - "Новая тема" или "Поиск по теме" — сбрасывает просмотренные мемы, запрашивает новую тему.
        - "Завершить поиск" — завершает сессию поиска, сбрасывает состояние.
        - Любой другой ввод — просит воспользоваться кнопками.

    Эта функция связывает все этапы диалога в единую цепочку.
    """
    if message.text == Texts.more_memes:
        user_data = await state.get_data()
        shown_count = len(user_data.get('shown_memes', []))
        total_memes = user_data.get('total_memes', 0)
        if shown_count >= total_memes:
            await message.answer(Texts.all_memes_viewed)
            await ask_for_action(message, state)
            return
        await message.answer(
            f"Сколько ещё мемов по теме '{user_data['last_topic']}'? (доступно: {total_memes - shown_count})",
            reply_markup=ReplyKeyboardRemove()
        )
        await state.set_state(MemeStates.waiting_for_count)

    elif message.text in [Texts.new_theme, Texts.start_search]:
        await state.update_data(shown_memes=[])
        await message.answer(Texts.enter_topic, reply_markup=ReplyKeyboardRemove())
        await state.set_state(MemeStates.waiting_for_topic)

    elif message.text == Texts.end_search:
        await message.answer(Texts.search_completed, reply_markup=ReplyKeyboardRemove())
        await state.clear()
    else:
        await message.answer(Texts.use_buttons)

async def main():
    """
    Основная асинхронная функция запуска Telegram-бота.
    В начале инициализирует Elasticsearch и индекс мемов.
    Далее запускает цикл приёма и обработки входящих сообщений через long polling.

    Args:
        None

    Returns:
        None

    Эта функция служит точкой входа для всего приложения.
    """
    logger.info("Инициализация зависимостей...")
    es_manager = ElasticsearchManager()
    es_manager.sync_db_to_elasticsearch()
    es_manager.initialize_elasticsearch()
    logger.info("Инициализация завершена. Запуск бота...")
    await dp.start_polling(bot)

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
