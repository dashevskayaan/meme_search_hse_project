import logging
import sqlite3
import random
from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_TOKEN = '8104370372:AAECNfDYdmJX5UWdp5cqD179d5ZEDANJAgc'
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

conn = sqlite3.connect('memes.db')
cursor = conn.cursor()

cursor.execute('CREATE INDEX IF NOT EXISTS idx_tags ON memes(tags)')
cursor.execute('CREATE INDEX IF NOT EXISTS idx_name ON memes(name)')
conn.commit()

class MemeStates(StatesGroup):
    waiting_for_topic = State()
    waiting_for_count = State()
    waiting_for_action = State()

def get_meme_word_form(number):
    last_digit = number % 10
    last_two_digits = number % 100
    
    if last_digit == 1 and last_two_digits != 11:
        return "мем"
    elif 2 <= last_digit <= 4 and not (12 <= last_two_digits <= 14):
        return "мема"
    else:
        return "мемов"

def get_found_start_word(number):
    if number % 10 == 1 and number % 100 != 11:
        return "Найден"
    else:
        return "Найдено"

async def send_meme_with_description(chat_id, meme_data):
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

@dp.message_handler(commands=['start'], state='*')
async def cmd_start(message: types.Message, state: FSMContext):
    await state.finish()
    welcome_text = """
🤖 <b>Мем-Поисковик</b> 🎭

Я помогу найти самые свежие и смешные мемы на любую тему!

🔍 <b>Как пользоваться:</b>
1. Введите тему для поиска 
2. Укажите количество мемов
3. Получайте результат!

📌 Доступные команды:
/start - начать новый поиск
/help - помощь по боту
"""

    await message.answer(welcome_text, parse_mode='HTML')
    await message.answer("Введите тему для поиска:")
    await MemeStates.waiting_for_topic.set()

@dp.message_handler(commands=['help'], state='*')
async def cmd_help(message: types.Message):
    help_text = """
🆘 <b>Помощь по боту</b>

🔍 <b>Поиск мемов:</b>
1. Напишите тему
2. Укажите количество
3. Получайте результат!

🔄 После получения мемов вы можете:
• Запросить ещё по той же теме
• Начать новый поиск
• Закончить работу

Если мемы не найдены - попробуйте изменить формулировку запроса.

Напишите /start чтобы начать поиск!
"""
    await message.answer(help_text, parse_mode='HTML')
    await ask_for_action_after_help(message)

async def ask_for_action_after_help(message: types.Message):
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.row("🔍 Начать поиск")
    keyboard.row("❌ Закончить")
    await message.answer("Выберите действие:", reply_markup=keyboard)
    await MemeStates.waiting_for_action.set()

@dp.message_handler(state=MemeStates.waiting_for_topic)
async def process_topic(message: types.Message, state: FSMContext):
    topic = message.text.strip()
    if not topic:
        await message.answer("Пожалуйста, укажите тему для поиска")
        return
    
    await state.update_data(topic=topic)
    await MemeStates.next()
    await message.answer(f"<b>Тема:</b> {topic}\nСколько мемов показать?", parse_mode='HTML')

@dp.message_handler(state=MemeStates.waiting_for_count)
async def process_count(message: types.Message, state: FSMContext):
    try:
        requested_count = int(message.text)
        if requested_count <= 0:
            await message.answer("Число должно быть положительным")
            return
        if requested_count > 20:
            requested_count = 20
            await message.answer("Установлено максимальное значение - 20 мемов")
    except ValueError:
        await message.answer("Пожалуйста, введите число")
        return
    
    user_data = await state.get_data()
    topic = user_data['topic']
    
    cursor.execute('''
    SELECT id, image, name, description FROM memes 
    WHERE tags LIKE '%' || ? || '%' OR name LIKE '%' || ? || '%'
    ''', (topic, topic))
    all_matching_memes = cursor.fetchall()
    
    if not all_matching_memes:
        await message.answer("По вашему запросу ничего не найдено, пожалуйста, выберите другую тему")
        await MemeStates.waiting_for_topic.set()
        return
    
    shown_memes = user_data.get('shown_memes', [])
    available_memes = [m for m in all_matching_memes if m[0] not in shown_memes]
    
    if not available_memes and shown_memes:
        await message.answer("Вы уже просмотрели все мемы по этой теме. Начните новый поиск.")
        await ask_for_action(message)
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
    
    word_form = get_meme_word_form(sent_count)
    start_word = get_found_start_word(sent_count)
    
    if sent_count < requested_count:
        if sent_count == 0:
            await message.answer("Не найдено ни одного мема, попробуйте другую тему")
            await MemeStates.waiting_for_topic.set()
            return
        else:
            await message.answer(f"{start_word} {sent_count} {word_form} из {requested_count} запрошенных (всего доступно: {len(all_matching_memes)}).")
    else:
        await message.answer(f"{start_word} {sent_count} {word_form}.")
    
    await state.update_data(
        last_topic=topic,
        last_count=sent_count,
        total_memes=len(all_matching_memes)
    )
    await ask_for_action(message)

async def ask_for_action(message: types.Message):
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    keyboard.row("🔍 Ещё мемы")
    keyboard.row("🔄 Новая тема")
    keyboard.row("❌ Закончить")
    await message.answer("Что делаем дальше?", reply_markup=keyboard)
    await MemeStates.waiting_for_action.set()

@dp.message_handler(state=MemeStates.waiting_for_action)
async def process_action(message: types.Message, state: FSMContext):
    if message.text == "🔍 Ещё мемы":
        user_data = await state.get_data()
        shown_count = len(user_data.get('shown_memes', []))
        total_memes = user_data.get('total_memes', 0)
        
        if shown_count >= total_memes:
            await message.answer("Вы уже просмотрели все мемы по этой теме. Начните новый поиск.")
            await ask_for_action(message)
            return
        
        await message.answer(
            f"Сколько ещё мемов по теме '{user_data['last_topic']}'? (доступно: {total_memes - shown_count})", 
            reply_markup=types.ReplyKeyboardRemove()
        )
        await MemeStates.waiting_for_count.set()
    
    elif message.text == "🔍 Начать поиск":
        await state.update_data(shown_memes=[])
        await message.answer("Введите тему для поиска мемов:", 
                           reply_markup=types.ReplyKeyboardRemove())
        await MemeStates.waiting_for_topic.set()
    
    elif message.text == "🔄 Новая тема":
        await state.update_data(shown_memes=[])
        await message.answer("Введите тему для поиска мемов:", 
                           reply_markup=types.ReplyKeyboardRemove())
        await MemeStates.waiting_for_topic.set()
    
    elif message.text == "❌ Закончить":
        await message.answer("Поиск завершён. Напишите /start для нового поиска.", 
                           reply_markup=types.ReplyKeyboardRemove())
        await state.finish()
    
    else:
        await message.answer("Пожалуйста, используйте кнопки для выбора действия")

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
