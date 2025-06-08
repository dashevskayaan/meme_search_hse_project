import logging
import sqlite3
import random
import os
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import ReplyKeyboardRemove
from aiogram.utils.keyboard import ReplyKeyboardBuilder
from dotenv import load_dotenv
from config import Texts
from elasticsearch_utils import ElasticsearchManager

es_manager = ElasticsearchManager()
es_manager.initialize_index()
es_manager.sync_from_sqlite()

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_TOKEN = os.getenv('BOT_TOKEN')
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

class MemeStates(StatesGroup):
    waiting_for_topic = State()
    waiting_for_count = State()
    waiting_for_action = State()

async def send_meme_with_description(chat_id: int, meme_data: tuple) -> bool:
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

@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer(Texts.start_message, parse_mode='HTML')
    await message.answer(Texts.enter_topic, reply_markup=ReplyKeyboardRemove())
    await state.set_state(MemeStates.waiting_for_topic)

@dp.message(Command("help"))
async def cmd_help(message: types.Message, state: FSMContext):
    await message.answer(Texts.help_message, parse_mode='HTML')
    await ask_for_action(message, state)

async def ask_for_action(message: types.Message, state: FSMContext):
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

    meme_ids = es_manager.find_in_elastic(topic, size=100)
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
        await message.answer(
            Texts.enter_topic,
            reply_markup=ReplyKeyboardRemove()
        )
        await state.set_state(MemeStates.waiting_for_topic)

    elif message.text == Texts.end_search:
        await message.answer(
            Texts.search_completed,
            reply_markup=ReplyKeyboardRemove()
        )
        await state.clear()
    else:
        await message.answer(Texts.use_buttons)

async def main():
    await dp.start_polling(bot)

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
