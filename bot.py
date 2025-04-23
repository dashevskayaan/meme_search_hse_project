import os
from telegram import Update, InputMediaPhoto, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, ConversationHandler, CallbackQueryHandler, filters
TOPIC, AMOUNT = range(2)
def get_meme_word(amount: int) -> str:
    if amount % 10 == 1 and amount % 100 != 11:
        return "мем"
    elif 2 <= amount % 10 <= 4 and (amount % 100 < 10 or amount % 100 >= 20):
        return "мема"
    else:
        return "мемов"
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Привет, на какую тему вывести мем?")
    return TOPIC
async def get_topic(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    topic = update.message.text
    context.user_data['topic'] = topic
    await update.message.reply_text(f"Тема: {topic}. Сколько мемов вывести?")
    if context.user_data.get('after_button'):
        context.user_data['state'] = AMOUNT
        return AMOUNT
    return AMOUNT
async def show_memes(update: Update, context: ContextTypes.DEFAULT_TYPE, amount: int):
    topic = context.user_data.get('topic', 'random')
    media_group = []
    for i in range(amount):
        image_url = f"https://picsum.photos/500/300?random={i}"
        media_group.append(InputMediaPhoto(media=image_url, caption=f"Мем {i+1} на тему '{topic}'"))
    await update.message.reply_media_group(media=media_group)
    keyboard = [
        [InlineKeyboardButton("Ещё на эту тему", callback_data="more_same"), InlineKeyboardButton("Другая тема", callback_data="new_topic")],
        [InlineKeyboardButton("Завершить", callback_data="cancel")]
    ]
    await update.message.reply_text("Что дальше?", reply_markup=InlineKeyboardMarkup(keyboard))
async def get_memes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        amount = int(update.message.text)
        if amount <= 0:
            await update.message.reply_text("Число должно быть положительным!")
            return AMOUNT
        amount = min(amount, 10)
        await show_memes(update, context, amount)
        context.user_data.pop('after_button', None)
    except ValueError:
        await update.message.reply_text("Пожалуйста, введите число!")
        return AMOUNT
    return ConversationHandler.END
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    if query.data == "more_same":
        await query.message.reply_text(f"Сколько ещё мемов вывести? (Тема: {context.user_data['topic']})")
        context.user_data['state'] = AMOUNT
    elif query.data == "new_topic":
        await query.message.reply_text("Хорошо! На какую новую тему вывести мемы?")
        context.user_data['after_button'] = True
        context.user_data['state'] = TOPIC
    elif query.data == "cancel":
        await query.message.reply_text("Рада была помочь! До свидания! Нажмите /start для нового поиска.")
async def handle_text_after_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    state = context.user_data.get('state')
    if state == TOPIC:
        return await get_topic(update, context)
    elif state == AMOUNT:
        return await get_memes(update, context)
    else:
        await update.message.reply_text("Пожалуйста, используйте /start")
        return ConversationHandler.END
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Диалог прерван. Нажмите /start для нового поиска.")
    return ConversationHandler.END
def main() -> None:
    token = os.getenv('TELEGRAM_BOT_TOKEN', '8104370372:AAECNfDYdmJX5UWdp5cqD179d5ZEDANJAgc')
    application = Application.builder().token(token).build()
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            TOPIC: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_topic)],
            AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_memes)],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
    )
    application.add_handler(conv_handler)
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_after_buttons))
    application.run_polling()
if __name__ == '__main__':
    main()
