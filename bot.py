import os
import logging
import sqlite3
import textwrap
import html
import random
import datetime
from functools import lru_cache
from logging.handlers import RotatingFileHandler
from flask import Flask

from telegram import Update, ParseMode, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (Updater, CommandHandler, MessageHandler, Filters, CallbackContext,
                          CallbackQueryHandler, ConversationHandler)
import openai
from dotenv import load_dotenv
from deep_translator import GoogleTranslator
from telegram.error import TimedOut, BadRequest

# Загрузка переменных окружения
load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
openai.api_key = OPENAI_API_KEY
DB_NAME = "hostel.db"
const port = process.env.PORT || 4000;
# Настройка логирования с ротацией: максимум 10 МБ на файл, 5 резервных копий
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = RotatingFileHandler("bot.log", maxBytes=10 * 1024 * 1024, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Константа для выбора языка (ConversationHandler)
SELECTING_LANGUAGE = 1

# Константы для работы с историей
HISTORY_UPDATE_THRESHOLD = 10  # каждые 10 новых сообщений обновлять резюме
LAST_MESSAGES_COUNT = 5        # для формирования запроса берём последние 5 сообщений

# Значения по умолчанию для баннеров
DEFAULT_BANNERS = {
    "tours": "https://example.com/default_tours_banner.jpg",
    "restaurants": "https://example.com/default_restaurants_banner.jpg",
    "accommodation": "https://example.com/default_accommodation_banner.jpg",
    "attractions": "https://example.com/default_attractions_banner.jpg",
    "events": "https://example.com/default_events_banner.jpg"
}
def get_banner(section: str) -> str:
    result = get_info_from_db("SELECT banner_url FROM banners WHERE section = ?", (section,))
    banner = result[0][0] if result and result[0][0] else DEFAULT_BANNERS.get(section, "")
    return banner.strip() if banner else ""
# ------------------------------------------------------------------------------
# Функция для установки режима WAL для SQLite (вызывается при старте)
def set_wal_mode():
    try:
        with sqlite3.connect(DB_NAME) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            logger.info("SQLite set to WAL mode.")
    except Exception as e:
        logger.error(f"Error setting WAL mode: {e}")

# ------------------------------------------------------------------------------
# Функция для безопасного получения поля (если None, возвращает пустую строку)
def safe_field(value):
    return html.escape(value) if value else ""

# ------------------------------------------------------------------------------
# Функция для разбиения длинного текста по границам перевода строки,
# чтобы HTML-теги не обрезались посередине.
def split_caption(caption: str, limit: int = 1024):
    if len(caption) <= limit:
        return caption, ""
    idx = caption.rfind("\n", 0, limit)
    if idx == -1:
        idx = limit
    first_part = caption[:idx].strip()
    second_part = caption[idx:].strip()
    return first_part, second_part

# ------------------------------------------------------------------------------
# Функция, которая безопасно отправляет фото с подписью, разбивая подпись, если она слишком длинная.
def safe_reply_photo(message_obj, photo, caption, parse_mode, context, reply_markup=None):
    if len(caption) > 1024:
        part1, part2 = split_caption(caption, 1024)
        message_obj.reply_photo(photo=photo, caption=part1, parse_mode=parse_mode, reply_markup=reply_markup)
        if part2:
            lang = context.user_data.get("lang", "en")
            message_obj.reply_text(text=part2, parse_mode=parse_mode, reply_markup=get_persistent_menu(lang))
    else:
        message_obj.reply_photo(photo=photo, caption=caption, parse_mode=parse_mode, reply_markup=reply_markup)

# ------------------------------------------------------------------------------
# Функция генерации ответа через OpenAI (используем модель GPT-4o-mini)
def generate_answer(prompt: str, language="English") -> str:
    system_prompt = (
        "You are a knowledgeable and reliable concierge for San Cristóbal. "
        "When answering a query, first check the database. If the requested information is not found, "
        "you may supplement your answer with data from reputable sources—such as official tourism websites, "
        "recognized travel guides, and local government resources. However, include additional information only "
        "if it is confirmed by at least two authoritative sources. If the data remains ambiguous or incomplete, "
        "ask the user for clarification rather than speculating.\n\n"
        "Format your answers with clear headings, bullet points, and numbered lists where appropriate. "
        "Separate each section with ample newlines for readability. Use bold for section titles (e.g., "
        "Description:, Price:, Details:) and italics for the actual content. Feel free to add emojis (like 😊 or 👍) "
        "to enhance the tone. Maintain a friendly and informative tone throughout.\n\n"
        "If a user's query is ambiguous or lacks sufficient details to provide a precise answer, prompt the user with "
        "a clarifying question. For example, if a user asks, 'What hotel should I choose?' respond with, "
        "'Could you please provide more details about your preferences (budget, location, type of experience) so I can offer "
        "a better recommendation?' This ensures that your response is both accurate and tailored to the user's needs.\n\n"
        "Answer in the language of the person."
    )
    try:
        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt}
            ],
            temperature=1.0,
        )
        answer = response.choices[0].message.content.strip()
        if language.lower() in ["spanish", "es", "esp"]:
            answer = translate_if_needed(answer, "es")
        return answer
    except Exception as e:
        logger.error(f"OpenAI API error: {e}")
        return "I'm sorry, I couldn't generate an answer at the moment."

# ------------------------------------------------------------------------------
# Функции форматирования с использованием HTML для вывода элементов
def format_tour_item(index, tour):
    name = safe_field(tour[1])
    description = safe_field(tour[2])
    price = safe_field(str(tour[3]))
    extra_info = safe_field(tour[4])
    return (f"<b>{index}. {name}</b>\n\n"
            f"<b>Description:</b> <i>{description}</i>\n\n"
            f"<b>Price:</b> <i>{price} pesos</i>\n\n"
            f"<b>Details:</b>\n<i>{extra_info}</i>\n\n")

def format_accommodation_item(index, accom):
    name = safe_field(accom[0])
    description = safe_field(accom[1])
    address = safe_field(accom[2])
    phone = safe_field(accom[3])
    website = safe_field(accom[4])
    extra_info = safe_field(accom[5])
    return (f"<b>{index}. {name}</b>\n\n"
            f"<b>Address:</b> <i>{address}</i>\n\n"
            f"<b>Description:</b> <i>{description}</i>\n\n"
            f"<b>Phone/WhatsApp:</b> <i>{phone}</i>\n\n"
            f"<b>Website/Social:</b> <i>{website}</i>\n\n"
            f"<b>Details:</b>\n<i>{extra_info}</i>\n\n")

def format_attraction_item(index, attr):
    name = safe_field(attr[1])
    address = safe_field(attr[2])
    shortinfo = safe_field(attr[3])
    date_time = safe_field(attr[5])
    return (f"<b>{index}. {name}</b>\n\n"
            f"<b>Address:</b> <i>{address}</i>\n\n"
            f"<b>Info:</b> <i>{shortinfo}</i>\n\n"
            f"<b>Schedule:</b> <i>{date_time}</i>\n\n")

def format_restaurant_item(index, rest):
    name = safe_field(rest[0])
    description = safe_field(rest[1])
    address = safe_field(rest[2])
    phone = safe_field(rest[3])
    website = safe_field(rest[4])
    extra_info = safe_field(rest[5])
    return (f"<b>{index}. {name}</b>\n\n"
            f"<b>Address:</b> <i>{address}</i>\n\n"
            f"<b>Description:</b> <i>{description}</i>\n\n"
            f"<b>Phone/WhatsApp:</b> <i>{phone}</i>\n\n"
            f"<b>Website/Social:</b> <i>{website}</i>\n\n"
            f"<b>Details:</b>\n<i>{extra_info}</i>\n\n")

# ------------------------------------------------------------------------------
# Функция динамического приветствия
def get_dynamic_greeting(user_name: str) -> str:
    greetings = [
        f"Hey {user_name}, welcome to your personal concierge!",
        f"Hello {user_name}! Great to see you here at San Cristóbal!",
        f"Hi {user_name}, ready to explore the best of San Cristóbal?",
        f"Greetings {user_name}, let's discover the city together!"
    ]
    return random.choice(greetings)

# ------------------------------------------------------------------------------
# Функция перевода (если выбран испанский)
def translate_if_needed(text: str, lang: str) -> str:
    if lang == "es":
        try:
            return GoogleTranslator(source='en', target="es").translate(text)
        except Exception as e:
            logger.error(f"Translation error: {e}")
            return text
    return text

# ------------------------------------------------------------------------------
# Функция для безопасной отправки длинных сообщений (HTML)
def send_long_message(update: Update, text: str, parse_mode=None, reply_markup=None):
    max_length = 4096
    if len(text) <= max_length:
        update.message.reply_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
    else:
        chunks = textwrap.wrap(text, width=max_length, break_long_words=False, replace_whitespace=False)
        for chunk in chunks:
            update.message.reply_text(chunk, parse_mode=parse_mode, reply_markup=reply_markup)

# ------------------------------------------------------------------------------
# Функции работы с базой данных для истории диалога
def save_message_to_db(chat_id: str, user_id: str, role: str, message_text: str):
    try:
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO chat_history (chat_id, user_id, role, message_text) VALUES (?, ?, ?, ?)",
                (chat_id, user_id, role, message_text)
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Error saving message to DB: {e}")

def get_summary_from_db(chat_id: str) -> str:
    try:
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT summary FROM conversation_summary WHERE chat_id = ?", (chat_id,))
            result = cursor.fetchone()
        return result[0] if result else ""
    except Exception as e:
        logger.error(f"Error retrieving summary from DB: {e}")
        return ""

def update_conversation_summary(chat_id: str, new_messages: list, lang: str = "en") -> str:
    prev_summary = get_summary_from_db(chat_id)
    new_text = "\n".join(new_messages)
    if prev_summary:
        prompt = (f"Update the following conversation summary using the previous summary and new messages.\n\n"
                  f"Previous summary: {prev_summary}\n\n"
                  f"New messages: {new_text}\n\n"
                  "Provide an updated, concise summary that preserves key details.")
    else:
        prompt = f"Summarize the following conversation concisely, preserving key details:\n\n{new_text}"
    
    new_summary = generate_answer(prompt, language="English")
    if lang.lower() in ["es", "spanish", "esp"]:
        new_summary = translate_if_needed(new_summary, "es")
    
    try:
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO conversation_summary (chat_id, summary, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP) "
                "ON CONFLICT(chat_id) DO UPDATE SET summary = ?, updated_at = CURRENT_TIMESTAMP",
                (chat_id, new_summary, new_summary)
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Error updating conversation summary in DB: {e}")
    return new_summary

def build_prompt_with_history(new_query: str, update: Update, context: CallbackContext) -> str:
    chat_id = str(update.effective_chat.id)
    summary = get_summary_from_db(chat_id)
    recent_messages = context.chat_data.get("recent_messages", [])
    history_text = ""
    if summary:
        history_text += f"Summary: {summary}\n"
    if recent_messages:
        history_text += "\n".join(recent_messages)
    prompt = f"Conversation history:\n{history_text}\nNow answer the following query: {new_query}"
    return prompt

# ------------------------------------------------------------------------------
# Функции формирования постоянного меню (ReplyKeyboardMarkup)
def persistent_menu_en() -> ReplyKeyboardMarkup:
    menu_buttons = [
        ["Tours", "Accommodation", "Attractions"],
        ["Restaurants", "Advices"],
        ["FAQ", "Events", "🔴 Reset"]
    ]
    return ReplyKeyboardMarkup(menu_buttons, resize_keyboard=True, one_time_keyboard=False)

def persistent_menu_es() -> ReplyKeyboardMarkup:
    menu_buttons = [
        ["Tours", "Alojamiento", "Atracciones"],
        ["Restaurantes", "Consejos"],
        ["FAQ", "Eventos", "🔴 Reset"]
    ]
    return ReplyKeyboardMarkup(menu_buttons, resize_keyboard=True, one_time_keyboard=False)

def get_persistent_menu(lang: str) -> ReplyKeyboardMarkup:
    return persistent_menu_es() if lang == "es" else persistent_menu_en()

# ------------------------------------------------------------------------------
# Inline-клавиатура для списка элементов (без кнопки "Back")
def get_list_inline_keyboard(items: list, prefix: str, lang: str) -> InlineKeyboardMarkup:
    keyboard = []
    for index, item in enumerate(items, start=1):
        item_id, name = item
        if index <= 3:
            name = "⭐ " + name
        keyboard.append([InlineKeyboardButton(name, callback_data=f"{prefix}:{item_id}")])
    return InlineKeyboardMarkup(keyboard)

# ------------------------------------------------------------------------------
# Inline-клавиатура для выбора языка
def language_inline_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton("English", callback_data="lang:en"),
         InlineKeyboardButton("Español", callback_data="lang:es")]
    ]
    return InlineKeyboardMarkup(buttons)

# ------------------------------------------------------------------------------
# Функции для хранения истории диалога в файле (если используется)
def init_chat_history(update: Update, context: CallbackContext) -> None:
    chat_id = update.effective_chat.id
    filename = f"chat_{chat_id}.txt"
    if not os.path.exists(filename):
        with open(filename, "w", encoding="utf-8") as f:
            f.write(f"Chat history for chat {chat_id}\n-----\n")
    context.chat_data["history_file"] = filename
    context.chat_data["message_count"] = 0
    context.chat_data["recent_messages"] = []
    context.chat_data["messages_since_summary"] = []
    context.chat_data["summary"] = ""
def get_info_from_db(query: str, params=()):
    try:
        with sqlite3.connect(DB_NAME) as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            result = cursor.fetchall()
        return result
    except Exception as e:
        logger.error(f"Database error: {e}")
        return []

# ------------------------------------------------------------------------------
# Обработчик inline callback для выбора языка
def language_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    data = query.data  # "lang:en" или "lang:es"
    if data.startswith("lang:"):
        lang = data.split(":")[1]
        context.user_data["lang"] = lang
        try:
            query.delete_message()
        except BadRequest as e:
            logger.error(f"Error deleting language selection message: {e}")
        chat_id = query.message.chat_id
        user = query.from_user
        greeting = get_dynamic_greeting(html.escape(user.first_name))
        if lang == "es":
            message = (f"{greeting}\n\n"
                       "Soy tu conserje impulsado por AI para San Cristóbal de las Casas.\n\n"
                       "Este bot te ayudará a encontrar información detallada sobre tours, alojamiento, atracciones, restaurantes, consejos y eventos en la ciudad. "
                       "Puedes interactuar mediante los comandos del menú o escribiendo directamente tu consulta. Por ejemplo:\n"
                       "• 'Estoy de viaje con mi pareja y busco un hotel tranquilo fuera del centro.'\n"
                       "• 'Viajamos en familia; ¿qué actividades recomiendas para niños?'\n\n"
                       "Para ver ejemplos de uso y aprender a aprovechar todas las funciones, visita: "
                       "<a href='https://example.com/usage-guide'>Usage Guide</a>\n\n"
                       "I look forward to helping you explore the city!")
        else:
            message = (f"{greeting}\n\n"
                       "I'm your AI-powered concierge for San Cristóbal de las Casas.\n\n"
                       "This bot is designed to provide you with detailed information on tours, accommodation, attractions, restaurants, advices, and events in the city. "
                       "You can interact with the bot using the menu commands or simply type your query. For example:\n"
                       "• 'I'm traveling with my partner and looking for a quiet hotel away from the center.'\n"
                       "• 'We're on a family trip; what activities do you recommend for children?'\n\n"
                       "For usage examples and to learn how to make the most of all the features, visit: "
                       "<a href='https://example.com/usage-guide'>Usage Guide</a>\n\n"
                       "I look forward to helping you explore the city!")
        context.bot.send_message(chat_id=chat_id,
                                 text=message,
                                 parse_mode=ParseMode.HTML,
                                 reply_markup=get_persistent_menu(lang))

# ------------------------------------------------------------------------------
# Обработчик /setlanguage для переустановки языка
def set_language_command(update: Update, context: CallbackContext) -> int:
    update.message.reply_text("Please choose your language:", reply_markup=language_inline_keyboard(), parse_mode=ParseMode.HTML)
    return SELECTING_LANGUAGE

def handle_language_choice(update: Update, context: CallbackContext) -> int:
    text = update.message.text.strip().lower()
    if text in ["english", "en"]:
        context.user_data["lang"] = "en"
        update.message.reply_text("Language set to English.", reply_markup=get_persistent_menu("en"), parse_mode=ParseMode.HTML)
    elif text in ["español", "esp", "es"]:
        context.user_data["lang"] = "es"
        update.message.reply_text("Idioma configurado a Español.", reply_markup=get_persistent_menu("es"), parse_mode=ParseMode.HTML)
    else:
        update.message.reply_text("Please choose a valid language: English or Español.", reply_markup=language_inline_keyboard(), parse_mode=ParseMode.HTML)
        return SELECTING_LANGUAGE
    logger.info(f"Language set to: {context.user_data.get('lang')}")
    return ConversationHandler.END

def cancel_language(update: Update, context: CallbackContext) -> int:
    update.message.reply_text("Language selection canceled.", reply_markup=get_persistent_menu(context.user_data.get("lang", "en")), parse_mode=ParseMode.HTML)
    return ConversationHandler.END

# ------------------------------------------------------------------------------
# Обработчик /reset для сброса сеанса (без очистки истории в БД)
def reset_command(update: Update, context: CallbackContext) -> None:
    context.user_data.clear()
    context.chat_data.clear()
    update.message.reply_text("Session data cleared. Please type /start to begin anew.",
                                reply_markup=ReplyKeyboardMarkup([[]], resize_keyboard=True),
                                parse_mode=ParseMode.HTML)

# ------------------------------------------------------------------------------
# Команда /start – улучшенное приветствие и описание бота.
def start_command(update: Update, context: CallbackContext) -> None:
    if not context.user_data.get("lang"):
        update.message.reply_text("Welcome! Please choose your language:", reply_markup=language_inline_keyboard(), parse_mode=ParseMode.HTML)
        return
    if "history_file" not in context.chat_data:
        init_chat_history(update, context)
    lang = context.user_data.get("lang")
    user = update.effective_user
    greeting = get_dynamic_greeting(html.escape(user.first_name))
    if lang == "es":
        message = (f"{greeting}\n\n"
                   "Soy tu conserje impulsado por AI para San Cristóbal de las Casas.\n\n"
                   "Este bot te ayudará a encontrar información detallada sobre tours, alojamiento, atracciones, restaurantes, consejos y eventos en la ciudad. "
                   "Puedes interactuar mediante los comandos del menú o escribiendo directamente tu consulta. Por ejemplo:\n"
                   "• 'Estoy de viaje con mi pareja y busco un hotel tranquilo fuera del centro.'\n"
                   "• 'Viajamos en familia; ¿qué actividades recomiendas para niños?'\n\n"
                   "Para ver ejemplos de uso y aprender a aprovechar todas las funciones, visita: "
                   "<a href='https://example.com/usage-guide'>Usage Guide</a>\n\n"
                   "I look forward to helping you explore the city!")
    else:
        message = (f"{greeting}\n\n"
                   "I'm your AI-powered concierge for San Cristóbal de las Casas.\n\n"
                   "This bot is designed to provide you with detailed information on tours, accommodation, attractions, restaurants, advices, and events in the city. "
                   "You can interact with the bot using the menu commands or simply type your query. For example:\n"
                   "• 'I'm traveling with my partner and looking for a quiet hotel away from the center.'\n"
                   "• 'We're on a family trip; what activities do you recommend for children?'\n\n"
                   "For usage examples and to learn how to make the most of all the features, visit: "
                   "<a href='https://example.com/usage-guide'>Usage Guide</a>\n\n"
                   "I look forward to helping you explore the city!")
    update.message.reply_text(message, parse_mode=ParseMode.HTML, reply_markup=get_persistent_menu(lang))

# ------------------------------------------------------------------------------
# Команда для раздела Tours – выводится только баннер и inline‑клавиатура с названиями туров.
def tours_command(update: Update, context: CallbackContext) -> None:
    lang = context.user_data.get("lang", "en")
    suffix = "_en" if lang == "en" else "_es"
    query = f"SELECT id, name{suffix} FROM tours"
    tours = get_info_from_db(query)
    if not tours:
        update.message.reply_text(translate_if_needed("No tour data found in the database.", lang),
                                    reply_markup=get_persistent_menu(lang), parse_mode=ParseMode.HTML)
        return
    banner_url = get_banner("tours")
    caption = translate_if_needed("Select a tour to get more details.", lang)
    inline_keyboard = get_list_inline_keyboard(tours, "tour", lang)
    if banner_url:
        safe_reply_photo(update.message, banner_url, caption, ParseMode.HTML, context, inline_keyboard)
    else:
        update.message.reply_text(caption, parse_mode=ParseMode.HTML, reply_markup=inline_keyboard)

def handle_tour_callback(tour_id: int, update: Update, context: CallbackContext) -> None:
    lang = context.user_data.get("lang", "en")
    suffix = "_en" if lang == "en" else "_es"
    query = f"SELECT name{suffix}, description{suffix}, price, extra_info{suffix}, mainimage FROM tours WHERE id = ?"
    tour_details = get_info_from_db(query, (tour_id,))
    if not tour_details:
        update.callback_query.edit_message_text(translate_if_needed("Tour details not found.", lang), parse_mode=ParseMode.HTML)
        return
    tour = tour_details[0]
    name = safe_field(tour[0])
    description = safe_field(tour[1])
    price = safe_field(str(tour[2]))
    extra_info = safe_field(tour[3])
    formatted = (f"<b>{name}</b>\n\n"
                 f"<b>Description:</b> <i>{description}</i>\n\n"
                 f"<b>Price:</b> <i>{price} pesos</i>\n\n"
                 f"<b>Details:</b>\n<i>{extra_info}</i>\n\n")
    image_to_use = tour[4] if tour[4] and tour[4].strip() != "" else get_banner("tours")
    if not image_to_use:
        update.callback_query.message.reply_text(formatted, parse_mode=ParseMode.HTML)
    else:
        safe_reply_photo(update.callback_query.message, image_to_use, formatted, ParseMode.HTML, context)

# ------------------------------------------------------------------------------
# Команда для раздела Accommodation – выводится баннер и inline‑клавиатура с вариантами.
def accommodation_command(update: Update, context: CallbackContext) -> None:
    lang = context.user_data.get("lang", "en")
    suffix = "_en" if lang == "en" else "_es"
    query = f"SELECT id, name{suffix} FROM accommodation"
    accom = get_info_from_db(query)
    if not accom:
        update.message.reply_text(translate_if_needed("No accommodation data found in the database.", lang),
                                    reply_markup=get_persistent_menu(lang), parse_mode=ParseMode.HTML)
        return
    banner_url = get_banner("accommodation")
    caption = translate_if_needed("Select an accommodation option to get more details.", lang)
    inline_keyboard = get_list_inline_keyboard(accom, "accom", lang)
    if banner_url:
        safe_reply_photo(update.message, banner_url, caption, ParseMode.HTML, context, inline_keyboard)
    else:
        update.message.reply_text(caption, parse_mode=ParseMode.HTML, reply_markup=inline_keyboard)

def handle_accom_callback(accom_id: int, update: Update, context: CallbackContext) -> None:
    lang = context.user_data.get("lang", "en")
    suffix = "_en" if lang == "en" else "_es"
    query = f"SELECT name{suffix}, description{suffix}, address{suffix}, phone, website{suffix}, extra_info{suffix}, mainimage{suffix} FROM accommodation WHERE id = ?"
    details = get_info_from_db(query, (accom_id,))
    if not details:
        update.callback_query.edit_message_text(translate_if_needed("Accommodation details not found.", lang), parse_mode=ParseMode.HTML)
        return
    item = details[0]
    name = safe_field(item[0])
    description = safe_field(item[1])
    address = safe_field(item[2])
    phone = safe_field(item[3])
    website = safe_field(item[4])
    extra_info = safe_field(item[5])
    formatted = (f"<b>{name}</b>\n\n"
                 f"<b>Address:</b> <i>{address}</i>\n\n"
                 f"<b>Description:</b> <i>{description}</i>\n\n"
                 f"<b>Phone/WhatsApp:</b> <i>{phone}</i>\n\n"
                 f"<b>Website/Social:</b> <i>{website}</i>\n\n"
                 f"<b>Details:</b>\n<i>{extra_info}</i>\n\n")
    photo = item[6]
    if not photo or photo.strip() == "":
        update.callback_query.message.reply_text(formatted, parse_mode=ParseMode.HTML)
    else:
        safe_reply_photo(update.callback_query.message, photo, formatted, ParseMode.HTML, context)

# ------------------------------------------------------------------------------
# Команда для раздела Attractions – выводится баннер и inline‑клавиатура с названиями.
def attractions_command(update: Update, context: CallbackContext) -> None:
    lang = context.user_data.get("lang", "en")
    suffix = "_en" if lang == "en" else "_es"
    query = f"SELECT id, name{suffix} FROM attractions"
    attractions = get_info_from_db(query)
    if not attractions:
        update.message.reply_text(translate_if_needed("No attractions data found in the database.", lang),
                                    reply_markup=get_persistent_menu(lang), parse_mode=ParseMode.HTML)
        return
    banner_url = get_banner("attractions")
    caption = translate_if_needed("Select an attraction to get more details.", lang)
    inline_keyboard = get_list_inline_keyboard(attractions, "attr", lang)
    if banner_url:
        safe_reply_photo(update.message, banner_url, caption, ParseMode.HTML, context, inline_keyboard)
    else:
        update.message.reply_text(caption, parse_mode=ParseMode.HTML, reply_markup=inline_keyboard)

def handle_attr_callback(attr_id: int, update: Update, context: CallbackContext) -> None:
    lang = context.user_data.get("lang", "en")
    suffix = "_en" if lang == "en" else "_es"
    query = f"SELECT name{suffix}, address{suffix}, shortinfo{suffix}, mainimage, date_time, fullinfo{suffix} FROM attractions WHERE id = ?"
    details = get_info_from_db(query, (attr_id,))
    if not details:
        update.callback_query.edit_message_text(translate_if_needed("Attraction details not found.", lang), parse_mode=ParseMode.HTML)
        return
    attr = details[0]
    name = safe_field(attr[0])
    address = safe_field(attr[1])
    shortinfo = safe_field(attr[2])
    date_time = safe_field(attr[4])
    fullinfo = safe_field(attr[5])
    formatted = (f"<b>{name}</b>\n\n"
                 f"<b>Address:</b> <i>{address}</i>\n\n"
                 f"<b>Info:</b> <i>{shortinfo}</i>\n\n"
                 f"<b>Schedule:</b> <i>{date_time}</i>\n\n"
                 f"{fullinfo}\n\n")
    image_to_use = attr[3] if attr[3] and attr[3].strip() != "" else get_banner("attractions")
    if not image_to_use:
        update.callback_query.message.reply_text(formatted, parse_mode=ParseMode.HTML)
    else:
        safe_reply_photo(update.callback_query.message, image_to_use, formatted, ParseMode.HTML, context)

# ------------------------------------------------------------------------------
# Команда для раздела Restaurants – выводится баннер и inline‑клавиатура с названиями.
def restaurants_command(update: Update, context: CallbackContext) -> None:
    lang = context.user_data.get("lang", "en")
    suffix = "_en" if lang == "en" else "_es"
    query = f"SELECT id, name{suffix} FROM restaurants"
    restaurants = get_info_from_db(query)
    if not restaurants:
        update.message.reply_text(translate_if_needed("No restaurant data found in the database.", lang),
                                    reply_markup=get_persistent_menu(lang), parse_mode=ParseMode.HTML)
        return
    banner_url = get_banner("restaurants")
    caption = translate_if_needed("Select a restaurant to get more details.", lang)
    inline_keyboard = get_list_inline_keyboard(restaurants, "rest", lang)
    if banner_url:
        safe_reply_photo(update.message, banner_url, caption, ParseMode.HTML, context, inline_keyboard)
    else:
        update.message.reply_text(caption, parse_mode=ParseMode.HTML, reply_markup=inline_keyboard)

def handle_rest_callback(rest_id: int, update: Update, context: CallbackContext) -> None:
    lang = context.user_data.get("lang", "en")
    suffix = "_en" if lang == "en" else "_es"
    query = f"SELECT name{suffix}, description{suffix}, address{suffix}, phone, website{suffix}, extra_info{suffix}, mainimage{suffix} FROM restaurants WHERE id = ?"
    details = get_info_from_db(query, (rest_id,))
    if not details:
        update.callback_query.edit_message_text(translate_if_needed("Restaurant details not found.", lang), parse_mode=ParseMode.HTML)
        return
    rest = details[0]
    name = safe_field(rest[0])
    description = safe_field(rest[1])
    address = safe_field(rest[2])
    phone = safe_field(rest[3])
    website = safe_field(rest[4])
    extra_info = safe_field(rest[5])
    formatted = (f"<b>{name}</b>\n\n"
                 f"<b>Address:</b> <i>{address}</i>\n\n"
                 f"<b>Description:</b> <i>{description}</i>\n\n"
                 f"<b>Phone/WhatsApp:</b> <i>{phone}</i>\n\n"
                 f"<b>Website/Social:</b> <i>{website}</i>\n\n"
                 f"<b>Details:</b> <i>{extra_info}</i>\n\n")
    image_url = safe_field(rest[6])
    if not image_url:
        update.callback_query.message.reply_text(formatted, parse_mode=ParseMode.HTML)
    else:
        safe_reply_photo(update.callback_query.message, image_url, formatted, ParseMode.HTML, context)

# ------------------------------------------------------------------------------
# Команды для разделов Advices, FAQ и Events (выводятся как текст)
def advices_command(update: Update, context: CallbackContext) -> None:
    lang = context.user_data.get("lang", "en")
    suffix = "_en" if lang == "en" else "_es"
    query = f"SELECT category{suffix}, advice_text{suffix} FROM advices"
    advices = get_info_from_db(query)
    if not advices:
        update.message.reply_text(translate_if_needed("No advices data found in the database.", lang),
                                    reply_markup=get_persistent_menu(lang), parse_mode=ParseMode.HTML)
        return
    response = ""
    for i, advice in enumerate(advices, start=1):
        category = safe_field(advice[0])
        advice_text = safe_field(advice[1])
        response += f"<b>{i}. {category}</b>\n\n<i>{advice_text}</i>\n\n"
    update.message.reply_text(response, parse_mode=ParseMode.HTML, reply_markup=get_persistent_menu(lang))

def faq_command(update: Update, context: CallbackContext) -> None:
    lang = context.user_data.get("lang", "en")
    suffix = "_en" if lang == "en" else "_es"
    query = f"SELECT question{suffix}, answer{suffix} FROM faq"
    faqs = get_info_from_db(query)
    if not faqs:
        update.message.reply_text(translate_if_needed("No FAQ data found in the database.", lang),
                                    reply_markup=get_persistent_menu(lang), parse_mode=ParseMode.HTML)
        return
    response = ""
    for i, faq in enumerate(faqs, start=1):
        question = safe_field(faq[0])
        answer = safe_field(faq[1])
        response += f"<b>{i}. Q: {question}</b>\n\n<i>A:</i> <i>{answer}</i>\n\n"
    update.message.reply_text(response, parse_mode=ParseMode.HTML, reply_markup=get_persistent_menu(lang))

def events_command(update: Update, context: CallbackContext) -> None:
    lang = context.user_data.get("lang", "en")
    if lang == "es":
        message = ("Puedes ver los próximos eventos en nuestra página de Instagram:\n"
                   "https://www.instagram.com/events.sancristobal/")
    else:
        message = ("You can view upcoming events on our Instagram page:\n"
                   "https://www.instagram.com/events.sancristobal/")
    update.message.reply_text(message, parse_mode=ParseMode.HTML, reply_markup=get_persistent_menu(lang))

# ------------------------------------------------------------------------------
# Общий обработчик текстовых сообщений (общий поиск) с сохранением истории и суммаризацией.
def handle_message(update: Update, context: CallbackContext) -> None:
    lang = context.user_data.get("lang", "en")
    text = update.message.text.strip()
    lower_text = text.lower()
    
    # Сохраняем сообщение в БД
    chat_id = str(update.effective_chat.id)
    user_id = str(update.effective_user.id)
    save_message_to_db(chat_id, user_id, "user", text)
    
    # Обновляем оперативное хранение последних сообщений
    if "recent_messages" not in context.chat_data:
        context.chat_data["recent_messages"] = []
    context.chat_data["recent_messages"].append(text)
    context.chat_data["recent_messages"] = context.chat_data["recent_messages"][-LAST_MESSAGES_COUNT:]
    
    # Обновляем сообщения для суммаризации
    if "messages_since_summary" not in context.chat_data:
        context.chat_data["messages_since_summary"] = []
    context.chat_data["messages_since_summary"].append(text)
    
    # Если накопилось 10 сообщений, обновляем резюме
    if len(context.chat_data["messages_since_summary"]) >= HISTORY_UPDATE_THRESHOLD:
        new_summary = update_conversation_summary(chat_id, context.chat_data["messages_since_summary"], lang)
        context.chat_data["summary"] = new_summary
        context.chat_data["messages_since_summary"] = []  # сбрасываем накопленные сообщения
    
    if lower_text in ["reset", "🔴 reset"]:
        reset_command(update, context)
        return

    if lang == "es":
        commands = {
            "tours": tours_command,
            "alojamiento": accommodation_command,
            "atracciones": attractions_command,
            "restaurantes": restaurants_command,
            "consejos": advices_command,
            "faq": faq_command,
            "eventos": events_command
        }
    else:
        commands = {
            "tours": tours_command,
            "accommodation": accommodation_command,
            "attractions": attractions_command,
            "restaurants": restaurants_command,
            "advices": advices_command,
            "faq": faq_command,
            "events": events_command
        }
    
    if lower_text in commands:
        commands[lower_text](update, context)
        return

    # Если запрос не совпадает с командами меню – формируем prompt с учетом истории
    prompt = build_prompt_with_history(text, update, context)
    lang_param = "Spanish" if lang == "es" else "English"
    answer = generate_answer(prompt, language=lang_param)
    update.message.reply_text(answer, parse_mode=ParseMode.HTML, reply_markup=get_persistent_menu(lang))

# ------------------------------------------------------------------------------
# Обработчик callback-запросов (inline кнопки)
def button_handler(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    try:
        query.answer(timeout=10)
    except TimedOut as e:
        logger.error(f"Callback query timed out: {e}")
    data = query.data
    lang = context.user_data.get("lang", "en")
    if data.startswith("lang:"):
        lang_choice = data.split(":")[1]
        context.user_data["lang"] = lang_choice
        try:
            query.delete_message()
        except BadRequest as e:
            logger.error(f"Error deleting language selection message: {e}")
        chat_id = query.message.chat_id
        user = query.from_user
        greeting = get_dynamic_greeting(html.escape(user.first_name))
        if lang_choice == "es":
            message = (f"{greeting}\n\n"
                       "Soy tu conserje impulsado por AI para San Cristóbal de las Casas.\n\n"
                       "Este bot te ayudará a encontrar información detallada sobre tours, alojamiento, atracciones, restaurantes, consejos y eventos en la ciudad. "
                       "Puedes interactuar mediante los comandos del menú o escribiendo directamente tu consulta. Por ejemplo:\n"
                       "• 'Estoy de viaje con mi pareja y busco un hotel tranquilo fuera del centro.'\n"
                       "• 'Viajamos en familia; ¿qué actividades recomiendas para niños?'\n\n"
                       "Para ver ejemplos de uso y aprender a aprovechar todas las funciones, visita: "
                       "<a href='https://example.com/usage-guide'>Usage Guide</a>\n\n"
                       "¡Estoy aquí para ayudarte a descubrir lo mejor de San Cristóbal!")
        else:
            message = (f"{greeting}\n\n"
                       "I'm your AI-powered concierge for San Cristóbal de las Casas.\n\n"
                       "This bot is designed to provide you with detailed information on tours, accommodation, attractions, restaurants, advices, and events in the city. "
                       "You can interact with the bot using the menu commands or simply type your query. For example:\n"
                       "• 'I'm traveling with my partner and looking for a quiet hotel away from the center.'\n"
                       "• 'We're on a family trip; what activities do you recommend for children?'\n\n"
                       "For usage examples and to learn how to make the most of all the features, visit: "
                       "<a href='https://example.com/usage-guide'>Usage Guide</a>\n\n"
                       "I look forward to helping you explore the city!")
        context.bot.send_message(chat_id=chat_id,
                                 text=message,
                                 parse_mode=ParseMode.HTML,
                                 reply_markup=get_persistent_menu(lang_choice))
    elif data.startswith("tour:"):
        try:
            tour_id = int(data.split("tour:")[1])
            handle_tour_callback(tour_id, update, context)
        except ValueError:
            query.edit_message_text(translate_if_needed("Invalid tour identifier.", lang), parse_mode=ParseMode.HTML)
    elif data.startswith("accom:"):
        try:
            accom_id = int(data.split("accom:")[1])
            handle_accom_callback(accom_id, update, context)
        except ValueError:
            query.edit_message_text(translate_if_needed("Invalid accommodation identifier.", lang), parse_mode=ParseMode.HTML)
    elif data.startswith("attr:"):
        try:
            attr_id = int(data.split("attr:")[1])
            handle_attr_callback(attr_id, update, context)
        except ValueError:
            query.edit_message_text(translate_if_needed("Invalid attraction identifier.", lang), parse_mode=ParseMode.HTML)
    elif data.startswith("rest:"):
        try:
            rest_id = int(data.split("rest:")[1])
            handle_rest_callback(rest_id, update, context)
        except ValueError:
            query.edit_message_text(translate_if_needed("Invalid restaurant identifier.", lang), parse_mode=ParseMode.HTML)
    else:
        query.edit_message_text(translate_if_needed("Invalid callback data received.", lang), parse_mode=ParseMode.HTML)

# ------------------------------------------------------------------------------
# Обработчик ошибок
def error_handler(update: object, context: CallbackContext) -> None:
    lang = context.user_data.get("lang", "en") if context and hasattr(context, "user_data") else "en"
    error_message = "An unexpected error occurred. Please try again later."
    error_message_translated = translate_if_needed(error_message, lang)
    
    logger.error(msg="Exception while handling an update:", exc_info=context.error)
    if update and update.effective_message:
        update.effective_message.reply_text(error_message_translated, parse_mode=ParseMode.HTML)

# ------------------------------------------------------------------------------
# Основная функция запуска бота
def main():
    # Устанавливаем режим WAL для SQLite
    set_wal_mode()
    
    updater = Updater(TELEGRAM_BOT_TOKEN)
    dispatcher = updater.dispatcher

    dispatcher.add_handler(CommandHandler("start", start_command))
    dispatcher.add_handler(CommandHandler("reset", reset_command))
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("setlanguage", set_language_command)],
        states={
            SELECTING_LANGUAGE: [MessageHandler(Filters.text & ~Filters.command, handle_language_choice)]
        },
        fallbacks=[CommandHandler("cancel", cancel_language)]
    )
    dispatcher.add_handler(conv_handler)
    dispatcher.add_handler(CommandHandler("tours", tours_command))
    dispatcher.add_handler(CommandHandler("rooms", accommodation_command))
    dispatcher.add_handler(CommandHandler("attractions", attractions_command))
    dispatcher.add_handler(CommandHandler("restaurants", restaurants_command))
    dispatcher.add_handler(CommandHandler("advices", advices_command))
    dispatcher.add_handler(CommandHandler("faq", faq_command))
    dispatcher.add_handler(CommandHandler("events", events_command))
    dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, handle_message))
    dispatcher.add_handler(CallbackQueryHandler(button_handler))
    dispatcher.add_error_handler(error_handler)

    updater.start_polling()
    updater.idle()

if __name__ == "__main__":
    main()
