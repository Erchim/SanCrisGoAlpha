import os
import re
import logging
import sqlite3
import textwrap
import html
import random
import datetime
import asyncio
import nest_asyncio
import pytz
import requests
from typing import Tuple
from bs4 import BeautifulSoup
from langdetect import detect
from geopy.geocoders import Nominatim
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    ContextTypes,
    filters
)
import openai
from dotenv import load_dotenv
from deep_translator import GoogleTranslator
from deep_translator.exceptions import TranslationNotFound
from telegram.error import TimedOut, BadRequest 

# Инициализация geopy с корректным User-Agent
osm_geolocator = Nominatim(user_agent="SanCrisGo/1.0 (estaticmona@gmail.com)")

# Применяем nest_asyncio для возможности повторного использования event loop
nest_asyncio.apply()

# Загрузка переменных окружения
load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
API_KEY = os.getenv("OPENWEATHER_API_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
openai.api_key = OPENAI_API_KEY
DB_NAME = "main.db"
DB_HISTORY = "chat_history.db"
admin_chat_id = os.getenv("ADMIN_CHAT_ID")
# Настройка логирования с ротацией: максимум 10 МБ на файл, 5 резервных копий
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
from logging.handlers import RotatingFileHandler
handler = RotatingFileHandler("bot.log", maxBytes=10*1024*1024, backupCount=5, encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
WHATSAPP_LINK = "https://wa.me/529984842518"  # Замените your-number на нужный номер

# Константа для выбора языка (ConversationHandler)
SELECTING_LANGUAGE = 1

# Константы для работы с историей
HISTORY_UPDATE_THRESHOLD = 5  # каждые 5 новых сообщений обновлять суммаризацию
LAST_MESSAGES_COUNT = 5        # для формирования запроса берём последние 5 сообщений

# Значения по умолчанию для баннеров
DEFAULT_BANNERS = {
    "tours": "https://example.com/default_tours_banner.jpg",
    "restaurants": "https://example.com/default_restaurants_banner.jpg",
    "accommodation": "https://example.com/default_accommodation_banner.jpg",
    "attractions": "https://example.com/default_attractions_banner.jpg",
    "events": "https://example.com/default_events_banner.jpg"
}

# ==================== Функция автоматического определения языка ====================
def language_code_to_target(lang_code: str) -> str:
    lang_code = lang_code.lower()
    if len(lang_code) == 2:
        return lang_code
    return lang_code[:2] if lang_code else "en"

# ==================== Работа с базой данных ====================
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
    
def detect_more_intent(query: str) -> bool:
    more_keywords = {"давай еще", "more", "ещё", "дальше", "next", "siguiente"}
    return any(keyword in query.lower() for keyword in more_keywords)    

def detect_places_intent(query: str) -> bool:
    prompt = (
        f"Определи, относится ли следующий запрос к поиску мест (например, ресторанов, кафе, отелей и т.д.):\n\n"
        f"Запрос: {query}\n\n"
        "Ответь 'True', если да, и 'False', если нет."
    )
    try:
        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": prompt}],
            temperature=0.1,
        )
        answer = response.choices[0].message.content.strip().lower()
        return "true" in answer
    except Exception as e:
        logger.error(f"Error in intent detection: {e}")
        return False


def search_places(query: str, location: tuple, radius: int = 5000) -> dict:
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        "location": f"{location[0]},{location[1]}",
        "radius": radius,
        "keyword": query,
        "key": GOOGLE_API_KEY
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        return {"error": f"Request failed with status code {response.status_code}"}
    
def validate_html(text: str) -> str:
    soup = BeautifulSoup(text, "html.parser")

    # 1) Заменяем <br> на \n
    for br in soup.find_all("br"):
        br.replace_with("\n")

    allowed_tags = ["b", "i", "a"]
    for tag in soup.find_all(True):
        if tag.name not in allowed_tags:
            tag.unwrap()
        elif tag.name == "a":
            if not tag.has_attr("href"):
                tag.unwrap()

    cleaned_html = str(soup)

    # Не удаляем \n
    # Не делаем .replace("<br>", "") и т.п.

    return cleaned_html


def format_places_for_prompt(places: dict) -> str:
    results = places.get("results", [])
    if not results:
        return "No places found."

    formatted = ""
    for place in results:
        name = place.get("name", "No name")
        place_id = place.get("place_id", "")
        map_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}" if place_id else None
        rating = place.get("rating", "No rating")
        description = "Some short description..."

        # Добавляем невидимый символ для разрыва абзацев
        zero_width_space = "\u200B"
        
        # Форматируем с учетом особенностей Telegram
        formatted += f"<b>{name}</b> 💲💲{zero_width_space}\n"
        formatted += f"<b>Rating: {rating}</b>{zero_width_space}\n"
        if map_url:
            formatted += f"<a href='{map_url}'>View on map</a>{zero_width_space}\n"
        
        # Описание курсивом в новом абзаце с невидимым символом
        formatted += f"\n<i>{description}</i>\n\n{zero_width_space}\n"

    return formatted or "No valid place data available."


def build_places_prompt(query: str, places_data: dict, lang: str) -> str:
    results = places_data.get("results", [])[:5]
    if not results:
        if lang.lower() in ["es", "spanish"]:
            return f"No se encontraron datos para la consulta: {query}"
        elif lang.lower() in ["ru", "russian"]:
            return f"Нет данных по запросу: {query}"
        else:
            return f"No data found for the query: {query}"

    if lang.lower() in ["es", "spanish"]:
        prompt = f"Consulta: '{query}'\n\nSe han encontrado los siguientes establecimientos:\n"
    elif lang.lower() in ["ru", "russian"]:
        prompt = f"Запрос: '{query}'\n\nНайденные заведения:\n"
    else:
        prompt = f"Query: '{query}'\n\nFound establishments:\n"
    
    prompt += format_places_for_prompt({"results": results})
    
    # Новая инструкция для GPT:
    prompt += (
        "Based on the information provided above, generate a concise, structured, and verified answer that includes recommendations and explanations as to why these places are worth visiting, in a few sentences."
         "Keep in mind that the information from the Google Places API is used for verification, so do not repeat it verbatim."
    )
    
    return prompt
    
async def handle_place_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    command = update.message.text.strip()
    if command.startswith("/place_"):
        place_id = command[len("/place_"):]
        lang = context.user_data.get("lang", "en")
        description, photo_url = await get_detailed_place_info(place_id, lang, context)  # Исправлено на 2 значения
        if photo_url:
            await safe_reply_photo(update.message, photo_url, description, ParseMode.HTML, context)
        else:
            await update.message.reply_text(description, parse_mode=ParseMode.HTML)

async def handle_places_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = update.message.text.strip()
    lang = context.user_data.get("lang", "en")
    
    center_coordinates = (16.737, -92.637)
    places_data = search_places(query=text, location=center_coordinates, radius=5000)
    
    if "error" in places_data:
        error_msg = "Error requesting Google Places API."
        bot_message = await send_long_message(update, translate_if_needed(error_msg, lang), ParseMode.HTML, get_persistent_menu(lang))
        if bot_message:
            context.chat_data["last_bot_message"] = {"id": bot_message.message_id, "text": error_msg}
            await add_feedback_buttons(bot_message, context, lang)
        return
    
    results = places_data.get("results", [])
    if not results:
        fallback_answer = generate_answer(text, language=lang)
        fallback_answer += "\n\nDisclaimer: The information provided is not verified."
        bot_message = await send_long_message(update, validate_html(fallback_answer), ParseMode.HTML, get_persistent_menu(lang))
        if bot_message:
            context.chat_data["last_bot_message"] = {"id": bot_message.message_id, "text": fallback_answer}
            await add_feedback_buttons(bot_message, context, lang)
        return

    context.chat_data["places_results"] = results
    context.chat_data["places_shown"] = context.chat_data.get("places_shown", 0)
    context.chat_data["last_places_query"] = text

    start_idx = context.chat_data["places_shown"]
    end_idx = min(start_idx + 5, len(results))
    current_results = results[start_idx:end_idx]
    
    prompt = build_places_prompt(text, {"results": current_results}, lang)
    answer = generate_answer(prompt, language=lang)
    
    # Создаём клавиатуру с названиями мест
    keyboard = []
    for place in current_results:
        place_id = place.get("place_id", "")
        name = place.get("name", "Unnamed Place")
        price_level = place.get("price_level", None)
        price_icon = "💲" * (price_level + 1) if price_level is not None else "💲?"
        keyboard.append([InlineKeyboardButton(f"{name} {price_icon}", callback_data=f"place:{place_id}")])
    
    instruction = "Click on the place name below to learn more details:"
    translated_instruction = validate_html(translate_if_needed(instruction, lang))  # Валидируем инструкцию
    full_answer = f"{answer}\n\n{translated_instruction}"
    
    # Логируем полный ответ для отладки
    logger.debug(f"Full answer before chunking: {full_answer}")
    
    # Разбиваем текст на чанки, сохраняя целостность записей
    max_length = 4096
    chunks = []
    current_chunk = ""
    
    # Разбиваем по записям о местах (каждая запись заканчивается двойным переносом строки)
    entries = full_answer.split('\n\n')
    for entry in entries:
        if not entry.strip():
            continue
        
        # Очищаем и добавляем закрывающие теги для каждой записи
        cleaned_entry = validate_html(entry)
        # Разбиваем cleaned_entry на подчанки, если он превышает max_length
        while cleaned_entry:
            if len(current_chunk) + len(cleaned_entry) + 2 <= max_length:
                current_chunk += cleaned_entry + '\n\n'
                cleaned_entry = ""
            else:
                # Находим точку разрыва, сохраняя целостность тегов
                chunk_end = max_length - len(current_chunk) - 2
                if chunk_end < 0:
                    chunk_end = 0
                # Ищем ближайший полный тег до chunk_end
                tag_pos = cleaned_entry.rfind('>', 0, chunk_end)
                if tag_pos == -1 or tag_pos >= chunk_end:
                    tag_pos = cleaned_entry.rfind('<', 0, chunk_end)
                if tag_pos == -1:
                    tag_pos = chunk_end
                
                current_chunk += validate_html(cleaned_entry[:tag_pos + 1]) + '\n'
                cleaned_entry = validate_html(cleaned_entry[tag_pos + 1:] + '</b></i></a>')
                if current_chunk:
                    chunks.append(validate_html(current_chunk.strip()))
                current_chunk = ""
    
    if current_chunk:
        chunks.append(validate_html(current_chunk.strip()))
    
    bot_message = None
    sent_chunks = 0
    for chunk in chunks:
        logger.debug(f"Sending chunk {sent_chunks + 1}/{len(chunks)}: {chunk}")
        try:
            bot_message = await update.message.reply_text(chunk, parse_mode=ParseMode.HTML)
            sent_chunks += 1
        except BadRequest as e:
            logger.error(f"Failed to send message chunk: {e}, Chunk text: {chunk}")
            # Исправляем чанк, добавляя недостающие теги
            fixed_chunk = validate_html(chunk)
            try:
                bot_message = await update.message.reply_text(fixed_chunk, parse_mode=ParseMode.HTML)
                sent_chunks += 1
            except BadRequest as e:
                logger.error(f"Failed to send fixed chunk: {e}, Fixed chunk text: {fixed_chunk}")
                fallback_text = translate_if_needed("Sorry, there was an issue displaying part of the results.", lang)
                bot_message = await update.message.reply_text(fallback_text, parse_mode=ParseMode.HTML)
                if sent_chunks == 0:  # Если ни один чанк не отправлен, добавляем клавиатуру
                    await add_feedback_buttons(bot_message, context, lang, existing_keyboard=keyboard)
    
    if bot_message and sent_chunks > 0:
        context.chat_data["last_bot_message"] = {"id": bot_message.message_id, "text": full_answer}
        await add_feedback_buttons(bot_message, context, lang, existing_keyboard=keyboard)
    
    context.chat_data["places_shown"] = end_idx

# Новая функция для добавления кнопок обратной связи
async def add_feedback_buttons(bot_message, context: ContextTypes.DEFAULT_TYPE, lang: str, existing_keyboard=None):
    good_text = translate_if_needed("Good 👍", lang)
    bad_text = translate_if_needed("Bad 👎", lang)
    
    # Создаём кнопки для отзывов
    feedback_row = [
        InlineKeyboardButton(good_text, callback_data=f"feedback:good:{bot_message.message_id}"),
        InlineKeyboardButton(bad_text, callback_data=f"feedback:bad:{bot_message.message_id}")
    ]
    
    # Если есть существующая клавиатура (с названиями мест), добавляем кнопки отзывов ниже с разделителем
    if existing_keyboard:
        updated_keyboard = existing_keyboard + [[InlineKeyboardButton("— Rate this response —", callback_data="noop")]] + [feedback_row]
    else:
        updated_keyboard = [feedback_row]
    
    feedback_markup = InlineKeyboardMarkup(updated_keyboard)
    await bot_message.edit_reply_markup(reply_markup=feedback_markup)


async def get_detailed_place_info(place_id: str, lang: str, context: ContextTypes.DEFAULT_TYPE) -> Tuple[str, str]:
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "key": GOOGLE_API_KEY,
        "fields": "name,formatted_address,types,website,formatted_phone_number,reviews,rating,photos,url,price_level,opening_hours",
        "language": lang
    }
    try:
        response = requests.get(url, params=params)
        if response.status_code != 200:
            logger.error(f"Google Places API error: {response.status_code} - {response.text}")
            return translate_if_needed("Unable to retrieve detailed information.", lang), None
        
        place_data = response.json().get("result", {})
        name = place_data.get("name", "No name")
        address = place_data.get("formatted_address", "No address")
        types = ", ".join(place_data.get("types", []))
        website = place_data.get("website", "No website")
        phone = place_data.get("formatted_phone_number", "No phone")
        rating = place_data.get("rating", "No rating")
        reviews = place_data.get("reviews", [])
        google_maps_url = place_data.get("url", f"https://www.google.com/maps/place/?q=place_id:{place_id}")
        price_level = place_data.get("price_level", None)

        # Форматируем номер телефона с кодом страны
        phone_digits = re.sub(r'\D', '', phone)
        if phone_digits and not phone_digits.startswith("52"):
            phone_digits = "52" + phone_digits
        phone_link = f"<a href='https://wa.me/{phone_digits}'>{phone}</a>" if phone_digits else phone
        
        # Получаем фото
        photo_url = None
        if "photos" in place_data and place_data["photos"]:
            photo_reference = place_data["photos"][0].get("photo_reference")
            if photo_reference:
                photo_url = f"https://maps.googleapis.com/maps/api/place/photo?maxwidth=400&photoreference={photo_reference}&key={GOOGLE_API_KEY}"
        
        # Формируем до 4 отзывов
        review_summary = ""
        if reviews:
            for review in reviews[:4]:
                review_text = review.get("text", "")
                sentences = re.split(r'(?<=[.!?])\s+', review_text)
                truncated_review = " ".join(sentences[:min(len(sentences), 2)])
                review_summary += f"  • {truncated_review}\n"
        
        # Обновлённый промпт для GPT с требованиями к структуре и эмодзи
        prompt = (
            f"Provide a detailed description in {lang} for a place named '{name}' located at '{address}'. "
            f"It is categorized as {types}, has a rating of {rating}, and here are some user reviews: {review_summary}. "
            "Structure your response in clear, concise paragraphs (2-4 sentences each) for readability. "
            "Incorporate relevant emojis (e.g., 🌟 for quality, 🍽️ for food, 🏡 for ambiance, 🎉 for fun) where appropriate "
            "to enhance the text and highlight positive aspects or key features. "
            "Explain why this place might be worth visiting based on the provided information, keeping the tone friendly and engaging."
        )
        try:
            response = openai.chat.completions.create(
                model="gpt-4o-mini",
                temperature=0.3,
                messages=[
                    {"role": "system", "content": "You are a knowledgeable concierge. Provide accurate, engaging, and well-structured descriptions based on the given data."},
                    {"role": "user", "content": prompt}
                ]
            )
            description = response.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"GPT error: {e}")
            description = translate_if_needed("Detailed description unavailable due to an error.", lang)
        
        opening_hours = place_data.get("opening_hours", {}).get("weekday_text", "No hours available")
        if isinstance(opening_hours, list):
            hours_text = "\n".join([f"  • {day}" for day in opening_hours])
        else:
            hours_text = opening_hours

        price_icon = "💲" * (price_level + 1) if price_level is not None else "💲?"
        # Форматируем итоговый ответ
        formatted = (
        f"<b>{name}</b> {price_icon}\n\n"
        f"🗺️ <b>Map link:</b> <a href='{google_maps_url}'>View on Google Maps</a>\n"
        f"📞 <b>Phone:</b> <i>{phone_link}</i>\n"
        f"🌐 <b>Website:</b> <i>{website}</i>\n"
        f"🏷️ <b>Types:</b> <i>{types}</i>\n"
        f"⭐ <b>Rating:</b> <i>{rating}</i>\n"
        f"⏰ <b>Opening Hours:</b>\n<i>{hours_text}</i>\n\n"
        f"📝 <b>Description:</b>\n{description}"
        )
        if review_summary:
            formatted += f"\n\n💬 <b>Guest Reviews:</b>\n<i>{review_summary}</i>"
    
        logger.debug(f"Formatted description for place_id {place_id}: length={len(formatted)}, content={formatted[:500]}...")  # Логируем первые 500 символов
        return formatted, photo_url
    
    except Exception as e:
        logger.error(f"Error retrieving place details: {e}")
        return translate_if_needed("Unable to retrieve detailed information.", lang), None
        
def weather_emoji(description: str) -> str:
    """Возвращает эмодзи для описания погоды."""
    desc = description.lower()
    if "clear" in desc:
        return "🌞"
    elif "cloud" in desc:
        return "☁️"
    elif "rain" in desc:
        return "🌧️"
    elif "snow" in desc:
        return "❄️"
    else:
        return ""
def is_new_chat(chat_id: str) -> bool:
    """
    Проверяет, зарегистрирован ли chat_id в таблице chats в базе chat_history.db.
    Если чат новый, возвращает True, иначе False.
    """
    try:
        with sqlite3.connect(DB_HISTORY) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT chat_id FROM chat_history WHERE chat_id = ?", (chat_id,))
            result = cursor.fetchone()
            return result is None
    except Exception as e:
        logger.error(f"Error checking new chat: {e}")
        return False

def register_chat(chat_id: str):
    """
    Регистрирует новый chat_id, добавляя его в таблицу chats в базе chat_history.db.
    """
    try:
        with sqlite3.connect(DB_HISTORY) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO chat_history (chat_id) VALUES (?)", (chat_id,))
            conn.commit()
    except Exception as e:
        logger.error(f"Error registering chat: {e}")

def get_24h_forecast(city: str, lang: str = "en") -> str:
    """
    Получает прогноз погоды на ближайшие 24 часа (с интервалом 3 часа) для указанного города
    через API OpenWeatherMap и возвращает отформатированный текст в виде таблицы.
    Прогноз выводится по местному времени (например, America/Mexico_City) и включает температуру, описание,
    влажность, скорость ветра и вероятность осадков с эмодзи.
    """
    API_KEY = os.getenv("OPENWEATHER_API_KEY")
    base_url = "https://api.openweathermap.org/data/2.5/forecast"
    params = {
        "q": city,
        "appid": API_KEY,
        "units": "metric",
        "lang": lang
    }
    try:
        response = requests.get(base_url, params=params)
        data = response.json()
        if data.get("cod") != "200":
            return f"Error: {data.get('message', 'Unable to get forecast data')}"
        
        # Определяем локальную таймзону для Сан-Кристобаля (например, для Мехико)
        local_tz = pytz.timezone("America/Mexico_City")
        now_local = datetime.datetime.now(local_tz)
        
        forecast_items = []
        for item in data["list"]:
            dt_item = datetime.datetime.fromtimestamp(item["dt"], pytz.utc).astimezone(local_tz)
            if now_local < dt_item <= now_local + datetime.timedelta(hours=24):
                forecast_items.append(item)
        
        if not forecast_items:
            return "No forecast data available for the next 24 hours."
        
        # Используем дату первого элемента для заголовка
        first_dt = datetime.datetime.fromtimestamp(forecast_items[0]["dt"], pytz.utc).astimezone(local_tz)
        header_date = first_dt.strftime("%Y-%m-%d")
        
        # Формируем заголовок таблицы
        header = (f"24-Hour Forecast for {city} on {header_date} (every 3 hours):\n"
                  "Get ready for the day ahead! Here's what the skies have in store:\n\n")
        table_header = ("Time                | Temp   | Description          | Humidity | Wind    | Precipitation\n"
                        "-------------------------------------------------------------------------------------------\n")
        
        lines = []
        for item in forecast_items:
            dt_item = datetime.datetime.fromtimestamp(item["dt"], pytz.utc).astimezone(local_tz)
            # Форматируем время с датой и временем
            time_str = dt_item.strftime("%d-%m %H:%M")
            temp = item["main"]["temp"]
            description = item["weather"][0]["description"]
            humidity = item["main"]["humidity"]
            wind_speed = item["wind"]["speed"]
            pop = int(item.get("pop", 0) * 100)
            # Получаем эмодзи для описания погоды
            emoji = weather_emoji(description)
            # Добавляем эмодзи для других полей
            humidity_emoji = "💧"
            wind_emoji = "🌬️"
            pop_emoji = "☔"
            
            line = (f"{time_str:<18} | {temp:>5}°C | {description:<20} {emoji:<2} | "
                    f"{humidity:>3}%{humidity_emoji}  | {wind_speed:>4} m/s{wind_emoji} | {pop:>3}%{pop_emoji}")
            lines.append(line)
        
        table = "\n".join(lines)
        result = f"<pre>{header}{table_header}{table}</pre>"
        return result
    except Exception as e:
        logger.error(f"24-hour forecast API error: {e}")
        return "Sorry, I could not retrieve the 24-hour forecast information at this moment."

def format_phone_number(phone: str) -> str:
    """
    Принимает номер телефона в виде строки, оставляет только цифры
    и возвращает HTML-ссылку для WhatsApp.
    """
    # Убираем все символы, кроме цифр
    digits = re.sub(r'\D', '', phone)
    if digits:
        return f"<a href='https://wa.me/{digits}'>{phone}</a>"
    return phone  # если номер пустой или не удалось извлечь цифры

def set_wal_mode():
    try:
        with sqlite3.connect(DB_NAME) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            logger.info("SQLite set to WAL mode.")
    except Exception as e:
        logger.error(f"Error setting WAL mode: {e}")

def build_welcome_message(lang: str, user_first_name: str) -> str:
    greeting = get_dynamic_greeting(html.escape(user_first_name))
    base_message = (
        f"{greeting}\n\n"
        "I'm your AI-powered concierge for San Cristóbal de las Casas.\n\n"
        "This bot is designed to provide you with detailed information on tours, accommodation, attractions, restaurants, advices, and events in the city.\n"
        "You can interact with the bot using the menu commands or simply type your query. For example:\n"
        "• I'm traveling with my partner and looking for a quiet hotel away from the center.\n"
        "• We're on a family trip; what activities do you recommend for children?\n\n"
        "For usage examples and to learn how to make the most of all the features, visit: "
        "<a href='https://example.com/usage-guide'>Usage Guide</a>\n\n"
        "I look forward to helping you explore the city!"
    )
    
    if lang == "en":
        return base_message
    
    # Защита и перевод
    protected_text, placeholders = protect_names(base_message)
    translated_text = translate_if_needed(protected_text, lang)
    final_message = restore_names(translated_text, placeholders)
    
    # Дополнительная проверка результата
    if translated_text == protected_text:
        logger.warning(f"Translation to '{lang}' did not occur, returning English message")
    
    return final_message
    
# ==================== Функции для баннеров ====================
def get_banner(section: str) -> str:
    result = get_info_from_db("SELECT banner_url FROM banners WHERE section = ?", (section,))
    banner = result[0][0] if result and result[0][0] else DEFAULT_BANNERS.get(section, "")
    return banner.strip() if banner else ""

# ==================== Вспомогательные функции форматирования ====================
def safe_field(value):
    return html.escape(value) if value else ""

def format_address(address: str) -> str:
    if not address:
        return ""
    # Заменяем пробелы на плюс для корректного формирования URL
    encoded_address = address.replace(" ", "+")
    url = f"https://www.google.com/maps/search/?api=1&query={encoded_address}"
    # Возвращаем HTML-ссылку с исходным текстом адреса
    return f"<a href='{url}' target='_blank'>{html.escape(address)}</a>"

def split_caption_by_paragraph(caption: str, limit: int = 1024) -> Tuple[str, str]:
    """
    Разбивает caption на две части по абзацам, стараясь, чтобы первая часть не превышала limit символов.
    Если невозможно найти разбиение по абзацам, возвращает полный текст и пустую строку.
    """
    if len(caption) <= limit:
        return caption, ""
    # Попытаемся разбить по двойному переносу строки
    paragraphs = caption.split("<p>")
    first_part = ""
    second_part = ""
    for i, para in enumerate(paragraphs):
        # Если добавление очередного абзаца не превышает лимит, добавляем его
        if len(first_part) + len(para) + 2 <= limit:
            first_part += para + "<p>"
        else:
            # Остальные абзацы собираем во вторую часть
            second_part = "<p>".join(paragraphs[i:]).strip()
            break
    return first_part.strip(), second_part


async def safe_reply_photo(message_obj, photo, caption, parse_mode, context, reply_markup=None):
    try:
        if len(caption) > 1024:
            part1, part2 = split_caption_by_paragraph(caption, 1024)
            bot_message = await message_obj.reply_photo(photo=photo, caption=part1, parse_mode=parse_mode, reply_markup=reply_markup)
            if part2:
                lang = context.user_data.get("lang", "en")
                await message_obj.reply_text(text=part2, parse_mode=parse_mode, reply_markup=get_persistent_menu(lang))
            return bot_message
        else:
            return await message_obj.reply_photo(photo=photo, caption=caption, parse_mode=parse_mode, reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"Error in safe_reply_photo: photo={photo}, caption_length={len(caption)}, error={e}")
        return None
    
# ==================== Функция генерации ответа через OpenAI ====================
def get_user_lang(context) -> str:
    try:
        return context.user_data.get("lang", "en") if context and context.user_data else "en"
    except Exception:
        return "en"
    
  # ==================== Функция перевода ====================
def translate_if_needed(text: str, lang: str) -> str:
    target_lang = language_code_to_target(lang)
    if target_lang == "en":
        return text
    try:
        translated = GoogleTranslator(source='auto', target=target_lang).translate(text)
        if translated and translated != text:  # Проверяем, что перевод выполнен
            logger.info(f"Translated text to '{target_lang}': {translated}")
            return translated
        else:
            logger.warning(f"Translation to '{target_lang}' failed or returned same text")
            return text
    except Exception as e:
        logger.error(f"Translation error to '{target_lang}': {e}")
        return text  # Fallback на исходный текст при ошибке

def protect_names(text: str) -> Tuple[str, dict]:
    """
    Находит все фрагменты, заключённые в <PN> и </PN>,
    заменяет их на уникальные плейсхолдеры и возвращает изменённый текст и словарь замен.
    """
    pattern = r'<PN>(.*?)</PN>'
    matches = re.findall(pattern, text)
    placeholders = {}
    for i, name in enumerate(matches):
        placeholder = f"__PROPNAME_{i}__"
        placeholders[placeholder] = name
        text = text.replace(f"<PN>{name}</PN>", placeholder)
    return text, placeholders

def restore_names(text: str, placeholders: dict) -> str:
    """
    Заменяет плейсхолдеры на оригинальные собственные имена,
    оборачивая их в жирный текст с двойными угловыми кавычками.
    """
    for placeholder, name in placeholders.items():
        text = text.replace(placeholder, f"<b>«{name}»</b>")
    return text

def protect_recommendations(text: str) -> Tuple[str, dict]:
    """
    Находит все фрагменты, заключённые в <REC> и </REC>,
    заменяет их на уникальные плейсхолдеры и возвращает изменённый текст и словарь замен.
    """
    pattern = r'<REC>(.*?)</REC>'
    matches = re.findall(pattern, text)
    placeholders = {}
    for i, rec in enumerate(matches):
        placeholder = f"__RECOMMEND_{i}__"
        placeholders[placeholder] = rec
        text = text.replace(f"<REC>{rec}</REC>", placeholder)
    return text, placeholders

def restore_recommendations(text: str, placeholders: dict) -> str:
    """
    Заменяет плейсхолдеры на оригинальные рекомендации, оборачивая их в курсив.
    """
    for placeholder, rec in placeholders.items():
        text = text.replace(placeholder, f"<i>{rec}</i>")
    return text
    
def generate_answer(prompt: str, language="English") -> str:
    target_lang = language_code_to_target(language)
    system_prompt = (
    "You are a knowledgeable and reliable concierge for San Cristóbal de las Casas. "
    "Answer in the user's language ({target_lang}). "
    "Incorporate relevant context from the conversation history to provide a helpful answer. "
    "Do not fabricate or hallucinate details unless absolutely necessary. Search places only in San Cristóbal de las Casas and Chiapas state. "
    "Ensure that your response is structured, accurate, and uses proper HTML formatting. "
    "For each establishment, output exactly as follows:\n"
    "1) On the first line, output: <b><Establishment Name></b> followed by a space and then the Price Level represented by the appropriate number of 💲 symbols (or 💲? if unknown).\n"
    "2) On the second line, output: - <a href='URL'>View on map</a> (omit this line if the URL is missing or invalid).\n"
    "3) On the third line, output: <b>Rating: <rating></b>\n"
    "4) On the fourth line, output: <i><A short recommendation in a few sentences></i>\n"
    "Then add a blank line to separate this entry from the next.\n"
    "Use '\\n' to separate lines exactly, and do NOT use <br> or <br/> tags.\n"
    "Do NOT use any tags other than <b>, <i>, and <a> with properly formatted attributes. "
    "Ensure all HTML tags are properly opened and closed, with no extra or mismatched closing tags. "
    "Each entry must end with </b></i></a> after the recommendation if needed, but do not replicate tags. "
    "Double-check that your response contains no syntax errors in HTML, including no extra '>' or duplicate tags. "
    "If you detect any duplicate, mismatched, or extra tags, remove them and ensure strict tag pairing."
)

    
    try:
        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0.3,  # Максимально низкая температура для точности
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt}
            ]
        )
        answer = response.choices[0].message.content.strip()
        logger.info(f"Generated answer: {answer}")
        
        detected_answer_lang = language_code_to_target(detect(answer)) if detect(answer) else "en"
        if detected_answer_lang != target_lang:
            protected_text, placeholders = protect_names(answer)
            translated_text = translate_if_needed(protected_text, target_lang)
            answer = restore_names(translated_text, placeholders)
        
        cleaned_answer = validate_html(answer)
        # Удаляем все дублирующиеся теги и лишние символы
        #cleaned_answer = re.sub(r'</?[bi]>\s*?</?[bi]>', '', cleaned_answer)
        #cleaned_answer = re.sub(r'</a>\s*?</a>', '', cleaned_answer)
        #cleaned_answer = re.sub(r'>>+', '>', cleaned_answer)  # Удаляем лишние '>'
        
               
        logger.debug(f"Validated and deduplicated answer: {cleaned_answer}")
        return cleaned_answer
    
    except Exception as e:
        logger.error(f"OpenAI API error: {e}")
        return "I'm sorry, I couldn't generate an answer at the moment."
    
# ==================== Функции форматирования для вывода элементов ====================
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
    address = format_address(accom[2])
    phone = safe_field(accom[3])
    website = safe_field(accom[4])
    features = safe_field(accom[5])
    phone_link = format_phone_number(phone)

    return (f"<b>{index}. {name}</b>\n\n"
            f"<b>Address:</b> <i>{address}</i>\n\n"
            f"<b>Description:</b> <i>{description}</i>\n\n"
            f"<b>Phone/WhatsApp:</b> <i>{phone_link}</i>\n\n"
            f"<b>Website/Social:</b> <i>{website}</i>\n\n"
            f"<b>Details:</b>\n<i>{features}</i>\n\n")

def format_attraction_item(index, attr):
    name = safe_field(attr[1])
    address = format_address(attr[2])
    shortinfo = safe_field(attr[3])
    date_time = safe_field(attr[5])
    return (f"<b>{index}. {name}</b>\n\n"
            f"<b>Address:</b> <i>{address}</i>\n\n"
            f"<b>Info:</b> <i>{shortinfo}</i>\n\n"
            f"<b>Schedule:</b> <i>{date_time}</i>\n\n")

def format_restaurant_item(index, rest):
    name = safe_field(rest[0])
    description = safe_field(rest[1])
    address = format_address(rest[2])
    phone = safe_field(rest[3])
    website = safe_field(rest[4])
    extra_info = safe_field(rest[5])
    phone_link = format_phone_number(phone)
    return (f"<b>{index}. {name}</b>\n\n"
            f"<b>Address:</b> <i>{address}</i>\n\n"
            f"<b>Description:</b> <i>{description}</i>\n\n"
            f"<b>Phone/WhatsApp:</b> <i>{phone_link}</i>\n\n"
            f"<b>Website/Social:</b> <i>{website}</i>\n\n"
            f"<b>Details:</b>\n<i>{extra_info}</i>\n\n")

# ==================== Функция динамического приветствия ====================
def get_dynamic_greeting(user_name: str) -> str:
    greetings = [
        f"Hey {user_name}, welcome to your personal concierge!",
        f"Hello {user_name}! Great to see you here at San Cristóbal!",
        f"Hi {user_name}, ready to explore the best of San Cristóbal?",
        f"Greetings {user_name}, let's discover the city together!"
    ]
    return random.choice(greetings)

def save_feedback_to_db(chat_id: str, user_id: str, message_text: str, rating: str):
    try:
        with sqlite3.connect(DB_HISTORY) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO feedback (chat_id, user_id, message_text, rating) VALUES (?, ?, ?, ?)",
                (chat_id, user_id, message_text, rating)
            )
            conn.commit()
            logger.info(f"Saved feedback: chat_id={chat_id}, user_id={user_id}, rating={rating}")
    except Exception as e:
        logger.error(f"Error saving feedback to DB: {e}")
        
# ------------------- Функция автоматического преобразования кода языка -------------------
def language_code_to_target(lang_code: str) -> str:
    if len(lang_code) == 2:
        return lang_code.lower()
    return lang_code[:2].lower() if lang_code else "en"

# ==================== Функция для отправки длинных сообщений ====================
async def send_long_message(update: Update, text: str, parse_mode=None, reply_markup=None):
    max_length = 4096
    if len(text) <= max_length:
        return await update.message.reply_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
    else:
        chunks = textwrap.wrap(text, width=max_length, break_long_words=False, replace_whitespace=False)
        bot_message = None
        for chunk in chunks:
            bot_message = await update.message.reply_text(chunk, parse_mode=parse_mode, reply_markup=reply_markup if chunk == chunks[-1] else None)
        return bot_message

# ==================== Функции для работы с историей и суммаризацией ====================
def init_chat_history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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

def get_cached_translation(context, entity_type, entity_id, field, lang, original_text):
    cache_key = f"{entity_type}_{entity_id}_{field}_{lang}"
    if cache_key in context.chat_data:
        logger.info(f"Retrieved from cache: {cache_key}")
        return context.chat_data[cache_key]
    
    # Проверяем, нужно ли переводить поле
    if field in ["name", "address"]:  # Не переводим name и address
        translated = original_text
    else:
        # Используем GPT для перевода
        translation_prompt = (
            f"Translate the following text into '{lang}', but do not translate proper names or addresses:\n\n"
            f"{original_text}"
        )
        try:
            response = openai.chat.completions.create(
                model="gpt-4o-mini",
                temperature=0.3,
                messages=[
                    {"role": "system", "content": "You are a translator. Provide accurate and natural translations, preserving proper names and addresses."},
                    {"role": "user", "content": translation_prompt}
                ]
            )
            translated = response.choices[0].message.content.strip()
            logger.info(f"GPT translated text to '{lang}': {translated}")
        except Exception as e:
            logger.error(f"GPT translation error: {e}")
            translated = original_text  # Fallback на исходный текст
    
    context.chat_data[cache_key] = translated
    logger.info(f"Cached translation: {cache_key} -> {translated}")
    return translated

def save_message_to_db(chat_id: str, user_id: str, role: str, message_text: str):
    try:
        with sqlite3.connect(DB_HISTORY) as conn:
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
        with sqlite3.connect(DB_HISTORY) as conn:
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
    if lang.lower() not in ["en", "english"]:
        new_summary = translate_if_needed(new_summary, lang)
    
    try:
        with sqlite3.connect(DB_HISTORY) as conn:
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

def build_prompt_with_history(new_query: str, update: Update, context: ContextTypes.DEFAULT_TYPE) -> str:
    chat_id = str(update.effective_chat.id)
    summary = get_summary_from_db(chat_id)
    recent_messages = context.chat_data.get("recent_messages", [])
    history_text = ""
    if summary:
        history_text += f"Conversation summary: {summary}\n"
    if recent_messages:
        history_text += "Recent messages:\n" + "\n".join([f"- {msg}" for msg in recent_messages]) + "\n"
    prompt = f"{history_text}Now answer the following query: {html.escape(new_query)}"
    return prompt

# ==================== Меню и inline клавиатуры ====================
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
    return persistent_menu_es() if lang.lower() in ["es", "spanish"] else persistent_menu_en()

def get_list_inline_keyboard(items: list, prefix: str, lang: str) -> InlineKeyboardMarkup:
    keyboard = []
    for index, item in enumerate(items, start=1):
        item_id, name = item
        if index <= 3:
            name = "⭐ " + name
        keyboard.append([InlineKeyboardButton(name, callback_data=f"{prefix}:{item_id}")])
    return InlineKeyboardMarkup(keyboard)

def language_inline_keyboard() -> InlineKeyboardMarkup:
    languages = [
        ("English", "en"),       # Английский
        ("Español", "es"),      # Испанский
        ("Français", "fr"),     # Французский
        ("Português", "pt"),    # Португальский
        ("Deutsch", "de"),      # Немецкий
        ("Italiano", "it"),     # Итальянский
        ("Nederlands", "nl"),   # Голландский
        ("日本語", "ja"),       # Японский
        ("中文 (简体)", "zh-cn"),# Китайский (упрощенный)
        ("한국어", "ko"),       # Корейский
        ("Русский", "ru"),      # Русский
        ("Українська", "uk"),   # Украинский
        ("العربية", "ar"),     # Арабский
        ("עברית", "he"),       # Иврит
        ("Svenska", "sv"),     # Шведский
        ("Norsk", "no"),       # Норвежский
        ("Dansk", "da"),       # Датский
        ("Türkçe", "tr"),      # Турецкий
        ("Ελληνικά", "el"),    # Греческий
        ("Polski", "pl"),      # Польский
        ("Čeština", "cs")      # Чешский
    ]
    buttons = [
        [InlineKeyboardButton(name, callback_data=f"lang:{code}") for name, code in languages[i:i+3]]
        for i in range(0, len(languages), 3)
    ]
    return InlineKeyboardMarkup(buttons)

# ==================== Обработчики языка ====================
async def language_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    data = query.data
    if data.startswith("lang:"):
        lang = data.split(":")[1]
        context.user_data["lang"] = lang
        logger.info(f"Language selected: {lang}")
        try:
            await query.delete_message()
        except BadRequest as e:
            logger.error(f"Error deleting language selection message: {e}")
        chat_id = query.message.chat_id
        user = query.from_user
        message = build_welcome_message(lang, user.first_name)
        logger.info(f"Sending welcome message in '{lang}': {message}")
        await context.bot.send_message(chat_id=chat_id,
                                       text=message,
                                       parse_mode=ParseMode.HTML,
                                       reply_markup=get_persistent_menu(lang))
        
async def set_language_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Please choose your language:", reply_markup=language_inline_keyboard(), parse_mode=ParseMode.HTML)
    return SELECTING_LANGUAGE

async def forecast_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Обработчик, который вызывается, если пользователь вводит ключевое слово для прогноза погоды.
    Если указан город, используется он, иначе – город по умолчанию.
    Отправляет прогноз погоды на ближайшие 24 часа, начиная с текущего местного времени.
    """
    query = update.message.text.strip()
    parts = query.split(maxsplit=1)
    if len(parts) > 1:
        city = parts[1]
    else:
        city = "San Cristóbal de las Casas, Chiapas, Mexico"
    
    lang = context.user_data.get("lang", "en")
    forecast_info = get_24h_forecast(city, lang=lang)
    await update.message.reply_text(forecast_info, parse_mode=ParseMode.HTML)

async def handle_language_choice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip().lower()
    if text in ["english", "en"]:
        context.user_data["lang"] = "en"
        await update.message.reply_text("Language set to English.", reply_markup=get_persistent_menu("en"), parse_mode=ParseMode.HTML)
    elif text in ["español", "esp", "es"]:
        context.user_data["lang"] = "es"
        await update.message.reply_text("Idioma configurado a Español.", reply_markup=get_persistent_menu("es"), parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text("Please choose a valid language: English or Español.", reply_markup=language_inline_keyboard(), parse_mode=ParseMode.HTML)
        return SELECTING_LANGUAGE
    logger.info(f"Language set to: {context.user_data.get('lang')}")
    return ConversationHandler.END

async def cancel_language(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("Language selection canceled.", reply_markup=get_persistent_menu(context.user_data.get("lang", "en")), parse_mode=ParseMode.HTML)
    return ConversationHandler.END

# ==================== Обработчики для /reset и /start ====================
async def reset_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Очистка пользовательских и чатовых данных
    context.user_data.clear()
    context.chat_data.clear()
    
    # Отправка сообщения с удалением клавиатуры
    await update.message.reply_text(
        "Session data cleared. Please type /start to begin anew.\nSe han eliminado los datos de sesión. Por favor, escribe /start para comenzar de nuevo.",
        reply_markup=ReplyKeyboardRemove(),
        parse_mode=ParseMode.HTML
    )
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = str(update.effective_chat.id)
    logger.info(f"Start command invoked for chat_id: {chat_id}")
    
    if is_new_chat(chat_id):
        logger.info(f"Chat {chat_id} is new. Registering and sending notification.")
        register_chat(chat_id)
        admin_chat_id = os.getenv("ADMIN_CHAT_ID")
        if admin_chat_id:
            await context.bot.send_message(
                chat_id=admin_chat_id,
                text=f"Новый чат зарегистрирован: {chat_id}"
            )
    
    # Если язык не выбран, показываем клавиатуру
    if not context.user_data.get("lang"):
        await update.message.reply_text(
            "Welcome! Please choose your language:\n¡Bienvenido! Por favor, seleccione su idioma:",
            reply_markup=language_inline_keyboard(),
            parse_mode=ParseMode.HTML
        )
        return
    
    # Если язык уже выбран, сразу отправляем приветствие
    if "history_file" not in context.chat_data:
        init_chat_history(update, context)
    lang = context.user_data.get("lang")
    user = update.effective_user
    message = build_welcome_message(lang, user.first_name)
    await update.message.reply_text(message, parse_mode=ParseMode.HTML, reply_markup=get_persistent_menu(lang))

# ==================== Обработчики для разделов ====================
async def tours_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    lang = context.user_data.get("lang", "en")
    suffix = "_en" if lang.lower() in ["en", "english"] else "_es"
    query = f"SELECT id, name{suffix} FROM tours"
    tours = get_info_from_db(query)
    if not tours:
        await update.message.reply_text(translate_if_needed("No tour data found in the database.", lang),
                                        reply_markup=get_persistent_menu(lang), parse_mode=ParseMode.HTML)
        return
    banner_url = get_banner("tours")
    caption = translate_if_needed("Select a tour to get more details.", lang)
    inline_keyboard = get_list_inline_keyboard(tours, "tour", lang)
    if banner_url:
        await safe_reply_photo(update.message, banner_url, caption, ParseMode.HTML, context, inline_keyboard)
    else:
        await update.message.reply_text(caption, parse_mode=ParseMode.HTML, reply_markup=inline_keyboard)
    
async def handle_tour_callback(tour_id: int, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    lang = context.user_data.get("lang", "en")
    suffix = "_en" if lang.lower() in ["en", "english"] else "_es"
    query = f"SELECT name{suffix}, description{suffix}, price, extra_info{suffix}, mainimage FROM tours WHERE id = ?"
    tour_details = get_info_from_db(query, (tour_id,))
    if not tour_details:
        await update.callback_query.edit_message_text(translate_if_needed("Tour details not found.", lang), parse_mode=ParseMode.HTML)
        return
    
    tour = tour_details[0]
    name = safe_field(tour[0])
    description = safe_field(tour[1])
    price = safe_field(str(tour[2]))
    extra_info = safe_field(tour[3])
    
    # Перевод с кэшированием, если язык не en или es
    if lang not in ["en", "es"]:
        name = get_cached_translation(context, "tours", tour_id, "name", lang, name)
        description = get_cached_translation(context, "tours", tour_id, "description", lang, description)
        extra_info = get_cached_translation(context, "tours", tour_id, "extra_info", lang, extra_info)
    
    formatted = (f"<b>{name}</b>\n\n"
                 f"<b>Description:</b> <i>{description}</i>\n\n"
                 f"<b>Price:</b> <i>{price} pesos</i>\n\n"
                 f"<b>Details:</b>\n<i>{extra_info}</i>\n\n"
                 f"\nBook now! Send a message on WhatsApp:\n ☎️{WHATSAPP_LINK}")
    image_to_use = tour[4] if tour[4] and tour[4].strip() != "" else get_banner("tours")
    if not image_to_use:
        await update.callback_query.message.reply_text(formatted, parse_mode=ParseMode.HTML)
    else:
        await safe_reply_photo(update.callback_query.message, image_to_use, formatted, ParseMode.HTML, context)

async def accommodation_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    lang = context.user_data.get("lang", "en")
    suffix = "_en" if lang.lower() in ["en", "english"] else "_es"
    query = "SELECT id, name_es FROM accommodation"
    accom = get_info_from_db(query)
    if not accom:
        await update.message.reply_text(translate_if_needed("No accommodation data found in the database.", lang),
                                        reply_markup=get_persistent_menu(lang), parse_mode=ParseMode.HTML)
        return
    banner_url = get_banner("accommodation")
    caption = translate_if_needed("Select an accommodation option to get more details.", lang)
    inline_keyboard = get_list_inline_keyboard(accom, "accom", lang)
    if banner_url:
        await safe_reply_photo(update.message, banner_url, caption, ParseMode.HTML, context, inline_keyboard)
    else:
        await update.message.reply_text(caption, parse_mode=ParseMode.HTML, reply_markup=inline_keyboard)

async def handle_accom_callback(accom_id: int, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    lang = context.user_data.get("lang", "en")
    suffix = "_en" if lang.lower() in ["en", "english"] else "_es"
    query = f"SELECT name{suffix}, description{suffix}, address{suffix}, phone, website{suffix}, features{suffix}, image_url{suffix} FROM accommodation WHERE id = ?"
    details = get_info_from_db(query, (accom_id,))
    if not details:
        msg = update.callback_query.message
        text_to_send = translate_if_needed("Accommodation details not found.", lang)
        if msg.text:
            try:
                await update.callback_query.edit_message_text(text_to_send, parse_mode=ParseMode.HTML)
            except Exception as e:
                logger.error(f"Error editing message: {e}")
                await msg.reply_text(text_to_send, parse_mode=ParseMode.HTML)
        else:
            await msg.reply_text(text_to_send, parse_mode=ParseMode.HTML)
        return
    
    item = details[0]
    name = safe_field(item[0])
    description = safe_field(item[1])
    address = safe_field(item[2])
    phone = safe_field(item[3])
    website = safe_field(item[4])
    features = safe_field(item[5])
    
    # Перевод с кэшированием, если язык не en или es
    if lang not in ["en", "es"]:
        name = get_cached_translation(context, "accommodation", accom_id, "name", lang, name)
        description = get_cached_translation(context, "accommodation", accom_id, "description", lang, description)
        address = get_cached_translation(context, "accommodation", accom_id, "address", lang, address)
        features = get_cached_translation(context, "accommodation", accom_id, "features", lang, features)
    
    formatted_address = format_address(address)
    phone_link = format_phone_number(phone)
    
    formatted = (f"<b>{name}</b>\n\n"
                 f"<b>Address:</b> <i>{formatted_address}</i>\n\n"
                 f"<b>Description:</b> <i>{description}</i>\n\n"
                 f"<b>Phone/WhatsApp:</b> <i>{phone_link}</i>\n\n"
                 f"<b>Website/Social:</b> <i>{website}</i>\n\n"
                 f"<b>Details:</b>\n<i>{features}</i>\n\n")
    photo = item[6]
    if not photo or photo.strip() == "":
        await update.callback_query.message.reply_text(formatted, parse_mode=ParseMode.HTML)
    else:
        await safe_reply_photo(update.callback_query.message, photo, formatted, ParseMode.HTML, context)

async def attractions_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    lang = context.user_data.get("lang", "en")
    query = "SELECT id, name_es FROM attractions"
    attractions = get_info_from_db(query)
    if not attractions:
        await update.message.reply_text(translate_if_needed("No attractions data found in the database.", lang),
                                        reply_markup=get_persistent_menu(lang), parse_mode=ParseMode.HTML)
        return
    banner_url = get_banner("attractions")
    caption = translate_if_needed("Select an attraction to get more details.", lang)
    inline_keyboard = get_list_inline_keyboard(attractions, "attr", lang)
    if banner_url:
        await safe_reply_photo(update.message, banner_url, caption, ParseMode.HTML, context, inline_keyboard)
    else:
        await update.message.reply_text(caption, parse_mode=ParseMode.HTML, reply_markup=inline_keyboard)

async def handle_attr_callback(attr_id: int, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    lang = context.user_data.get("lang", "en")
    suffix = "_en" if lang.lower() in ["en", "english"] else "_es"
    query = f"SELECT name{suffix}, address{suffix}, shortinfo{suffix}, mainimage, date_time, fullinfo{suffix} FROM attractions WHERE id = ?"
    details = get_info_from_db(query, (attr_id,))
    if not details:
        await update.callback_query.edit_message_text(translate_if_needed("Attraction details not found.", lang), parse_mode=ParseMode.HTML)
        return
    
    attr = details[0]
    name = safe_field(attr[0])       # Не переводим название
    address = safe_field(attr[1])    # Не переводим адрес
    shortinfo = safe_field(attr[2])
    date_time = safe_field(attr[4])
    fullinfo = safe_field(attr[5])
    
    # Перевод только описательных полей, если язык не en или es
    if lang not in ["en", "es"]:
        shortinfo = get_cached_translation (context, "attractions", attr_id, "shortinfo", lang, shortinfo)
        fullinfo = get_cached_translation(context, "attractions", attr_id, "fullinfo", lang, fullinfo)
    
    formatted_address = format_address(address)
    
    formatted = (f"<b>{name}</b>\n\n"
                 f"<b>Address:</b> <i>{formatted_address}</i>\n\n"
                 f"<b>Info:</b> <i>{shortinfo}</i>\n\n"
                 f"<b>Schedule:</b> <i>{date_time}</i>\n\n"
                 f"{fullinfo}\n\n")
    image_to_use = attr[3] if attr[3] and attr[3].strip() != "" else get_banner("attractions")
    if not image_to_use:
        await update.callback_query.message.reply_text(formatted, parse_mode=ParseMode.HTML)
    else:
        await safe_reply_photo(update.callback_query.message, image_to_use, formatted, ParseMode.HTML, context)

async def handle_rest_callback(rest_id: int, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    lang = context.user_data.get("lang", "en")
    suffix = "_en" if lang.lower() in ["en", "english"] else "_es"
    query = f"SELECT name{suffix}, description{suffix}, address{suffix}, phone, website{suffix}, extra_info{suffix}, mainimage{suffix} FROM restaurants WHERE id = ?"
    details = get_info_from_db(query, (rest_id,))
    if not details:
        await update.callback_query.edit_message_text(translate_if_needed("Restaurant details not found.", lang), parse_mode=ParseMode.HTML)
        return
    
    rest = details[0]
    name = safe_field(rest[0])       # Не переводим название
    description = safe_field(rest[1])
    address = safe_field(rest[2])    # Не переводим адрес
    phone = safe_field(rest[3])
    website = safe_field(rest[4])
    extra_info = safe_field(rest[5])
    
    # Перевод только описательных полей, если язык не en или es
    if lang not in ["en", "es"]:
        description = get_cached_translation(context, "restaurants", rest_id, "description", lang, description)
        extra_info = get_cached_translation(context, "restaurants", rest_id, "extra_info", lang, extra_info)
    
    formatted_address = format_address(address)
    phone_link = format_phone_number(phone)
    
    formatted = (f"<b>{name}</b>\n\n"
                 f"<b>Address:</b> <i>{formatted_address}</i>\n\n"
                 f"<b>Description:</b> <i>{description}</i>\n\n"
                 f"<b>Phone/WhatsApp:</b> <i>{phone_link}</i>\n\n"
                 f"<b>Website/Social:</b> <i>{website}</i>\n\n"
                 f"<b>Details:</b> <i>{extra_info}</i>\n\n")
    image_url = safe_field(rest[6])
    if not image_url:
        await update.callback_query.message.reply_text(formatted, parse_mode=ParseMode.HTML)
    else:
        await safe_reply_photo(update.callback_query.message, image_url, formatted, ParseMode.HTML, context)

# ==================== Функция поиска ресторанов через Nominatim (используется только при текстовом промте) ====================
def search_restaurants_osm(query, city="San Cristóbal de las Casas, Chiapas, Mexico", limit=5):
    try:
        location = osm_geolocator.geocode(city)
        if not location:
            logger.error("Could not geocode the city")
            return None
        lat = location.latitude
        lon = location.longitude
        lat_offset = 0.05
        lon_offset = 0.05
        # Формируем viewbox как строку в правильном порядке: (left, top, right, bottom)
        viewbox_str = f"{lon - lon_offset},{lat + lat_offset},{lon + lon_offset},{lat - lat_offset}"
        results = osm_geolocator.geocode(query, exactly_one=False, limit=limit, viewbox=viewbox_str, bounded=True)
        return results
    except Exception as e:
        logger.error(f"OSM search error: {e}")
        return None

async def restaurants_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Обработчик для кнопки/команды "Restaurants". 
    Возвращает данные о ресторанах из базы данных.
    """
    lang = context.user_data.get("lang", "en")
    # Выбираем суффикс для запроса в зависимости от языка
    suffix = "_en" if lang.lower() in ["en", "english"] else "_es"
    query = f"SELECT id, name{suffix} FROM restaurants"
    restaurants = get_info_from_db(query)
    if not restaurants:
        await update.message.reply_text(
            translate_if_needed("No restaurant data found in the database.", lang),
            reply_markup=get_persistent_menu(lang),
            parse_mode=ParseMode.HTML
        )
        return
    banner_url = get_banner("restaurants")
    caption = translate_if_needed("Select a restaurant to get more details.", lang)
    inline_keyboard = get_list_inline_keyboard(restaurants, "rest", lang)
    if banner_url:
        await safe_reply_photo(update.message, banner_url, caption, ParseMode.HTML, context, inline_keyboard)
    else:
        await update.message.reply_text(caption, parse_mode=ParseMode.HTML, reply_markup=inline_keyboard)

async def advices_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Обработчик для команды "Advices".
    Получает советы из базы данных и отправляет их пользователю.
    """
    lang = context.user_data.get("lang", "en")
    suffix = "_en" if lang.lower() in ["en", "english"] else "_es"
    query = f"SELECT category{suffix}, advice_text{suffix} FROM advices"
    advices = get_info_from_db(query)
    if not advices:
        await update.message.reply_text(
            translate_if_needed("No advices data found in the database.", lang),
            reply_markup=get_persistent_menu(lang),
            parse_mode=ParseMode.HTML
        )
        return
    response = ""
    for i, advice in enumerate(advices, start=1):
        category = safe_field(advice[0])
        advice_text = safe_field(advice[1])
        response += f"<b>{i}. {category}</b>\n\n<i>{advice_text}</i>\n\n"
    await update.message.reply_text(response, parse_mode=ParseMode.HTML, reply_markup=get_persistent_menu(lang))


async def faq_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Обработчик для команды "FAQ".
    Получает список вопросов и ответов из базы данных и отправляет их пользователю.
    """
    lang = context.user_data.get("lang", "en")
    suffix = "_en" if lang.lower() in ["en", "english"] else "_es"
    query = f"SELECT question{suffix}, answer{suffix} FROM faq"
    faqs = get_info_from_db(query)
    if not faqs:
        await update.message.reply_text(
            translate_if_needed("No FAQ data found in the database.", lang),
            reply_markup=get_persistent_menu(lang),
            parse_mode=ParseMode.HTML
        )
        return
    response = ""
    for i, faq in enumerate(faqs, start=1):
        question = safe_field(faq[0])
        answer = safe_field(faq[1])
        response += f"<b>{i}. Q: {question}</b>\n\n<i>A:</i> <i>{answer}</i>\n\n"
    await update.message.reply_text(response, parse_mode=ParseMode.HTML, reply_markup=get_persistent_menu(lang))


async def events_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Обработчик для команды "Events".
    Отправляет сообщение с ссылкой на страницу событий (Instagram).
    """
    lang = context.user_data.get("lang", "en")
    if lang.lower() in ["es", "spanish"]:
        message = ("Puedes ver los próximos eventos en nuestra página de Instagram:\n"
                   "https://www.instagram.com/events.sancristobal/")
    else:
        message = ("You can view upcoming events on our Instagram page:\n"
                   "https://www.instagram.com/events.sancristobal/")
    await update.message.reply_text(message, parse_mode=ParseMode.HTML, reply_markup=get_persistent_menu(lang))

async def translate_last_answer(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_text = update.message.text.strip().lower()
    
    # Проверяем, содержит ли сообщение слово "переведи" или "translate"
    if "переведи" in user_text or "translate" in user_text:
        last_message_id = context.chat_data.get("last_bot_message_id")
        last_answer = context.chat_data.get("last_bot_answer")
        if not last_message_id or not last_answer:
            await update.message.reply_text("Нет ответа для перевода.", parse_mode=ParseMode.HTML)
            return
        
        # Определяем целевой язык из сообщения (расширяйте список по необходимости)
        target_lang = "en"  # значение по умолчанию
        if "на испанский" in user_text or "to spanish" in user_text:
            target_lang = "es"
        elif "на русский" in user_text or "to russian" in user_text:
            target_lang = "ru"
        
        current_lang = context.user_data.get("lang", "en")
        if target_lang == current_lang:
            await update.message.reply_text("Ответ уже на этом языке.", parse_mode=ParseMode.HTML)
            return

        # Защищаем специальные теги, переводим и восстанавливаем их
        protected_text, placeholders = protect_names(last_answer)
        translated_text = translate_if_needed(protected_text, target_lang)
        final_answer = restore_names(translated_text, placeholders)
        
        try:
            await context.bot.edit_message_text(
                chat_id=update.effective_chat.id,
                message_id=last_message_id,
                text=final_answer,
                parse_mode=ParseMode.HTML
            )
            context.chat_data["last_bot_answer"] = final_answer
        except Exception as e:
            logger.error(f"Error editing message: {e}")
            await update.message.reply_text("Не удалось обновить сообщение с переводом.", parse_mode=ParseMode.HTML)
    else:
        await update.message.reply_text("Команда перевода не распознана.", parse_mode=ParseMode.HTML)
# ==================== Изменённый обработчик текстовых сообщений ====================

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = update.message.text.strip()
    text_lower = text.lower()

    commands = {
        "tours": tours_command,
        "accommodation": accommodation_command,
        "attractions": attractions_command,
        "restaurants": restaurants_command,
        "рестораны": restaurants_command,
        "advices": advices_command,
        "faq": faq_command,
        "events": events_command
    }

    if text_lower in commands:
        lang = context.user_data.get("lang", "en")
        await commands[text_lower](update, context)
        return
    
    weather_keywords = {"weather", "погода", "forecast", "прогноз", "tiempo", "météo", "wetter"}
    if text_lower in weather_keywords:
        await forecast_command(update, context)
        return

    if text_lower in ["reset", "🔴 reset"]:
        await reset_command(update, context)
        return

    if text_lower.startswith("osm:"):
        osm_query = text[4:].strip()
        results = search_restaurants_osm(osm_query, city="San Cristóbal de las Casas, Chiapas, Mexico", limit=5)
        lang = context.user_data.get("lang", "en")
        if results:
            response = "Aquí hay algunos restaurantes encontrados via OSM:\n\n" if lang.lower() in ["es", "spanish"] else "Here are some restaurants found via OSM:\n\n"
            for idx, place in enumerate(results, start=1):
                response += f"{idx}. {place.address}\nCoordinates: ({float(place.latitude):.5f}, {float(place.longitude):.5f})\n\n"
        else:
            response = translate_if_needed("No restaurant data found via OSM.", lang)
        await update.message.reply_text(response, parse_mode=ParseMode.HTML, reply_markup=get_persistent_menu(lang))
        return

    if "переведи" in text_lower or "translate" in text_lower:
        await translate_last_answer(update, context)
        return

    chat_id = str(update.effective_chat.id)
    user_id = str(update.effective_user.id)
    save_message_to_db(chat_id, user_id, "user", text)

    if "recent_messages" not in context.chat_data:
        context.chat_data["recent_messages"] = []
    context.chat_data["recent_messages"].append(text)
    context.chat_data["recent_messages"] = context.chat_data["recent_messages"][-LAST_MESSAGES_COUNT:]

    if "messages_since_summary" not in context.chat_data:
        context.chat_data["messages_since_summary"] = []
    context.chat_data["messages_since_summary"].append(text)

    if len(context.chat_data["messages_since_summary"]) >= HISTORY_UPDATE_THRESHOLD:
        new_summary = update_conversation_summary(chat_id, context.chat_data["messages_since_summary"], context.user_data.get("lang", "en"))
        context.chat_data["summary"] = new_summary
        context.chat_data["messages_since_summary"] = []

    lang = context.user_data.get("lang", "en")
    detected_lang = language_code_to_target(detect(text)) if detect(text) else lang

    # Проверяем, является ли запрос продолжением предыдущего поиска мест
    if detect_more_intent(text) and "places_results" in context.chat_data and "last_places_query" in context.chat_data:
        results = context.chat_data["places_results"]
        start_idx = context.chat_data["places_shown"]
        end_idx = min(start_idx + 5, len(results))
        if start_idx >= len(results):
            await update.message.reply_text(
                translate_if_needed("No more places to show.", lang),
                parse_mode=ParseMode.HTML,
                reply_markup=get_persistent_menu(lang)
            )
            return

        current_results = results[start_idx:end_idx]
        prompt = build_places_prompt(context.chat_data["last_places_query"], {"results": current_results}, lang)
        prompt += "\n\nDisclaimer: The above information is sourced from Google Places API and may not be verified."
        answer_raw = generate_answer(prompt, language=detected_lang)
        answer = validate_html(answer_raw)
        
        logger.debug(f"Sending answer: {answer}")
        try:
            bot_message = await update.message.reply_text(
                answer,
                parse_mode=ParseMode.HTML,
                reply_markup=get_persistent_menu(lang)
            )
            context.chat_data["places_shown"] = end_idx
            context.chat_data["last_bot_answer"] = answer
            context.chat_data["last_bot_message_id"] = bot_message.message_id
        except BadRequest as e:  # Используем импортированный BadRequest
            logger.error(f"Failed to send message: {e}, Original text: {answer}")
            fallback_text = "Произошла ошибка при обработке ответа. Попробуйте снова."
            await update.message.reply_text(fallback_text, parse_mode=ParseMode.HTML)
        return

    # Обычная обработка запроса о местах
    if detect_places_intent(text):
        await handle_places_query(update, context)
        return

    # Стандартная генерация ответа через OpenAI
    prompt = build_prompt_with_history(text, update, context)
    answer_raw = generate_answer(prompt, language=detected_lang)
    answer = validate_html(answer_raw)
    
    logger.debug(f"Sending answer: {answer}")
    try:
        bot_message = await update.message.reply_text(
            answer,
            parse_mode=ParseMode.HTML,
            reply_markup=get_persistent_menu(lang)
        )
        context.chat_data["last_bot_answer"] = answer
        context.chat_data["last_bot_message_id"] = bot_message.message_id
    except BadRequest as e:  # Используем импортированный BadRequest
        logger.error(f"Failed to send message: {e}, Original text: {answer}")
        fallback_text = "Произошла ошибка при обработке ответа. Попробуйте снова."
        await update.message.reply_text(fallback_text, parse_mode=ParseMode.HTML)

def build_prompt_with_history(new_query: str, update: Update, context: ContextTypes.DEFAULT_TYPE) -> str:
    chat_id = str(update.effective_chat.id)
    summary = get_summary_from_db(chat_id)
    recent_messages = context.chat_data.get("recent_messages", [])
    history_text = ""
    if summary:
        history_text += f"Summary: {summary}\n"
    if recent_messages:
        history_text += "\n".join(recent_messages)
    prompt = f"Conversation history:\n{history_text}\nNow answer the following query: {html.escape(new_query)}"
    return prompt

# ==================== Обработчик inline callback запросов ====================
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    try:
        await query.answer()
    except TimedOut as e:
        logger.error(f"Callback query timed out: {e}")
    data = query.data
    lang = context.user_data.get("lang", "en")
    
    if data.startswith("place:"):
        place_id = data.split(":")[1]
        description, photo_url = await get_detailed_place_info(place_id, lang, context)
        logger.debug(f"Place details for {place_id}: photo_url={photo_url}, description_length={len(description)}")
        
        try:
            if photo_url:
                bot_message = await safe_reply_photo(query.message, photo_url, description, ParseMode.HTML, context)
            else:
                bot_message = await send_long_message(update, description, ParseMode.HTML)
            
            if bot_message:
                context.chat_data["last_bot_message"] = {"id": bot_message.message_id, "text": description}
                await add_feedback_buttons(bot_message, context, lang)
            else:
                logger.error(f"Failed to send message for place_id {place_id}: bot_message is None")
                await query.message.reply_text(
                    translate_if_needed("Sorry, something went wrong while sending the details.", lang),
                    parse_mode=ParseMode.HTML
                )
        except Exception as e:
            logger.error(f"Error sending place details for place_id {place_id}: {e}")
            await query.message.reply_text(
                translate_if_needed("Sorry, an error occurred while fetching the details.", lang),
                parse_mode=ParseMode.HTML
            )
    
    elif data.startswith("feedback:"):
        parts = data.split(":")
        rating, message_id = parts[1], parts[2]
        chat_id = str(update.effective_chat.id)
        user_id = str(update.effective_user.id)
        
        last_message = context.chat_data.get("last_bot_message", {})
        if last_message.get("id") == int(message_id):
            message_text = last_message.get("text", "Unknown message")  # Полный текст ответа
            save_feedback_to_db(chat_id, user_id, message_text, rating)
            
            current_markup = query.message.reply_markup
            if current_markup and current_markup.inline_keyboard:
                updated_keyboard = [row for row in current_markup.inline_keyboard if not any(btn.callback_data.startswith("feedback:") for btn in row)]
                new_markup = InlineKeyboardMarkup(updated_keyboard) if updated_keyboard else None
                await query.edit_message_reply_markup(reply_markup=new_markup)
            
            await query.message.reply_text(
                translate_if_needed(f"Thank you for your feedback ({rating})!", lang),
                parse_mode=ParseMode.HTML
            )
        else:
            logger.warning(f"Feedback for message {message_id} not found in context.chat_data")

    elif data.startswith("lang:"):
        # ... (оставляем как есть)
        lang_choice = data.split(":")[1]
        context.user_data["lang"] = lang_choice
        try:
            await query.delete_message()
        except BadRequest as e:
            logger.error(f"Error deleting language selection message: {e}")
        chat_id = query.message.chat_id
        user = query.from_user
        greeting = get_dynamic_greeting(html.escape(user.first_name))
        if lang_choice.lower() in ["es", "spanish"]:
            message = (f"{greeting}\n\n"
                       "Soy tu conserje impulsado por AI para San Cristóbal de las Casas.\n\n"
                       "Este bot te ayudará a encontrar información detallada sobre tours, alojamiento, atracciones, restaurantes, consejos y eventos en la ciudad. "
                       "Puedes interactuar mediante los comandos del menú o escribiendo directamente tu consulta. \nPor ejemplo:\n"
                       "• 'Estoy de viaje con mi pareja y busco un hotel tranquilo fuera del centro.'\n"
                       "• 'Viajamos en familia; ¿qué actividades recomiendas para niños?'\n\n"
                       "Para ver ejemplos de uso y aprender a aprovechar todas las funciones, visita: "
                       "<a href='https://example.com/usage-guide'>Usage Guide</a>\n\n"
                       "I look forward to helping you explore the city!")
        else:
            message = (f"{greeting}\n\n"
                       "I'm your AI-powered concierge for San Cristóbal de las Casas.\n\n"
                       "This bot is designed to provide you with detailed information on tours, accommodation, attractions, restaurants, advices, and events in the city. "
                       "You can interact with the bot using the menu commands or simply type your query.\nFor example:\n"
                       "• 'I'm traveling with my partner and looking for a quiet hotel away from the center.'\n"
                       "• 'We're on a family trip; what activities do you recommend for children?'\n\n"
                       "For usage examples and to learn how to make the most of all the features, visit: "
                       "<a href='https://example.com/usage-guide'>Usage Guide</a>\n\n"
                       "I look forward to helping you explore the city!")
        await context.bot.send_message(chat_id=chat_id,
                                       text=message,
                                       parse_mode=ParseMode.HTML,
                                       reply_markup=get_persistent_menu(lang_choice))
        
    
    elif data == "noop":
        await query.answer()

    elif data.startswith("tour:"):
        try:
            tour_id = int(data.split("tour:")[1])
            await handle_tour_callback(tour_id, update, context)
        except ValueError:
            await query.edit_message_text(translate_if_needed("Invalid tour identifier.", lang), parse_mode=ParseMode.HTML)
    elif data.startswith("accom:"):
        try:
            accom_id = int(data.split("accom:")[1])
            await handle_accom_callback(accom_id, update, context)
        except ValueError:
            await query.edit_message_text(translate_if_needed("Invalid accommodation identifier.", lang), parse_mode=ParseMode.HTML)
    elif data.startswith("attr:"):
        try:
            attr_id = int(data.split("attr:")[1])
            await handle_attr_callback(attr_id, update, context)
        except ValueError:
            await query.edit_message_text(translate_if_needed("Invalid attraction identifier.", lang), parse_mode=ParseMode.HTML)
    elif data.startswith("rest:"):
        try:
            rest_id = int(data.split("rest:")[1])
            await handle_rest_callback(rest_id, update, context)
        except ValueError:
            await query.edit_message_text(translate_if_needed("Invalid restaurant identifier.", lang), parse_mode=ParseMode.HTML)
    else:
        await query.edit_message_text(translate_if_needed("Invalid callback data received.", lang), parse_mode=ParseMode.HTML)

# ==================== Обработчик ошибок ====================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    lang = get_user_lang(context)
    error_message = "An unexpected error occurred. Please try again later."
    error_message_translated = translate_if_needed(error_message, lang)
    
    logger.error(msg="Exception while handling an update:", exc_info=context.error)
    if update and update.effective_message:
        await update.effective_message.reply_text(error_message_translated, parse_mode=ParseMode.HTML)

# ==================== Основная функция запуска бота ====================
async def main():
    set_wal_mode()
    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("reset", reset_command))
    app.add_handler(CommandHandler("place", handle_place_command, filters=filters.Regex(r'^/place_')))  # Новый обработчик
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("setlanguage", set_language_command)],
        states={
            SELECTING_LANGUAGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_language_choice)]
        },
        fallbacks=[CommandHandler("cancel", cancel_language)]
    )
    app.add_handler(conv_handler)
    app.add_handler(CommandHandler("tours", tours_command))
    app.add_handler(CommandHandler("rooms", accommodation_command))
    app.add_handler(CommandHandler("attractions", attractions_command))
    app.add_handler(CommandHandler("restaurants", restaurants_command))
    app.add_handler(CommandHandler("advices", advices_command))
    app.add_handler(CommandHandler("faq", faq_command))
    app.add_handler(CommandHandler("events", events_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_error_handler(error_handler)
    logger.info("Bot started. Polling for updates...")
    await app.run_polling()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except RuntimeError as e:
        if "already running" in str(e):
            loop = asyncio.get_event_loop()
            loop.create_task(main())
            loop.run_forever()
        else:
            raise
