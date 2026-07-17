"""
Telegram-бот для сбора истории чата и ежедневного саммари через Claude API.

Как это работает:
1. Бот слушает все сообщения в чате/группе и сохраняет их в локальную БД (SQLite).
2. Раз в сутки (по расписанию) или по команде /summary бот берёт сообщения
   за последние 24 часа и отправляет их в Claude API с просьбой сделать сводку.
3. Готовое саммари бот публикует в тот же чат.
"""

import asyncio
import logging
import os
import sqlite3
from datetime import datetime, timedelta, timezone

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from anthropic import Anthropic
from dotenv import load_dotenv

# ---------- Настройки ----------

load_dotenv()  # подтягивает переменные из .env файла

BOT_TOKEN = os.getenv("BOT_TOKEN")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
DB_PATH = "chat_history.db"

# Часовой пояс GMT+3 (Москва / Калининград не путать, тут просто фиксированный сдвиг)
TIMEZONE = timezone(timedelta(hours=3))

# Во сколько (по времени GMT+3) публиковать ежедневное саммари
SUMMARY_HOUR = 22
SUMMARY_MINUTE = 0

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
claude = Anthropic(api_key=ANTHROPIC_API_KEY)


# ---------- База данных ----------

def init_db():
    """Создаёт таблицу для хранения сообщений, если её ещё нет."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            username TEXT,
            text TEXT,
            timestamp TEXT
        )
    """)
    conn.commit()
    conn.close()


def save_message(chat_id: int, username: str, text: str):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO messages (chat_id, username, text, timestamp) VALUES (?, ?, ?, ?)",
        (chat_id, username, text, datetime.now(TIMEZONE).isoformat()),
    )
    conn.commit()
    conn.close()


def get_messages_last_24h(chat_id: int) -> list[tuple[str, str]]:
    """Возвращает список (username, text) за последние 24 часа для конкретного чата."""
    since = (datetime.now(TIMEZONE) - timedelta(hours=24)).isoformat()
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT username, text FROM messages WHERE chat_id = ? AND timestamp >= ? ORDER BY timestamp",
        (chat_id, since),
    ).fetchall()
    conn.close()
    return rows


# ---------- Саммари через Claude ----------

def build_summary_prompt(messages: list[tuple[str, str]]) -> str:
    formatted = "\n".join(f"{username}: {text}" for username, text in messages)
    return (
        "Ниже — переписка из группового чата за последние сутки. "
        "Сделай краткую структурированную сводку на русском языке:\n"
        "- Основные темы обсуждения\n"
        "- Важные решения или договорённости (если были)\n"
        "- Вопросы, оставшиеся без ответа (если есть)\n"
        "Пиши кратко, без вступлений, сразу по делу.\n\n"
        f"Переписка:\n{formatted}"
    )


def generate_summary(messages: list[tuple[str, str]]) -> str:
    if not messages:
        return "За последние сутки сообщений не было."

    prompt = build_summary_prompt(messages)
    response = claude.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text


# ---------- Обработчики команд и сообщений ----------

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "Привет! Я собираю историю чата и раз в сутки делаю краткую сводку.\n"
        "Команда /summary — сделать сводку прямо сейчас."
    )


@dp.message(Command("summary"))
async def cmd_summary(message: Message):
    await message.answer("Собираю сводку за последние 24 часа...")
    messages = get_messages_last_24h(message.chat.id)
    summary = generate_summary(messages)
    await message.answer(summary)


@dp.message(F.text)
async def handle_message(message: Message):
    """Сохраняет каждое текстовое сообщение в БД."""
    username = message.from_user.username or message.from_user.full_name
    save_message(message.chat.id, username, message.text)


# ---------- Ежедневная задача по расписанию ----------

async def send_daily_summary(chat_id: int):
    messages = get_messages_last_24h(chat_id)
    summary = generate_summary(messages)
    await bot.send_message(chat_id, f"📋 Сводка за сутки:\n\n{summary}")


def setup_scheduler(chat_id: int):
    scheduler = AsyncIOScheduler(timezone=TIMEZONE)
    scheduler.add_job(
        send_daily_summary,
        trigger=CronTrigger(hour=SUMMARY_HOUR, minute=SUMMARY_MINUTE),
        args=[chat_id],
    )
    scheduler.start()


# ---------- Точка входа ----------

async def main():
    init_db()

    # ВАЖНО: впишите сюда ID своего чата/канала, чтобы включить автоматическую
    # ежедневную рассылку сводки. Узнать chat_id можно, например, через бота @userinfobot,
    # добавив его в свой чат, либо распечатав message.chat.id в handle_message при первом запуске.
    TARGET_CHAT_ID = None  # например: -1001234567890

    if TARGET_CHAT_ID:
        setup_scheduler(TARGET_CHAT_ID)

    logger.info("Бот запущен")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
