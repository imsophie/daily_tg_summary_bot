"""
Telegram-бот для сбора истории чата и ежедневного саммари через Claude API.

Как это работает:
1. Бот слушает все сообщения в чате/группе и сохраняет их в локальную БД (SQLite),
   отдельно по каждому разделу (теме), если в группе включён режим "Форум".
2. Раз в сутки (по расписанию) или по команде /summary бот берёт сообщения
   за последние 24 часа из ТОГО ЖЕ раздела, где была вызвана команда,
   и отправляет их в Claude API с просьбой сделать сводку.
3. Команда /summary доступна только администраторам чата.
4. Готовое саммари бот публикует в тот же раздел, откуда была вызвана команда.
"""

import asyncio
import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from anthropic import AsyncAnthropic
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
claude = AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

# Сколько секунд максимум ждать ответ от Claude API, прежде чем сдаться
CLAUDE_TIMEOUT_SECONDS = 60

# Максимальное число сообщений, которое отправляем в одном запросе на саммари
# (защита от слишком больших/медленных запросов при очень активных чатах)
MAX_MESSAGES_FOR_SUMMARY = 1500

# Минимальный перерыв между вызовами /summary в одном разделе (в секундах).
# Если команду вызвали повторно раньше — бот молча игнорирует вызов, без сообщений.
SUMMARY_COOLDOWN_SECONDS = 5 * 60  # 5 минут

# Хранит время последнего успешного запуска /summary для каждой пары (чат, раздел)
_last_summary_call: dict[tuple[int, int | None], float] = {}


# ---------- База данных ----------

def init_db():
    """Создаёт таблицу для хранения сообщений, если её ещё нет."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER,
            thread_id INTEGER,
            username TEXT,
            text TEXT,
            timestamp TEXT
        )
    """)
    conn.commit()
    conn.close()


def save_message(chat_id: int, thread_id: int | None, username: str, text: str):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO messages (chat_id, thread_id, username, text, timestamp) VALUES (?, ?, ?, ?, ?)",
        (chat_id, thread_id, username, text, datetime.now(TIMEZONE).isoformat()),
    )
    conn.commit()
    conn.close()


def get_messages_last_24h(chat_id: int, thread_id: int | None) -> list[tuple[str, str]]:
    """Возвращает список (username, text) за последние 24 часа для конкретного чата и раздела (темы).

    thread_id=None означает "общий раздел" (чат без тем, либо сообщения вне тем).
    """
    since = (datetime.now(TIMEZONE) - timedelta(hours=24)).isoformat()
    conn = sqlite3.connect(DB_PATH)
    if thread_id is None:
        rows = conn.execute(
            "SELECT username, text FROM messages WHERE chat_id = ? AND thread_id IS NULL AND timestamp >= ? ORDER BY timestamp",
            (chat_id, since),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT username, text FROM messages WHERE chat_id = ? AND thread_id = ? AND timestamp >= ? ORDER BY timestamp",
            (chat_id, thread_id, since),
        ).fetchall()
    conn.close()
    return rows


# ---------- Саммари через Claude ----------

def build_summary_prompt(messages: list[tuple[str, str]]) -> str:
    formatted = "\n".join(f"{username}: {text}" for username, text in messages)
    return (
        "Ниже — переписка из группового чата за последние сутки. "
        "Напиши сводку на русском языке в стиле самого этого чата — "
        "живо, неформально, будто ты такой же участник и просто пересказываешь "
        "подруге, что было за день.\n\n"
        "Ориентируйся на характер общения в чате:\n"
        "- Тон тёплый и слегка ироничный, без канцелярита и официоза\n"
        "- Уместны эмодзи (в меру, не в каждом предложении) — 😂🤩💜🙏\n"
        "- Можно использовать смайлики скобочками вроде \"))\" вместо точек в конце фразы\n"
        "- Разговорные словечки уместны (\"щас\", \"мож\", \"ток\", \"чет\"), но не переусердствуй\n"
        "- Обращайся к темам так же тепло, как участники обращаются друг к другу\n\n"
        "Структура (не делай формальные заголовки, пусть текст течёт естественно):\n"
        "- О чём вообще говорили\n"
        "- Если до чего-то договорились или что-то решили — упомяни\n"
        "- Если какие-то вопросы повисли без ответа — тоже упомяни\n"
        "Пиши компактно, без длинных вступлений.\n\n"
        f"Переписка:\n{formatted}"
    )


async def generate_summary(messages: list[tuple[str, str]]) -> str:
    if not messages:
        return "За последние сутки сообщений не было."

    # Если сообщений очень много — берём только последние N, чтобы не упереться
    # в лимиты API и не ждать ответ слишком долго
    if len(messages) > MAX_MESSAGES_FOR_SUMMARY:
        messages = messages[-MAX_MESSAGES_FOR_SUMMARY:]

    prompt = build_summary_prompt(messages)

    try:
        response = await asyncio.wait_for(
            claude.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=1000,
                messages=[{"role": "user", "content": prompt}],
            ),
            timeout=CLAUDE_TIMEOUT_SECONDS,
        )
    except asyncio.TimeoutError:
        logger.error("Claude API не ответил за %s секунд", CLAUDE_TIMEOUT_SECONDS)
        return "Не получилось собрать сводку — сервер долго не отвечал. Попробуйте ещё раз чуть позже."
    except Exception as e:
        logger.error("Ошибка при обращении к Claude API: %s", e)
        return "Не получилось собрать сводку — произошла ошибка при обращении к Claude API."

    return response.content[0].text


# ---------- Проверка прав администратора ----------

async def is_admin(chat_id: int, user_id: int) -> bool:
    """Проверяет, является ли пользователь админом или создателем чата."""
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return member.status in ("administrator", "creator")
    except Exception:
        # Если бот не может получить статус — на всякий случай считаем, что не админ
        return False


# ---------- Обработчики команд и сообщений ----------

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "Привет! Я собираю историю чата и раз в сутки делаю краткую сводку.\n"
        "Команда /summary — сделать сводку прямо сейчас (только для админов)."
    )


@dp.message(Command("summary"))
async def cmd_summary(message: Message):
    if not await is_admin(message.chat.id, message.from_user.id):
        await message.answer("Эта команда доступна только администраторам чата.")
        return

    thread_id = message.message_thread_id  # None, если раздел (тема) не используется

    # Проверка отката: если команду вызывали недавно в этом же разделе — молча игнорируем
    key = (message.chat.id, thread_id)
    now = time.monotonic()
    last_call = _last_summary_call.get(key)
    if last_call is not None and (now - last_call) < SUMMARY_COOLDOWN_SECONDS:
        return
    _last_summary_call[key] = now

    await message.answer("Собираю сводку за последние 24 часа по этому разделу...")
    messages = get_messages_last_24h(message.chat.id, thread_id)
    summary = await generate_summary(messages)
    await message.answer(summary)


@dp.message(F.text)
async def handle_message(message: Message):
    """Сохраняет каждое текстовое сообщение в БД, привязывая к конкретному разделу (теме)."""
    username = message.from_user.username or message.from_user.full_name
    save_message(message.chat.id, message.message_thread_id, username, message.text)


# ---------- Ежедневная задача по расписанию ----------

def get_active_threads_last_24h(chat_id: int) -> list[int | None]:
    """Возвращает список ID разделов (тем), в которых были сообщения за последние 24 часа."""
    since = (datetime.now(TIMEZONE) - timedelta(hours=24)).isoformat()
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT DISTINCT thread_id FROM messages WHERE chat_id = ? AND timestamp >= ?",
        (chat_id, since),
    ).fetchall()
    conn.close()
    return [row[0] for row in rows]


async def send_daily_summary(chat_id: int):
    """Отправляет отдельную сводку в каждый раздел (тему), где были сообщения за сутки."""
    thread_ids = get_active_threads_last_24h(chat_id)
    for thread_id in thread_ids:
        messages = get_messages_last_24h(chat_id, thread_id)
        summary = await generate_summary(messages)
        await bot.send_message(
            chat_id,
            f"📋 Сводка за сутки:\n\n{summary}",
            message_thread_id=thread_id,
        )


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
