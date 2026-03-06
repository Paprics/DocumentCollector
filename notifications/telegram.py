# notifications/telegram.py

import os
import asyncio
from dotenv import load_dotenv
from telethon import TelegramClient

load_dotenv()

API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
TG_CHANNEL = os.getenv('TG_CHANEL')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SESSION_FILE = os.path.join(BASE_DIR, 'session')


# ─── ASYNC ───────────────────────────────────────────────────────────────────

async def send_notification_async(msg: str):
    """Асинхронная — использовать с await внутри async-функций."""
    async with TelegramClient(SESSION_FILE, API_ID, API_HASH) as client:
        await client.send_message(TG_CHANNEL, msg)


# ─── SYNC ────────────────────────────────────────────────────────────────────

def send_notification(msg: str):
    """Синхронная — можно вызывать из обычного кода."""
    try:
        asyncio.run(send_notification_async(msg))
    except Exception as e:
        print(f"[TG ERROR] Не удалось отправить уведомление: {e}")


if __name__ == '__main__':
    send_notification("Тестовое синхронное уведомление")

    asyncio.run(send_notification_async("Тестовое асинхронное уведомление"))