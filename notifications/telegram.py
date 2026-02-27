# notifications/telegram.py

import os
from dotenv import load_dotenv
from telethon.sync import TelegramClient as SyncTelegramClient
from telethon.sync import TelegramClient

load_dotenv()

API_ID = int(os.getenv('API_ID'))
API_HASH = os.getenv('API_HASH')
TG_CHANNEL = os.getenv('TG_CHANEL')  # @username или invite link

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SESSION_FILE = os.path.join(BASE_DIR, 'session')


def create_client() -> SyncTelegramClient:

    client = SyncTelegramClient(SESSION_FILE, API_ID, API_HASH)
    client.start()  # синхронная авторизация
    return client

_client = None

def get_sync_client():
    """
    Инициализация Telegram: создаёт и авторизует сессию для последующей работы.
    """
    global _client
    if _client is None:
        from telethon.sync import TelegramClient
        _client = TelegramClient(SESSION_FILE, API_ID, API_HASH)
        _client.start()
    return _client


def send_notification(msg: str):
    """
    Отправляет уведомление в Telegram-канал синхронно.
    """
    client = create_client()
    try:
        client.send_message(TG_CHANNEL, msg)
    finally:
        client.disconnect()


async def send_notification_async(msg: str):
    """
    Асинхронная функция для отправки уведомления в Telegram.
    """
    async with TelegramClient(SESSION_FILE, API_ID, API_HASH) as client:
        await client.send_message(TG_CHANNEL, msg)


if __name__ == '__main__':

    send_notification("Тестовое синхронное уведомление")

    import asyncio
    asyncio.run(send_notification_async("Тестовое асинхронное уведомление"))