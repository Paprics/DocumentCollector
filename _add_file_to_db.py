"""
Скрипт для добавления hash значений в БД(Синхронизация).
Одноразово.
"""
import asyncio
import hashlib
from pathlib import Path

import aiohttp
from bs4 import BeautifulSoup

from db.core.session import async_session
from db.crud import insert_file_hash, file_hash_exists  # убедись, что эти функции есть


KEYWORDS = ("пас", "pas")


def match_filename(name: str) -> bool:
    name_lower = name.lower()
    return any(k in name_lower for k in KEYWORDS)


def extract_links(html: str) -> list[tuple[str, str]]:
    """
    Возвращает [(имя файла, url)]
    """
    soup = BeautifulSoup(html, "html.parser")
    result = []

    for a in soup.find_all("a"):
        url = a.get("href")
        name = a.text.strip()

        if not url or not name:
            continue

        if match_filename(name):
            result.append((name, url))

    return result


async def download_and_hash(session: aiohttp.ClientSession, url: str) -> tuple[bytes, str]:
    """
    Скачивает файл в память и возвращает (данные, SHA256 хеш)
    """
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
        resp.raise_for_status()
        data = await resp.read()
        sha256 = hashlib.sha256(data).hexdigest()
        return data, sha256


async def process_html_file(path: Path, http: aiohttp.ClientSession):
    print(f"\nОбрабатываю HTML-файл: {path.name}")

    html = path.read_text(encoding="utf-8")
    links = extract_links(html)

    if not links:
        print("  Совпадающие ссылки не найдены.")
        return

    async with async_session() as db:
        for filename, url in links:
            try:
                data, sha256 = await download_and_hash(http, url)

                exists = await file_hash_exists(db, sha256)
                if exists:
                    action = "ПРОПУЩЕНО"
                    print(f"[{action}] {filename} | {url} | размер={len(data)} байт | хеш={sha256[:12]}")
                    continue

                inserted = await insert_file_hash(db, sha256)
                action = "НОВЫЙ" if inserted else "ДУБЛИКАТ"

                print(f"[{action}] {filename} | {url} | размер={len(data)} байт | хеш={sha256[:12]}")

            except Exception as e:
                print(f"[ОШИБКА] {filename} | {url} | {e}")


async def main():
    html_files = Path("output_data").glob("*.html")

    async with aiohttp.ClientSession() as http:
        for file_path in html_files:
            await process_html_file(file_path, http)


if __name__ == "__main__":
    asyncio.run(main())