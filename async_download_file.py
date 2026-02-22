import asyncio
import hashlib
from pathlib import Path

import aiohttp
from bs4 import BeautifulSoup

from db.core.session import async_session
from db.crud import file_hash_exists, insert_file_hash  # функции проверки и вставки хэша


async def download_files_from_html(
    html_path: Path,
    save_dir: Path | str = "downloads",
    keywords: tuple[str, ...] = ("пас", "pas")
):
    """
    Парсит HTML, ищет ссылки с ключевыми словами, скачивает файлы,
    проверяет их SHA256 в базе данных, сохраняет новые файлы с порядковым номером.
    """
    save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nОбработка HTML файла: {html_path.name}")
    html_content = html_path.read_text(encoding="utf-8")
    soup = BeautifulSoup(html_content, "html.parser")

    # Парсим ссылки с ключевыми словами
    all_links = []
    for a in soup.find_all("a"):
        url = a.get("href")
        name = a.text.strip()
        if url and name and any(kw.lower() in name.lower() for kw in keywords):
            all_links.append((name, url))

    print(f"Найдено ссылок с ключевыми словами: {len(all_links)}")
    if not all_links:
        return

    async with aiohttp.ClientSession() as session:
        async with async_session() as db:
            for idx, (file_name, url) in enumerate(all_links, 1):
                try:
                    print(f"\n[{idx}/{len(all_links)}] Файл: {file_name}")
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                        resp.raise_for_status()
                        data = await resp.read()
                        sha256 = hashlib.sha256(data).hexdigest()

                    # Проверка в базе
                    exists = await file_hash_exists(db, sha256)
                    if exists:
                        print(f"[ПРОПУЩЕНО] Файл уже есть в базе | хеш={sha256[:12]}")
                        continue

                    # Сохраняем файл с порядковым номером
                    extension = Path(file_name).suffix
                    stem = Path(file_name).stem
                    new_name = f"{idx}_{stem}{extension}"
                    file_path = save_dir / new_name
                    file_path.write_bytes(data)

                    # Вставляем хеш в базу
                    await insert_file_hash(db, sha256)
                    print(f"[СОХРАНЕНО] {new_name} | размер={len(data)} байт | хеш={sha256[:12]}")

                except Exception as e:
                    print(f"[ОШИБКА] {file_name} | {url} | {e}")


async def main():
    html_file = Path("output_data/output.html")
    await download_files_from_html(
        html_file,
        save_dir="downloads",
        keywords=("пас", "pas")
    )


if __name__ == "__main__":
    asyncio.run(main())