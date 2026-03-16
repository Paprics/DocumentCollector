import asyncio
import hashlib
from pathlib import Path

import aiohttp
from bs4 import BeautifulSoup

from db.core.session import AsyncSessionLocal
from db.crud import file_hash_exists, insert_file_hash

from sources import SOURCES
from notifications.telegram import send_notification_async


# ────────────────────────────────────────────────
#  Здесь удобно менять ключевые слова и стоп-слова
# ────────────────────────────────────────────────

PASSPORT_KEYWORDS = (
    "пас",
    "pas",
    "паспорт",
    "пасп",
    "паспорта",
)

STOP_WORDS = (
    "p7s",
    "сертификат",
    "сертифікат",
    "ключ",
    "якості",
    "товару",
    "талон",
    "технічний",
    "тех",
    "виробу",
    "шафа",
    "шафи",
    "товар",
    "інструк",
    "експлуатац",
    "драбина",
    "техничний",
    "паспорт якості",
    "сертификат соответствия",
    "сейф",
    "люк"
)


async def download_single_file(session, db, sem, idx, file_name, url, save_dir):
    """
    Скачивает один файл.

    Возвращает:
        "saved"     — успешно сохранён
        "exists"    — уже есть в базе
        "error"     — ошибка
    """

    async with sem:
        try:
            print(f"\n[{idx}] Файл: {file_name}")

            async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                resp.raise_for_status()
                data = await resp.read()

            sha256 = hashlib.sha256(data).hexdigest()

            if await file_hash_exists(db, sha256):
                print(f"[ПРОПУЩЕНО] 🟡 Уже в базе | {sha256[:12]}…")
                return "exists"

            extension = Path(file_name).suffix
            stem = Path(file_name).stem

            new_name = f"{idx:03d}_{stem}{extension}"
            file_path = save_dir / new_name

            file_path.write_bytes(data)

            await insert_file_hash(db, sha256)

            print(f"[СОХРАНЕНО] ✅ {new_name} | {len(data):,} байт | {sha256[:12]}…")

            return "saved"

        except Exception as e:
            print(f"[ОШИБКА] {file_name} → {url} | {e}")
            return "error"


async def download_files_from_html(
    html_path: Path,
    save_dir: Path | str = "downloads",
    keywords: tuple[str, ...] = PASSPORT_KEYWORDS,
    stop_words: tuple[str, ...] = STOP_WORDS,
    concurrent_limit: int = 10,
):
    """
    Возвращает статистику обработки HTML файла.
    """

    stats = {
        "links_total": 0,
        "filtered_no_keyword": 0,
        "filtered_stop_word": 0,
        "links_selected": 0,
        "saved": 0,
        "exists": 0,
        "errors": 0,
    }

    save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nОбработка: {html_path.name}")

    html_content = html_path.read_text(encoding="utf-8", errors="replace")
    soup = BeautifulSoup(html_content, "html.parser")

    all_links = []

    for a in soup.find_all("a"):

        stats["links_total"] += 1

        url = a.get("href")
        name = (a.text or "").strip()

        if not url or not name:
            stats["filtered_no_keyword"] += 1
            continue

        name_lower = name.lower()

        if not any(kw.lower() in name_lower for kw in keywords):
            stats["filtered_no_keyword"] += 1
            continue

        if any(sw.lower() in name_lower for sw in stop_words):
            stats["filtered_stop_word"] += 1
            print(f"[СТОП] {name}")
            continue

        all_links.append((name, url))

    stats["links_selected"] = len(all_links)

    print(f"Найдено подходящих ссылок: {len(all_links)}")

    if not all_links:
        return stats

    sem = asyncio.Semaphore(concurrent_limit)

    async with aiohttp.ClientSession() as session:
        async with AsyncSessionLocal() as db:

            tasks = []

            for idx, (file_name, url) in enumerate(all_links, 1):
                tasks.append(
                    asyncio.create_task(
                        download_single_file(session, db, sem, idx, file_name, url, save_dir)
                    )
                )

            results = await asyncio.gather(*tasks, return_exceptions=True)

    for r in results:

        if r == "saved":
            stats["saved"] += 1

        elif r == "exists":
            stats["exists"] += 1

        elif r == "error":
            stats["errors"] += 1

    return stats


async def start_download(sources_ids: tuple[int, ...]):
    """
    Обрабатывает список источников.
    """

    for filter_id in sources_ids:

        src = SOURCES[filter_id]
        file_name = src['name']

        html_file = Path("output_data") / f"{filter_id}. {file_name}.html"
        save_dir = Path("downloads") / f"{filter_id}. {file_name}"

        print(f"\n{'='*50}")
        print(f"Источник [{filter_id}]: {file_name}")
        print(f"{'='*50}")

        stats = await download_files_from_html(
            html_file,
            save_dir=save_dir,
            keywords=PASSPORT_KEYWORDS,
            stop_words=STOP_WORDS,
            concurrent_limit=8,
        )

        if DEBUG:

            msg = (
                f"📊 Отчет загрузки\n"
                f"source id: {filter_id} | {SOURCES.get(filter_id, {}).get('name')}\n\n"
                f"Всего ссылок: {stats['links_total']}\n"
                f"Отфильтровано (нет ключевых слов): {stats['filtered_no_keyword']}\n"
                f"Отфильтровано (стоп-слова): {stats['filtered_stop_word']}\n"
                f"Подходящих ссылок: {stats['links_selected']}\n\n"
                f"Сохранено: {stats['saved']}\n"
                f"Уже в базе: {stats['exists']}\n"
                f"Ошибки: {stats['errors']}"
            )

            await send_notification_async(msg)


if __name__ == "__main__":

    DEBUG = True
    sources_ids = (27,)

    asyncio.run(start_download(sources_ids))