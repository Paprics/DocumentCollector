import asyncio
import hashlib
from pathlib import Path
import aiohttp
from bs4 import BeautifulSoup
from db.core.session import AsyncSessionLocal
from db.crud import file_hash_exists, insert_file_hash
from sources import SOURCES

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
    "товар",
    "інструк",
    "експлуатац",
    "драбина",
    "техничний",
    "паспорт якості",
    "сертификат соответствия",
)

async def download_single_file(session, db, sem, idx, file_name, url, save_dir):
    async with sem:
        try:
            print(f"\n[{idx}] Файл: {file_name}")
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                resp.raise_for_status()
                data = await resp.read()

            sha256 = hashlib.sha256(data).hexdigest()

            if await file_hash_exists(db, sha256):
                print(f"[ПРОПУЩЕНО] 🟡 Уже в базе | {sha256[:12]}…")
                return

            extension = Path(file_name).suffix
            stem = Path(file_name).stem
            new_name = f"{idx:03d}_{stem}{extension}"
            file_path = save_dir / new_name
            file_path.write_bytes(data)

            await insert_file_hash(db, sha256)
            print(f"[СОХРАНЕНО] ✅ {new_name} | {len(data):,} байт | {sha256[:12]}…")
        except Exception as e:
            print(f"[ОШИБКА] {file_name} → {url} | {e}")


async def download_files_from_html(
    html_path: Path,
    save_dir: Path | str = "downloads",
    keywords: tuple[str, ...] = PASSPORT_KEYWORDS,
    stop_words: tuple[str, ...] = STOP_WORDS,
    concurrent_limit: int = 10,
):
    """
    Ищет ссылки, где в названии есть хотя бы одно ключевое слово (как подстрока),
    но нет ни одного стоп-слова (тоже подстрока).
    Скачивает параллельно, проверяет хеш в БД.
    """
    save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nОбработка: {html_path.name}")
    html_content = html_path.read_text(encoding="utf-8", errors="replace")
    soup = BeautifulSoup(html_content, "html.parser")

    all_links = []
    for a in soup.find_all("a"):
        url = a.get("href")
        name = (a.text or "").strip()
        if not url or not name:
            continue

        name_lower = name.lower()

        has_keyword = any(kw.lower() in name_lower for kw in keywords)
        if not has_keyword:
            continue

        has_stop = any(sw.lower() in name_lower for sw in stop_words)
        if has_stop:
            print(f"[СТОП] {name}")
            continue

        all_links.append((name, url))

    print(f"Найдено подходящих ссылок: {len(all_links)}")
    if not all_links:
        return

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
            await asyncio.gather(*tasks, return_exceptions=True)


async def start_download(filter_id: int):
    src = SOURCES[filter_id]
    file_name = src['name']
    html_file = Path("output_data") / f"{filter_id}. {file_name}.html"
    save_dir = Path("downloads") / f"{filter_id}. {file_name}"

    await download_files_from_html(
        html_file,
        save_dir=save_dir,
        keywords=PASSPORT_KEYWORDS,
        stop_words=STOP_WORDS,
        concurrent_limit=10,
    )


if __name__ == "__main__":
    sources_id = 4
    asyncio.run(start_download(sources_id))