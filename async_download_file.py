import asyncio
import aiohttp
import aiofiles
from pathlib import Path
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parent
MAX_CONCURRENT_DOWNLOADS = 5
download_count = 2191             # глобальный счётчик реально сохранённых файлов
count_lock = asyncio.Lock()    # для безопасного обновления счётчика

def sanitize_name(name: str) -> str:
    """Сделать имя файла безопасным для Windows"""
    forbidden = '<>:"/\\|?*'
    table = str.maketrans({c: '_' for c in forbidden})
    return name.translate(table) or 'file'

def iter_links(html_path: Path):
    """Генератор (текст ссылки, URL) из HTML"""
    with open(html_path, encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')
        for a in soup.find_all('a', href=True):
            yield a.get_text(strip=True), a['href']

async def download_file(semaphore: asyncio.Semaphore, session: aiohttp.ClientSession, url: str, base_name: str, output_dir: Path, simulate: bool = False, retries: int = 5):
    """
    Надёжная загрузка файла. Файл получает имя с глобальным счётчиком.
    """
    global download_count
    async with semaphore:
        for attempt in range(1, retries + 1):
            try:
                # Если симуляция — просто печатаем
                if simulate:
                    async with count_lock:
                        download_count += 1
                        numbered_name = f"{download_count}_{sanitize_name(base_name)}"
                        print(f"{download_count}: [SIMULATE] Would download: {output_dir / numbered_name}")
                    return

                async with session.get(url, timeout=30) as resp:
                    resp.raise_for_status()
                    content = await resp.read()
                    if not content:
                        raise Exception("Empty content")

                    # резервное имя — сначала без счётчика
                    temp_name = sanitize_name(base_name)
                    temp_path = output_dir / temp_name
                    async with aiofiles.open(temp_path, 'wb') as f:
                        await f.write(content)

                # проверка файла на диске
                if not temp_path.exists() or temp_path.stat().st_size == 0:
                    raise Exception("File not written")

                # Если всё ок — увеличиваем счётчик и переименовываем файл
                async with count_lock:
                    download_count += 1
                    numbered_name = f"{download_count}_{sanitize_name(base_name)}"
                    final_path = output_dir / numbered_name
                    temp_path.rename(final_path)
                    print(f"{download_count}: Saved {final_path}")

                return

            except Exception as e:
                print(f"Attempt {attempt} failed for {url}: {e}")
                if attempt < retries:
                    await asyncio.sleep(2)
                else:
                    print(f"Failed after {retries} attempts: {url}")

async def download_files_by_name(html_path: Path, output_dir: Path, keywords: tuple[str, ...], simulate: bool = False):
    """Основная функция загрузки всех файлов по ключевым словам"""
    output_dir.mkdir(parents=True, exist_ok=True)
    keywords = tuple(k.lower() for k in keywords)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for name, url in iter_links(html_path):
            name_lower = name.lower()
            if not any(k in name_lower for k in keywords):
                continue
            tasks.append(download_file(semaphore, session, url, name, output_dir, simulate))

        await asyncio.gather(*tasks)

if __name__ == '__main__':
    html_file = BASE_DIR / 'output_data' / 'Легковые авто [200 -290 стр.].html'
    download_dir = BASE_DIR / 'downloads'
    keywords = ('pas', 'пас')
    simulate_mode = False

    asyncio.run(download_files_by_name(html_file, download_dir, keywords, simulate=simulate_mode))
