import asyncio
import random
import time
import os
from functools import wraps
from typing import Optional, Dict, List, Tuple
from urllib.parse import urlparse, parse_qs
from itertools import cycle

import httpx
from dotenv import load_dotenv

from clients.data import cookies_list
from sources import SOURCES
from utils.funcs import save_files_as_html
from db.crud import async_tender_exists, async_insert_tender_to_db
from async_download_file import start_download
from wakepy import keep
from notifications.telegram import send_notification_async

load_dotenv()

DEBUG = True
DOWNLOAD_FILES = False
WORKERS_COUNT = 1  # кол-во параллельных воркеров (расширяй до ~10)


# ─── Прокси ───────────────────────────────────────────────────────────────────

def load_proxies() -> List[str]:
    raw = os.getenv("PROXIES", "")
    proxies = [p.strip() for p in raw.split(",") if p.strip()]
    if not proxies:
        print("⚠️  [PROXY] Прокси не найдены в .env — работаем без прокси")
    else:
        print(f"✅ [PROXY] Загружено прокси: {len(proxies)} шт.")
    return proxies


PROXIES: List[str] = load_proxies()
_proxy_cycle = cycle(PROXIES) if PROXIES else None


def get_next_proxy() -> Optional[str]:
    if _proxy_cycle is None:
        return None
    return next(_proxy_cycle)


# ─── Cookies ──────────────────────────────────────────────────────────────────

def get_random_cookies() -> Dict[str, str]:
    if not cookies_list:
        return {}
    return random.choice(cookies_list).get("cookies", {})


# ─── Retry decorator (async) ──────────────────────────────────────────────────

def async_retry(max_attempts=80, base_delay=2.5, jitter=0.5):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            attempt = 1
            while attempt <= max_attempts:
                try:
                    result = await func(*args, **kwargs)
                    if result is not None:
                        if DEBUG:
                            print(f"[OK] {func.__name__} успех на попытке {attempt}")
                        return result
                    if DEBUG:
                        print(f"[WARN] {func.__name__} → None, попытка {attempt}/{max_attempts}")
                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 429:
                        if DEBUG:
                            print(f"[429] Rate limit, попытка {attempt}/{max_attempts}")
                    else:
                        raise
                except Exception as e:
                    print(f"[EXC] {func.__name__} → {type(e).__name__}: {e}")
                    if attempt == max_attempts:
                        raise
                delay = base_delay * (1.5 ** (attempt - 1)) + random.uniform(-jitter, jitter)
                delay = min(delay, 60)
                if DEBUG and attempt < max_attempts:
                    print(f"→ ждём {delay:.2f} сек...")
                await asyncio.sleep(delay)
                attempt += 1
            print(f"[FAIL] {func.__name__} исчерпаны все {max_attempts} попыток")
            return None
        return wrapper
    return decorator


# ─── HTTP клиент с прокси ─────────────────────────────────────────────────────

def make_client(proxy: Optional[str] = None) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        proxy=proxy,
        timeout=15,
        follow_redirects=True,
    )


# ─── Fetch функции ────────────────────────────────────────────────────────────

HEADERS_BASE = {
    "accept": "application/json, text/plain, */*",
    "accept-language": "uk",
    "origin": "https://prozorro.gov.ua",
    "referer": "https://prozorro.gov.ua/uk/search/tender",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
}


@async_retry(max_attempts=80, base_delay=2.5)
async def fetch_tender_detail(tender_id: str, client: httpx.AsyncClient) -> Optional[dict]:
    url = f"https://prozorro.gov.ua/api/tenders/{tender_id}/details"
    try:
        r = await client.get(url, headers=HEADERS_BASE, cookies=get_random_cookies())
        r.raise_for_status()
        data = r.json()
        return data if data else None
    except Exception as e:
        if DEBUG:
            print(f"[ERR detail] {tender_id} → {str(e)[:150].replace(chr(10), ' ')}")
        return None


@async_retry(max_attempts=40, base_delay=3.0)
async def fetch_tender_lots(tender_id: str, client: httpx.AsyncClient) -> Optional[dict]:
    url = f"https://prozorro.gov.ua/api/tenders/{tender_id}/lots"
    try:
        r = await client.get(url, headers=HEADERS_BASE, cookies=get_random_cookies())
        r.raise_for_status()
        return r.json()
    except Exception as e:
        if DEBUG:
            print(f"[ERR lots] {tender_id} → {e}")
        return None


@async_retry(max_attempts=20, base_delay=4.0)
async def fetch_search_page(params: dict, client: httpx.AsyncClient) -> Optional[dict]:
    url = "https://prozorro.gov.ua/api/search/tenders"
    try:
        r = await client.post(url, headers=HEADERS_BASE, params=params, cookies=get_random_cookies())
        print(f"  [SEARCH URL] {r.url}")
        if r.status_code == 429:
            return None
        r.raise_for_status()
        return r.json()
    except Exception as e:
        if DEBUG:
            print(f"[ERR search page] → {e}")
        return None


# ─── Парсинг документов ───────────────────────────────────────────────────────

def parse_bids_documents(bids: list) -> List[Tuple[str, str]]:
    if not bids:
        return []
    result, seen = [], set()
    for bid in bids:
        for group_docs in bid.get("publicDocuments", {}).values():
            for doc in group_docs:
                if not isinstance(doc, dict):
                    continue
                url = doc.get("url", "").strip()
                if not url or url in seen:
                    continue
                seen.add(url)
                result.append((doc.get("title", "Без назви").strip(), url))
    return result


def parse_lots_documents(lots_data: dict) -> List[Tuple[str, str]]:
    lots = lots_data.get("lots", [])
    if not lots:
        return []
    result, seen_titles = [], set()
    for lot in lots:
        for bid in lot.get("bids", []):
            for group_docs in bid.get("publicDocuments", {}).values():
                for doc in group_docs:
                    if not isinstance(doc, dict):
                        continue
                    title = doc.get("title", "Без назви").strip()
                    url = doc.get("url", "").strip()
                    if url and title not in seen_titles:
                        seen_titles.add(title)
                        result.append((title, url))
    return result


async def parse_tender_documents(raw_data: dict, client: httpx.AsyncClient) -> List[Tuple[str, str]]:
    try:
        if not raw_data:
            return []
        if raw_data.get("lots"):
            lots_data = await fetch_tender_lots(raw_data["tenderID"], client)
            return parse_lots_documents(lots_data) if lots_data else []
        bids = raw_data.get("bids")
        if bids:
            return parse_bids_documents(bids)
        return []
    except Exception as e:
        tender_id = raw_data.get("tenderID", "unknown")
        msg = f"⚠️ [WARN] parse_tender_documents тендер {tender_id} → {e}"
        print(msg)
        await send_notification_async(msg)
        return []


# ─── Вспомогательные ──────────────────────────────────────────────────────────

def build_query_params(query_params: dict) -> dict:
    KEYS_WITHOUT_INDEX = {"region"}
    result = {}
    for key, values in query_params.items():
        if key in KEYS_WITHOUT_INDEX:
            result[key] = values[0]
        else:
            for i, value in enumerate(values):
                result[f"{key}[{i}]"] = value
    return result


def extract_tender_ids(response: dict) -> List[str]:
    return [item["tenderID"] for item in response.get("data", []) if "tenderID" in item]


# ─── Producer ─────────────────────────────────────────────────────────────────

async def producer(search_params: dict, queue: asyncio.Queue, stats: dict):
    proxy = get_next_proxy()
    async with make_client(proxy) as client:
        page = 1
        while True:
            params = search_params.copy()
            if page > 1:
                params["page"] = page

            print(f"\n[PRODUCER] 📄 Страница {page}...")
            page_data = await fetch_search_page(params, client)

            if not page_data:
                print(f"[PRODUCER] ❌ Страница {page} — нет данных, завершаем")
                break

            total = page_data.get("total", 0)
            per_page = page_data.get("per_page", 100)
            data_list = page_data.get("data", [])

            if total >= 10000:
                print(f"🔴 [PRODUCER] Найдено {total} тендеров — подозрительно много, прерываем!")
                break
            elif total >= 5000:
                print(f"⚠️  [PRODUCER] Найдено {total} тендеров (~{(total // per_page) + 1} стр.)")

            if not data_list:
                print(f"[PRODUCER] ✅ Страница {page} пуста — конец результатов")
                break

            tender_ids = extract_tender_ids(page_data)
            print(f"[PRODUCER] Страница {page}: {len(tender_ids)} тендеров (всего в системе: {total})")

            for tender_id in tender_ids:
                # TODO: заменить на async версию
                if await async_tender_exists(tender_id):
                    if DEBUG:
                        print(f"[DB] 🔷 {tender_id} уже в базе → пропускаем")
                    stats["skipped"] += 1
                    continue
                await queue.put(tender_id)

            if len(data_list) < per_page or stats.get("processed_total", 0) >= total:
                print("[PRODUCER] ✅ Достигнут конец результатов")
                break

            page += 1
            await asyncio.sleep(random.uniform(1.5, 3.0))

    await queue.put(None)  # сигнал завершения


# ─── Worker ───────────────────────────────────────────────────────────────────

async def worker(worker_id: int, queue: asyncio.Queue, base_name: str, source_idx: int,
                 stats: dict, stop_event: asyncio.Event):
    proxy = PROXIES[worker_id % len(PROXIES)] if PROXIES else None
    proxy_label = proxy.split("@")[-1] if proxy else "без прокси"
    print(f"[WORKER-{worker_id}] 🚀 Старт | прокси: {proxy_label}")

    async with make_client(proxy) as client:
        while not stop_event.is_set():
            try:
                tender_id = await asyncio.wait_for(queue.get(), timeout=30)
            except asyncio.TimeoutError:
                if stop_event.is_set():
                    break
                continue

            if tender_id is None:
                # Передаём сигнал дальше для других воркеров
                await queue.put(None)
                stop_event.set()
                break

            async with stats["lock"]:
                stats["processed_total"] += 1
                count_so_far = stats["processed_total"]

            if count_so_far % 10 == 0:
                print(f"[WORKER-{worker_id}] 📊 Обработано всего: {count_so_far}")

            detail = await fetch_tender_detail(tender_id, client)
            if not detail:
                print(f"[WORKER-{worker_id}] ⚠️  {tender_id} → detail не получен")
                queue.task_done()
                continue

            docs = await parse_tender_documents(detail, client)
            count = len(docs)

            async with stats["lock"]:
                stats["total_documents"] += count

            if count > 0:
                async with stats["lock"]:
                    stats["successful_tenders"] += 1
                print(f"[WORKER-{worker_id}] ✅ {tender_id} | документов: {count}")
                save_files_as_html(tender_id, docs, base_name, source_idx)

            # TODO: заменить на async версию
            inserted = await async_insert_tender_to_db(tender_id)
            if DEBUG and not inserted:
                print(f"[DB ERROR] 🔴 {tender_id} НЕ вставлен")

            queue.task_done()

    print(f"[WORKER-{worker_id}] 🏁 Завершил работу")


# ─── Main ─────────────────────────────────────────────────────────────────────

async def run_source(source_idx: int):
    source = SOURCES.get(source_idx, {})
    if not source or "url" not in source:
        print(f"❌ Нет источника с idx={source_idx}")
        return

    base_name = source.get("name", f"без_имени_{source_idx}")
    parsed_url = urlparse(source["url"])
    query_params = parse_qs(parsed_url.query)
    search_params = build_query_params(query_params)

    stats = {
        "processed_total": 0,
        "successful_tenders": 0,
        "total_documents": 0,
        "skipped": 0,
        "lock": asyncio.Lock(),
    }

    queue: asyncio.Queue = asyncio.Queue(maxsize=200)
    stop_event = asyncio.Event()

    workers = [
        asyncio.create_task(
            worker(i, queue, base_name, source_idx, stats, stop_event)
        )
        for i in range(WORKERS_COUNT)
    ]

    prod = asyncio.create_task(producer(search_params, queue, stats))

    await asyncio.gather(prod, *workers)

    return stats


async def main_async():
    source_indexes = (5,)

    for source_idx in source_indexes:
        print(f"\n{'='*100}")
        msg = f"▶️  Запуск source_idx={source_idx}"
        print(msg)
        await send_notification_async(msg)
        print(f"{'='*100}")

        start_time = time.time()
        start_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time))
        print(f"Скрипт запущен: {start_str}")

        try:
            stats = await run_source(source_idx)
        except Exception as e:
            msg = f"🔴 [ОШИБКА] source_idx={source_idx} завершился с исключением: {e}"
            print(msg)
            await send_notification_async(f"[ERROR] Я упала - {msg}")
            print("Продолжаем следующий источник...")
            continue

        end_time = time.time()
        end_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time))
        duration_sec = end_time - start_time
        hours, remainder = divmod(int(duration_sec), 3600)
        minutes, seconds = divmod(remainder, 60)

        print("\n" + "="*100)
        print("Завершено.")
        print(f"Старт:    {start_str}")
        print(f"Финиш:    {end_str}")
        print(f"Время работы: {hours:02d}:{minutes:02d}:{seconds:02d} ({duration_sec:.1f} сек)")
        if stats:
            print(f"Всего обработано тендеров:          {stats['processed_total']:>6}")
            print(f"Пропущено (уже в базе):             {stats['skipped']:>6}")
            print(f"Тендеров с документами (успешных):  {stats['successful_tenders']:>6}")
            print(f"Всего собрано документов:           {stats['total_documents']:>6}")
            if stats["successful_tenders"] > 0:
                avg = stats["total_documents"] / stats["successful_tenders"]
                print(f"Среднее документов на тендер:       {avg:>9.2f}")
        print("="*100)

        if DOWNLOAD_FILES:
            print("📥 Скачивание файлов...")
            await start_download(filter_id=source_idx)


if __name__ == "__main__":
    with keep.running():
        asyncio.run(main_async())