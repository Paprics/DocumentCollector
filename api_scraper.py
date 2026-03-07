import json
from pathlib import Path

import httpx
import time
import random
from functools import wraps
from typing import Optional, Dict, List, Tuple
from urllib.parse import urlparse, parse_qs

from clients.data import cookies_list
from sources import SOURCES
from utils.funcs import save_files_as_html
from db.crud import sync_tender_exists, sync_insert_tender_to_db
from async_download_file import start_download
from wakepy import keep
from notifications.telegram import send_notification
import asyncio


def retry_on_none_or_429(max_attempts=100, delay=1.0):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 1
            while attempt <= max_attempts:
                try:
                    result = func(*args, **kwargs)

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

                if DEBUG and attempt < max_attempts:
                    print(f"→ ждём {delay:.2f} сек...")

                time.sleep(delay)
                attempt += 1

            print(f"[FAIL] {func.__name__} исчерпаны все {max_attempts} попыток")
            return None

        return wrapper
    return decorator


def get_random_cookies() -> Dict[str, str]:
    if not cookies_list:
        return {}
    idx = random.randint(0, len(cookies_list) - 1)
    return cookies_list[idx].get("cookies", {})


def build_query_params(query_params: dict) -> dict:
    """
    Преобразует query_params из parse_qs в правильный формат для API Prozorro.

    Правила:
      - 'region' (и любой ключ с одним значением) → key=value  (без индекса)
      - 'cpv' (и любой ключ с 1+ значениями)      → key[0]=v0, key[1]=v1, ...

    Исключение: если ключ == 'region' — всегда без индекса, даже если значений > 1.
    """
    result = {}

    # print(f'Пришел словарь: {query_params}')

    has_value_filter = False

    for key, values in query_params.items():

        if key == 'region':
            result[key] = values[0]

        elif key == 'cpv':
            for index, cpv in enumerate(values):
                result[f'cpv[{index}]'] = cpv

        elif key == 'value.start':
            result['value[amount][start]'] = values[0]
            has_value_filter = True

        elif key == 'value.end':
            result['value[amount][end]'] = values[0]
            has_value_filter = True

    if has_value_filter:
        result['value[currency]'] = 'UAH'

    # print(f'[RESULT] - {result}')
    # input('[STOP DEBUG]')

    return result



@retry_on_none_or_429(max_attempts=100)
def fetch_tender_detail(tender_id: str, cookies: Optional[Dict] = None) -> Optional[dict]:
    if cookies is None:
        cookies = get_random_cookies()
    url = f"https://prozorro.gov.ua/api/tenders/{tender_id}/details"
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "uk",
        "origin": "https://prozorro.gov.ua",
        "referer": "https://prozorro.gov.ua/uk/search/tender",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    }
    try:
        r = httpx.get(url, headers=headers, cookies=cookies, timeout=12)
        r.raise_for_status()
        data = r.json()
        return data if data else None
    except Exception as e:
        if DEBUG:
            print(f"[ERR detail] {tender_id} → {str(e).replace('\n',' ')[:150]}")
        return None


@retry_on_none_or_429(max_attempts=100)
def fetch_tender_lots(tender_id: str, cookies: Optional[Dict] = None) -> Optional[dict]:
    if cookies is None:
        cookies = get_random_cookies()
    url = f"https://prozorro.gov.ua/api/tenders/{tender_id}/lots"
    headers = {
        "accept": "application/json, text/plain, */*",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    try:
        r = httpx.get(url, headers=headers, cookies=cookies, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        if DEBUG:
            print(f"[ERR lots] {tender_id} → {e}")
        return None


@retry_on_none_or_429(max_attempts=100)
def fetch_search_page(params: dict, cookies: Optional[Dict] = None) -> Optional[dict]:
    if cookies is None:
        cookies = get_random_cookies()
    url = "https://prozorro.gov.ua/api/search/tenders"
    headers = {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/x-www-form-urlencoded",
        "origin": "https://prozorro.gov.ua",
        "referer": "https://prozorro.gov.ua/uk/search/tender",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/129 Safari/537.36",
    }

    try:
        r = httpx.post(url, headers=headers, params=params, cookies=cookies, timeout=15)

        print(r.url)

        if r.status_code == 429:
            return None
        r.raise_for_status()

        return r.json()
    except Exception as e:
        if DEBUG:
            print(f"[ERR search page] → {e} | status={getattr(r, 'status_code', '—')}")
        return None


def extract_tender_ids_from_search(response: dict) -> List[str]:
    data = response.get("data", [])
    return [item["tenderID"] for item in data if "tenderID" in item]


def parse_bids_documents(bids: list) -> List[Tuple[str, str]]:
    if not bids:
        return []
    result = []
    seen = set()
    for bid in bids:
        docs = bid.get("publicDocuments", {})
        if not docs or not isinstance(docs, dict):
            continue
        for group_docs in docs.values():
            if not isinstance(group_docs, list):
                continue
            for doc in group_docs:
                if not isinstance(doc, dict):
                    continue
                url = doc.get("url", "").strip()
                if not url or url in seen:
                    continue
                seen.add(url)
                title = doc.get("title", "Без назви").strip()
                result.append((title, url))
    return result


def parse_lots_documents(lots_data: dict) -> List[Tuple[str, str]]:
    lots = lots_data.get("lots", [])
    if not lots:
        return []
    result = []
    seen_titles = set()
    for lot in lots:
        for bid in lot.get("bids", []):
            docs = bid.get("publicDocuments", {})
            for group_docs in docs.values():
                for doc in group_docs:
                    if not isinstance(doc, dict):
                        continue
                    title = doc.get("title", "Без назви").strip()
                    url = doc.get("url", "").strip()
                    if url and title not in seen_titles:
                        seen_titles.add(title)
                        result.append((title, url))
    return result


def parse_tender_documents(raw_data: dict) -> List[Tuple[str, str]]:
    try:
        if not raw_data:
            return []

        lots = raw_data.get("lots")
        if lots:
            lots_data = fetch_tender_lots(raw_data["tenderID"])
            if lots_data:
                return parse_lots_documents(lots_data)
            return []

        bids = raw_data.get("bids")
        if bids:
            return parse_bids_documents(bids)
        return []

    except Exception as e:
        tender_id = raw_data.get("tenderID", "unknown")

        msg = f"⚠️ [WARN] parse_tender_documents тендер {tender_id} → {e}"
        print(msg)
        send_notification(msg)

        try:
            log_dir = Path("LOGS/error")
            log_dir.mkdir(parents=True, exist_ok=True)

            file_path = log_dir / f"tender_error_{tender_id}.json"

            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(raw_data, f, ensure_ascii=False, indent=2)

        except Exception as log_err:
            print(f"[LOG ERROR] Не удалось сохранить JSON → {log_err}")

        return []


def main(source_idx: int):
    start_time = time.time()
    start_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time))
    print(f"Скрипт запущен: {start_str}")

    source = SOURCES.get(source_idx, {})
    if not source or "url" not in source:
        print("Нет такого источника или отсутствует url")
        return

    # Имя файла — только из 'name', без расширения пока
    base_name = source.get('name', f"без_имени_{source_idx}")

    parsed_url = urlparse(source["url"])
    query_params = parse_qs(parsed_url.query)

    search_params = build_query_params(query_params)

    cookies = get_random_cookies()

    total_documents = 0
    processed_tenders = 0
    successful_tenders = 0
    page = 1

    while True:
        params = search_params.copy()
        if page > 1:
            params["page"] = page

        print(f"\n[PAGE {page}] Запрос страницы поиска...")
        page_data = fetch_search_page(params, cookies)

        if DEBUG:
            print(f'[DEB] {page_data}')


        if not page_data:
            print(f"[PAGE {page}] Не удалось получить данные → прерываем")
            break

        total = page_data.get("total", 0)
        per_page = page_data.get("per_page", 100)
        data_list = page_data.get("data", [])

        # ── Проверка на подозрительно большое количество тендеров ──
        TOTAL_ERROR_THRESHOLD = 10000  # вероятный баг

        if total >= TOTAL_ERROR_THRESHOLD:
            msg = f"🔴 [ОШИБКА] Найдено {total} тендеров — проверь URL или параметры! | {base_name}"
            print(msg)
            send_notification(msg)
            return

        if not data_list:
            print(f"[PAGE {page}] Пустой список тендеров → завершаем")
            break

        tender_ids_on_page = extract_tender_ids_from_search(page_data)

        pages_total = (total + 19) // 20

        print(f"[PAGE {page}/{pages_total}] Найдено тендеров на странице: {len(tender_ids_on_page)} "
              f"(всего в системе: {total})")

        for idx, tender_id in enumerate(tender_ids_on_page, 1):
            processed_tenders += 1

            if processed_tenders % 10 == 0:
                print(f" Обработано тендеров всего: {processed_tenders}")

            if sync_tender_exists(tender_id):
                if DEBUG:
                    print(f"[DB] 🔷 Тендер {tender_id} уже в базе → пропускаем")
                continue

            detail = fetch_tender_detail(tender_id, cookies)
            if not detail:
                print(f" {tender_id} → detail не получен")
                continue

            docs = parse_tender_documents(detail)
            count = len(docs)
            total_documents += count

            if count > 0:
                successful_tenders += 1
                print(f"\n{tender_id} | документов: {count}")
                # if DEBUG:
                #     for title, url in docs:
                #         print(f" {title}")
                #         print(f" {url}")
                #         print("-" * 80)

                # Передаё только имя без пути — функция сама разберётся
                save_files_as_html(tender_id, docs, base_name, source_idx)

            # ────────────── [ВСТАВКА: вставка тендера в базу] ──────────────
            inserted = sync_insert_tender_to_db(tender_id)

            if DEBUG:
                if inserted:
                    pass
                else:
                    print(f"[DB ERROR] 🔴 Тендер {tender_id} НЕ вставлен (возможно уже есть)")
            # ────────────────────────────────────────────────────────────────

        if DEBUG:
            print(f"[PAGE {page}] Итого документов после страницы: {total_documents}")

        if len(data_list) < per_page or processed_tenders >= total:
            print("Достигнут конец результатов поиска")
            break

        page += 1
        time.sleep(random.uniform(2.0, 4.5))

    end_time = time.time()
    end_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_time))
    duration_sec = end_time - start_time
    hours, remainder = divmod(int(duration_sec), 3600)
    minutes, seconds = divmod(remainder, 60)

    print("\n" + "=" * 100)

    msg = (
        f"Завершено.\n"
        f"Старт:    {start_str}\n"
        f"Финиш:    {end_str}\n"
        f"Время работы: {hours:02d}:{minutes:02d}:{seconds:02d} "
        f"\n"
        f"Всего обработано тендеров:          {processed_tenders:>6}\n"
        f"Тендеров с документами (успешных):  {successful_tenders:>6}\n"
        f"Всего собрано документов:           {total_documents:>6}"
    )

    # вывод в терминал
    print(msg)

    # отправка того же текста в Telegram
    send_notification(f"[Завершено] ✅✅✅ {base_name}\n\n{msg}")

    if successful_tenders > 0:
        avg_docs = total_documents / successful_tenders
        print(f"В среднем документов на успешный тендер: {avg_docs:.2f}")
    print("="*100)

    if DOWNLOAD_FILES:
        print(f'Скачивание файлов...')
        asyncio.run(start_download(filter_id=source_idx))



if __name__ == "__main__":

    DEBUG = False
    DOWNLOAD_FILES = False

    source_indexes = range(37, 48)  # ← нужные индексы

    with keep.running():
        for source_idx in source_indexes:
            print(f"\n{'='*100}")
            msg= f"▶️  Запуск source_idx={source_idx}"
            print(msg)
            send_notification(msg)
            print(f"{'='*100}")
            try:
                main(source_idx)
            except Exception as e:
                msg = f"🔴 [ОШИБКА] source_idx={source_idx} завершился с исключением: {e}"
                print(msg)
                send_notification(f'[ERROR] Я упала - {msg}')
                print("   Продолжаем следующий источник...")