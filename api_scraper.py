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

DEBUG = True
DOWNLOAD_FILES = True



def retry_on_none_or_429(max_attempts=100, base_delay=2.0, jitter=0.5):
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
                delay = base_delay * (1.5 ** (attempt - 1)) + random.uniform(-jitter, jitter)
                delay = min(delay, 60)
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
    KEYS_WITHOUT_INDEX = {"region"}  # ключи, которые всегда идут без [i]

    result = {}
    for key, values in query_params.items():
        if key in KEYS_WITHOUT_INDEX:
            # region=1-6  (берём первое значение)
            result[key] = values[0]
        else:
            # cpv[0]=..., cpv[1]=..., — всегда с индексом, даже если один
            for i, value in enumerate(values):
                result[f"{key}[{i}]"] = value

    print(query_params)
    print(result)
    print('должно быть https://prozorro.gov.ua/api/search/tenders?cpv%5B0%5D=09240000-3')

    return result

    # query_params = {'region': ['1-6'], 'cpv': ['44100000-1', '44200000-2', '44420000-0', '44900000-9']}
    # нужно - region=1-6&cpv[0]=44100000-1&cpv[1]=44200000-2&cpv[2]=44420000-0&cpv[3]=44900000-9

    # query_params = {'cpv': ['09240000-3']}
    # нужно - cpv[0]=09240000-3

    # for key, values in query_params.items():
    #     for i, value in enumerate(values):
    #         search_params[f"{key}[{i}]"] = value



@retry_on_none_or_429(max_attempts=80, base_delay=2.5)
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
            print(f"[ERR detail] {tender_id} → {str(e)[:150].replace('\n', ' ')}")
        return None


@retry_on_none_or_429(max_attempts=40, base_delay=3.0)
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


@retry_on_none_or_429(max_attempts=20, base_delay=4.0)
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
        for group_docs in docs.values():
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
        if raw_data.get("lots"):
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
        TOTAL_WARN_THRESHOLD = 5000  # предупреждение
        TOTAL_ERROR_THRESHOLD = 10000  # вероятный баг

        if total >= TOTAL_ERROR_THRESHOLD:
            print(f"🔴 [ОШИБКА] Найдено {total} тендеров — проверь URL или параметрах!")
            print(f"   Ожидаемых страниц: ~{(total // per_page) + 1}. Прерываем.")
            return

        elif total >= TOTAL_WARN_THRESHOLD:
            print(
                f"⚠️  [ВНИМАНИЕ] Найдено {total} тендеров (~{(total // per_page) + 1} стр.) — проверь параметры запроса.")

        if not data_list:
            print(f"[PAGE {page}] Пустой список тендеров → завершаем")
            break

        tender_ids_on_page = extract_tender_ids_from_search(page_data)

        print(f"[PAGE {page}] Найдено тендеров на странице: {len(tender_ids_on_page)} "
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
                if DEBUG:
                    for title, url in docs:
                        print(f" {title}")
                        print(f" {url}")
                        print("-" * 80)

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

    print("\n" + "="*100)
    print("Завершено.")
    print(f"Старт:    {start_str}")
    print(f"Финиш:    {end_str}")
    print(f"Время работы: {hours:02d}:{minutes:02d}:{seconds:02d} (всего {duration_sec:.1f} сек)")
    print(f"Всего обработано тендеров:          {processed_tenders:>6}")
    print(f"Тендеров с документами (успешных):  {successful_tenders:>6}")
    print(f"Всего собрано документов:           {total_documents:>6}")
    if successful_tenders > 0:
        avg_docs = total_documents / successful_tenders
        print(f"В среднем документов на успешный тендер: {avg_docs:.2f}")
    print("="*100)

    if DOWNLOAD_FILES:
        print(f'Скачивание файлов...')
        asyncio.run(start_download(filter_id=source_idx))



if __name__ == "__main__":

    source_indexes = (2, 3, 4)  # ← добавляй нужные индексы

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