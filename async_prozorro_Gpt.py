import asyncio
import sys
import time
import random
from functools import wraps
from playwright.async_api import async_playwright, Page, TimeoutError as PWTimeout
from utils.funcs import save_files_as_html
from datetime import datetime

from db.crud import insert_tender, tender_exists
from db.core.session import async_session
import argparse
from async_download_file import start_download
from notifications.telegram import send_notification_async

from sources import SOURCES


def cli(default_source, default_max_tabs):
    """
    Функция получения source и tabs:
    - default_source: int, tuple, list или "all"
    - default_max_tabs: int

    Возвращает:
    - source_tuple: кортеж int ключей SOURCES
    - tabs: int
    """

    parser = argparse.ArgumentParser(
        description="Run Scraper with selected source(s) и concurrency tabs",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # Приведение дефолта source к списку строк для argparse
    if default_source == "all":
        default_arg = ["all"]
    elif isinstance(default_source, int):
        default_arg = [str(default_source)]
    elif isinstance(default_source, (tuple, list)):
        default_arg = [str(x) for x in default_source]
    else:
        raise ValueError("default_source должен быть int, tuple, list или 'all'")

    parser.add_argument(
        "-s", "--source",
        nargs="+",
        default=default_arg,
        help="Resource IDs to run, e.g., 1 2 3 or 'all'"
    )

    parser.add_argument(
        "-t", "--tabs",
        type=int,
        default=default_max_tabs,
        metavar="TABS",
        help="Number of parallel browser tabs (concurrency)"
    )

    # Парсим CLI аргументы
    args, unknown = parser.parse_known_args()

    # Нормализация source
    raw_sources = args.source

    if len(raw_sources) == 1 and str(raw_sources[0]).lower() == "all":
        source_tuple = tuple(SOURCES.keys())
    else:
        source_list = []
        for item in raw_sources:
            try:
                source_list.append(int(item))
            except ValueError:
                raise ValueError(f"Source IDs должны быть числами или 'all', получили '{item}'")
        source_tuple = tuple(source_list)

    return source_tuple, args.tabs


# -----------------------
# Retry decorator: повторяет функцию, если вернула None
# -----------------------
def retry(attempts=100, delay_range=(2, 3)):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(1, attempts + 1):
                result = await func(*args, **kwargs)
                if result is not None:
                    print(f"[INFO] {func.__name__} — успех с попытки {attempt}")
                    return result
                print(f"[WARN] попытка {attempt}/{attempts} — результат None")
                if attempt < attempts:
                    await asyncio.sleep(random.uniform(*delay_range))
            print(f"[ERROR] {func.__name__} — попытки исчерпаны")
            return None

        return wrapper

    return decorator


# -----------------------
# Получает список документов участника
# -----------------------
async def fetch_documents(documents):
    result = []
    for i in range(await documents.count()):
        document_item = documents.nth(i)
        links = await document_item.locator('a').all()
        for link in links:
            spans = await link.locator('span').all_text_contents()
            title = spans[0] if spans else await link.text_content() or "Без названия"
            href = await link.get_attribute('href') or "Без ссылки"
            result.append((title, href))
    return result


# -----------------------
# Обработка отдельного участника тендера
# -----------------------
async def process_participant(page: Page, participant_block, filter_id, source_name):
    try:
        await participant_block.scroll_into_view_if_needed()
        await page.wait_for_timeout(300)

        accordion_triger = participant_block.locator(
            'xpath=.//button[contains(@class, "accordion__trigger")]'
        ).first
        await accordion_triger.click()
        await page.wait_for_timeout(200)

        try:
            await participant_block.locator(
                'xpath=.//span[@class="select__text"]'
            ).click(timeout=4000)
            await page.wait_for_timeout(200)
            await participant_block.locator(
                'xpath=.//div[@class="select__element"][last()]'
            ).click()
            await page.wait_for_timeout(200)
        except Exception:
            pass

        document_block = participant_block.locator(
            'xpath=.//div[@class="documents"]/div/ul'
        )
        count = await document_block.count()
        print(f'[INFO] на тендере найдено {count} документов')

        if count > 0:
            result = await fetch_documents(document_block)
            filename = f'output_data/{filter_id}. {source_name}.html'
            await asyncio.to_thread(save_files_as_html, url=page.url, files=result,
                                    filename=filename)

        await accordion_triger.scroll_into_view_if_needed()
        await page.wait_for_timeout(200)
        await accordion_triger.click()
        await page.wait_for_timeout(200)


    except Exception as e:
        print(f'[ERROR] (НЕПРЕДВИДЕННАЯ ОШИБКА) {e}')


# -----------------------
# Обработка страницы тендера
# -----------------------
@retry(attempts=100)
async def process_tender_page(page: Page, tender_url: str):
    """
    Обработка страницы тендера. Получает или "Предложения" или "Лоты"
    :param page:
    :param tender_url:
    :return:
    """
    await page.goto(tender_url)
    # Ожидание загрузки страницы (проверка)
    try:
        title_locator = page.locator('//h2[contains(@class, "title--large")]')
        await title_locator.first.wait_for(timeout=2000)
        title_text = await title_locator.first.text_content()
        if not title_text:
            return None
    except PWTimeout:
        return None

    try:
        participants_locator = page.locator(
            '//section[contains(@class, "register")]//div[contains(@class, "accordion")]'
        )
        await participants_locator.first.wait_for(timeout=2500)
        count = await participants_locator.count()
        if count == 0:
            print(f'[INFO] 🟡 тендер {tender_url} — нет участников')
            return []
        print(f'[INFO] 🟢 тендер {tender_url} — {count} участников')
        return participants_locator
    except PWTimeout:
        print(f'[DEB] 🟡 участников не найдено {page.url}')
        return []


# -----------------------
# Получение ссылок на тендеры со страницы поиска
# -----------------------
@retry(attempts=100)
async def fetch_tender_links(page: Page, page_url: str):
    try:
        await page.goto(page_url)
        links_locator = page.locator(
            '//ul[@class="search-result__list"]//a[contains(@class,"item-title__title")]'
        )
        await links_locator.first.wait_for(timeout=10000)
        links = await links_locator.evaluate_all(
            "els => els.map(e => e.getAttribute('href'))"
        )
        return [l for l in links if l] or None
    except PWTimeout:
        return None


# -----------------------
# Обработка отдельного тендера
# -----------------------
async def handle_tender(context, tender_url, filter_id, source_name):
    page = await context.new_page()
    try:
        participants = await process_tender_page(page, tender_url)
        if participants:
            participant_blocks = await participants.all()
            for i, participant_block in enumerate(participant_blocks):
                print(f'[INFO] Обработка {i + 1} участника тендера - {page.url}')
                await process_participant(page, participant_block, filter_id, source_name)
    finally:
        await page.close()


# -----------------------
# Worker для обработки тендеров из очереди
# -----------------------
async def tender_worker(name: str, context, queue: asyncio.Queue, filter_id: int, source_name: str):
    async with async_session() as session:
        while True:
            tender_url = await queue.get()
            try:
                tender_id = tender_url.rstrip('/').split('/')[-1]

                # exists = await tender_exists(session, tender_id)
                # if exists:
                #     print(f"[INFO] {name} — тендер {tender_id} уже в базе, пропускаем")
                #     continue

                await insert_tender(session, tender_id)
                print(f"[INFO] {name} обрабатывает {tender_url}")
                await handle_tender(context, tender_url, filter_id, source_name)

            except Exception as e:
                print(f"[ERROR] {name} — ошибка обработки {tender_url}: {e}")
            finally:
                queue.task_done()


# -----------------------
# Producer для заполнения очереди параллельно с воркерами
# -----------------------
async def fetch_links_worker(
        name: str,
        context,
        start_page: int,
        end_page: int,
        queue: asyncio.Queue,
        source: dict,
):
    async with async_session() as session:
        page = await context.new_page()
        try:
            for page_index in range(start_page, end_page + 1):
                page_url = source['url'].format(page=page_index)
                print(f"[INFO] {name} — обработка страницы {page_index}")

                tender_links = await fetch_tender_links(page, page_url)

                if not tender_links:
                    continue

                for link in tender_links:
                    tender_id = link.rstrip('/').split('/')[-1]

                    exists = await tender_exists(session, tender_id)
                    if exists:
                        print(f"[INFO] {name} — тендер {tender_id} уже в базе, пропускаем")
                        continue

                    await queue.put(f'https://prozorro.gov.ua/uk{link}')
        except Exception as e:
            msg = f'🔴 [ERROR] (НЕПРЕДВИДЕННАЯ ОШИБКА) в fetch_links_worker {name}: {type(e).__name__}: {e}'
            print(msg)
        finally:
            await page.close()


# -----------------------
# Главный запуск скрапера с параллельным Producer + Consumers
# -----------------------
async def run_scraper(
    start_page: int,
    end_page: int,
    headless: bool,
    max_tabs: int,
    source: dict,
    filter_id: int,
    source_name: str,
):
    queue = asyncio.Queue()

    async with async_playwright() as pw:
        try:
            browser = await pw.chromium.launch(
                headless=headless,
                args=["--no-sandbox", "--disable-setuid-sandbox"]
            )
            context = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/117.0.0.0 Safari/537.36"
                ),
                locale="uk-UA"
            )

            workers = [
                asyncio.create_task(tender_worker(f"Worker-{i + 1}", context, queue, filter_id, source_name))
                for i in range(max_tabs)
            ]

            producer = asyncio.create_task(fetch_links_worker(
                "Producer",
                context,
                start_page,
                end_page,
                queue,
                source=source
            ))

            await producer
            await queue.join()

            for w in workers:
                w.cancel()

            await browser.close()

        except Exception as e:
            msg = f'🔴 [ERROR] (НЕПРЕДВИДЕННАЯ ОШИБКА) в run_scraper (filter_id={filter_id}): {type(e).__name__}: {e}'
            print(msg)
            await send_notification_async(msg)


# -----------------------
# Entry point
# -----------------------
def main(
    default_source_id,
    default_max_tabs,
    headless,
    download,
):
    try:
        selected_source, concurrency_tabs = cli(default_source_id, default_max_tabs)
        print(f'[INFO] Старт выполнения. Выбраны: {selected_source} | Кол-во вкладок: {concurrency_tabs}')

        if selected_source == 'all':
            sources_to_process = list(SOURCES.keys())
        elif isinstance(selected_source, (int, str)):
            sources_to_process = [selected_source]
        else:
            sources_to_process = list(selected_source)

        for filter_id in sources_to_process:
            if filter_id not in SOURCES:
                print(f"[WARNING] Источник {filter_id} не найден в SOURCES — пропускаем")
                continue

            current_source = SOURCES[filter_id]

            print(
                f"[INFO] Выбран: {filter_id}, "
                f"Max page: {current_source['max_page']}, "
                f"Downloads: {download}, "
                f"Concurrency Tabs: {concurrency_tabs}, "
                f"Title: {current_source['name']} | {current_source['comment']}\n"
                f"{current_source['url']}\n"
            )

            asyncio.run(send_notification_async(
                f"🟡🟡🟡🟡🟡\nSTART {datetime.now():%d-%m-%Y %H:%M:%S}\n"
                f"Источник: {filter_id} | {current_source['name']}\n"
                f"Max page: {current_source['max_page']}, "
                f"Concurrency: {concurrency_tabs}, "
                f"Downloads: {download}"
            ))

            END_PAGE = current_source['max_page']
            start_time = time.time()

            asyncio.run(
                run_scraper(
                    start_page     = 1,
                    end_page       = END_PAGE,
                    headless       = headless,
                    max_tabs       = concurrency_tabs,
                    source         = current_source,
                    filter_id      = filter_id,
                    source_name    = current_source['name'],
                )
            )

            elapsed = time.time() - start_time
            hours, remainder = divmod(int(elapsed), 3600)
            minutes, _ = divmod(remainder, 60)

            print(
                f"\n[INFO] Источник {filter_id} завершён. "
                f"Время: {hours} ч {minutes} мин\n"
            )

            asyncio.run(
                send_notification_async(
                    f"🟢 Источник {filter_id} завершён\n"
                    f"{datetime.now():%d-%m-%Y %H:%M:%S}\n"
                    f"Время работы: {hours} ч {minutes} мин"
                )
            )

            if download:
                asyncio.run(send_notification_async(
                    f"🟩 Старт загрузки файлов для {filter_id}\n"
                    f"{datetime.now():%d-%m-%Y %H:%M:%S}"
                ))

                asyncio.run(start_download(filter_id))

                asyncio.run(send_notification_async(
                    f'✅ Загрузка файлов для {filter_id} завершена\n'
                    f'{datetime.now():%d-%m-%Y %H:%M:%S}'
                ))

    except Exception as e:
        msg = f'🔴 [ERROR] (НЕПРЕДВИДЕННАЯ ОШИБКА) в main (глобальная): {type(e).__name__}: {e}'
        print(msg)
        asyncio.run(send_notification_async(msg))
        raise   # полный стек — оставь


if __name__ == "__main__":

    DEFAULT_SOURCE_ID  = 30, 31         # int, tuple или 'all'
    DEFAULT_MAX_TABS   = 2
    HEADLESS           = True
    DOWNLOAD           = True

    SEND_NOTIFICATION = False

    main(
        default_source_id = DEFAULT_SOURCE_ID,
        default_max_tabs  = DEFAULT_MAX_TABS,
        headless          = HEADLESS,
        download          = DOWNLOAD
    )