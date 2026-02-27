import asyncio
import time
import random
from functools import wraps
from playwright.async_api import async_playwright, Page, TimeoutError as PWTimeout
from utils.funcs import save_files_as_html
from datetime import datetime


from sqlalchemy.ext.asyncio import AsyncSession
from db.crud import insert_tender, tender_exists
from db.core.session import async_session
import argparse
from async_download_file import start_download
from notifications.telegram import send_notification, send_notification_async

from sources import SOURCES


def cli(default_source_id, default_max_tabs):
    parser = argparse.ArgumentParser(
        description="Run scraper with selectable source and concurrency",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "-s", "--source",
        type=int,
        default=default_source_id,
        metavar="ID",
        help="Source ID from SOURCES dictionary"
    )

    parser.add_argument(
        "-c", "--concurrency",
        type=int,
        default=default_max_tabs,
        metavar="N",
        help="Number of parallel browser tabs"
    )

    args = parser.parse_args()
    return args.source, args.concurrency


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
async def process_participant(page: Page, participant_block):
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
            filename = f'output_data/{selected_source}. {current_source['name']}.html'
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
    await page.goto(tender_url)
    try:
        title_locator = page.locator('//h2[contains(@class, "title--large")]')
        await title_locator.first.wait_for(timeout=5000)
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
    await page.goto(page_url)
    try:
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
async def handle_tender(context, tender_url):
    page = await context.new_page()
    try:
        participants = await process_tender_page(page, tender_url)
        if participants:
            participant_blocks = await participants.all()  # собираем сразу все локаторы
            for i, participant_block in enumerate(participant_blocks):
                print(f'[INFO] Обработка {i + 1} участника тендера - {page.url}')
                await process_participant(page, participant_block)
    finally:
        await page.close()


# -----------------------
# Worker для обработки тендеров из очереди
# -----------------------
async def tender_worker(name: str, context, queue: asyncio.Queue):
    async with async_session() as session:
        while True:
            tender_url = await queue.get()
            try:
                tender_id = tender_url.rstrip('/').split('/')[-1]

                # Проверяем в базе перед обработкой
                exists = await tender_exists(session, tender_id)
                if exists:
                    print(f"[INFO] {name} — тендер {tender_id} уже в базе, пропускаем")
                    continue

                await insert_tender(session, tender_id)
                print(f"[INFO] {name} обрабатывает {tender_url}")
                await handle_tender(context, tender_url)

            except Exception as e:
                print(f"[ERROR] {name} — ошибка обработки {tender_url}: {e}")
            finally:
                queue.task_done()


# -----------------------
# Producer для заполнения очереди параллельно с воркерами
# -----------------------
async def fetch_links_worker(name: str, context, start_page: int, end_page: int, queue: asyncio.Queue):
    async with async_session() as session:
        page = await context.new_page()
        for page_index in range(start_page, end_page + 1):
            page_url = current_source['url'].format(page=page_index)
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
        await page.close()


# -----------------------
# Главный запуск скрапера с параллельным Producer + Consumers
# -----------------------
async def run_scraper(start_page: int, end_page: int,
                      headless: bool, max_tabs: int):
    queue = asyncio.Queue()

    async with async_playwright() as pw:
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

        # Запускаем воркеров сразу
        workers = [
            asyncio.create_task(tender_worker(f"Worker-{i + 1}", context, queue))
            for i in range(max_tabs)
        ]

        # Producer отдельно
        producer = asyncio.create_task(fetch_links_worker(
            "Producer", context, start_page, end_page, queue
        ))

        await producer  # ждём, пока Producer закончит
        await queue.join()  # ждём, пока воркеры обработают всё

        for w in workers:
            w.cancel()

        await browser.close()


# -----------------------
# Entry point
# -----------------------
if __name__ == "__main__":

    try:
        DOWNLOAD = True

        # дефолты для дебага / запуска без CLI
        DEFAULT_SOURCE_ID = 0
        DEFAULT_MAX_TABS = 10

        selected_source, concurrency_tabs = cli(DEFAULT_SOURCE_ID, DEFAULT_MAX_TABS)

        if selected_source not in SOURCES:
            raise ValueError(f"Source ID {selected_source} не найден в SOURCES")

        current_source = SOURCES[selected_source]

        print(
            f"[INFO] Выбран: {selected_source}, "
            f"Max page: {current_source['max_page']}, Downloads: {DOWNLOAD}, "
            f"Concurrency Tabs: {concurrency_tabs}, "
            f"Title: {current_source['name']} | {current_source['comment']}\n"
            f"{current_source['url']}\n"
        )

        send_notification(
            f"🟡 START {datetime.now():%d-%m-%Y %H:%M:%S}\n"
            f"Выбран: {selected_source}, "
            f"Max page: {current_source['max_page']}, Downloads: {DOWNLOAD}, "
            f"Concurrency Tabs: {concurrency_tabs}, "
            f"Title: {current_source['name']}"
        )

        HEADLESS = True
        END_PAGE = current_source['max_page']

        start_time = time.time()

        asyncio.run(run_scraper(
            start_page=1,
            end_page=END_PAGE,
            headless=HEADLESS,
            max_tabs=concurrency_tabs
        ))

        elapsed = time.time() - start_time
        hours, remainder = divmod(int(elapsed), 3600)
        minutes, _ = divmod(remainder, 60)

        print(f"\n[INFO] Скрипт завершён. Время работы: {hours} часов {minutes} минут, Current source: {selected_source}\n{current_source}")

        asyncio.run(send_notification_async(
            f"🟢 [INFO] Скрипт завершён\n{datetime.now():%d-%m-%Y %H:%M:%S}."
            f"\nВремя работы: {hours} часов {minutes} минут, "
            f"Current source: {selected_source}"
        ))

        # Загрузка файлов
        if DOWNLOAD:
            asyncio.run(send_notification_async(f"🟢 [INFO] Старт загрузки файлов\n{datetime.now():%d %m %Y %H:%M:%S}."))
            asyncio.run(start_download(selected_source))
            asyncio.run(send_notification_async(f'✅ [INFO] Загрузка завершена\n{datetime.now():%d %m %Y %H:%M:%S}.'))

    except Exception as e:
        print(f"\n[ERROR] Скрипт упал с ошибкой: {type(e).__name__} - {e}")

        asyncio.run(send_notification_async(
            f"🔴 [ERROR] Скрипт упал\n{datetime.now():%d-%m-%Y %H:%M:%S}\n"
            f"Ошибка: {type(e).__name__} - {e}"
        ))