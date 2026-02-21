import asyncio
import time
import random
from functools import wraps
from playwright.async_api import async_playwright, Page, TimeoutError as PWTimeout
from utils.funcs import save_files_as_html


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

async def process_participant(page: Page, participant_block):
    try:
        await participant_block.scroll_into_view_if_needed()
        await page.wait_for_timeout(300)

        accordion_triger = participant_block.locator('xpath=.//button[contains(@class, "accordion__trigger")]').first
        await accordion_triger.click()
        await page.wait_for_timeout(200)

        try:
            await participant_block.locator('xpath=.//span[@class="select__text"]').click()
            await page.wait_for_timeout(200)
            await participant_block.locator('xpath=.//div[@class="select__element"][last()]').click()
            await page.wait_for_timeout(200)
        except Exception:
            pass

        document_block = participant_block.locator('xpath=.//div[@class="documents"]/div/ul')
        count = await document_block.count()
        print(f'[INFO] на тендере найдено {count} документов')

        if count > 0:
            result = await fetch_documents(document_block)
            await asyncio.to_thread(save_files_as_html, url=page.url, files=result)

        await accordion_triger.scroll_into_view_if_needed()
        await page.wait_for_timeout(200)
        await accordion_triger.click()
        await page.wait_for_timeout(200)

    except Exception as e:
        print(f'[ERROR] (НЕПРЕДВИДЕННАЯ ОШИБКА) {e}')

@retry(attempts=100)
async def process_tender_page(page: Page, tender_url: str):
    url = f'https://prozorro.gov.ua/uk{tender_url}'
    await page.goto(url)

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
            print(f'[INFO] тендер {url} — нет участников')
            return []
        print(f'[INFO] тендер {url} — {count} участников')
        return participants_locator
    except PWTimeout:
        print(f'[DEB] участников не найдено 😌 {page.url}')
        return []

@retry(attempts=100)
async def fetch_tender_links(page: Page, page_index: int):
    url = (
        f"https://prozorro.gov.ua/uk/search/tender?"
        f"cpv=34110000-1&page={page_index}&status=complete&sort=publication_date,asc"
    )
    await page.goto(url)
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

async def handle_tender(context, tender_url):
    page = await context.new_page()
    try:
        participants = await process_tender_page(page, tender_url)
        if participants:
            for i in range(await participants.count()):
                participant_block = participants.nth(i)
                print(f'[INFO] Обработка {i + 1} участника тендера - {page.url}')
                await process_participant(page, participant_block)
    finally:
        await page.close()

async def run_scraper(start_page: int, end_page: int, headless: bool, max_concurrent_tenders: int):
    semaphore = asyncio.Semaphore(max_concurrent_tenders)

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

        page = await context.new_page()
        for page_index in range(start_page, end_page + 1):
            print(f"[INFO] Обработка страницы {page_index}")
            tender_links = await fetch_tender_links(page, page_index)
            if not tender_links:
                continue

            tasks = []
            for tender_url in tender_links:
                async def sem_task(url=tender_url):
                    async with semaphore:
                        await handle_tender(context, url)
                tasks.append(asyncio.create_task(sem_task()))

            await asyncio.gather(*tasks)
        await page.close()
        await browser.close()

if __name__ == "__main__":
    HEADLESS = False
    START_PAGE = 200
    END_PAGE = 300
    MAX_CONCURRENT_TENDERS = 7

    start_time = time.time()
    asyncio.run(run_scraper(
        start_page=START_PAGE,
        end_page=END_PAGE,
        headless=HEADLESS,
        max_concurrent_tenders=MAX_CONCURRENT_TENDERS
    ))
    end_time = time.time()
    elapsed = end_time - start_time
    print(f"[INFO] Скрипт завершён. Время работы: {elapsed:.2f} секунд ({elapsed/60:.2f} минут)")