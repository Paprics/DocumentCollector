import asyncio
import random
import time
from functools import wraps
from typing import Optional, List, Tuple

from playwright.async_api import async_playwright, Page, TimeoutError as PWTimeout, BrowserContext

from utils.funcs import save_files_as_html   # предполагается, что эта функция уже асинхронная или синхронная — адаптируйте при необходимости


def retry(attempts: int = 100, delay_range: tuple = (2, 3)):
    """
    Декоратор повторов для асинхронных функций
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(page: Page, *args, **kwargs):
            for attempt in range(1, attempts + 1):
                result = await func(page, *args, **kwargs)
                if result is not None:
                    print(f"[INFO] {func.__name__} — успех с попытки {attempt}")
                    return result
                print(f"[WARN] попытка {attempt}/{attempts} — результат None")
                if attempt < attempts:
                    sleep_time = random.uniform(*delay_range)
                    await asyncio.sleep(sleep_time)
                    try:
                        await page.reload()
                    except Exception as e:
                        print(f"[WARN] reload failed: {e}")
            print(f"[ERROR] {func.__name__} — попытки исчерпаны")
            try:
                await page.close()
            except Exception:
                pass
            return None
        return wrapper
    return decorator


async def fetch_documents(documents) -> List[Tuple[str, str]]:
    result = []
    count = await documents.count()
    for i in range(count):
        document_item = documents.nth(i)
        links = await document_item.locator('a').all()
        for link in links:
            spans = await link.locator('span').all_text_contents()
            title = spans[0] if spans else (await link.text_content() or "Без названия")
            href = await link.get_attribute('href') or "Без ссылки"
            result.append((title, href))
    return result


async def process_participant(page: Page, participant_block):
    try:
        await participant_block.scroll_into_view_if_needed()
        await page.wait_for_timeout(300)

        # OPEN ACCORDION
        accordion_trigger = participant_block.locator('xpath=.//button[contains(@class, "accordion__trigger")]').first
        await accordion_trigger.click()
        await page.wait_for_timeout(200)

        # Попытка открыть все документы
        try:
            await participant_block.locator('xpath=.//span[@class="select__text"]').click(timeout=4000)
            await page.wait_for_timeout(200)
            await participant_block.locator('xpath=.//div[@class="select__element"][last()]').click()
            await page.wait_for_timeout(200)
        except Exception as e:
            print(f"[ERROR] ошибка получения всех док или < 5 → {e}")

        try:
            document_block = participant_block.locator('xpath=.//div[@class="documents"]/div/ul')
            doc_count = await document_block.count()
            print(f'[INFO] на тендере найдено {doc_count} документов')

            if doc_count > 0:
                docs = await fetch_documents(document_block)
                # Если save_files_as_html синхронная — оборачиваем в loop.run_in_executor
                await asyncio.get_running_loop().run_in_executor(
                    None, save_files_as_html, page.url, docs
                )
            else:
                print(f'[WARN] Документов == 0 | {doc_count}')
        except Exception as e:
            print(f'[ERROR 🔴] (ошибка блока документов) {e}')

        # CLOSE ACCORDION
        await accordion_trigger.scroll_into_view_if_needed()
        await page.wait_for_timeout(200)
        await accordion_trigger.click()
        await page.wait_for_timeout(200)

    except Exception as e:
        print(f'[ERROR 🔴] (непредвиденная ошибка в участнике) {e}')


@retry(attempts=80)
async def process_tender_page(page: Page, tender_url: str):
    url = f'https://prozorro.gov.ua/uk{tender_url}'
    await page.goto(url, wait_until="domcontentloaded")

    try:
        title_locator = page.locator('//h2[contains(@class, "title--large")]')
        await title_locator.first.wait_for(timeout=7000)
        title_text = await title_locator.first.text_content()
        if not title_text:
            return None
    except PWTimeout:
        return None

    try:
        participants_locator = page.locator(
            '//section[contains(@class, "register")]//div[contains(@class, "accordion")]'
        )
        await participants_locator.first.wait_for(timeout=5000)
        count = await participants_locator.count()

        if count == 0:
            print(f'[INFO] тендер {url} — нет участников')
            return []
        else:
            print(f'[INFO] тендер {url} — {count} участников')
            return participants_locator
    except PWTimeout:
        print(f'[INFO] участников не найдено → {page.url}')
        return []


@retry(attempts=60)
async def fetch_tender_links(page: Page, page_index: int) -> Optional[List[str]]:
    url = (
        f"https://prozorro.gov.ua/uk/search/tender?"
        f"cpv=34110000-1&page={page_index}&status=complete&sort=publication_date,asc"
    )
    await page.goto(url, wait_until="domcontentloaded")

    try:
        links_locator = page.locator(
            '//ul[@class="search-result__list"]//a[contains(@class,"item-title__title")]'
        )
        await links_locator.first.wait_for(timeout=15000)

        links = await links_locator.evaluate_all(
            "els => els.map(e => e.getAttribute('href'))"
        )
        links = [l for l in links if l]
        return links if links else None
    except PWTimeout:
        return None


async def process_one_tender(context: BrowserContext, tender_url: str):
    page = await context.new_page()
    try:
        participants = await process_tender_page(page, tender_url)
        if not participants:
            return

        count = await participants.count()
        for i in range(count):
            print(f'[INFO] Обработка {i+1}/{count} участника → {page.url}')
            participant_block = participants.nth(i)
            await process_participant(page, participant_block)

        print("----" * 40)

    except Exception as e:
        print(f"[ERROR] Ошибка обработки тендера {tender_url} → {e}")
    finally:
        try:
            await page.close()
        except:
            pass


async def run_scraper(start_page: int, end_page: int, headless: bool = True, max_concurrent_tenders: int = 3):
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
                "Chrome/128.0.0.0 Safari/537.36"
            ),
            locale="uk-UA",
            # bypass_csp=True,   # можно включить при проблемах с CSP
        )

        # Семфор ограничивает количество одновременно открытых тендеров
        semaphore = asyncio.Semaphore(max_concurrent_tenders)

        async def bounded_process(tender_url):
            async with semaphore:
                await process_one_tender(context, tender_url)

        for page_index in range(start_page, end_page + 1):
            print(f"\n[PAGE] Обработка страницы поиска {page_index}")
            search_page = await context.new_page()

            try:
                tender_links = await fetch_tender_links(search_page, page_index)
                if not tender_links:
                    print(f"[WARN] На странице {page_index} ничего не найдено")
                    continue

                print(f"[INFO] Найдено тендеров на странице {page_index}: {len(tender_links)}")
            finally:
                await search_page.close()

            if tender_links:
                # Запускаем обработку тендеров параллельно (ограничено семафором)
                tasks = [bounded_process(url) for url in tender_links]
                await asyncio.gather(*tasks, return_exceptions=True)

        await context.close()
        await browser.close()


if __name__ == "__main__":
    import asyncio

    HEADLESS = True
    START_PAGE = 290
    END_PAGE = 300
    MAX_CONCURRENT_TENDERS = 10

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