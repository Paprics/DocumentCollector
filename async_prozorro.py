import asyncio
from playwright.async_api import async_playwright, TimeoutError
from utils.funcs import save_files_as_html
import time
import random

MAX_CONCURRENT_TENDERS = 1


async def fetch_tender_links(page, index: int, expected_count=20, max_retries=20):
    print(f"[INFO] Page: {index} — начинаем сбор ссылок")
    url = f'https://prozorro.gov.ua/uk/search/tender?cpv=34110000-1&page={index}&status=complete&sort=publication_date,asc'

    try:
        await page.goto(url, wait_until="networkidle", timeout=30_000)
    except Exception as e:
        print(f"[ERROR] Не удалось открыть страницу {index}: {e}")
        return []

    retries = 0
    items = []
    while retries < max_retries:
        try:
            items = await page.query_selector_all("li.search-result-card__wrap")
            if len(items) >= expected_count:
                break
        except Exception as e:
            print(f"[ERROR] Ошибка при поиске тендеров на странице {index}: {e}")

        retries += 1
        await asyncio.sleep(5)

        if retries % 3 == 0:
            try:
                await page.reload(wait_until="networkidle")
            except Exception as e:
                print(f"[ERROR] Ошибка перезагрузки страницы {index}: {e}")

    links = []
    for i_tender, item in enumerate(items):
        try:
            link_element = await item.query_selector("a.item-title__title")
            href = await link_element.get_attribute("href") if link_element else None
            if href:
                links.append("https://prozorro.gov.ua" + href)
        except Exception as e:
            print(f"[ERROR] href тендера {i_tender}: {e}")

    return links


async def process_participant(bid):
    files = []

    trigger = bid.locator('xpath=.//button')
    btn = trigger.first

    if await trigger.count() == 0:
        print("[WARN] Не удалось раскрыть аккордеон")
        return files

    await btn.scroll_into_view_if_needed()
    await btn.click()

    try:
        open_all = btn.locator('xpath=.//p[@class="select__label"]')
        await open_all.click()
        select_all = open_all.locator('xpath=(.//*[@class="select__element"])[last()]')
        await select_all.click()
    except Exception as e:
        print(f'Не удалось выбрать все документы или меньше 5 док. {e}')

    return files


async def parse_tender(context, url, max_retries=20, wait_time=4):
    print(f"[INFO] Открытие страницы тендера: {url}")
    try:
        page_tender = await context.new_page()
        await page_tender.goto(url, wait_until="networkidle", timeout=30_000)
    except Exception as e:
        print(f"[ERROR] Не удалось открыть страницу тендера {url}: {e}")
        return

    retries = 0
    while retries < max_retries:
        try:
            await page_tender.locator("h2.title.title--large").wait_for(timeout=wait_time * 1000)
            break
        except TimeoutError:
            retries += 1
            await asyncio.sleep(wait_time)
            if retries % 3 == 0:
                try:
                    await page_tender.reload(wait_until="networkidle")
                except Exception:
                    pass
    else:
        await page_tender.close()
        return

    await asyncio.sleep(1)

    if await page_tender.locator("section#register_of_proposals").count() == 0:
        print(f"[WARN] Нет блока предложений: {url}")
        await page_tender.close()
        return

    bids = page_tender.locator(
        '//*[@id="register_of_proposals"]//div[@class="tender-info__content"]/div[contains(@class, "accordion")]'
    )

    count_bids = await bids.count()
    for i_bid in range(count_bids):
        bid = bids.nth(i_bid)
        try:
            print(f"[INFO] Обработка участника {i_bid+1} | {url}")
            files = await process_participant(bid)
            if files:
                save_files_as_html(url=url, files=files)
            else:
                print(f"[SKIP] Нет документов {i_bid+1}")
        except Exception as e:
            print(f"[ERROR] Аккордеон {i_bid+1}: {e}")

    print('[INFO] --------------------------------------------------------------')
    await page_tender.close()


async def parse_tenders_on_page(context, links):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TENDERS)

    async def sem_parse(url):
        await asyncio.sleep(random.uniform(0, 1))
        async with semaphore:
            await parse_tender(context, url)

    await asyncio.gather(*(sem_parse(url) for url in links))


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        for index in range(300, 401):
            links = await fetch_tender_links(page, index)
            print(f"[INFO] Страница {index}: {len(links)} тендеров")

            await parse_tenders_on_page(context, links)

            print(f"[INFO] === Страница {index} завершена ===\n")
            await asyncio.sleep(0.3)

        await browser.close()


if __name__ == "__main__":
    start = time.time()
    asyncio.run(main())
    t = int(time.time() - start)
    print(f"Execution time: {t//3600:02}:{t%3600//60:02}:{t%60:02}")