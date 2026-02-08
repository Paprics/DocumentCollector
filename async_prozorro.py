import asyncio
from playwright.async_api import async_playwright, TimeoutError
from utils.funcs import save_files_as_html

MAX_CONCURRENT_TENDERS = 7  # лимит одновременных вкладок

async def fetch_tender_links(page, index: int, expected_count=20, max_retries=20):
    """
    Получаем ссылки на тендеры с одной страницы поиска.
    Ждем, пока на странице появятся все expected_count тендеров.
    """
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
                break  # Все ок, есть 20+ тендеров
        except Exception as e:
            print(f"[ERROR] Ошибка при поиске тендеров на странице {index}: {e}")

        retries += 1
        wait_time = 5.0
        print(f"[WARN] На странице {index} найдено {len(items)} тендеров, ожидаем {expected_count}. Попытка {retries}/{max_retries}, ждем {wait_time}s")
        await asyncio.sleep(wait_time)

        if retries % 3 == 0:
            print(f"[DEB] Перезагружаем страницу {index} для повторной загрузки данных")
            try:
                await page.reload(wait_until="networkidle")
            except Exception as e:
                print(f"[ERROR] Ошибка перезагрузки страницы {index}: {e}")

    if len(items) < expected_count:
        print(f"[WARN] Страница {index} завершена с {len(items)} тендерами вместо {expected_count}")

    links = []
    for i_tender, item in enumerate(items):
        try:
            link_element = await item.query_selector("a.item-title__title")
            href = await link_element.get_attribute("href") if link_element else None
            if href:
                links.append("https://prozorro.gov.ua" + href)
            else:
                print(f"[SKIP] Тендер {i_tender} на странице {index} без ссылки")
        except Exception as e:
            print(f"[ERROR] Не удалось получить href для тендера {i_tender} на странице {index}: {e}")

    return links


async def process_accordion(bid):
    """Обработка аккордеона внутри тендера и сбор документов"""
    files = []

    trigger = bid.locator("button.accordion__trigger")
    await trigger.scroll_into_view_if_needed()
    await trigger.click()
    await asyncio.sleep(0.3)
    print(f"[DEB] Аккордеон раскрыт")

    # селект "Всі"
    select_locator = bid.locator("div.select.app-list-nav__select")
    if await select_locator.count() > 0:
        await select_locator.locator("p.select__label").click()
        await asyncio.sleep(0.3)
        option_all = select_locator.locator("div.select__element", has_text="Всі")
        if await option_all.count() > 0:
            await option_all.first.click()
            await asyncio.sleep(0.3)
            print(f"[INFO] Выбрано 'Всі' для всех строк")
        else:
            print(f"[WARN] Элемент 'Всі' не найден в селекте")

    # собираем документы
    documents_blocks = bid.locator("div.documents")
    for b_index in range(await documents_blocks.count()):
        block = documents_blocks.nth(b_index)
        items_docs = block.locator("li.documents__item")
        for i_doc in range(await items_docs.count()):
            doc_item = items_docs.nth(i_doc)
            link_locator = doc_item.locator("a.documents__link")
            href_doc = await link_locator.get_attribute("href")
            name_locator = link_locator.locator("span.link-blank__text")
            name_doc = (await name_locator.inner_text()).strip() if await name_locator.count() > 0 else "unknown"
            if href_doc:
                files.append((name_doc, href_doc))
            else:
                print(f"[SKIP] Документ {i_doc} в блоке {b_index} без ссылки")

    await trigger.click()  # закрываем аккордеон
    await asyncio.sleep(0.1)
    print(f"[DEB] Аккордеон закрыт")
    return files


async def parse_tender(context, url, max_retries=20, wait_time=4):
    """Открываем страницу тендера и собираем документы с гарантированным ожиданием заголовка и построчным сохранением участников"""
    print(f"[INFO] Открытие страницы тендера: {url}")
    try:
        page_tender = await context.new_page()
        await page_tender.goto(url, wait_until="networkidle", timeout=30_000)
    except Exception as e:
        print(f"[ERROR] Не удалось открыть страницу тендера {url}: {e}")
        return

    # Ждем появления заголовка тендера
    retries = 0
    while retries < max_retries:
        try:
            await page_tender.locator("h2.title.title--large").wait_for(timeout=wait_time * 1000)
            break
        except TimeoutError:
            retries += 1
            print(f"[WARN] Заголовок тендера не найден на странице {url}, попытка {retries}/{max_retries}")
            await asyncio.sleep(wait_time)
            if retries % 3 == 0:
                print(f"[DEB] Перезагружаем страницу {url}")
                try:
                    await page_tender.reload(wait_until="networkidle")
                except Exception as e:
                    print(f"[ERROR] Ошибка перезагрузки страницы {url}: {e}")
    else:
        print(f"[ERROR] Не удалось загрузить заголовок тендера после {max_retries} попыток: {url}")
        await page_tender.close()
        return

    await asyncio.sleep(1)
    # После появления заголовка — безопасно ищем блок с предложениями
    if await page_tender.locator("section#register_of_proposals").count() == 0:
        print(f"[WARN] Блок 'Реєстр пропозицій' отсутствует на странице {url} после загрузки заголовка")
        await page_tender.close()
        return

    bids = page_tender.locator("section#register_of_proposals div.accordion")
    count_bids = await bids.count()
    if count_bids == 0:
        print(f"[INFO] Заявок нет на странице {url}")
    else:
        for i_bid in range(count_bids):
            bid = bids.nth(i_bid)
            try:
                files = await process_accordion(bid)
                if files:
                    save_files_as_html(url=url, files=files)  # сохраняем сразу после обработки участника
                    print(f"[INFO] Сохранено {len(files)} документов для участника {i_bid + 1} на тендере {url}")
                else:
                    print(f"[SKIP] Документы не найдены для участника {i_bid + 1} на тендере {url}")
            except Exception as e:
                print(f"[ERROR] Ошибка обработки аккордеона {i_bid + 1} на странице {url}: {e}")

    await page_tender.close()


async def parse_tenders_on_page(context, links):
    """Асинхронно парсим все тендеры с одной страницы поиска"""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TENDERS)

    async def sem_parse(url):
        async with semaphore:
            await parse_tender(context, url)

    await asyncio.gather(*(sem_parse(url) for url in links))


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        for index in range(100, 201):  # пример диапазона страниц
            links = await fetch_tender_links(page, index)
            print(f"[INFO] Страница {index} собрала {len(links)} тендеров")

            await parse_tenders_on_page(context, links)

            print(f"[INFO] === Страница {index} завершена ===\n")
            await asyncio.sleep(0.3)  # небольшая пауза между страницами

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
