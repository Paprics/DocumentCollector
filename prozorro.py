import random
import time
from functools import wraps
from playwright.sync_api import sync_playwright, Page
from playwright.sync_api import TimeoutError as PWTimeout
from utils.funcs import save_files_as_html


def retry(attempts=100, delay_range=(2, 3)):
    def decorator(func):
        @wraps(func)
        def wrapper(page, *args, **kwargs):
            for attempt in range(1, attempts + 1):
                result = func(page, *args, **kwargs)

                if result is not None:
                    print(f"[INFO] {func.__name__} — успех с попытки {attempt}")
                    return result

                print(f"[WARN] попытка {attempt}/{attempts} — результат None")

                if attempt < attempts:
                    sleep_time = random.uniform(*delay_range)
                    time.sleep(sleep_time)
                    try:
                        page.reload()
                    except Exception as e:
                        print(f"[WARN] reload failed: {e}")

            print(f"[ERROR] {func.__name__} — попытки исчерпаны")
            try:
                page.close()
            except Exception:
                pass
            return None

        return wrapper

    return decorator


def fetch_documents(documents):
    result = []
    for i in range(documents.count()):
        document_item = documents.nth(i)
        links = document_item.locator('a').all()
        for link in links:
            spans = link.locator('span').all_text_contents()
            title = spans[0] if spans else link.text_content() or "Без названия"
            href = link.get_attribute('href') or "Без ссылки"
            result.append((title, href))
    return result


def process_participant(page: Page, participant_block):
    """
    Обработка одного участника тендера.
    """
    try:
        participant_block.scroll_into_view_if_needed()
        page.wait_for_timeout(300)

        # OPEN ACCORDION
        accordion_triger = participant_block.locator('xpath=.//button[contains(@class, "accordion__trigger")]').first
        accordion_triger.click()
        page.wait_for_timeout(200)

        # Попытка открыть все докементы
        try:
            participant_block.locator('xpath=.//span[@class="select__text"]').click()
            page.wait_for_timeout(200)
            participant_block.locator('xpath=.//div[@class="select__element"][last()]').click()
            page.wait_for_timeout(200)

        except Exception as e:
            print(f"[ERROR] ошибка получения всех док или < 5")

        try:
            document_block = participant_block.locator('xpath=.//div[@class="documents"]/div/ul')
            print(f'[INFO] на тендеде найдено {document_block.count()} докементов')

            # print(f'[DEBUG TIMEE 5S]')
            # page.wait_for_timeout(5000)

            if document_block.count() > 0:
                result = fetch_documents(document_block)
                save_files_as_html(url=page.url, files=result)
            else:
                print(f'[WARN] Документов == 0', '|', document_block.count())

        except Exception as e:
            print(f'[ERROR 🔴🔴🔴🔴] (НЕ ПРЕДВИДЕНАЯ ОШИБКА ПОСКА ДОКУМЕНТОВ) {e}')

        # CLOSE ACCORDION
        accordion_triger = participant_block.locator('xpath=.//button[contains(@class, "accordion__trigger")]').first
        accordion_triger.scroll_into_view_if_needed()
        page.wait_for_timeout(200)
        accordion_triger.click()
        page.wait_for_timeout(200)

    except Exception as e:
        print(f'[ERROR 🔴🔴🔴🔴] (НЕ ПРЕДВИДЕНАЯ ОШИБКА) {e}')


@retry(attempts=100)
def process_tender_page(page, tender_url: str):
    url = f'https://prozorro.gov.ua/uk{tender_url}'
    page.goto(url)

    # Ждём title
    try:
        title_locator = page.locator('//h2[contains(@class, "title--large")]')
        title_locator.first.wait_for(timeout=5000)
        title_text = title_locator.first.text_content()
        if not title_text:
            return None
    except PWTimeout:
        return None

    # Извлечение участников
    try:
        participants_locator = page.locator(
            '//section[contains(@class, "register")]//div[contains(@class, "accordion")]'
        )

        participants_locator.first.wait_for(timeout=2500)
        participants_count = participants_locator.count()

        if participants_count == 0:
            print(f'[INFO] тендер {url} — нет участников')
            return []
        else:
            print(f'[INFO] тендер {url} — {participants_count} участников')
            # participants_locator.first.scroll_into_view_if_needed()
            return participants_locator
    except PWTimeout:
        print(f'[DEB] участников не найдено 😌{page.url}')
        page.close()
        return []


@retry(attempts=100)
def fetch_tender_links(page, page_index: int):
    url = (
        f"https://prozorro.gov.ua/uk/search/tender?"
        f"cpv=34110000-1&page={page_index}&status=complete&sort=publication_date,asc"
    )

    page.goto(url)

    try:
        links_locator = page.locator(
            '//ul[@class="search-result__list"]//a[contains(@class,"item-title__title")]'
        )
        links_locator.first.wait_for(timeout=10000)

        links = links_locator.evaluate_all(
            "els => els.map(e => e.getAttribute('href'))"
        )

        links = [l for l in links if l]
        return links or None

    except PWTimeout:
        return None


def run_scraper(start_page: int, end_page: int, headless: bool):
    """
    Основной цикл: перебор страниц и обработка тендеров.
    """
    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=headless,
            args=["--no-sandbox", "--disable-setuid-sandbox"]
        )
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/117.0.0.0 Safari/537.36"
            ),
            locale="uk-UA"
        )

        page = context.new_page()

        for page_index in range(start_page, end_page + 1):
            print(f"[INFO] Обработка страницы {page_index}")

            try:
                # Получить список тендеров
                tender_links = fetch_tender_links(page, page_index)
                print(f'[DED] На странице {page_index} собраны тендеры - {len(tender_links)} |)')
            except Exception as e:
                print(f'[ERROR] {e}')

            # Итерация по тендерам
            for tender_url in tender_links:

                page = context.new_page()

                # Получить на стр. тендера список участников
                try:
                    participants = process_tender_page(page, tender_url)

                    # Итерация по учасиникам
                    if not participants:  # Если список с участниками пустой(нет участников)
                        continue
                    for i in range(participants.count()):
                        print(f'[INFO] Обработка {i + 1} участника тендера - {page.url}')
                        participant_block = participants.nth(i)
                        process_participant(page, participant_block)



                    page.close()
                    print('----' * 100)

                # #         save_results(documents, tender_url)
                except Exception as e:
                    print(f'[ERROR] {e}')


if __name__ == "__main__":
    HEADLESS = False
    START_PAGE = 293
    END_PAGE = 300
    run_scraper(START_PAGE, END_PAGE, HEADLESS)
    # MAX_CONCURRENT_TENDERS = 1   # не удалять
