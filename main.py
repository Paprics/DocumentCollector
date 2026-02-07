from time import sleep
from playwright.sync_api import sync_playwright
import os


def save_files_as_html(url: str, files: list, filename="output.html"):

    # убеждаемся, что директория существует
    os.makedirs(os.path.dirname(filename) or ".", exist_ok=True)

    with open(filename, "a", encoding="utf-8") as f:
        f.write(f"<p><strong>Страница:</strong> <a href='{url}'>{url}</a></p>\n")
        f.write("<ul>\n")
        for name, href in files:
            f.write(f"  <li><a href='{href}'>{name}</a></li>\n")
        f.write("</ul>\n")
        f.write("<hr>\n")

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    context = browser.new_context()

    page = context.new_page()

    for index in range(10, 501):
        print(f"[INFO]: page: {index}")

        url = (
            "https://prozorro.gov.ua/uk/search/tender"
            "?status=complete"
            "&cpv=45100000-8&cpv=45200000-9&cpv=45300000-0"
            "&cpv=45400000-1&cpv=50230000-6&cpv=50720000-8"
            "&cpv=51130000-2&cpv=70000000-1&cpv=71000000-8"
            "&cpv=76110000-7&cpv=76210000-8&cpv=76300000-6"
            "&cpv=76400000-7&cpv=45500000-2"
            f"&page={index}&sort=publication_date,asc"
        )

        try:
            page.goto(url, wait_until="networkidle", timeout=30_000)
            page.wait_for_selector("li.search-result-card__wrap", timeout=10_000)
        except Exception as e:
            print(f"[ERROR] Не удалось открыть страницу {index}: {e}")
            continue

        items = page.locator("li.search-result-card__wrap")
        count_items = items.count()
        print(f"[INFO]: found {count_items} items")

        if count_items == 0:
            break  # дальше страниц нет

        for i_tender in range(count_items):
            try:
                item = items.nth(i_tender)
                href = item.locator("a.item-title__title").get_attribute("href")
            except Exception as e:
                print(f"[ERROR] Не удалось получить href для тендера {i_tender}: {e}")
                continue

            if not href:
                continue

            full_url = "https://prozorro.gov.ua" + href
            print(f'[INFO]: open page: {full_url}')

            try:
                page_tender = context.new_page()
                page_tender.goto(full_url, wait_until="networkidle", timeout=30_000)
                sleep(1)
            except Exception as e:
                print(f"[ERROR] Не удалось открыть страницу тендера: {e}")
                continue

            try:
                # ждём сам блок "Реєстр пропозицій"
                if page_tender.locator("section#register_of_proposals").count() == 0:
                    print("[INFO] Блок 'Реєстр пропозицій' отсутствует")
                    page_tender.close()
                    continue
            except Exception as e:
                print(f"[ERROR] Ошибка при проверке блока 'Реєстр пропозицій': {e}")
                page_tender.close()
                continue

            try:
                # получаем все аккордеоны с заявками
                bids = page_tender.locator("section#register_of_proposals div.accordion")
                count_bids = bids.count()

                if count_bids == 0:
                    print("Заявок нет")
                else:
                    print(f"Количество заявок: {count_bids}")

                    for i_bid in range(count_bids):
                        try:
                            bid = bids.nth(i_bid)
                            trigger = bid.locator("button.accordion__trigger")

                            # скроллим к текущему элементу
                            trigger.scroll_into_view_if_needed()

                            # кликаем аккордеон, раскрываем
                            trigger.click()
                            page_tender.wait_for_timeout(3000)
                            print(f"[DEB]: раскрыт accordion {i_bid + 1}/{count_bids}")

                            # ищем селект "Показати рядків" внутри текущего bid
                            select_locator = bid.locator("div.select.app-list-nav__select")
                            if select_locator.count() > 0:
                                print("[INFO] Селект найден, раскрываем список")
                                select_locator.locator("p.select__label").click()
                                page_tender.wait_for_timeout(500)  # небольшая пауза для анимации
                                option_all = select_locator.locator("div.select__element", has_text="Всі")
                                if option_all.count() > 0:
                                    option_all.first.click()
                                    page_tender.wait_for_timeout(1000)
                                    print("[INFO] Выбрано 'Всі'")
                                else:
                                    print("[WARN] Элемент 'Всі' не найден")

                            # ищем все блоки документов внутри текущего bid
                            documents_blocks = bid.locator("div.documents")
                            files = []

                            for b_index in range(documents_blocks.count()):
                                block = documents_blocks.nth(b_index)
                                items_docs = block.locator("li.documents__item")
                                for i_doc in range(items_docs.count()):
                                    doc_item = items_docs.nth(i_doc)
                                    link_locator = doc_item.locator("a.documents__link")
                                    href_doc = link_locator.get_attribute("href")
                                    name_locator = link_locator.locator("span.link-blank__text")
                                    name_doc = name_locator.inner_text().strip() if name_locator.count() > 0 else "unknown"

                                    if href_doc:
                                        files.append((name_doc, href_doc))

                            if files:
                                print(f"[INFO] Найдено {len(files)} документов")
                                current_url = page_tender.url
                                save_files_as_html(url=current_url, files=files)
                                # for f in files:
                                #     print(f)
                            else:
                                print("[INFO] Документы не найдены")

                            # закрываем аккордеон
                            trigger.click()
                            page_tender.wait_for_timeout(1000)
                            print(f"[DEB]: закрыт accordion {i_bid + 1}/{count_bids}")

                        except Exception as e:
                            print(f"[ERROR] Ошибка обработки аккордеона {i_bid + 1}: {e}")
                            continue

            except Exception as e:
                print(f"[ERROR] Ошибка обработки блока 'Реєстр пропозицій': {e}")

            page_tender.close()
            print('----')


