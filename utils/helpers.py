from playwright.sync_api import sync_playwright


def process_company(*args):
    context, title, href, status, company_type = args
    url = f'https://find-and-update.company-information.service.gov.uk{href}'

    page = context.new_page()
    try:
        page.goto(url, wait_until="networkidle")

        # print(f'[INFO] open {title}')

        # Блок Warning
        page.wait_for_timeout(timeout=1000)
        overdue_blocks = page.locator('//div[contains(@class,"help-notice") and contains(@class,"overdue")]')

        if overdue_blocks.count() != 0:
            return

        filling_history_tab = page.locator('//a[@id="filing-history-tab"]')
        filling_history_tab.scroll_into_view_if_needed(timeout=3000)
        filling_history_tab.click(timeout=1000)

        checkbox = page.locator('//input[@id="filter-category-incorporation"]')
        if not checkbox.is_checked():
            checkbox.scroll_into_view_if_needed()
            checkbox.click()

        page.wait_for_selector('//table[@id="fhTable"]/tbody/tr', timeout=10000)

        documents_container = page.locator('//table[@id="fhTable"]/tbody/tr')
        count = documents_container.count()

        if count != 2:
            return

        print(f'{title}  |  {url}')

        with open('../output_data/Companies/uk_companies.txt', 'a') as f:
            f.write(url + '  |  ' + title + '\n')

    finally:
        page.close()


def fetch_company(country: str, headless: bool, key_word) -> None:
    if country != "uk":
        raise ValueError("Unsupported country code")

    url = (
        "https://find-and-update.company-information.service.gov.uk/"
        "advanced-search/get-results?"
        "companyNameIncludes=&companyNameExcludes=&registeredOfficeAddress=&"
        "incorporationFromDay=&incorporationFromMonth=&incorporationFromYear=&"
        "incorporationToDay=&incorporationToMonth=&incorporationToYear=&"
        "status=active&sicCodes=&type=ltd&type=plc&"
        "dissolvedFromDay=&dissolvedFromMonth=&dissolvedFromYear=&"
        "dissolvedToDay=&dissolvedToMonth=&dissolvedToYear="
    )

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
            locale="en-GB"
        )

        page = context.new_page()
        page.goto(url, wait_until="networkidle")

        page.locator('//button[@aria-controls="Search-content-1"]').click()

        input_field = page.locator('//input[@id="companyNameIncludes"]')
        input_field.scroll_into_view_if_needed()
        input_field.fill(key_word)

        update_bitton = page.locator(
            '//button[@class="govuk-button" and @data-event-id="advanced-search-results-page-update"]'
        ).first
        update_bitton.click()

        page.wait_for_load_state("networkidle")

        while True:
            cards = page.locator('//tbody[@class="govuk-table__body"]/tr')

            for i in range(cards.count()):
                card = cards.nth(i)

                title_el = card.locator('h2 a')

                title = title_el.inner_text().strip().replace("(link opens a new window)", '').strip()
                href = title_el.get_attribute('href')

                status = card.locator('p span').first.inner_text().strip()
                company_type = card.locator('ul li').first.inner_text().strip()

                if (
                        status.lower() == "active" and
                        company_type.lower() == "private limited company"
                ):
                    process_company(context, title, href, status, company_type)

            next_button = page.locator('//span[@class="govuk-pagination__link-title"]').last
            next_button.scroll_into_view_if_needed(timeout=3000)
            next_button.click()

            page.wait_for_load_state("networkidle")


if __name__ == "__main__":
    fetch_company(
        country="uk",
        headless=True,
        key_word='repair', )