from playwright.sync_api import sync_playwright


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

        update_bitton = page.locator('//button[@class="govuk-button" and @data-event-id="advanced-search-results-page-update"]').first
        update_bitton.click()

        result_container = page.locator(' //tbody[@class="govuk-table__body"]')

        next_button = page.locator(' //span[@class="govuk-pagination__link-title"]')

        page.wait_for_timeout(2000)


        page.wait_for_timeout(10000)






if __name__ == "__main__":

    fetch_company(
        country="uk",
        headless=False,
        key_word='repair',)