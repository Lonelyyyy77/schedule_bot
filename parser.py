import asyncio
from playwright.async_api import async_playwright

URL = "https://harmonogramy.dsw.edu.pl/Plany/PlanyTokow/1178"

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # headless=True если без окна
        page = await browser.new_page()
        await page.goto(URL)
        print("🚀 Страница открыта")

        # Принять куки
        try:
            await page.click("button:has-text('Zezwól')")
            print("✅ Куки приняты")
        except:
            print("⚠️ Кнопка куки не найдена или уже принята")

        # Выбираем радиокнопку "Cały semestr"
        labels = await page.query_selector_all("label.custom-control-label")
        for lbl in labels:
            text = (await lbl.inner_text()).strip()
            if text == "Cały semestr":
                await lbl.click()
                print("✅ Фильтр выбран: Cały semestr")
                break

        # Нажимаем кнопку Szukaj
        await page.click("a#SzukajLogout")
        print("✅ Нажата кнопка Szukaj")

        # Ждём загрузку результатов (можно чуть больше времени)
        await asyncio.sleep(40)

        # Ждём именно скачивание CSV
        async with page.expect_download() as download_info:
            await page.locator("a[href*='WydrukTokuCsv']").click(no_wait_after=True)

        download = await download_info.value
        await download.save_as("schedule.csv")
        print("✅ CSV файл сохранён как schedule.csv")

        await browser.close()

asyncio.run(main())
