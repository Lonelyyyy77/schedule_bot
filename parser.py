import asyncio
from playwright.async_api import async_playwright
import logging


URL = "https://harmonogramy.dsw.edu.pl/Plany/PlanyTokow/1178"


async def download_schedule(url: str, save_path: str) -> str:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(url, timeout=60000)

        # cookies
        try:
            await page.click("button:has-text('Zezwól')", timeout=5000)
        except Exception:
            pass

        # wybór "Cały semestr"
        try:
            labels = await page.query_selector_all("label.custom-control-label")
            for lbl in labels:
                text = (await lbl.inner_text()).strip()
                if text == "Cały semestr":
                    await lbl.click()
                    break
        except Exception as e:
            logging.debug("Ошибка при выборе фильтра: %s", e)

        # ⚠️ ПРАВИЛЬНАЯ КНОПКА — #Szukaj
        try:
            await page.wait_for_selector("#Szukaj", timeout=60000)
            await page.click("#Szukaj")
        except Exception as e:
            logging.exception("Ошибка при клике Szukaj: %s", e)
            await page.screenshot(path="debug_szukaj.png")
            await browser.close()
            raise

        await asyncio.sleep(5)

        # скачивание CSV
        try:
            link = await page.wait_for_selector("a[href*='WydrukTokuCsv']:visible", timeout=60000)
            async with page.expect_download(timeout=120000) as download_info:
                await link.click()

            download = await download_info.value
            await download.save_as(save_path)
        except Exception as e:
            logging.exception("Ошибка при скачивании CSV: %s", e)
            await page.screenshot(path="debug_download.png")
            await browser.close()
            raise

        await browser.close()
        return save_path


async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(URL)
        print("Страница открыта")

        try:
            await page.click("button:has-text('Zezwól')")
            print("Куки приняты")
        except:
            print("Куки уже приняты")

        labels = await page.query_selector_all("label.custom-control-label")
        for lbl in labels:
            text = (await lbl.inner_text()).strip()
            if text == "Cały semestr":
                await lbl.click()
                print("Фильтр выбран")
                break

        await page.click("#Szukaj")
        print("Нажата кнопка Szukaj")

        async with page.expect_download() as download_info:
            await page.click("a[href*='WydrukTokuCsv']")

        download = await download_info.value
        await download.save_as("schedule.csv")
        print("CSV файл сохранён")

        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())