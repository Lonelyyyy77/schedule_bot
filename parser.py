import asyncio
from playwright.async_api import async_playwright
import logging


URL = "https://harmonogramy.dsw.edu.pl/Plany/PlanyTokow/1178"


async def download_schedule(url: str, save_path: str) -> str:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(url, timeout=60000)

        try:
            await page.click("button:has-text('Zezwól')", timeout=5000)
        except Exception:
            pass

        try:
            labels = await page.query_selector_all("label.custom-control-label")
            for lbl in labels:
                text = (await lbl.inner_text()).strip()
                if text == "Cały semestr":
                    await lbl.click()
                    break
        except Exception as e:
            logging.debug("Ошибка при выборе 'Cały semestr': %s", e)

        try:
            await page.wait_for_selector("a#SzukajLogout", timeout=60000)
            await page.click("a#SzukajLogout")
        except Exception as e:
            logging.exception("Ошибка при клике SzukajLogout: %s", e)
            await page.screenshot(path="debug_szukaj.png")
            await browser.close()
            raise

        await asyncio.sleep(5)

        try:
            link = await page.wait_for_selector("a[href*='WydrukTokuCsv']:visible", timeout=60000)
            async with page.expect_download(timeout=120000) as download_info:
                try:
                    await link.click()
                except Exception:
                    await link.evaluate("el => el.click()")
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
        browser = await p.chromium.launch(headless=False)  
        page = await browser.new_page()
        await page.goto(URL)
        print("Страница открыта")

        try:
            await page.click("button:has-text('Zezwól')")
            print("Куки приняты")
        except:
            print("Кнопка куки не найдена или уже принята")

        labels = await page.query_selector_all("label.custom-control-label")
        for lbl in labels:
            text = (await lbl.inner_text()).strip()
            if text == "Cały semestr":
                await lbl.click()
                print("Фильтр выбран: Cały semestr")
                break

        await page.click("a#SzukajLogout")
        print("Нажата кнопка Szukaj")

        await asyncio.sleep(40)

        async with page.expect_download() as download_info:
            await page.locator("a[href*='WydrukTokuCsv']").click(no_wait_after=True)

        download = await download_info.value
        await download.save_as("schedule.csv")
        print("CSV файл сохранён как schedule.csv")

        await browser.close()


asyncio.run(main())
