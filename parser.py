import asyncio
from playwright.async_api import async_playwright
import logging
import random


async def download_schedule(url: str, save_path: str) -> str:
    logging.info("▶ Старт скачивания расписания")

    chromium_args = [
        "--disable-dev-shm-usage",
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-gpu",
        "--disable-web-security",
        "--disable-features=IsolateOrigins,site-per-process",
        "--disable-blink-features=AutomationControlled",
    ]

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=chromium_args
        )

        context = await browser.new_context(
            user_agent=f"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       f"(KHTML, like Gecko) Chrome/{random.randint(110, 121)} Safari/537.36"
        )

        page = await context.new_page()

        # Загружаем страницу
        await page.goto(url, wait_until="networkidle", timeout=120000)

        # Пробуем нажать кнопку cookies
        try:
            await page.locator("button:has-text('Zezwól')").click(timeout=3000)
            logging.info("Cookies приняты")
        except:
            logging.info("Кнопки cookies нет")

        # Выбор фильтра
        try:
            labels = page.locator("label.custom-control-label")
            count = await labels.count()
            for i in range(count):
                text = (await labels.nth(i).inner_text()).strip()
                if text == "Cały semestr":
                    await labels.nth(i).click()
                    logging.info("Выбран фильтр Cały semestr")
                    break
        except Exception as e:
            logging.error(f"Ошибка выбора фильтра: {e}")

        # Кнопка поиска ("Szukaj")
        try:
            button = page.locator("#SzukajLogout")
            await button.wait_for(state="visible", timeout=90000)
            try:
                await button.click()
            except:
                # fallback на JS
                await button.evaluate("el => el.click()")

            logging.info("Кнопка Szukaj нажата")
        except Exception as e:
            logging.error(f"❌ Ошибка при клике Szukaj: {e}")
            await page.screenshot(path="debug_szukaj.png")
            await browser.close()
            raise

        # Даем сайту сформировать файл
        await asyncio.sleep(4)

        # Скачивание CSV
        try:
            link = page.locator("a[href*='WydrukTokuCsv']")
            await link.wait_for(state="visible", timeout=120000)

            async with page.expect_download(timeout=180000) as dl:
                try:
                    await link.click()
                except:
                    await link.evaluate("el => el.click()")

            download = await dl.value
            await download.save_as(save_path)
            logging.info("CSV успешно скачан")
        except Exception as e:
            logging.error(f"❌ Ошибка при скачивании CSV: {e}")
            await page.screenshot(path="debug_download.png")
            await browser.close()
            raise

        await browser.close()
        return save_path