import asyncio
from playwright.async_api import async_playwright
import logging
from playwright_stealth import stealth_async


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
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
            java_script_enabled=True,
        )

        page = await context.new_page()
        await stealth_async(page)

        # Загружаем страницу (только load!)
        try:
            await page.goto(url, wait_until="load", timeout=120000)
        except:
            await page.screenshot(path="debug_goto_failed.png")
            raise Exception("❌ Сайт не загрузился — вероятная блокировка серверного IP")

        # Проверяем, есть ли вообще HTML
        html = await page.content()
        if len(html) < 50000:  # обычно >400k
            await page.screenshot(path="debug_empty_html.png")
            raise Exception("❌ Страница загрузилась частично — сайт блокирует headless браузер")

        # Cookie button
        try:
            await page.locator("button:has-text('Zezwól')").click(timeout=3000)
            logging.info("Cookies приняты")
        except:
            logging.info("Куки отсутствуют")

        # Фильтр: Cały semestr
        try:
            labels = page.locator("label.custom-control-label")
            for i in range(await labels.count()):
                text = (await labels.nth(i).inner_text()).strip()
                if text == "Cały semestr":
                    await labels.nth(i).click()
                    logging.info("Фильтр установлен")
                    break
        except Exception as e:
            logging.error(f"Ошибка выбора фильтра: {e}")

        # Кнопка Szukaj
        try:
            button = page.locator("#SzukajLogout")

            await button.wait_for(state="visible", timeout=70000)

            try:
                await button.click()
            except:
                await button.evaluate("el => el.click()")

            logging.info("Кнопка Szukaj нажата")
        except Exception as e:
            await page.screenshot(path="debug_szukaj.png")
            raise Exception(f"❌ Ошибка клика Szukaj: {e}")

        # Ждем подготовки CSV
        await asyncio.sleep(5)

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
            logging.info("CSV скачан успешно")

        except Exception as e:
            await page.screenshot(path="debug_download.png")
            raise Exception(f"❌ Ошибка скачивания CSV: {e}")

        await browser.close()
        return save_path
