import os
import asyncio
from playwright.async_api import async_playwright
import pandas as pd
from datetime import datetime, timedelta
import logging

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message


class ScheduleStates(StatesGroup):
    waiting_for_url = State()


def parse_group_info(grupa_val: str) -> str:
    if not isinstance(grupa_val, str):
        return ""
    grupa_val = grupa_val.strip()
    if "WykS" in grupa_val:
        return "Wykład"
    elif "Cw" in grupa_val:
        import re
        match = re.search(r"Cw(\d+)S", grupa_val)
        if match:
            return f"Ćwiczenia (grupa {match.group(1)})"
        else:
            return "Ćwiczenia"
    return grupa_val


async def download_schedule(url: str, save_path: str) -> str:
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)  # Headless
        page = await browser.new_page()

        # Переходим по URL с увеличенным таймаутом
        await page.goto(url, timeout=60000)

        # Попытка нажать "Zezwól" на куки/политику конфиденциальности
        try:
            await page.click("button:has-text('Zezwól')", timeout=5000)
        except:
            pass  # Если кнопки нет, продолжаем

        # Ждём и кликаем радиокнопку "Cały semestr"
        try:
            labels = await page.query_selector_all("label.custom-control-label")
            for lbl in labels:
                text = (await lbl.inner_text()).strip()
                if text == "Cały semestr":
                    await lbl.click()
                    break
        except Exception as e:
            print("Ошибка при выборе 'Cały semestr':", e)

        # Ждём появления кнопки SzukajLogout и кликаем
        try:
            await page.wait_for_selector("a#SzukajLogout", timeout=60000)
            await page.click("a#SzukajLogout")
        except Exception as e:
            print("Ошибка при клике SzukajLogout:", e)
            await page.screenshot(path="debug_szukaj.png")
            await browser.close()
            raise e

        # Даем странице подгрузиться
        await asyncio.sleep(5)

        # Скачать CSV
        try:
            link = await page.wait_for_selector("a[href*='WydrukTokuCsv']:visible", timeout=60000)
            async with page.expect_download(timeout=120000) as download_info:
                try:
                    # Сначала обычный клик
                    await link.click()
                except:
                    # Если обычный клик не сработал (headless / невидимый элемент), кликаем через JS
                    await link.evaluate("el => el.click()")
            download = await download_info.value
            await download.save_as(save_path)
        except Exception as e:
            print("Ошибка при скачивании CSV:", e)
            await page.screenshot(path="debug_download.png")
            await browser.close()
            raise e

        await browser.close()
        return save_path


# --- НАСТРОЙКИ ---
BOT_TOKEN = '7353399540:AAHtYxx9ftGvs10iWXhvDSVPQgA4tDYKVEE'
SCHEDULE_FILE = 'Plany.csv'
logging.basicConfig(level=logging.INFO)

user_groups: dict[int, int] = {}

USER_SCHEDULES_DIR = "user_schedules"

if not os.path.exists(USER_SCHEDULES_DIR):
    os.makedirs(USER_SCHEDULES_DIR)

# --- ИНИЦИАЛИЗАЦИЯ ---
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Состояние уведомлений пользователей
user_notifications: dict[int, bool] = {}


def get_user_schedule_file(user_id: int) -> str:
    folder = "user_schedules"
    os.makedirs(folder, exist_ok=True)  # создаём папку если нет
    return os.path.join(folder, f"{user_id}.csv")


# --- ФОН. ЗАДАЧА УВЕДОМЛЕНИЙ ---
async def send_notifications():
    """Фоновая проверка: отправляет сообщение за 5 минут до пары тем, у кого включены уведомления."""
    while True:
        try:
            now = datetime.now()
            time_plus_5 = (now + timedelta(minutes=5)).strftime("%H:%M")

            for user_id, enabled in list(user_notifications.items()):
                if not enabled:
                    continue
                try:
                    # читаем расписание конкретного пользователя
                    df = read_schedule(user_id)
                    if df.empty or 'Czas od' not in df.columns:
                        continue

                    # нормализуем время
                    df['Czas od'] = df['Czas od'].astype(str).str.strip()

                    # фильтруем пары на сегодня с временем = now+5min
                    upcoming = df[(df['Data_dt'] == now.date()) & (df['Czas od'] == time_plus_5)]
                    for _, row in upcoming.iterrows():
                        text = f"⏰ Через 5 минут: {row.get('Zajecia', '(предмет)')} | {row.get('Sala', '(зала)')}"
                        await bot.send_message(user_id, text)
                except Exception as e_user:
                    logging.exception(f"Ошибка при отправке уведомления пользователю {user_id}: {e_user}")

            await asyncio.sleep(60)  # проверяем каждую минуту
        except Exception as e:
            logging.exception(f"Ошибка в send_notifications loop: {e}")
            await asyncio.sleep(5)  # короткая пауза при общей ошибке


# --- КЛАВИАТУРЫ ---
def get_main_keyboard(user_id: int) -> InlineKeyboardMarkup:
    notif_state = user_notifications.get(user_id, False)
    notif_text = "🔔 Напоминания ВКЛ" if notif_state else "🔕 Напоминания ВЫКЛ"

    group_num = user_groups.get(user_id, 0)
    if group_num == 0:
        group_text = "👥 Фильтр: Все группы"
    else:
        group_text = f"👥 Фильтр: {group_num} группа"

    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗓️ Сегодня", callback_data="show_today"),
         InlineKeyboardButton(text="🗓️ Завтра", callback_data="show_tomorrow")],
        [InlineKeyboardButton(text="📅 На этот месяц", callback_data="show_month"),
         InlineKeyboardButton(text="📅 На след месяц", callback_data="show_next_month")],
        [InlineKeyboardButton(text=notif_text, callback_data="toggle_notifications")],
        [InlineKeyboardButton(text=group_text, callback_data="toggle_group")],
        [InlineKeyboardButton(text="🔄 Обновить расписание", callback_data="update_schedule")]
    ])


def get_back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
    ])


def get_day_navigation_keyboard(current_date: datetime.date, min_date: datetime.date,
                                max_date: datetime.date) -> InlineKeyboardMarkup:
    nav_buttons = []

    if current_date > min_date:
        prev_day = current_date - timedelta(days=1)
        nav_buttons.append(InlineKeyboardButton(
            text="⬅️ Назад",
            callback_data=f"day_{prev_day.isoformat()}"
        ))

    if current_date < max_date:
        next_day = current_date + timedelta(days=1)
        nav_buttons.append(InlineKeyboardButton(
            text="Вперед ➡️",
            callback_data=f"day_{next_day.isoformat()}"
        ))

    # всегда создаём список рядов
    keyboard = []
    if nav_buttons:
        keyboard.append(nav_buttons)  # первый ряд с кнопками перелистывания

    # второй ряд — кнопка в меню
    keyboard.append([InlineKeyboardButton(text="⬅️ Меню", callback_data="main_menu")])

    return InlineKeyboardMarkup(inline_keyboard=keyboard)


# --- ЧТЕНИЕ И ОБРАБОТКА CSV ---
def read_schedule(user_id: int) -> pd.DataFrame:
    """Читает CSV-файл пользователя и возвращает DataFrame с колонкой Data_dt (datetime.date)."""
    SCHEDULE_FILE = get_user_schedule_file(user_id)

    if not os.path.exists(SCHEDULE_FILE):
        logging.info(f"Файл расписания для пользователя {user_id} не найден")
        return pd.DataFrame()

    try:
        # читаем CSV без header (у тебя есть лишние строки), пропускаем первые 2 строки
        df = pd.read_csv(SCHEDULE_FILE, sep=';', skiprows=2, header=None, skipinitialspace=True)
    except Exception as e:
        logging.exception(f"Не удалось прочитать CSV для пользователя {user_id}: {e}")
        return pd.DataFrame()

    df.dropna(how="all", inplace=True)
    if df.empty:
        return pd.DataFrame()

    n_cols = df.shape[1]
    logging.info(f"Количество колонок в файле пользователя {user_id}: {n_cols}")

    # Подготовим имена колонок
    default_cols = ["temp0", "Czas od", "Czas do", "Liczba godzin", "Grupy",
                    "Zajecia", "Sala", "Forma zaliczenia", "Uwagi", "temp_extra"]
    if n_cols > len(default_cols):
        extra = [f"temp{idx}" for idx in range(len(default_cols), n_cols)]
        col_names = default_cols + extra
    else:
        col_names = default_cols[:n_cols]

    df.columns = col_names

    # Создаём колонку с датами
    current_date = None
    dates = []
    for _, row in df.iterrows():
        first_col = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""
        if first_col.startswith("Data Zajec"):
            try:
                parts = first_col.split()
                current_date = datetime.strptime(parts[2], "%Y.%m.%d").date()
            except Exception as e:
                logging.warning(f"Не удалось распарсить дату '{first_col}' для пользователя {user_id}: {e}")
                current_date = None
            dates.append(None)
        else:
            dates.append(current_date)

    df["Data_dt"] = dates

    # Нормализуем времена и оставляем только реальные пары
    if "Czas od" in df.columns:
        df["Czas od"] = df["Czas od"].astype(str).str.strip()
    else:
        logging.warning(f"В файле пользователя {user_id} отсутствует колонка 'Czas od'")

    df = df[df['Data_dt'].notna() & df['Czas od'].notna()].copy()
    logging.info(f"После фильтров строк для пользователя {user_id}: {len(df)}")

    return df


# --- ФОРМАТИРОВАНИЕ РАСПИСАНИЯ ---
def format_schedule(df: pd.DataFrame, title: str, user_id: int) -> str:
    if df.empty:
        return f"{title} пусто 📭"

    # фильтр по группе
    group_num = user_groups.get(user_id, 0)  # 0 = все группы
    if group_num > 0 and "Grupy" in df.columns:
        def belongs_to_group(grupa_val: str) -> bool:
            if not isinstance(grupa_val, str):
                return False
            grupa_val = grupa_val.strip()
            if "WykS" in grupa_val:  # лекция (для всех)
                return True
            return f"Cw{group_num}S" in grupa_val  # только если совпадает группа

        df = df[df["Grupy"].apply(belongs_to_group)]

    if df.empty:
        return f"{title} (после фильтра) пусто 📭"

    # остальное без изменений
    lines = [f"📅 {title}:\n"]
    days_map = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]

    for date, group in df.groupby("Data_dt"):
        day_of_week = days_map[date.weekday()]
        lines.append(f"🗓️ {day_of_week}, {date:%d.%m.%Y}")
        lines.append('')

        group = group.copy()
        group['czas_od_dt'] = pd.to_datetime(group['Czas od'], format="%H:%M", errors='coerce')

        for _, row in group.sort_values(by='czas_od_dt').iterrows():
            zajecia_type = parse_group_info(row.get("Grupy", ""))
            lines.append(f"⏰ {row['Czas od']} - {row['Czas do']}")
            lines.append(f"👥 {zajecia_type}")
            lines.append(f"📖 {row['Zajecia']}")
            lines.append(f"🏫 {row['Sala']}")

            uwagi = str(row.get('Uwagi', '')).strip()
            if uwagi and uwagi.lower() != 'nan':
                lines.append(f"📝 {uwagi}")

            lines.append("")
        lines.append("")

    return "\n".join(lines)


# --- ПОЛУЧЕНИЕ РАСПИСАНИЯ ---
async def get_schedule_data_for_day(date: datetime.date, user_id: int) -> str:
    df = read_schedule(user_id)
    if df.empty:
        return "❌ Ваш файл расписания не найден или пуст."
    day_df = df[df['Data_dt'] == date]
    return format_schedule(day_df, f"Расписание на {date:%d.%m.%Y}", user_id)


# --- ОБРАБОТЧИКИ ---
@dp.message(CommandStart())
async def send_welcome(message: Message):
    user_id = message.from_user.id
    text = (
        "👋 Привет! Я ваш бот для просмотра расписания.\n\n"
        "Выберите опцию ниже, чтобы посмотреть занятия.\n\n"
        "Для обновления данных просто отправьте мне новый файл `Plany.csv`."
    )
    await message.answer(text, reply_markup=get_main_keyboard(user_id))


@dp.message(F.document)
async def handle_file_upload(message: Message):
    user_id = message.from_user.id
    document = message.document

    if document.file_name.lower().endswith('.csv'):
        try:
            file = await bot.get_file(document.file_id)
            file_path = get_user_schedule_file(user_id)
            await bot.download_file(file.file_path, file_path)
            await message.reply("✅ Ваш файл расписания успешно обновлен!")
            await send_welcome(message)
        except Exception as e:
            logging.error(f"Ошибка сохранения файла: {e}")
            await message.reply(f"❌ Не удалось сохранить файл. Ошибка: {e}")
    else:
        await message.reply("❗️ Пожалуйста, отправьте файл в формате `.csv`.")


@dp.callback_query(F.data.startswith('show_'))
async def show_schedule_callback(callback: types.CallbackQuery):
    timeframe = callback.data[5:]
    user_id = callback.from_user.id
    await callback.answer("Загружаю расписание...")

    today = datetime.now().date()

    if timeframe == 'today':
        text = await get_schedule_data_for_day(today, user_id)
        keyboard = get_day_navigation_keyboard(today, today, today)
        await callback.message.edit_text(text, reply_markup=keyboard)

    elif timeframe == 'tomorrow':
        date = today + timedelta(days=1)
        text = await get_schedule_data_for_day(date, user_id)
        # min/max для кнопок ограничим текущим месяцем
        min_d = date.replace(day=1)
        if date.month == 12:
            max_d = date.replace(year=date.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            max_d = date.replace(month=date.month + 1, day=1) - timedelta(days=1)
        keyboard = get_day_navigation_keyboard(date, min_d, max_d)
        await callback.message.edit_text(text, reply_markup=keyboard)

    elif timeframe in ['month', 'next_month']:
        if timeframe == 'month':
            first_day = today.replace(day=1)
        else:
            # следующий месяц
            if today.month == 12:
                first_day = today.replace(year=today.year + 1, month=1, day=1)
            else:
                first_day = today.replace(month=today.month + 1, day=1)

        if first_day.month == 12:
            last_day = first_day.replace(year=first_day.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            last_day = first_day.replace(month=first_day.month + 1, day=1) - timedelta(days=1)

        text = await get_schedule_data_for_day(first_day, user_id)
        keyboard = get_day_navigation_keyboard(first_day, first_day, last_day)
        await callback.message.edit_text(text, reply_markup=keyboard)

    else:
        await callback.message.edit_text(
            "⚠️ Неверный timeframe. Используй 'today', 'tomorrow', 'month' или 'next_month'.",
            reply_markup=get_back_keyboard()
        )


@dp.callback_query(lambda c: c.data == "update_schedule")
async def process_update(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("Вставь ссылку на расписание (из сайта универа):")
    await state.set_state(ScheduleStates.waiting_for_url)


@dp.callback_query(F.data == "toggle_group")
async def toggle_group(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    current_group = user_groups.get(user_id, 0)

    # переключаем 0 -> 1 -> 2 -> 3 -> снова 0
    new_group = (current_group + 1) % 4
    user_groups[user_id] = new_group

    keyboard = get_main_keyboard(user_id)
    await callback.message.edit_reply_markup(reply_markup=keyboard)

    group_text = "Все группы" if new_group == 0 else f"{new_group} группа"
    await callback.answer(f"Фильтр установлен: {group_text}")


# Получение ссылки
@dp.message(ScheduleStates.waiting_for_url)
async def get_schedule_url(message: types.Message, state: FSMContext):
    url = message.text.strip()

    # Отправляем одно сообщение, которое будем редактировать
    status_message = await message.answer("⏳ Загружаю и обрабатываю расписание, подождите 1 минуту...")

    try:
        # Генерируем имя файла для пользователя
        user_id = message.from_user.id
        file_name = f"{user_id}.csv"
        file_path = os.path.join(USER_SCHEDULES_DIR, file_name)

        # Если файл уже существует, перезаписываем его
        if os.path.exists(file_path):
            os.remove(file_path)

        # Скачиваем CSV через парсер
        file_path = await download_schedule(url, file_path)

        # Отправляем файл пользователю
        # await message.answer_document(types.FSInputFile(file_path), caption="📁 Ваше расписание (CSV)")

        # Обработка CSV (если нужно)
        df = pd.read_csv(file_path, sep=";")  # уточни разделитель CSV, если нужно
        print(df.head())  # можно добавить сохранение/обновление локальной БД

        # Редактируем исходное сообщение, чтобы показать, что всё успешно
        await status_message.edit_text(
            "✅ Расписание успешно обновлено!",
            reply_markup=get_main_keyboard(user_id)
        )

    except Exception as e:
        # Редактируем исходное сообщение с текстом ошибки
        await status_message.edit_text(f"❌ Ошибка при загрузке расписания:\n{e}")

    await state.clear()


@dp.callback_query(F.data == "toggle_notifications")
async def toggle_notifications(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    current_state = user_notifications.get(user_id, False)
    user_notifications[user_id] = not current_state
    new_state = user_notifications[user_id]
    keyboard = get_main_keyboard(user_id)
    status_text = "включены" if new_state else "выключены"
    await callback.message.edit_reply_markup(reply_markup=keyboard)
    await callback.answer(f"Напоминания {status_text} ✅")


@dp.callback_query(F.data.startswith('day_'))
async def navigate_day(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    date_str = callback.data.split('_', 1)[1]
    date = datetime.strptime(date_str, "%Y-%m-%d").date()

    # Получаем расписание именно для этого пользователя
    text = await get_schedule_data_for_day(date, user_id)

    # Начало и конец месяца для кнопок
    min_date = date.replace(day=1)
    if date.month == 12:
        max_date = date.replace(year=date.year + 1, month=1, day=1) - timedelta(days=1)
    else:
        max_date = date.replace(month=date.month + 1, day=1) - timedelta(days=1)

    keyboard = get_day_navigation_keyboard(date, min_date, max_date)
    await callback.message.edit_text(text, reply_markup=keyboard)


@dp.callback_query(F.data == 'main_menu')
async def back_to_main_menu_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    text = (
        "👋 Привет! Я ваш бот для просмотра расписания.\n\n"
        "Выберите опцию ниже, чтобы посмотреть занятия.\n\n"
        "Для обновления данных просто отправьте мне новый файл `Plany.csv`."
    )
    await callback.message.edit_text(text, reply_markup=get_main_keyboard(user_id))


# --- ЗАПУСК ---
async def main():
    # запускаем фоновую задачу без await
    asyncio.create_task(send_notifications())
    # запускаем поллинг
    await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())
