import asyncio
import logging
import json
import os
from datetime import datetime, date, timedelta
import pandas as pd

from aiogram import Bot, Dispatcher, types, F, Router
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
import chardet

from config import token
from storage import (
    get_user_schedule_file,
    user_groups,
    user_notifications
)
from parser import download_schedule


URLS_FILE = "schedule_urls.json"


def load_urls() -> dict:
    if not os.path.exists(URLS_FILE):
        return {}
    try:
        with open(URLS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {}


def save_urls(urls: dict):
    with open(URLS_FILE, "w", encoding="utf-8") as f:
        json.dump(urls, f, indent=4, ensure_ascii=False)


def get_user_url(user_id: int) -> str | None:
    urls = load_urls()
    return urls.get(str(user_id))


def set_user_url(user_id: int, url: str):
    urls = load_urls()
    urls[str(user_id)] = url
    save_urls(urls)

# ---------------------------- БОТ ----------------------------
bot = Bot(token=token)
dp = Dispatcher()
router = Router()


# ---------------------------- КЛАВИАТУРЫ ----------------------------
def get_main_keyboard(user_id: int) -> InlineKeyboardMarkup:
    notif_state = user_notifications.get(user_id, False)
    notif_text = "🔔 Напоминания ВКЛ" if notif_state else "🔕 Напоминания ВЫКЛ"

    group_num = user_groups.get(user_id, 0)
    group_text = "👥 Фильтр: Все группы" if group_num == 0 else f"👥 Фильтр: {group_num} группа"

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


def get_day_navigation_keyboard(current_date, min_date, max_date) -> InlineKeyboardMarkup:
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

    keyboard = []
    if nav_buttons:
        keyboard.append(nav_buttons)

    keyboard.append([InlineKeyboardButton(text="⬅️ Меню", callback_data="main_menu")])

    return InlineKeyboardMarkup(inline_keyboard=keyboard)


# ---------------------------- РАСПИСАНИЕ ----------------------------
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


def read_schedule(user_id: int) -> pd.DataFrame:
    SCHEDULE_FILE = get_user_schedule_file(user_id)
    df = None

    if not os.path.exists(SCHEDULE_FILE):
        logging.info(f"Файл расписания для пользователя {user_id} не найден")
        return pd.DataFrame()

    # ---------- Определяем кодировку файла ----------
    try:
        with open(SCHEDULE_FILE, "rb") as f:
            raw = f.read()
            detected = chardet.detect(raw)
            encoding = detected["encoding"] or "utf-8"
            logging.info(f"Определена кодировка файла {SCHEDULE_FILE}: {encoding}")
    except Exception as e:
        logging.error(f"Ошибка определения кодировки: {e}")
        encoding = "utf-8"

    # ---------- Пробуем прочитать CSV ----------
    for enc in [encoding, "utf-8", "cp1250", "cp1251"]:
        try:
            df = pd.read_csv(
                SCHEDULE_FILE,
                sep=';',
                skiprows=2,
                header=None,
                skipinitialspace=True,
                encoding=enc,
                engine="python"  # устойчивее к странным символам
            )
            logging.info(f"Файл успешно прочитан с кодировкой: {enc}")
            break
        except Exception as e:
            logging.warning(f"Ошибка чтения с кодировкой {enc}: {e}")
            df = None

    if df is None:
        logging.error(f"❌ Не удалось прочитать CSV для пользователя {user_id}")
        return pd.DataFrame()

    # ---------- Чистка пустых строк ----------
    df.dropna(how="all", inplace=True)
    if df.empty:
        return pd.DataFrame()

    # ---------- Установка колонок ----------
    default_cols = ["temp0", "Czas od", "Czas do", "Liczba godzin", "Grupy",
                    "Zajecia", "Sala", "Forma zaliczenia", "Uwagi", "temp_extra"]

    if df.shape[1] > len(default_cols):
        extra = [f"temp{idx}" for idx in range(len(default_cols), df.shape[1])]
        cols = default_cols + extra
    else:
        cols = default_cols[:df.shape[1]]

    df.columns = cols

    # ---------- Парсим даты ----------
    current_date = None
    dates = []

    for _, row in df.iterrows():
        first_col = str(row.iloc[0]).strip()
        if first_col.startswith("Data Zajec"):
            try:
                parts = first_col.split()
                current_date = datetime.strptime(parts[2], "%Y.%m.%d").date()
            except:
                current_date = None
            dates.append(None)
        else:
            dates.append(current_date)

    df["Data_dt"] = dates

    # Удаляем строки без даты и времени
    df = df[df["Data_dt"].notna() & df["Czas od"].notna()]

    return df

def format_schedule(df: pd.DataFrame, title: str, user_id: int) -> str:
    if df.empty:
        return f"{title} пусто 📭"

    group_num = user_groups.get(user_id, 0)
    if group_num > 0:
        df = df[df["Grupy"].astype(str).str.contains(f"Cw{group_num}S") | df["Grupy"].astype(str).str.contains("WykS")]

    if df.empty:
        return f"{title} (после фильтра) пусто 📭"

    days = ["Понедельник","Вторник","Среда","Четверг","Пятница","Суббота","Воскресенье"]
    out = [f"📅 {title}:\n"]

    for date_val, group in df.groupby("Data_dt"):
        out.append(f"🗓️ {days[date_val.weekday()]}, {date_val:%d.%m.%Y}\n")

        group['czas_dt'] = pd.to_datetime(group['Czas od'], format="%H:%M", errors='coerce')

        for _, row in group.sort_values("czas_dt").iterrows():
            zajecia_type = parse_group_info(row["Grupy"])
            out.append(f"⏰ {row['Czas od']} - {row['Czas do']}")
            out.append(f"👥 {zajecia_type}")
            out.append(f"📖 {row['Zajecia']}")
            out.append(f"🏫 {row['Sala']}\n")

    return "\n".join(out)


async def get_schedule_data_for_day(date: date, user_id: int) -> str:
    df = read_schedule(user_id)
    if df.empty:
        return "❌ Ваш файл расписания не найден или пуст."
    df_day = df[df["Data_dt"] == date]
    return format_schedule(df_day, f"Расписание на {date:%d.%m.%Y}", user_id)


# ---------------------------- FSM ----------------------------
class ScheduleStates(StatesGroup):
    waiting_for_url = State()


# ---------------------------- ХЕНДЛЕРЫ ----------------------------
@router.message(CommandStart())
async def send_welcome(message: Message):
    user_id = message.from_user.id
    text = (
        "👋 Привет! Я ваш бот для просмотра расписания.\n\n"
        "Выберите опцию ниже.\n\n"
        "Для обновления данных отправьте новый файл `Plany.csv`."
    )
    await message.answer(text, reply_markup=get_main_keyboard(user_id))


@router.message(F.document)
async def handle_file_upload(message: Message):
    user_id = message.from_user.id
    doc = message.document

    if not doc.file_name.lower().endswith(".csv"):
        return await message.answer("❗ Отправьте файл в формате .csv")

    try:
        file = await bot.get_file(doc.file_id)
        save_path = get_user_schedule_file(user_id)
        await bot.download_file(file.file_path, save_path)

        await message.answer("✅ Файл расписания обновлён!")
        await send_welcome(message)

    except Exception as e:
        await message.answer(f"❌ Ошибка сохранения файла:\n{e}")


@router.callback_query(F.data.startswith("show_"))
async def show_schedule_callback(callback: types.CallbackQuery):
    timeframe = callback.data[5:]
    user_id = callback.from_user.id
    today = datetime.now().date()

    await callback.answer()

    if timeframe == "today":
        date = today
    elif timeframe == "tomorrow":
        date = today + timedelta(days=1)
    elif timeframe == "month":
        date = today.replace(day=1)
    elif timeframe == "next_month":
        if today.month == 12:
            date = today.replace(year=today.year+1, month=1, day=1)
        else:
            date = today.replace(month=today.month+1, day=1)
    else:
        return await callback.message.edit_text("⚠️ Неверный timeframe")

    text = await get_schedule_data_for_day(date, user_id)

    # границы месяца для кнопок
    min_d = date.replace(day=1)
    if date.month == 12:
        max_d = date.replace(year=date.year+1, month=1, day=1) - timedelta(days=1)
    else:
        max_d = date.replace(month=date.month+1, day=1) - timedelta(days=1)

    keyboard = get_day_navigation_keyboard(date, min_d, max_d)

    await callback.message.edit_text(text, reply_markup=keyboard)


@router.callback_query(lambda c: c.data == "update_schedule")
async def process_update(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    links = load_urls()

    if str(user_id) in links:
        url = links[str(user_id)]
        file_path = get_user_schedule_file(user_id)

        loading = await callback.message.edit_text("⏳ Обновляю расписание...")

        try:
            if os.path.exists(file_path):
                os.remove(file_path)

            await download_schedule(url, file_path)

            await loading.edit_text("✅ Расписание обновлено!", 
                                    reply_markup=get_main_keyboard(user_id))
        except Exception as e:
            await loading.edit_text(f"❌ Ошибка:\n{e}")

        return

    # --- Если ссылки нет — просим ввести ---
    await callback.message.edit_text("Вставь ссылку на расписание:")
    await state.set_state(ScheduleStates.waiting_for_url)



@router.message(ScheduleStates.waiting_for_url)
async def get_schedule_url(message: Message, state: FSMContext):
    url = message.text.strip()
    user_id = message.from_user.id

    set_user_url(user_id, url)

    loading = await message.answer("⏳ Загружаю расписание...")

    try:
        file_path = get_user_schedule_file(user_id)
        if os.path.exists(file_path):
            os.remove(file_path)

        await download_schedule(url, file_path)

        await loading.edit_text("✅ Расписание обновлено!", reply_markup=get_main_keyboard(user_id))
    except Exception as e:
        await loading.edit_text(f"❌ Ошибка:\n{e}")

    await state.clear()



@router.callback_query(F.data == "toggle_group")
async def toggle_group(callback: types.CallbackQuery):
    user_id = callback.from_user.id

    new_group = (user_groups.get(user_id, 0) + 1) % 4
    user_groups[user_id] = new_group

    await callback.answer(f"Группа: {new_group or 'Все'}")
    await callback.message.edit_reply_markup(get_main_keyboard(user_id))


@router.callback_query(F.data == "toggle_notifications")
async def toggle_notifications(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    user_notifications[user_id] = not user_notifications.get(user_id, False)

    await callback.answer(
        "Напоминания включены" if user_notifications[user_id] else "Напоминания выключены"
    )
    await callback.message.edit_reply_markup(get_main_keyboard(user_id))


@router.callback_query(F.data.startswith("day_"))
async def navigate_day(callback: types.CallbackQuery):
    date_val = datetime.fromisoformat(callback.data.split("_")[1]).date()
    user_id = callback.from_user.id

    text = await get_schedule_data_for_day(date_val, user_id)

    min_d = date_val.replace(day=1)
    if date_val.month == 12:
        max_d = date_val.replace(year=date_val.year+1, month=1, day=1) - timedelta(days=1)
    else:
        max_d = date_val.replace(month=date_val.month+1, day=1) - timedelta(days=1)

    keyboard = get_day_navigation_keyboard(date_val, min_d, max_d)

    await callback.message.edit_text(text, reply_markup=keyboard)


@router.callback_query(F.data == "main_menu")
async def main_menu(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    await callback.message.edit_text(
        "👋 Привет! Выберите опцию ниже.",
        reply_markup=get_main_keyboard(user_id)
    )


# ---------------------------- MAIN ----------------------------
async def main():
    logging.basicConfig(level=logging.INFO)
    dp.include_router(router)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
