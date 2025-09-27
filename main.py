import asyncio
import os
import pandas as pd
from datetime import datetime, timedelta
import logging

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message

# --- НАСТРОЙКИ ---
BOT_TOKEN = '7353399540:AAHtYxx9ftGvs10iWXhvDSVPQgA4tDYKVEE'
SCHEDULE_FILE = 'Plany.csv'
logging.basicConfig(level=logging.INFO)

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

    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗓️ Сегодня", callback_data="show_today"),
         InlineKeyboardButton(text="🗓️ Завтра", callback_data="show_tomorrow")],
        [InlineKeyboardButton(text="📅 На этот месяц", callback_data="show_month"),
         InlineKeyboardButton(text="📅 На след месяц", callback_data="show_next_month")],
        [InlineKeyboardButton(text=notif_text, callback_data="toggle_notifications")]
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
def format_schedule(df: pd.DataFrame, title: str) -> str:
    if df.empty:
        return f"{title} пусто 📭"

    lines = [f"📅 {title}:\n"]
    days_map = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]

    for date, group in df.groupby("Data_dt"):
        day_of_week = days_map[date.weekday()]
        lines.append(f"🗓️ {day_of_week}, {date:%d.%m.%Y}")
        lines.append('')  # разделитель

        group = group.copy()
        group['czas_od_dt'] = pd.to_datetime(group['Czas od'], format="%H:%M", errors='coerce')

        for _, row in group.sort_values(by='czas_od_dt').iterrows():
            lines.append(f"⏰ {row['Czas od']} - {row['Czas do']}")
            lines.append(f"📖 {row['Zajecia']}")
            lines.append(f"🏫 {row['Sala']}")

            # Добавляем комментарий только если он не пустой
            uwagi = str(row.get('Uwagi', '')).strip()
            if uwagi and uwagi.lower() != 'nan':
                lines.append(f"📝 {uwagi}")

            lines.append("")  # пустая строка между предметами

        lines.append("")  # пустая строка между днями

    return "\n".join(lines)


# --- ПОЛУЧЕНИЕ РАСПИСАНИЯ ---
async def get_schedule_data_for_day(date: datetime.date, user_id: int) -> str:
    df = read_schedule(user_id)
    if df.empty:
        return "❌ Ваш файл расписания не найден или пуст."
    day_df = df[df['Data_dt'] == date]
    return format_schedule(day_df, f"Расписание на {date:%d.%m.%Y}")


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

    if document.file_name.endswith('.csv'):
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
