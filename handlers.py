import os
import logging
import pandas as pd
from datetime import datetime, timedelta

from aiogram import types, F
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import Message

from .bot import bot, dp
from .keyboards import get_main_keyboard, get_back_keyboard, get_day_navigation_keyboard
from .schedules import get_schedule_data_for_day, format_schedule, read_schedule, parse_group_info
from .parser import download_schedule
from .storage import get_user_schedule_file, user_groups, user_notifications


class ScheduleStates(StatesGroup):
    waiting_for_url = State()


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
    new_group = (current_group + 1) % 4
    user_groups[user_id] = new_group

    keyboard = get_main_keyboard(user_id)
    await callback.message.edit_reply_markup(reply_markup=keyboard)

    group_text = "Все группы" if new_group == 0 else f"{new_group} группа"
    await callback.answer(f"Фильтр установлен: {group_text}")


@dp.message(ScheduleStates.waiting_for_url)
async def get_schedule_url(message: types.Message, state: FSMContext):
    url = message.text.strip()
    status_message = await message.answer("⏳ Загружаю и обрабатываю расписание, подождите 1 минуту...")

    try:
        user_id = message.from_user.id
        file_name = f"{user_id}.csv"
        file_path = get_user_schedule_file(user_id)

        if os.path.exists(file_path):
            os.remove(file_path)

        file_path = await download_schedule(url, file_path)

        df = pd.read_csv(file_path, sep=';')
        print(df.head())

        await status_message.edit_text(
            "✅ Расписание успешно обновлено!",
            reply_markup=get_main_keyboard(user_id)
        )

    except Exception as e:
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

    text = await get_schedule_data_for_day(date, user_id)

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
