import asyncio
import logging
from aiogram import Bot, Dispatcher
from .config import BOT_TOKEN

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# импортируем handlers после создания dp, чтобы декораторы могли использовать dp
from . import handlers  # noqa: E402, F401

from .notifier import send_notifications


async def main():
    logging.info("Starting bot...")
    # запустить фоновую задачу уведомлений
    asyncio.create_task(send_notifications(bot))
    await dp.start_polling(bot)
