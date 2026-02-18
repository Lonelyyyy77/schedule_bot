import asyncio
import logging
from aiogram import Bot, Dispatcher
from config import BOT_TOKEN

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

import handlers

from notifier import send_notifications


async def main():
    logging.info("Starting bot...")
    asyncio.create_task(send_notifications(bot))
    await dp.start_polling(bot)
