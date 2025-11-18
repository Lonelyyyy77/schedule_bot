import asyncio
import logging
from datetime import datetime, timedelta
from .schedules import read_schedule
from .storage import user_notifications


async def send_notifications(bot):
    while True:
        try:
            now = datetime.now()
            time_plus_5 = (now + timedelta(minutes=5)).strftime("%H:%M")

            for user_id, enabled in list(user_notifications.items()):
                if not enabled:
                    continue
                try:
                    df = read_schedule(user_id)
                    if df.empty or 'Czas od' not in df.columns:
                        continue

                    df['Czas od'] = df['Czas od'].astype(str).str.strip()

                    upcoming = df[(df['Data_dt'] == now.date()) & (df['Czas od'] == time_plus_5)]
                    for _, row in upcoming.iterrows():
                        text = f"⏰ Через 5 минут: {row.get('Zajecia', '(предмет)')} | {row.get('Sala', '(зала)')}"
                        await bot.send_message(user_id, text)
                except Exception as e_user:
                    logging.exception(f"Ошибка при отправке уведомления пользователю {user_id}: {e_user}")

            await asyncio.sleep(60)
        except Exception as e:
            logging.exception(f"Ошибка в send_notifications loop: {e}")
            await asyncio.sleep(5)
