import os
import logging

BOT_TOKEN = os.getenv("BOT_TOKEN")
USER_SCHEDULES_DIR = "user_schedules"
SCHEDULE_FILE = 'Plany.csv'

logging.basicConfig(level=logging.INFO)
