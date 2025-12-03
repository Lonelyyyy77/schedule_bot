from dotenv import load_dotenv
import os
import logging

load_dotenv()

token = os.getenv("BOT_TOKEN")
USER_SCHEDULES_DIR = "user_schedules"
SCHEDULE_FILE = 'Plany.csv'

logging.basicConfig(level=logging.INFO)
