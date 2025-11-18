import os
from typing import Dict
from .config import USER_SCHEDULES_DIR


# Хранилище данных в памяти (простое)
user_groups: Dict[int, int] = {}
user_notifications: Dict[int, bool] = {}


def ensure_user_dir() -> None:
    os.makedirs(USER_SCHEDULES_DIR, exist_ok=True)


def get_user_schedule_file(user_id: int) -> str:
    ensure_user_dir()
    return os.path.join(USER_SCHEDULES_DIR, f"{user_id}.csv")
