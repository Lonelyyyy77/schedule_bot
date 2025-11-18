import logging
import os
from datetime import datetime, date
import pandas as pd
from .storage import get_user_schedule_file, user_groups


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

    if not os.path.exists(SCHEDULE_FILE):
        logging.info(f"Файл расписания для пользователя {user_id} не найден")
        return pd.DataFrame()

    try:
        df = pd.read_csv(SCHEDULE_FILE, sep=';', skiprows=2, header=None, skipinitialspace=True)
    except Exception as e:
        logging.exception(f"Не удалось прочитать CSV для пользователя {user_id}: {e}")
        return pd.DataFrame()

    df.dropna(how="all", inplace=True)
    if df.empty:
        return pd.DataFrame()

    n_cols = df.shape[1]
    logging.info(f"Количество колонок в файле пользователя {user_id}: {n_cols}")

    default_cols = ["temp0", "Czas od", "Czas do", "Liczba godzin", "Grupy",
                    "Zajecia", "Sala", "Forma zaliczenia", "Uwagi", "temp_extra"]
    if n_cols > len(default_cols):
        extra = [f"temp{idx}" for idx in range(len(default_cols), n_cols)]
        col_names = default_cols + extra
    else:
        col_names = default_cols[:n_cols]

    df.columns = col_names

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

    if "Czas od" in df.columns:
        df["Czas od"] = df["Czas od"].astype(str).str.strip()
    else:
        logging.warning(f"В файле пользователя {user_id} отсутствует колонка 'Czas od'")

    df = df[df['Data_dt'].notna() & df['Czas od'].notna()].copy()
    logging.info(f"После фильтров строк для пользователя {user_id}: {len(df)}")

    return df


def format_schedule(df: pd.DataFrame, title: str, user_id: int) -> str:
    if df.empty:
        return f"{title} пусто 📭"

    group_num = user_groups.get(user_id, 0)
    if group_num > 0 and "Grupy" in df.columns:
        def belongs_to_group(grupa_val: str) -> bool:
            if not isinstance(grupa_val, str):
                return False
            grupa_val = grupa_val.strip()
            if "WykS" in grupa_val:
                return True
            return f"Cw{group_num}S" in grupa_val

        df = df[df["Grupy"].apply(belongs_to_group)]

    if df.empty:
        return f"{title} (после фильтра) пусто 📭"

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


def get_schedule_data_for_day(date: date, user_id: int) -> str:
    df = read_schedule(user_id)
    if df.empty:
        return "❌ Ваш файл расписания не найден или пуст."
    day_df = df[df['Data_dt'] == date]
    return format_schedule(day_df, f"Расписание на {date:%d.%m.%Y}", user_id)
