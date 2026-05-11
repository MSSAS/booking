"""
holidays_ru.py
==============
Модуль формирует датафрейм праздников для Prophet.

Prophet принимает праздники в формате:
    holiday (str) | ds (datetime) | lower_window (int) | upper_window (int)

lower_window / upper_window — сколько дней ДО и ПОСЛЕ события учитывать.
Например, lower_window=-1, upper_window=1 значит: день до праздника, сам праздник, день после.

Включаем:
  1. Официальные праздники РФ (Производственный календарь)
  2. Школьные каникулы — влияют на семейный спрос (сегменты C0, C3)
  3. Майские праздники — длинные выходные, мощный сигнал для санаториев
"""

import pandas as pd
from typing import Optional


# ─────────────────────────────────────────────────────────────────────────────
# 1. ОФИЦИАЛЬНЫЕ ПРАЗДНИКИ РФ
#    Источник: Производственный календарь РФ, ст. 112 ТК РФ
# ─────────────────────────────────────────────────────────────────────────────

# Даты официальных праздников (фиксированные + перенесённые по годам)
# Формат: список строк 'YYYY-MM-DD'
OFFICIAL_HOLIDAYS = {
    # Новогодние каникулы: 1-8 января каждый год
    "new_year": {
        "dates": [
            f"{year}-01-0{d}" if d < 10 else f"{year}-01-{d}"
            for year in range(2017, 2027)
            for d in range(1, 9)
        ],
        "lower_window": -1,
        "upper_window": 1,
    },
    # Рождество: 7 января
    "christmas_orthodox": {
        "dates": [f"{year}-01-07" for year in range(2017, 2027)],
        "lower_window": 0,
        "upper_window": 0,
    },
    # День защитника Отечества: 23 февраля
    "defender_day": {
        "dates": [f"{year}-02-23" for year in range(2017, 2027)],
        "lower_window": -1,
        "upper_window": 1,
    },
    # Международный женский день: 8 марта
    "womens_day": {
        "dates": [f"{year}-03-08" for year in range(2017, 2027)],
        "lower_window": -1,
        "upper_window": 1,
    },
    # Праздник Весны и Труда: 1 мая
    "labor_day": {
        "dates": [f"{year}-05-01" for year in range(2017, 2027)],
        "lower_window": -1,
        "upper_window": 2,
    },
    # День Победы: 9 мая
    "victory_day": {
        "dates": [f"{year}-05-09" for year in range(2017, 2027)],
        "lower_window": -2,
        "upper_window": 2,
    },
    # День России: 12 июня
    "russia_day": {
        "dates": [f"{year}-06-12" for year in range(2017, 2027)],
        "lower_window": -1,
        "upper_window": 1,
    },
    # День народного единства: 4 ноября
    "unity_day": {
        "dates": [f"{year}-11-04" for year in range(2017, 2027)],
        "lower_window": -1,
        "upper_window": 1,
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# 2. ШКОЛЬНЫЕ КАНИКУЛЫ
#    Примерные даты (могут незначительно отличаться по регионам)
#    Влияют на семейные сегменты (C0 — Summer Family, C3 — Last-minute Winter)
# ─────────────────────────────────────────────────────────────────────────────

SCHOOL_HOLIDAYS = {
    # Осенние каникулы: конец октября — начало ноября
    "autumn_school_break": {
        "dates": [
            # 2017-2023 (примерные диапазоны)
            *pd.date_range("2017-10-28", "2017-11-05").strftime("%Y-%m-%d").tolist(),
            *pd.date_range("2018-10-27", "2018-11-04").strftime("%Y-%m-%d").tolist(),
            *pd.date_range("2019-10-26", "2019-11-03").strftime("%Y-%m-%d").tolist(),
            *pd.date_range("2022-10-29", "2022-11-06").strftime("%Y-%m-%d").tolist(),
            *pd.date_range("2023-10-28", "2023-11-05").strftime("%Y-%m-%d").tolist(),
            *pd.date_range("2024-10-26", "2024-11-03").strftime("%Y-%m-%d").tolist(),
        ],
        "lower_window": 0,
        "upper_window": 0,
    },
    # Зимние каникулы: конец декабря — начало января
    "winter_school_break": {
        "dates": [
            *pd.date_range("2017-12-28", "2018-01-08").strftime("%Y-%m-%d").tolist(),
            *pd.date_range("2018-12-28", "2019-01-08").strftime("%Y-%m-%d").tolist(),
            *pd.date_range("2019-12-28", "2020-01-08").strftime("%Y-%m-%d").tolist(),
            *pd.date_range("2022-12-28", "2023-01-08").strftime("%Y-%m-%d").tolist(),
            *pd.date_range("2023-12-28", "2024-01-08").strftime("%Y-%m-%d").tolist(),
            *pd.date_range("2024-12-28", "2025-01-08").strftime("%Y-%m-%d").tolist(),
        ],
        "lower_window": 0,
        "upper_window": 0,
    },
    # Весенние каникулы: конец марта
    "spring_school_break": {
        "dates": [
            *pd.date_range("2017-03-25", "2017-04-02").strftime("%Y-%m-%d").tolist(),
            *pd.date_range("2018-03-24", "2018-04-01").strftime("%Y-%m-%d").tolist(),
            *pd.date_range("2019-03-23", "2019-03-31").strftime("%Y-%m-%d").tolist(),
            *pd.date_range("2022-03-26", "2022-04-03").strftime("%Y-%m-%d").tolist(),
            *pd.date_range("2023-03-25", "2023-04-02").strftime("%Y-%m-%d").tolist(),
            *pd.date_range("2024-03-23", "2024-03-31").strftime("%Y-%m-%d").tolist(),
        ],
        "lower_window": 0,
        "upper_window": 0,
    },
    # Летние каникулы: июнь — август
    "summer_school_break": {
        "dates": [
            *pd.date_range("2017-06-01", "2017-08-31").strftime("%Y-%m-%d").tolist(),
            *pd.date_range("2018-06-01", "2018-08-31").strftime("%Y-%m-%d").tolist(),
            *pd.date_range("2019-06-01", "2019-08-31").strftime("%Y-%m-%d").tolist(),
            *pd.date_range("2022-06-01", "2022-08-31").strftime("%Y-%m-%d").tolist(),
            *pd.date_range("2023-06-01", "2023-08-31").strftime("%Y-%m-%d").tolist(),
            *pd.date_range("2024-06-01", "2024-08-31").strftime("%Y-%m-%d").tolist(),
            *pd.date_range("2025-06-01", "2025-08-31").strftime("%Y-%m-%d").tolist(),
        ],
        "lower_window": 0,
        "upper_window": 0,
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# 3. ФУНКЦИЯ СБОРКИ ДАТАФРЕЙМА ДЛЯ PROPHET
# ─────────────────────────────────────────────────────────────────────────────

def build_holidays_df(
    include_official: bool = True,
    include_school: bool = True,
) -> pd.DataFrame:
    """
    Строит датафрейм праздников в формате Prophet.

    Параметры
    ----------
    include_official : bool
        Включать официальные праздники РФ (по умолчанию True).
    include_school : bool
        Включать школьные каникулы (по умолчанию True).

    Возвращает
    ----------
    pd.DataFrame с колонками:
        holiday       — название события
        ds            — дата (datetime64)
        lower_window  — дней до события
        upper_window  — дней после события
    """
    records = []

    sources = {}
    if include_official:
        sources.update(OFFICIAL_HOLIDAYS)
    if include_school:
        sources.update(SCHOOL_HOLIDAYS)

    for holiday_name, config in sources.items():
        for date_str in config["dates"]:
            records.append({
                "holiday":      holiday_name,
                "ds":           pd.to_datetime(date_str),
                "lower_window": config["lower_window"],
                "upper_window": config["upper_window"],
            })

    df = pd.DataFrame(records).drop_duplicates(subset=["holiday", "ds"])
    df = df.sort_values("ds").reset_index(drop=True)
    return df


def get_school_holidays_flag(date_series: pd.Series) -> pd.Series:
    """
    Возвращает бинарный флаг: 1 если дата попадает в школьные каникулы.
    Используется как внешний регрессор в Prophet.

    Параметры
    ----------
    date_series : pd.Series of datetime64

    Возвращает
    ----------
    pd.Series[int] — 0 или 1
    """
    school_df = build_holidays_df(include_official=False, include_school=True)
    school_dates = set(school_df["ds"].dt.date)
    return date_series.dt.date.isin(school_dates).astype(int)


# ─────────────────────────────────────────────────────────────────────────────
# Проверка при запуске модуля напрямую
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    holidays = build_holidays_df()
    print(f"Всего записей в календаре праздников: {len(holidays):,}")
    print(f"Уникальных праздников: {holidays['holiday'].nunique()}")
    print("\nСписок праздников:")
    for name, cnt in holidays.groupby("holiday").size().items():
        print(f"  {name}: {cnt} дней")
    print("\nПервые строки датафрейма:")
    print(holidays.head(10).to_string())
