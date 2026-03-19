from __future__ import annotations

import os
from datetime import datetime, timedelta

from clickhouse_driver import Client
from airflow import DAG
from airflow.operators.python import PythonOperator

# ===== ClickHouse connection =====
CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CH_USER = os.getenv("CLICKHOUSE_USER", "airflow")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "airflow")
CH_DB = os.getenv("CLICKHOUSE_DB", "dwh")


def get_client() -> Client:
    return Client(
        host=CH_HOST,
        port=CH_PORT,
        user=CH_USER,
        password=CH_PASSWORD,
        database=CH_DB,
        send_receive_timeout=300,
    )


# ===== Tables =====
BRONZE_DB = "bronze"
BRONZE_TABLE = "bookings_raw"

SILVER_DB = "silver"
SILVER_BOOKINGS = "bookings"
SILVER_DQ = "dq_bookings_daily"

OVERLAP_DAYS = 2


def ensure_silver_tables():
    """
    Silver = очищенная/типизированная OBT + флаг качества + нормализованный grp_norm.

    Важно:
    - created_dt (Date) НЕ Nullable и используется в partition/order key.
    - Nullable поля НЕ входят в sorting key.
    """
    c = get_client()
    c.execute(f"CREATE DATABASE IF NOT EXISTS {SILVER_DB}")

    c.execute(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_DB}.{SILVER_BOOKINGS} (
        created_dt Date,

        check_in_dt Nullable(DateTime),
        check_out_dt Nullable(DateTime),

        activity_dt Nullable(DateTime),

        is_valid_for_rfm UInt8,

        tariff Nullable(String),
        discount Nullable(Float64),
        composition Nullable(String),

        grp Nullable(String),              -- как в источнике (может содержать пробелы/NBSP)
        grp_norm Nullable(String),         -- только цифры (ключ для GOLD)

        room_type Nullable(String),
        currency Nullable(String),

        guest_id Nullable(UInt64),
        guest_last_name Nullable(String),
        citizenship Nullable(String),
        gender Nullable(String),
        guest_age Nullable(UInt16),

        prev_stays Nullable(UInt16),

        rooms Nullable(UInt16),
        places Nullable(UInt16),
        extra_places Nullable(UInt16),
        children Nullable(UInt16),
        adults Nullable(UInt16),

        _load_id UUID,
        _ingested_at DateTime,
        _source_file String,
        _row_num UInt64
    )
    ENGINE = MergeTree
    PARTITION BY toYYYYMM(created_dt)
    ORDER BY (created_dt, _load_id, _row_num)
    """)

    c.execute(f"""
    CREATE TABLE IF NOT EXISTS {SILVER_DB}.{SILVER_DQ} (
        dt Date,
        total_rows UInt64,
        valid_for_rfm_rows UInt64,
        missing_guest_rows UInt64,
        missing_activity_dt_rows UInt64,
        missing_grp_norm_rows UInt64,
        updated_at DateTime
    )
    ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY dt
    """)


def detect_silver_watermark(**context):
    """
    Watermark по created_dt в silver.bookings.
    - если таблица пуста -> initial, watermark=1970-01-01
    - иначе watermark = max(created_dt) - OVERLAP_DAYS
    """
    c = get_client()
    cnt = c.execute(f"SELECT count() FROM {SILVER_DB}.{SILVER_BOOKINGS}")[0][0]

    if cnt == 0:
        initial = True
        watermark = datetime(1970, 1, 1).date()
    else:
        initial = False
        max_dt = c.execute(f"SELECT max(created_dt) FROM {SILVER_DB}.{SILVER_BOOKINGS}")[0][0]
        watermark = datetime(1970, 1, 1).date() if max_dt is None else (max_dt - timedelta(days=OVERLAP_DAYS))

    context["ti"].xcom_push(key="initial_silver", value=initial)
    context["ti"].xcom_push(key="watermark_silver", value=watermark.isoformat())


def delete_silver_overlap_window(**context):
    """
    Идемпотентность: перед перезаливкой overlap окна удаляем созданные строки.
    """
    initial = context["ti"].xcom_pull(key="initial_silver", task_ids="detect_silver_watermark")
    watermark = context["ti"].xcom_pull(key="watermark_silver", task_ids="detect_silver_watermark")
    if initial:
        return

    c = get_client()
    c.execute(
        f"ALTER TABLE {SILVER_DB}.{SILVER_BOOKINGS} DELETE WHERE created_dt >= toDate(%(w)s)",
        {"w": watermark},
    )


def upsert_silver_from_bronze(**context):
    """
    Перезаливаем overlap-окно из bronze -> silver:
    - типизация дат/чисел
    - activity_dt = check_out_dt, иначе check_in_dt
    - is_valid_for_rfm = guest_id not null AND activity_dt not null
    - grp_norm = только цифры из "Группа"
    """
    watermark = context["ti"].xcom_pull(key="watermark_silver", task_ids="detect_silver_watermark")

    c = get_client()
    sql = f"""
    INSERT INTO {SILVER_DB}.{SILVER_BOOKINGS}
    SELECT
        B._created_dt AS created_dt,

        parseDateTimeBestEffortOrNull(nullIf(trim(B.`Заезд`), '')) AS check_in_dt,
        parseDateTimeBestEffortOrNull(nullIf(trim(B.`Выезд`), '')) AS check_out_dt,

        ifNull(
            parseDateTimeBestEffortOrNull(nullIf(trim(B.`Выезд`), '')),
            parseDateTimeBestEffortOrNull(nullIf(trim(B.`Заезд`), ''))
        ) AS activity_dt,

        (
            toUInt64OrNull(replaceAll(nullIf(trim(B.`Гость`), ''), '.0', '')) IS NOT NULL
            AND ifNull(
                parseDateTimeBestEffortOrNull(nullIf(trim(B.`Выезд`), '')),
                parseDateTimeBestEffortOrNull(nullIf(trim(B.`Заезд`), ''))
            ) IS NOT NULL
        ) AS is_valid_for_rfm,

        nullIf(trim(B.`Тариф`), '') AS tariff,
        toFloat64OrNull(replaceAll(nullIf(trim(B.`Скидка`), ''), ',', '.')) AS discount,
        nullIf(trim(B.`Состав`), '') AS composition,

        nullIf(trim(B.`Группа`), '') AS grp,
        nullIf(replaceRegexpAll(trim(B.`Группа`), '[^0-9]', ''), '') AS grp_norm,

        nullIf(trim(B.`Тип номера`), '') AS room_type,
        nullIf(trim(B.`Валюта`), '') AS currency,

        toUInt64OrNull(replaceAll(nullIf(trim(B.`Гость`), ''), '.0', '')) AS guest_id,
        nullIf(trim(B.`Гость.1`), '') AS guest_last_name,
        nullIf(trim(B.`Гражданство`), '') AS citizenship,
        nullIf(trim(B.`Пол`), '') AS gender,
        toUInt16OrNull(replaceAll(nullIf(trim(B.`Гость.2`), ''), '.0', '')) AS guest_age,

        toUInt16OrNull(replaceAll(nullIf(trim(B.`Пред. заезды`), ''), '.0', '')) AS prev_stays,

        toUInt16OrNull(replaceAll(nullIf(trim(B.`Номеров`), ''), '.0', '')) AS rooms,
        toUInt16OrNull(replaceAll(nullIf(trim(B.`Мест`), ''), '.0', '')) AS places,
        toUInt16OrNull(replaceAll(nullIf(trim(B.`Доп. мест`), ''), '.0', '')) AS extra_places,
        toUInt16OrNull(replaceAll(nullIf(trim(B.`Детей`), ''), '.0', '')) AS children,
        toUInt16OrNull(replaceAll(nullIf(trim(B.`Взр.`), ''), '.0', '')) AS adults,

        B._load_id,
        B._ingested_at,
        B._source_file,
        B._row_num
    FROM {BRONZE_DB}.{BRONZE_TABLE} B
    WHERE B._created_dt >= toDate(%(w)s)
    """
    c.execute(sql, {"w": watermark})


def refresh_dq_summary(**context):
    """
    DQ-метрики по дням created_dt (только overlap-окно).
    """
    watermark = context["ti"].xcom_pull(key="watermark_silver", task_ids="detect_silver_watermark")
    c = get_client()

    sql = f"""
    INSERT INTO {SILVER_DB}.{SILVER_DQ}
    SELECT
        created_dt AS dt,
        count() AS total_rows,
        sum(is_valid_for_rfm) AS valid_for_rfm_rows,
        sum(guest_id IS NULL) AS missing_guest_rows,
        sum(activity_dt IS NULL) AS missing_activity_dt_rows,
        sum(grp_norm IS NULL OR grp_norm = '') AS missing_grp_norm_rows,
        now() AS updated_at
    FROM {SILVER_DB}.{SILVER_BOOKINGS}
    WHERE created_dt >= toDate(%(w)s)
    GROUP BY created_dt
    """
    c.execute(sql, {"w": watermark})


default_args = {"owner": "airflow", "retries": 2, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="bronze_to_silver_bookings",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bronze", "silver", "clickhouse"],
) as dag:

    t1 = PythonOperator(task_id="ensure_silver_tables", python_callable=ensure_silver_tables)
    t2 = PythonOperator(task_id="detect_silver_watermark", python_callable=detect_silver_watermark)
    t3 = PythonOperator(task_id="delete_silver_overlap_window", python_callable=delete_silver_overlap_window)
    t4 = PythonOperator(task_id="upsert_silver_from_bronze", python_callable=upsert_silver_from_bronze)
    t5 = PythonOperator(task_id="refresh_dq_summary", python_callable=refresh_dq_summary)

    t1 >> t2 >> t3 >> t4 >> t5
