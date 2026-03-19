from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta

import pandas as pd
import pyarrow.parquet as pq
from clickhouse_driver import Client

from airflow import DAG
from airflow.operators.python import PythonOperator


# ========== НАСТРОЙКИ ==========
PARQUET_PATH = "/opt/airflow/data/bookings.parquet"

BRONZE_DB = "bronze"
BRONZE_TABLE = "bookings_raw"

DATE_COL = "Дата создания"      # колонка в parquet
OVERLAP_DAYS = 2                # overlap-окно
BATCH_SIZE = 50_000             # вставка пачками

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


def _to_str(v) -> str:
    # bronze: всё как строка; пустые/NaN -> ""
    if v is None or (isinstance(v, float) and pd.isna(v)) or pd.isna(v):
        return ""
    return str(v)


def ensure_tables():
    """
    Создаём базу bronze и таблицу bronze.bookings_raw.
    Все исходные колонки: String.
    + техполя:
      _created_dt Date (для watermark)
      _load_id UUID
      _ingested_at DateTime
      _source_file String
      _row_num UInt64
    """
    if not os.path.exists(PARQUET_PATH):
        raise FileNotFoundError(f"Не найден файл: {PARQUET_PATH}")

    client = get_client()
    client.execute(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

    pf = pq.ParquetFile(PARQUET_PATH)
    src_cols = [f.name for f in pf.schema_arrow]

    if DATE_COL not in src_cols:
        raise ValueError(f"В parquet нет колонки '{DATE_COL}'. Найдено: {src_cols}")


    cols_ddl = [f"`{c.replace('`','')}` String" for c in src_cols]


    cols_ddl += [
        "`_created_dt` Date",
        "`_load_id` UUID",
        "`_ingested_at` DateTime",
        "`_source_file` String",
        "`_row_num` UInt64",
    ]

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {BRONZE_DB}.{BRONZE_TABLE} (
        {", ".join(cols_ddl)}
    )
    ENGINE = MergeTree
    ORDER BY (_created_dt, _load_id, _row_num)
    """

    client.execute(ddl)


def detect_mode_and_watermark(**context):
    """
    1) Если bronze пуста -> initial load (watermark = 1970-01-01)
    2) Иначе watermark = max(_created_dt) - OVERLAP_DAYS
    Кладём в XCom:
      - initial (bool)
      - watermark_date (YYYY-MM-DD)
    """
    client = get_client()

    cnt = client.execute(f"SELECT count() FROM {BRONZE_DB}.{BRONZE_TABLE}")[0][0]
    if cnt == 0:
        initial = True
        watermark = datetime(1970, 1, 1).date()
    else:
        initial = False
        max_dt = client.execute(f"SELECT max(_created_dt) FROM {BRONZE_DB}.{BRONZE_TABLE}")[0][0]
        if max_dt is None:
            watermark = datetime(1970, 1, 1).date()
        else:
            watermark = (max_dt - timedelta(days=OVERLAP_DAYS))

    context["ti"].xcom_push(key="initial", value=initial)
    context["ti"].xcom_push(key="watermark_date", value=watermark.isoformat())


def delete_overlap_window(**context):
    """
    Чтобы не накапливать дубли при overlap, удаляем окно:
      _created_dt >= watermark_date
    Только если НЕ initial.
    """
    initial = context["ti"].xcom_pull(key="initial", task_ids="detect_mode_and_watermark")
    watermark_date = context["ti"].xcom_pull(key="watermark_date", task_ids="detect_mode_and_watermark")

    if initial:
        return

    client = get_client()

    client.execute(
        f"ALTER TABLE {BRONZE_DB}.{BRONZE_TABLE} DELETE WHERE _created_dt >= toDate(%(w)s)",
        {"w": watermark_date},
    )


def load_parquet_filtered(**context):
    """
    Загружаем:
      - если initial: весь файл
      - если incremental: только строки где _created_dt >= watermark_date
    Пишем все src поля как String + _created_dt + мета.
    """
    watermark_date = context["ti"].xcom_pull(key="watermark_date", task_ids="detect_mode_and_watermark")
    watermark = pd.to_datetime(watermark_date).date()

    client = get_client()
    pf = pq.ParquetFile(PARQUET_PATH)
    src_cols = [f.name for f in pf.schema_arrow]

    load_id = str(uuid.uuid4())
    ingested_at = datetime.utcnow()
    source_file = os.path.basename(PARQUET_PATH)

    insert_cols = [f"`{c.replace('`','')}`" for c in src_cols] + [
        "`_created_dt`",
        "`_load_id`",
        "`_ingested_at`",
        "`_source_file`",
        "`_row_num`",
    ]
    insert_sql = f"INSERT INTO {BRONZE_DB}.{BRONZE_TABLE} ({', '.join(insert_cols)}) VALUES"

    row_num = 0
    buffer = []

    for batch in pf.iter_batches(batch_size=BATCH_SIZE):
        df = batch.to_pandas()

        # Парсим "Дата создания" -> Date
        created = pd.to_datetime(df[DATE_COL], errors="coerce", dayfirst=True).dt.date

        # Фильтр по watermark (окно)
        mask = created >= watermark
        df = df.loc[mask]
        created = created.loc[mask]

        if df.empty:
            continue

        # Идём по строкам, приводим к String, дописываем техполя
        for rec, cdt in zip(df.itertuples(index=False, name=None), created):
            row_num += 1
            str_fields = [_to_str(x) for x in rec]

            # если дата не распарсилась, можно либо пропускать, либо складывать в 1970-01-01
            if pd.isna(cdt) or cdt is None:
                cdt = datetime(1970, 1, 1).date()

            buffer.append(tuple(str_fields + [cdt, load_id, ingested_at, source_file, row_num]))

            if len(buffer) >= BATCH_SIZE:
                client.execute(insert_sql, buffer)
                buffer = []

    if buffer:
        client.execute(insert_sql, buffer)


default_args = {"owner": "airflow", "retries": 2, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="bronze_bookings_incremental",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bronze", "clickhouse", "incremental"],
) as dag:

    t1 = PythonOperator(task_id="ensure_tables", python_callable=ensure_tables)
    t2 = PythonOperator(task_id="detect_mode_and_watermark", python_callable=detect_mode_and_watermark)
    t3 = PythonOperator(task_id="delete_overlap_window", python_callable=delete_overlap_window)
    t4 = PythonOperator(task_id="load_parquet_filtered", python_callable=load_parquet_filtered)

    t1 >> t2 >> t3 >> t4