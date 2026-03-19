# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Diploma Project

**Тема:** «Проектирование и реализация модуля аналитики для сегментации клиентов и прогнозирования спроса на туристические услуги (на примере санатория)»

**Бизнес-цель:** Помочь санаторию управлять загрузкой номерного фонда и маркетингом через три взаимосвязанных компонента:
1. **Сегментация клиентов** — кластеризация гостей по RFM-метрикам и поведенческим признакам
2. **Прогноз спроса** — предсказание загрузки по датам и типам номеров/тарифов
3. **Рекомендации** — по прогнозу находим периоды недозагрузки, по кластерам определяем какие сегменты и каким предложением привлекать

**Текущий подход:** все модели разрабатываются в ноутбуках напрямую из `silver.bookings`. Gold-слой не используется — был экспериментальным и удалён. В Airflow DAG модели выносятся только после валидации в ноутбуках.

### Планируемые модели

| Модель | Алгоритм | Входные данные | Выход |
|---|---|---|---|
| Кластеризация клиентов | K-Means | guest-level features из silver | cluster_id + профиль кластера |
| Прогноз спроса | Prophet → XGBoost | временной ряд загрузки из silver | forecasted_rooms по дате и room_type |
| Рекомендации | бизнес-логика | кластеры + прогноз | какой сегмент привлекать в какой период |

### Ключевое ограничение данных
Выручки нет — `monetary` является прокси: `nights × rooms`.

---

## Project Overview

Санаторная аналитическая платформа на Apache Airflow + ClickHouse. Источник данных — `data/bookings.parquet` (~12MB, колонки на русском языке), загружается через медальонную архитектуру Bronze → Silver.

## Services & Access

Start all services:
```bash
docker-compose up -d
```

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Superset | http://localhost:8088 | admin / admin |
| ClickHouse HTTP | http://localhost:8123 | airflow / airflow |
| ClickHouse Native | localhost:9000 | airflow / airflow |


## Architecture

### Medallion Data Pipeline (dags/)

All three DAGs have `schedule=None` (manually triggered) and use a **2-day overlap window** for idempotent reruns.

**`00_bronz.py` — `bronze_bookings_incremental`**
- Reads `data/bookings.parquet` in batches of 50,000 rows
- All source columns stored as `String` (no transformation)
- Adds metadata: `_created_dt`, `_load_id` (UUID), `_ingested_at`, `_source_file`, `_row_num`
- Table: `bronze.bookings_raw` (MergeTree, ordered by `_created_dt, _load_id, _row_num`)
- Incremental watermark uses `max(_created_dt) - 2 days`

**`01_silver.py` — `bronze_to_silver_bookings`**
- Type-converts raw strings to typed columns (dates, numerics, nullable types)
- Normalizes `grp_norm` (digits-only via regex from source "Группа" field)
- Sets `is_valid_for_rfm = 1` where `guest_id IS NOT NULL AND activity_dt IS NOT NULL`
- `activity_dt` defaults to `check_out_dt`, falls back to `check_in_dt`
- Table: `silver.bookings` (MergeTree, partitioned by `toYYYYMM(created_dt)`)
- Also writes: `silver.dq_bookings_daily` (data quality metrics, ReplacingMergeTree)


### Key ClickHouse Design Decisions
- Silver partitioned by `toYYYYMM(created_dt)`
- `is_valid_for_rfm = 1` filter required for all ML modeling (ensures guest_id + activity_dt are present)
- No Gold layer — modeling done directly from silver in notebooks

## Tech Stack

| Component | Version |
|---|---|
| Apache Airflow | 2.9.3 (Python 3.11) |
| ClickHouse | 23.8 |
| PostgreSQL | 15 (Airflow metadata only) |
| Apache Superset | 3.1.3 |
| pandas | 2.2.2 |
| pyarrow | 17.0.0 |
| clickhouse-driver | 0.2.10 |

## Environment Configuration

All credentials and service config are in `.env`. Key variables:
- `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_DB`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`
- `AIRFLOW_FERNET_KEY`, `AIRFLOW_SECRET_KEY`
- `SUPERSET_SECRET_KEY`

The `.env` file is tracked in git (credentials are non-production defaults).
