import json
from pathlib import Path
from typing import Mapping

import pandas as pd
import streamlit as st

from paths import FORECAST_DIR, RECOMMENDER_DIR, REQUIRED_FILES


SEASON_LABELS = {
    "winter": "Зима",
    "spring": "Весна",
    "summer": "Лето",
    "autumn": "Осень",
}

SEGMENT_LABELS = {
    "Summer Family ULTRA": "C0 Летние семьи",
    "Weekend Getaway": "C1 Короткий отдых / выходные",
    "Weekday Adults": "C2 Будничные взрослые",
    "Last-minute Winter": "C3 Срочные зимние семьи",
    "Young Spring Groups": "C4 Молодежные / групповые",
    "Medical Treatment": "C5 Медицинские / оздоровительные",
}

SEGMENT_SHORT_LABELS = {
    "Summer Family ULTRA": "C0 Летние семьи",
    "Weekend Getaway": "C1 Выходные",
    "Weekday Adults": "C2 Будни",
    "Last-minute Winter": "C3 Зимние семьи",
    "Young Spring Groups": "C4 Группы",
    "Medical Treatment": "C5 Оздоровление",
}


def season_label(value: str) -> str:
    return SEASON_LABELS.get(str(value), str(value))


def segment_label(value: str) -> str:
    return SEGMENT_LABELS.get(str(value), str(value))


def segment_short_label(value: str) -> str:
    return SEGMENT_SHORT_LABELS.get(str(value), str(value))


def segment_list_label(value: str) -> str:
    return ", ".join(segment_label(part.strip()) for part in str(value).split(","))


def format_int(value: float) -> str:
    return f"{float(value):,.0f}".replace(",", " ")


def validate_artifacts() -> dict[str, Path]:
    return {name: path for name, path in REQUIRED_FILES.items() if not path.exists()}


@st.cache_data(show_spinner="Загружаем артефакты...")
def load_data() -> dict[str, pd.DataFrame | dict]:
    df_daily = pd.read_parquet(FORECAST_DIR / "df_daily.parquet")
    df_forecast = pd.read_parquet(FORECAST_DIR / "forecast_best.parquet")
    df_underload = pd.read_parquet(FORECAST_DIR / "underload_dates.parquet")
    df_periods = pd.read_parquet(FORECAST_DIR / "underload_periods.parquet")
    df_segments = pd.read_parquet(RECOMMENDER_DIR / "segment_history.parquet").reset_index()
    df_affinity = pd.read_parquet(RECOMMENDER_DIR / "season_affinity.parquet")
    df_scores = pd.read_parquet(RECOMMENDER_DIR / "segment_scores.parquet")
    df_recs = pd.read_parquet(RECOMMENDER_DIR / "recommendations.parquet")

    for df, date_col in [
        (df_daily, "ds"),
        (df_forecast, "ds"),
        (df_underload, "ds"),
        (df_scores, "ds"),
        (df_recs, "date"),
    ]:
        df[date_col] = pd.to_datetime(df[date_col])

    for col in ["period_start", "period_end"]:
        df_periods[col] = pd.to_datetime(df_periods[col])

    with open(FORECAST_DIR / "baseline_metrics.json", encoding="utf-8") as file:
        baseline_metrics = json.load(file)
    with open(FORECAST_DIR / "model_comparison.json", encoding="utf-8") as file:
        model_comparison = json.load(file)

    return {
        "daily": df_daily,
        "forecast": df_forecast,
        "underload": df_underload,
        "periods": df_periods,
        "segments": df_segments,
        "affinity": df_affinity,
        "scores": df_scores,
        "recommendations": df_recs,
        "baseline_metrics": baseline_metrics,
        "model_comparison": model_comparison,
    }


def filter_date_range(df: pd.DataFrame, date_col: str, start, end) -> pd.DataFrame:
    start_ts = pd.to_datetime(start)
    end_ts = pd.to_datetime(end)
    return df[(df[date_col] >= start_ts) & (df[date_col] <= end_ts)].copy()


def future_frame(df_forecast: pd.DataFrame, history_end: pd.Timestamp) -> pd.DataFrame:
    future = df_forecast[df_forecast["ds"] > history_end].copy()
    future["forecast_load"] = future["yhat"]
    future["forecast_lower"] = future["yhat_lower"]
    future["forecast_upper"] = future["yhat_upper"]
    return future


def seasonal_target_loads(
    df_daily: pd.DataFrame,
    seasonal_quantile: float = 0.75,
    floor_quantile: float = 0.50,
) -> dict[str, float]:
    """Return business target by season with a global floor for low seasons."""
    history = df_daily.copy()
    if "season" not in history.columns:
        history["season"] = history["ds"].dt.month.map(month_to_season)
    target_col = "load_rate" if "load_rate" in history.columns else "y"
    precision = 4 if target_col == "load_rate" else 1
    seasonal = history.groupby("season")[target_col].quantile(seasonal_quantile)
    floor = float(history[target_col].quantile(floor_quantile))
    return seasonal.clip(lower=floor).round(precision).to_dict()


def add_risk_columns(future: pd.DataFrame, target_load: float | Mapping[str, float]) -> pd.DataFrame:
    result = future.copy()
    result["season"] = result["ds"].dt.month.map(month_to_season)
    if isinstance(target_load, Mapping):
        fallback = pd.Series(target_load, dtype="float64").median()
        target = result["season"].map(target_load).fillna(fallback)
        if "capacity_est" in result.columns:
            result["target_rate"] = target
            result["target_load"] = result["target_rate"] * result["capacity_est"]
        else:
            result["target_load"] = target
    else:
        result["target_load"] = target_load
    result["load_gap"] = (result["target_load"] - result["forecast_load"]).clip(lower=0)
    result["underload_pct"] = result["load_gap"] / result["target_load"] * 100
    result["is_risk"] = result["forecast_load"] < result["target_load"]
    return result


def month_to_season(month: int) -> str:
    if month in (12, 1, 2):
        return "winter"
    if month in (3, 4, 5):
        return "spring"
    if month in (6, 7, 8):
        return "summer"
    return "autumn"


def csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8-sig")
