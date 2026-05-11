from pathlib import Path


APP_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = APP_DIR.parent

FORECAST_DIR = PROJECT_ROOT / "forecasting" / "artifacts_forecasting"
RECOMMENDER_DIR = PROJECT_ROOT / "recommender" / "artifacts_recommender"

REQUIRED_FILES = {
    "История активных бронирований": FORECAST_DIR / "df_daily.parquet",
    "Прогноз": FORECAST_DIR / "forecast_best.parquet",
    "Даты риска": FORECAST_DIR / "underload_dates.parquet",
    "Периоды риска": FORECAST_DIR / "underload_periods.parquet",
    "Метрики baseline": FORECAST_DIR / "baseline_metrics.json",
    "Сравнение моделей": FORECAST_DIR / "model_comparison.json",
    "Профили сегментов": RECOMMENDER_DIR / "segment_history.parquet",
    "Сезонная пригодность": RECOMMENDER_DIR / "season_affinity.parquet",
    "Скоринг сегментов": RECOMMENDER_DIR / "segment_scores.parquet",
    "Рекомендации": RECOMMENDER_DIR / "recommendations.parquet",
}
