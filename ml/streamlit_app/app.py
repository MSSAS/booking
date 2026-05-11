from datetime import date

import pandas as pd
import streamlit as st

from data import (
    csv_bytes,
    filter_date_range,
    format_int,
    future_frame,
    add_risk_columns,
    load_data,
    season_label,
    seasonal_target_loads,
    segment_label,
    segment_list_label,
    segment_short_label,
    validate_artifacts,
)
from plots import (
    forecast_ribbon_chart,
    model_metrics_chart,
    next_30_days_chart,
    risk_heatmap,
    segment_bookings_chart,
    segment_month_chart,
)

STATUS_OPTIONS = ["Не запущено", "В работе", "Запущено", "Завершено"]


st.set_page_config(
    page_title="Активные бронирования санатория",
    layout="wide",
    initial_sidebar_state="expanded",
)


st.markdown(
    """
    <style>
    :root {
        --bg: #F6F8FA;
        --panel: #FFFFFF;
        --panel-soft: #F9FAFB;
        --text: #1F2937;
        --muted: #6B7280;
        --border: #E5E7EB;
        --green: #2ECC71;
        --orange: #F39C12;
        --red: #E74C3C;
        --blue: #3498DB;
        --light-blue: #AED6F1;
        --dark-red: #C0392B;
        --amber-soft: #FFF7E6;
        --red-soft: #FFF1F0;
        --green-soft: #ECFDF3;
        --blue-soft: #EFF6FF;
    }
    .stApp,
    [data-testid="stAppViewContainer"] {
        background: var(--bg);
        color: var(--text);
    }
    [data-testid="stHeader"] {
        background: rgba(246, 248, 250, 0.92);
    }
    .block-container {
        max-width: 1500px;
        padding-top: 1.3rem;
        padding-bottom: 2rem;
    }
    [data-testid="stSidebar"] {
        background: #FFFFFF;
        border-right: 1px solid var(--border);
    }
    [data-testid="stSidebar"] * {
        color: var(--text) !important;
    }
    [data-testid="stSidebar"] [data-baseweb="tag"] {
        background-color: var(--blue) !important;
        border-color: var(--blue) !important;
    }
    [data-testid="stSidebar"] [data-baseweb="tag"] * {
        color: #FFFFFF !important;
    }
    [data-baseweb="select"] > div,
    [data-baseweb="input"] > div,
    input {
        background: #FFFFFF !important;
        color: var(--text) !important;
        border-color: var(--border) !important;
    }
    [data-baseweb="select"] span,
    [data-baseweb="select"] input,
    [data-baseweb="select"] svg {
        color: var(--text) !important;
        fill: var(--text) !important;
    }
    [data-testid="stWidgetLabel"] p,
    [data-testid="stWidgetLabel"] label,
    [data-testid="stMarkdownContainer"] p {
        color: var(--text) !important;
    }
    [data-baseweb="popover"],
    [data-baseweb="menu"],
    [role="listbox"] {
        background: #FFFFFF !important;
        color: var(--text) !important;
    }
    [role="option"] {
        background: #FFFFFF !important;
        color: var(--text) !important;
    }
    [role="option"]:hover,
    [role="option"][aria-selected="true"] {
        background: #EAF4FB !important;
        color: var(--text) !important;
    }
    h1, h2, h3, p, span, label, div {
        letter-spacing: 0 !important;
    }
    h1, h2, h3 {
        color: var(--text) !important;
    }
    .hero {
        background: linear-gradient(180deg, #FFFFFF 0%, #F9FAFB 100%);
        border: 1px solid var(--border);
        border-radius: 8px;
        padding: 20px 22px;
        margin-bottom: 16px;
    }
    .hero-title {
        font-size: 28px;
        line-height: 1.2;
        font-weight: 800;
        color: var(--text);
        margin: 0 0 8px 0;
    }
    .hero-subtitle {
        color: var(--muted);
        font-size: 15px;
        line-height: 1.45;
        margin: 0;
    }
    .kpi-grid {
        display: grid;
        grid-template-columns: repeat(4, minmax(180px, 1fr));
        gap: 12px;
        margin: 12px 0 18px 0;
    }
    .kpi-card {
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 8px;
        padding: 15px 16px;
        min-height: 118px;
        border-left: 5px solid var(--border);
        box-shadow: 0 1px 2px rgba(31, 41, 55, 0.04);
    }
    .kpi-card.red { border-left-color: var(--red); }
    .kpi-card.orange { border-left-color: var(--orange); }
    .kpi-card.green { border-left-color: var(--green); }
    .kpi-card.blue { border-left-color: var(--blue); }
    .kpi-label {
        color: var(--muted);
        font-size: 13px;
        font-weight: 700;
        margin-bottom: 10px;
    }
    .kpi-value {
        color: var(--text);
        font-size: 27px;
        line-height: 1.08;
        font-weight: 850;
    }
    .kpi-help {
        color: var(--muted);
        font-size: 12px;
        margin-top: 9px;
        line-height: 1.35;
    }
    .panel {
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 8px;
        padding: 16px;
        margin-bottom: 14px;
    }
    .panel-title {
        color: var(--text);
        font-size: 18px;
        font-weight: 780;
        margin-bottom: 6px;
    }
    .panel-text {
        color: var(--muted);
        font-size: 14px;
        line-height: 1.5;
    }
    .period-card {
        background: var(--panel);
        border: 1px solid var(--border);
        border-left: 5px solid var(--red);
        border-radius: 8px;
        padding: 15px;
        min-height: 150px;
    }
    .period-title {
        color: var(--text);
        font-size: 17px;
        font-weight: 800;
        margin-bottom: 8px;
    }
    .period-row {
        color: var(--muted);
        font-size: 13px;
        line-height: 1.55;
    }
    .rec-card {
        background: var(--panel);
        border: 1px solid var(--border);
        border-left: 5px solid var(--blue);
        border-radius: 8px;
        padding: 14px 15px;
        margin-bottom: 10px;
        box-shadow: 0 1px 2px rgba(31, 41, 55, 0.04);
    }
    .rec-title {
        color: var(--text);
        font-size: 17px;
        font-weight: 800;
        margin-bottom: 4px;
    }
    .rec-meta {
        color: var(--muted);
        font-size: 13px;
        line-height: 1.45;
    }
    .action-box {
        margin-top: 10px;
        background: #FEF5E7;
        border: 1px solid #FAD7A0;
        color: #7E5109;
        border-radius: 8px;
        padding: 10px 12px;
        line-height: 1.45;
    }
    .priority-row {
        display: flex;
        align-items: center;
        gap: 8px;
        flex-wrap: wrap;
        margin-bottom: 7px;
    }
    .priority-badge {
        display: inline-flex;
        align-items: center;
        border-radius: 999px;
        padding: 3px 9px;
        font-size: 12px;
        font-weight: 800;
        border: 1px solid var(--border);
    }
    .priority-high {
        background: var(--red-soft);
        color: #B42318;
        border-color: #FDA29B;
    }
    .priority-medium {
        background: var(--amber-soft);
        color: #B54708;
        border-color: #FEDF89;
    }
    .priority-low {
        background: var(--green-soft);
        color: #067647;
        border-color: #ABEFC6;
    }
    .action-card {
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 8px;
        padding: 14px 15px;
        min-height: 168px;
        box-shadow: 0 1px 2px rgba(31, 41, 55, 0.04);
    }
    .action-title {
        font-size: 17px;
        font-weight: 850;
        color: var(--text);
        margin-bottom: 6px;
    }
    .action-meta {
        color: var(--muted);
        font-size: 13px;
        line-height: 1.45;
    }
    .impact-note {
        margin-top: 8px;
        color: #175CD3;
        background: var(--blue-soft);
        border: 1px solid #B2DDFF;
        border-radius: 6px;
        padding: 8px 10px;
        font-size: 13px;
        line-height: 1.4;
    }
    .segment-card {
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 8px;
        padding: 16px;
        margin-bottom: 12px;
    }
    .segment-title {
        color: var(--text);
        font-size: 20px;
        font-weight: 850;
        margin-bottom: 12px;
    }
    .metric-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(130px, 1fr));
        gap: 8px;
    }
    .mini-metric {
        background: var(--panel-soft);
        border: 1px solid var(--border);
        border-radius: 6px;
        padding: 9px 10px;
    }
    .mini-label {
        color: var(--muted);
        font-size: 12px;
        margin-bottom: 3px;
    }
    .mini-value {
        color: var(--text);
        font-size: 16px;
        font-weight: 800;
    }
    div[data-testid="stDataFrame"] {
        background: var(--panel);
    }
    @media (max-width: 1100px) {
        .kpi-grid { grid-template-columns: repeat(2, minmax(180px, 1fr)); }
        .metric-grid { grid-template-columns: 1fr; }
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def kpi_card(label: str, value: str, help_text: str, color: str) -> str:
    return (
        f'<div class="kpi-card {color}">'
        f'<div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{value}</div>'
        f'<div class="kpi-help">{help_text}</div>'
        f"</div>"
    )


def priority_label(row: pd.Series) -> str:
    pct = float(row.get("underload_pct", 0))
    gap = float(row.get("load_gap", 0))
    if pct >= 30 or gap >= 120:
        return "Высокий"
    if pct >= 15 or gap >= 60:
        return "Средний"
    return "Низкий"


def priority_class(label: str) -> str:
    return {
        "Высокий": "priority-high",
        "Средний": "priority-medium",
        "Низкий": "priority-low",
    }.get(label, "priority-low")


def add_business_columns(df: pd.DataFrame, df_segments: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    result = df.copy()
    result["priority"] = result.apply(priority_label, axis=1)
    result["risk_weight"] = result["load_gap"] * result["underload_pct"]
    result["campaign_status"] = "Не запущено"
    return result


def panel(title: str, text: str) -> None:
    st.markdown(
        f'<div class="panel"><div class="panel-title">{title}</div><div class="panel-text">{text}</div></div>',
        unsafe_allow_html=True,
    )


def period_card(index: int, row: pd.Series) -> str:
    return (
        '<div class="period-card">'
        f'<div class="period-title">Период {index}</div>'
        f'<div class="period-row"><b>{row["period_start"].date()} — {row["period_end"].date()}</b></div>'
        f'<div class="period-row">Дней риска: {int(row["n_days"])}</div>'
        f'<div class="period-row">Средний дефицит: −{format_int(row["avg_gap"])} броней/день</div>'
        f'<div class="period-row">Макс. дефицит: −{format_int(row["max_gap"])}</div>'
        f'<div class="period-row">Сезон: {season_label(row["season"])}</div>'
        "</div>"
    )


def recommendation_card(row: pd.Series) -> None:
    priority = row.get("priority", priority_label(row))
    st.markdown(
        (
            '<div class="rec-card">'
            '<div class="priority-row">'
            f'<span class="priority-badge {priority_class(priority)}">{priority}</span>'
            f'<span class="rec-meta">{row["date"].date()} · {season_label(row["season"])}</span>'
            '</div>'
            f'<div class="rec-title">{segment_label(row["top_segment_name"])}</div>'
            f'<div class="rec-meta">Дефицит: −{format_int(row["load_gap"])} · '
            f'Прогноз: {format_int(row["forecast_load"])} из {format_int(row["target_load"])} · '
            f'Ниже порога: {row["underload_pct"]:.1f}%</div>'
            f'<div class="rec-meta">Также подходят: {segment_list_label(row["top_3_segment_names"])}</div>'
            f'<div class="rec-meta"><b>Почему:</b> {row["recommendation_reason"]}</div>'
            f'<div class="action-box"><b>Действие:</b> {row["suggested_action"]}</div>'
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def action_card(row: pd.Series) -> str:
    priority = row.get("priority", priority_label(row))
    return (
        '<div class="action-card">'
        '<div class="priority-row">'
        f'<span class="priority-badge {priority_class(priority)}">{priority}</span>'
        f'<span class="action-meta">{row["date"].date()} · {season_label(row["season"])}</span>'
        '</div>'
        f'<div class="action-title">{segment_label(row["top_segment_name"])}</div>'
        f'<div class="action-meta">Дефицит: −{format_int(row["load_gap"])} броней/день · '
        f'ниже порога на {row["underload_pct"]:.1f}%</div>'
        f'<div class="impact-note">Операционный масштаб: <b>{format_int(row["load_gap"])} недостающих бронирований/день</b></div>'
        f'<div class="action-box"><b>Запустить:</b> {row["suggested_action"]}</div>'
        '</div>'
    )


def risk_style(row: pd.Series) -> list[str]:
    pct = row.get("Ниже цели, %", 0)
    if pct >= 30:
        color = "background-color: #FADBD8; color: #1F2937"
    elif pct >= 10:
        color = "background-color: #FDEBD0; color: #1F2937"
    else:
        color = "background-color: #E8F8F0; color: #1F2937"
    return [color for _ in row]


def recommendations_table(df: pd.DataFrame) -> pd.DataFrame:
    table = df.copy()
    table["Дата"] = table["date"].dt.date
    table["Сезон"] = table["season"].map(season_label)
    table["Прогноз"] = table["forecast_load"].round(0).astype(int)
    table["Цель"] = table["target_load"].round(0).astype(int)
    table["Дефицит"] = -table["load_gap"].round(0).astype(int)
    table["Ниже цели, %"] = table["underload_pct"].round(1)
    table["Приоритет"] = table["priority"] if "priority" in table.columns else table.apply(priority_label, axis=1)
    table["Статус кампании"] = table["campaign_status"] if "campaign_status" in table.columns else "Не запущено"
    table["Сегмент"] = table["top_segment_name"].map(segment_label)
    table["Действие"] = table["suggested_action"]
    return table[["Дата", "Сезон", "Приоритет", "Прогноз", "Цель", "Дефицит", "Ниже цели, %", "Сегмент", "Действие", "Статус кампании"]]


def underload_table(df: pd.DataFrame) -> pd.DataFrame:
    table = df.copy()
    table["Дата"] = table["ds"].dt.date
    table["Сезон"] = table["season"].map(season_label)
    table["Прогноз"] = table["forecast_load"].round(0).astype(int)
    table["Цель"] = table["target_load"].round(0).astype(int)
    table["Дефицит"] = -table["load_gap"].round(0).astype(int)
    table["Ниже цели, %"] = table["underload_pct"].round(1)
    return table[["Дата", "Сезон", "Прогноз", "Цель", "Дефицит", "Ниже цели, %"]]


def align_recommendations_with_risk(df_recs: pd.DataFrame, df_future: pd.DataFrame) -> pd.DataFrame:
    risk_context = df_future[["ds", "target_load", "load_gap", "underload_pct", "is_risk"]].rename(
        columns={"ds": "date"}
    )
    aligned = df_recs.drop(columns=["target_load", "load_gap", "underload_pct"], errors="ignore").merge(
        risk_context,
        on="date",
        how="inner",
    )
    return aligned[aligned["is_risk"]].drop(columns=["is_risk"])


def build_underload_periods(df_future: pd.DataFrame) -> pd.DataFrame:
    underload = df_future[df_future["is_risk"]].sort_values("ds").copy()
    if underload.empty:
        return pd.DataFrame(columns=["period_start", "period_end", "n_days", "avg_gap", "max_gap", "season"])

    underload["day_diff"] = underload["ds"].diff().dt.days.fillna(1)
    underload["period_id"] = (underload["day_diff"] > 2).cumsum()
    periods = (
        underload.groupby("period_id", as_index=False)
        .agg(
            period_start=("ds", "min"),
            period_end=("ds", "max"),
            n_days=("ds", "count"),
            avg_gap=("load_gap", "mean"),
            max_gap=("load_gap", "max"),
            season=("season", lambda values: values.mode().iloc[0]),
        )
        .sort_values(["period_start", "period_end"])
    )
    return periods


missing = validate_artifacts()
if missing:
    st.error("Не найдены обязательные артефакты. Сначала выполните notebooks forecasting и recommender.")
    st.dataframe(
        pd.DataFrame([{"Артефакт": name, "Путь": str(path)} for name, path in missing.items()]),
        use_container_width=True,
        hide_index=True,
    )
    st.stop()


data = load_data()
df_daily = data["daily"]
df_forecast = data["forecast"]
df_underload_raw = data["underload"]
df_periods = data["periods"]
df_segments = data["segments"]
df_affinity = data["affinity"]
df_recs = data["recommendations"]
baseline_metrics = data["baseline_metrics"]
model_comparison = data["model_comparison"]

history_start = df_daily["ds"].min()
history_end = df_daily["ds"].max()
target_loads = seasonal_target_loads(df_daily)
df_future = add_risk_columns(future_frame(df_forecast, history_end), target_loads)
df_periods = build_underload_periods(df_future)
df_recs = align_recommendations_with_risk(df_recs, df_future)
target_load = float(df_future["target_load"].median())
forecast_start = df_future["ds"].min()
forecast_end = df_future["ds"].max()
today = pd.Timestamp(date.today())
default_start = max(forecast_start, today) if today <= forecast_end else forecast_start


with st.sidebar:
    st.markdown("## Навигация")
    page = st.radio(
        "Раздел",
        ["Обзор", "Периоды риска", "Сегменты клиентов", "Методология"],
        label_visibility="collapsed",
    )

    st.markdown("## Фильтры")
    quick_period = st.selectbox(
        "Быстрый период",
        ["От сегодня до конца прогноза", "Ближайшие 30 дней", "Ближайшие 60 дней", "Ближайшие 90 дней", "Весь прогноз"],
    )
    if quick_period == "Ближайшие 30 дней":
        preset_start = default_start
        preset_end = min(default_start + pd.Timedelta(days=29), forecast_end)
    elif quick_period == "Ближайшие 60 дней":
        preset_start = default_start
        preset_end = min(default_start + pd.Timedelta(days=59), forecast_end)
    elif quick_period == "Ближайшие 90 дней":
        preset_start = default_start
        preset_end = min(default_start + pd.Timedelta(days=89), forecast_end)
    elif quick_period == "Весь прогноз":
        preset_start = forecast_start
        preset_end = forecast_end
    else:
        preset_start = default_start
        preset_end = forecast_end

    selected_range = st.date_input(
        "Период",
        value=(preset_start.date(), preset_end.date()),
        min_value=forecast_start.date(),
        max_value=forecast_end.date(),
    )
    if isinstance(selected_range, tuple) and len(selected_range) == 2:
        date_start, date_end = selected_range
    else:
        date_start, date_end = forecast_start.date(), forecast_end.date()

    seasons = ["autumn", "winter", "spring", "summer"]
    selected_seasons = st.multiselect(
        "Сезоны",
        seasons,
        default=seasons,
        format_func=season_label,
    )
    if not selected_seasons:
        selected_seasons = seasons


filtered_future = filter_date_range(df_future, "ds", date_start, date_end)
filtered_future = filtered_future[filtered_future["season"].isin(selected_seasons)].copy()
filtered_recs = filter_date_range(df_recs, "date", date_start, date_end)
filtered_recs = filtered_recs[filtered_recs["season"].isin(selected_seasons)].copy()
filtered_recs = add_business_columns(filtered_recs, df_segments)
filtered_underload = filtered_future[filtered_future["is_risk"]].copy()
filtered_periods = build_underload_periods(filtered_future)

st.markdown(
    """
    <div class="hero">
        <div class="hero-title">Активные бронирования санатория</div>
        <p class="hero-subtitle">
            Рабочий дашборд для менеджера: где ожидается провал активных бронирований, насколько он критичен,
            какой клиентский сегмент привлекать и какое действие запускать.
        </p>
    </div>
    """,
    unsafe_allow_html=True,
)

risk_days = int(filtered_future["is_risk"].sum())
total_days = int(len(filtered_future))
avg_gap = float(filtered_underload["load_gap"].mean()) if not filtered_underload.empty else 0
first_risk = filtered_underload["ds"].min().date() if not filtered_underload.empty else "нет"
total_gap = float(filtered_underload["load_gap"].sum()) if not filtered_underload.empty else 0

kpis = [
    kpi_card("Дней риска", f"{risk_days} из {total_days}", "Количество дней ниже бизнес-порога активных бронирований", "red"),
    kpi_card("Первый прогнозный риск", str(first_risk), "Ближайшая дата риска в выбранном периоде", "red"),
    kpi_card("Средний дефицит", f"−{format_int(avg_gap)}", "Средний разрыв до цели в рискованные дни", "orange"),
    kpi_card("Суммарный дефицит", f"−{format_int(total_gap)}", "Сумма недостающих бронирований по выбранному периоду", "blue"),
]
st.markdown(f'<div class="kpi-grid">{"".join(kpis)}</div>', unsafe_allow_html=True)


if page == "Обзор":
    st.subheader("Приоритет")
    priority_rank = {"Высокий": 0, "Средний": 1, "Низкий": 2}
    top_actions = filtered_recs.assign(
        priority_rank=filtered_recs["priority"].map(priority_rank).fillna(3)
    ).sort_values(
        ["priority_rank", "risk_weight", "load_gap", "date"],
        ascending=[True, False, False, True],
    ).head(5)
    if top_actions.empty:
        st.success("В выбранном периоде нет действий для запуска.")
    else:
        action_cols = st.columns(min(3, len(top_actions)))
        for idx, (_, row) in enumerate(top_actions.iterrows()):
            with action_cols[idx % len(action_cols)]:
                st.markdown(action_card(row), unsafe_allow_html=True)
                st.selectbox(
                    "Статус кампании",
                    STATUS_OPTIONS,
                    key=f"status_{row['date'].date()}_{row['top_segment_id']}",
                    label_visibility="collapsed",
                )

    st.subheader("Лента прогноза")
    st.plotly_chart(
        forecast_ribbon_chart(df_daily, df_future, filtered_periods, target_load),
        use_container_width=True,
        theme=None,
    )

    left, right = st.columns([1.45, 1])
    with left:
        st.subheader("Ближайшие 30 дней от начала фильтра")
        st.plotly_chart(
            next_30_days_chart(df_future, pd.to_datetime(date_start), target_load),
            use_container_width=True,
            theme=None,
        )
    with right:
        st.subheader("Топ-3 ближайших дефицита")
        top_near = filtered_underload.sort_values(["ds", "load_gap"], ascending=[True, False]).head(30)
        top_near = top_near.sort_values("load_gap", ascending=False).head(3)
        if top_near.empty:
            st.success("В выбранном периоде нет дат ниже цели.")
        else:
            st.dataframe(
                underload_table(top_near).style.apply(risk_style, axis=1),
                use_container_width=True,
                hide_index=True,
            )


elif page == "Периоды риска":
    panel(
        "Какие периоды самые проблемные",
        "Периоды собраны из последовательных дат, где прогноз активных бронирований ниже бизнес-порога. "
        "Чем выше дефицит и длиннее период, тем раньше нужно запускать действие.",
    )

    if filtered_periods.empty:
        st.success("По текущим фильтрам нет периодов риска.")
    else:
        period_html = "".join(period_card(i + 1, row) for i, (_, row) in enumerate(filtered_periods.iterrows()))
        st.markdown(f'<div class="kpi-grid">{period_html}</div>', unsafe_allow_html=True)

    st.subheader("Тепловая карта риска")
    if filtered_future.empty:
        st.warning("По текущим фильтрам нет данных для тепловой карты.")
    else:
        st.plotly_chart(risk_heatmap(filtered_future), use_container_width=True, theme=None)

    st.subheader("Детальная таблица дат риска")
    risk_view = underload_table(filtered_underload.sort_values("load_gap", ascending=False))
    st.dataframe(risk_view.style.apply(risk_style, axis=1), use_container_width=True, hide_index=True)
    st.download_button(
        "Скачать даты риска CSV",
        data=csv_bytes(risk_view),
        file_name="underload_dates.csv",
        mime="text/csv",
    )


elif page == "Сегменты клиентов":
    panel(
        "Кого привлекать и что делать",
        "Этот экран связывает риск провала активных бронирований с клиентскими сегментами. "
        "Слева — профиль выбранного сегмента, справа — конкретные рекомендации по датам риска.",
    )

    left, right = st.columns([0.9, 1.15])
    with left:
        st.subheader("Профиль сегмента")
        segment_options = df_segments["segment_name"].tolist()
        selected_segment = st.selectbox(
            "Выберите сегмент",
            segment_options,
            format_func=segment_label,
        )
        seg = df_segments[df_segments["segment_name"] == selected_segment].iloc[0]
        affinity = df_affinity.loc[int(seg["cluster_v4"])].sort_values(ascending=False)
        active_seasons = ", ".join(season_label(season) for season in affinity.head(2).index)

        st.markdown(
            (
                '<div class="segment-card">'
                f'<div class="segment-title">{segment_label(selected_segment)}</div>'
                '<div class="metric-grid">'
                f'<div class="mini-metric"><div class="mini-label">Бронирований</div><div class="mini-value">{format_int(seg["total_bookings"])}</div></div>'
                f'<div class="mini-metric"><div class="mini-label">Доля</div><div class="mini-value">{seg["pct_of_total"]:.1f}%</div></div>'
                f'<div class="mini-metric"><div class="mini-label">Средняя длительность</div><div class="mini-value">{seg["avg_nights"]:.1f} дн.</div></div>'
                f'<div class="mini-metric"><div class="mini-label">Ценовой уровень</div><div class="mini-value">{seg["avg_price_tier"]:.2f}</div></div>'
                f'<div class="mini-metric"><div class="mini-label">С детьми</div><div class="mini-value">{seg["pct_with_children"]:.1%}</div></div>'
                f'<div class="mini-metric"><div class="mini-label">С лечением</div><div class="mini-value">{seg["pct_treatment"]:.1%}</div></div>'
                f'<div class="mini-metric"><div class="mini-label">Средний возраст</div><div class="mini-value">{seg["avg_age"]:.0f} лет</div></div>'
                f'<div class="mini-metric"><div class="mini-label">Активные сезоны</div><div class="mini-value">{active_seasons}</div></div>'
                "</div></div>"
            ),
            unsafe_allow_html=True,
        )
        st.plotly_chart(segment_bookings_chart(df_segments), use_container_width=True, theme=None)

    with right:
        st.subheader("Рекомендации по датам риска")
        segment_filter = st.selectbox(
            "Показать рекомендации",
            ["Все сегменты"] + sorted(filtered_recs["top_segment_name"].unique().tolist()),
            format_func=lambda value: value if value == "Все сегменты" else segment_label(value),
        )
        recs_view = filtered_recs.copy()
        if segment_filter != "Все сегменты":
            recs_view = recs_view[recs_view["top_segment_name"] == segment_filter]

        if recs_view.empty:
            st.warning("По текущим фильтрам рекомендаций нет.")
        else:
            st.dataframe(
                recommendations_table(recs_view.sort_values("load_gap", ascending=False)).head(20).style.apply(risk_style, axis=1),
                use_container_width=True,
                hide_index=True,
            )
            st.download_button(
                "Скачать рекомендации CSV",
                data=csv_bytes(recommendations_table(recs_view.sort_values("load_gap", ascending=False))),
                file_name="recommendations.csv",
                mime="text/csv",
            )

            st.subheader("Детали ближайших рекомендаций")
            for _, row in recs_view.sort_values(["date", "load_gap"], ascending=[True, False]).head(5).iterrows():
                recommendation_card(row)

    st.subheader("Какой сегмент закрывает какой месяц")
    if filtered_recs.empty:
        st.info("По текущим фильтрам нет дат риска с рекомендациями.")
    else:
        st.plotly_chart(segment_month_chart(filtered_recs), use_container_width=True, theme=None)


elif page == "Методология":
    panel(
        "Как это работает",
        "Прогноз построен моделью Prophet на исторических данных 2022–2025. "
        "Горизонт прогноза — 365 дней после даты выгрузки. Риск считается как прогноз активных бронирований ниже бизнес-порога загрузки.",
    )

    c1, c2, c3 = st.columns(3)
    c1.info(f"Данные актуальны на: {history_end.date()}")
    c2.info(f"Прогноз: {forecast_start.date()} — {forecast_end.date()}")
    c3.info("Сегментация: KMeans v4, 6 сегментов")

    with st.expander("Метрики качества модели", expanded=False):
        selected_model = model_comparison.get("selected_model", baseline_metrics["model"])
        selected_metrics = model_comparison.get(selected_model, baseline_metrics)
        metrics_df = pd.DataFrame(
            [
                {
                    "Модель": selected_model,
                    "MAE": selected_metrics["mae"],
                    "RMSE": selected_metrics["rmse"],
                    "MAPE, %": selected_metrics["mape_pct"],
                }
            ]
        )
        st.dataframe(metrics_df, use_container_width=True, hide_index=True)
        st.plotly_chart(model_metrics_chart(model_comparison), use_container_width=True, theme=None)

    with st.expander("Как считается риск", expanded=True):
        st.markdown(
            f"""
            - Номерной фонд оценивается по каждому году как **99-й перцентиль активных бронирований/день**.
            - Модель прогнозирует `load_rate = active_bookings / capacity_est`, а затем переводит прогноз обратно в активные бронирования.
            - Порог считается как **max(75-й перцентиль сезонного load_rate, 50-й перцентиль общего load_rate)**, умноженный на емкость года.
            - Это не историческая "норма", а минимальный бизнес-ориентир для запуска действий в низкий сезон.
            - Текущая медиана порогов: **{format_int(target_load)} бронирований/день**.
            - Дата считается рискованной, если `forecast_load < target_load`.
            - Дефицит считается как `target_load - forecast_load`.
            - Сейчас расчет использует прогноз активных бронирований. Для промышленного сценария следующим шагом нужно добавить `booked_load`,
              то есть уже известные будущие активные бронирования.
            """
        )

    with st.expander("Описание клиентских сегментов", expanded=False):
        seg_table = df_segments.copy()
        seg_table["Сегмент"] = seg_table["segment_name"].map(segment_label)
        seg_table = seg_table.rename(
            columns={
                "cluster_v4": "ID",
                "total_bookings": "Бронирований",
                "pct_of_total": "Доля, %",
                "avg_nights": "Сред. ночей",
                "avg_price_tier": "Ценовой уровень",
                "pct_with_children": "С детьми",
                "pct_treatment": "С лечением",
                "avg_age": "Возраст",
            }
        )
        st.dataframe(
            seg_table[
                ["ID", "Сегмент", "Бронирований", "Доля, %", "Сред. ночей", "Ценовой уровень", "С детьми", "С лечением", "Возраст"]
            ],
            use_container_width=True,
            hide_index=True,
        )

st.caption(
    "Версия использует прогноз активных бронирований. Для операционного использования следующий шаг — добавить booked_load по уже известным будущим броням."
)
