import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

from data import season_label, segment_short_label


COLORS = {
    "green": "#2ECC71",
    "orange": "#F39C12",
    "red": "#E74C3C",
    "blue": "#3498DB",
    "light_blue": "#AED6F1",
    "gray": "#95A5A6",
    "dark_red": "#C0392B",
    "text": "#1F2937",
    "muted": "#6B7280",
    "grid": "#E5E7EB",
}

SEGMENT_COLORS = {
    "C0 Летние семьи": "#3498DB",
    "C1 Выходные": "#9B59B6",
    "C2 Будни": "#16A085",
    "C3 Зимние семьи": "#E74C3C",
    "C4 Группы": "#F39C12",
    "C5 Оздоровление": "#2ECC71",
}


def base_layout(fig: go.Figure, height: int = 440) -> go.Figure:
    fig.update_layout(
        template="plotly_white",
        height=height,
        margin=dict(l=16, r=16, t=32, b=24),
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FFFFFF",
        font=dict(color=COLORS["text"], family="Arial"),
        legend=dict(orientation="h", y=1.08, x=0),
        hovermode="closest",
    )
    fig.update_xaxes(gridcolor=COLORS["grid"], zeroline=False)
    fig.update_yaxes(gridcolor=COLORS["grid"], zeroline=False)
    return fig


def forecast_ribbon_chart(
    df_daily: pd.DataFrame,
    df_future: pd.DataFrame,
    df_periods: pd.DataFrame,
    target_load: float,
) -> go.Figure:
    fig = go.Figure()

    history_start = df_daily["ds"].min()
    history_end = df_daily["ds"].max()
    future_end = df_future["ds"].max()

    fig.add_vrect(
        x0=history_start,
        x1=history_end,
        fillcolor="rgba(149,165,166,0.12)",
        line_width=0,
        annotation_text="История",
        annotation_position="top left",
    )
    for _, row in df_periods.iterrows():
        fig.add_vrect(
            x0=row["period_start"],
            x1=row["period_end"],
            fillcolor="rgba(231,76,60,0.12)",
            line_width=0,
        )

    fig.add_trace(
        go.Scatter(
            x=df_daily["ds"],
            y=df_daily["y"],
            name="История",
            mode="lines",
            line=dict(color=COLORS["gray"], width=1.2),
            hovertemplate="Дата: %{x|%Y-%m-%d}<br>История: %{y:.0f}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=pd.concat([df_future["ds"], df_future["ds"][::-1]]),
            y=pd.concat([df_future["forecast_upper"], df_future["forecast_lower"][::-1]]),
            fill="toself",
            fillcolor="rgba(174,214,241,0.45)",
            line=dict(color="rgba(255,255,255,0)"),
            hoverinfo="skip",
            name="Коридор прогноза",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df_future["ds"],
            y=df_future["forecast_load"],
            name="Прогноз",
            mode="lines",
            line=dict(color=COLORS["blue"], width=2.4),
            hovertemplate="Дата: %{x|%Y-%m-%d}<br>Прогноз: %{y:.0f}<extra></extra>",
        )
    )
    risk = df_future[df_future["is_risk"]]
    fig.add_trace(
        go.Scatter(
            x=risk["ds"],
            y=risk["forecast_load"],
            name="Даты риска",
            mode="markers",
            marker=dict(color=COLORS["red"], size=6),
            customdata=risk[["target_load", "load_gap", "underload_pct"]],
            hovertemplate=(
                "Дата: %{x|%Y-%m-%d}"
                "<br>Прогноз: %{y:.0f}"
                "<br>Цель: %{customdata[0]:.0f}"
                "<br>Дефицит: −%{customdata[1]:.0f}"
                "<br>Ниже цели: %{customdata[2]:.1f}%"
                "<extra></extra>"
            ),
        )
    )
    if "target_load" in df_future.columns:
        fig.add_trace(
            go.Scatter(
                x=df_future["ds"],
                y=df_future["target_load"],
                name="Бизнес-порог",
                mode="lines",
                line=dict(color=COLORS["dark_red"], width=1.8, dash="dash"),
                hovertemplate="Дата: %{x|%Y-%m-%d}<br>Цель: %{y:.0f}<extra></extra>",
            )
        )
    else:
        fig.add_hline(
            y=target_load,
            line_dash="dash",
            line_color=COLORS["dark_red"],
            annotation_text=f"Порог: {target_load:.0f}",
            annotation_position="top left",
        )
    fig.add_shape(
        type="line",
        x0=history_end,
        x1=history_end,
        y0=0,
        y1=1,
        xref="x",
        yref="paper",
        line=dict(color=COLORS["muted"], dash="dot", width=1.5),
    )
    fig.add_annotation(
        x=history_end,
        y=1,
        xref="x",
        yref="paper",
        text="Дата выгрузки",
        showarrow=False,
        yshift=16,
        font=dict(color=COLORS["muted"], size=11),
    )
    fig.update_xaxes(range=[history_start, future_end])
    fig.update_yaxes(title="Бронирований / день")
    return base_layout(fig, height=500)


def next_30_days_chart(df_future: pd.DataFrame, start_date: pd.Timestamp, target_load: float) -> go.Figure:
    end_date = pd.to_datetime(start_date) + pd.Timedelta(days=29)
    plot_df = df_future[(df_future["ds"] >= pd.to_datetime(start_date)) & (df_future["ds"] <= end_date)].copy()
    plot_df["date_label"] = plot_df["ds"].dt.strftime("%d.%m")
    plot_df["status"] = plot_df["is_risk"].map({True: "Риск", False: "Норма"})
    plot_df["color"] = plot_df["is_risk"].map({True: COLORS["red"], False: COLORS["green"]})

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=plot_df["forecast_load"],
            y=plot_df["date_label"],
            orientation="h",
            marker_color=plot_df["color"],
            text=plot_df["forecast_load"].round(0),
            textposition="outside",
            hovertemplate="Дата: %{y}<br>Прогноз: %{x:.0f}<extra></extra>",
            name="Прогноз",
        )
    )
    if "target_load" in plot_df.columns:
        fig.add_trace(
            go.Scatter(
                x=plot_df["target_load"],
                y=plot_df["date_label"],
                mode="markers",
                marker=dict(color=COLORS["dark_red"], size=8, symbol="diamond"),
                name="Бизнес-порог",
                hovertemplate="Дата: %{y}<br>Цель: %{x:.0f}<extra></extra>",
            )
        )
    else:
        fig.add_vline(x=target_load, line_dash="dash", line_color=COLORS["dark_red"])
    fig.update_yaxes(autorange="reversed", title=None)
    fig.update_xaxes(title="Прогноз активных бронирований")
    return base_layout(fig, height=520)


def risk_heatmap(df_future: pd.DataFrame) -> go.Figure:
    plot_df = df_future.copy()
    plot_df["month_label"] = plot_df["ds"].dt.strftime("%Y-%m")
    plot_df["day"] = plot_df["ds"].dt.day
    plot_df["risk_value"] = plot_df["underload_pct"].where(plot_df["is_risk"], 0)
    plot_df["hover"] = (
        plot_df["ds"].dt.strftime("%Y-%m-%d")
        + "<br>Прогноз: "
        + plot_df["forecast_load"].round(0).astype(int).astype(str)
        + "<br>Дефицит: "
        + plot_df["load_gap"].round(0).astype(int).astype(str)
        + "<br>Ниже цели: "
        + plot_df["underload_pct"].round(1).astype(str)
        + "%"
    )

    pivot = plot_df.pivot_table(index="month_label", columns="day", values="risk_value", aggfunc="max")
    hover = plot_df.pivot_table(index="month_label", columns="day", values="hover", aggfunc="first")
    pivot = pivot.reindex(sorted(pivot.index))
    hover = hover.reindex(pivot.index)

    fig = go.Figure(
        data=go.Heatmap(
            z=pivot.values,
            x=pivot.columns,
            y=pivot.index,
            text=hover.values,
            hovertemplate="%{text}<extra></extra>",
            colorscale=[
                [0.0, "#E8F8F0"],
                [0.35, "#FDEBD0"],
                [1.0, "#FADBD8"],
            ],
            colorbar=dict(title="% ниже цели"),
            zmin=0,
            zmax=max(50, float(plot_df["underload_pct"].max())),
        )
    )
    fig.update_xaxes(title="День месяца", dtick=1)
    fig.update_yaxes(title=None, autorange="reversed")
    return base_layout(fig, height=520)


def segment_month_chart(df_recs: pd.DataFrame) -> go.Figure:
    plot_df = df_recs.copy()
    plot_df["month_label"] = plot_df["date"].dt.strftime("%Y-%m")
    plot_df["segment"] = plot_df["top_segment_name"].map(segment_short_label)
    grouped = plot_df.groupby(["month_label", "segment"]).size().reset_index(name="days")

    fig = px.bar(
        grouped,
        x="month_label",
        y="days",
        color="segment",
        labels={"month_label": "Месяц", "days": "Дней риска", "segment": "Сегмент"},
        color_discrete_map=SEGMENT_COLORS,
    )
    fig.update_layout(barmode="stack")
    return base_layout(fig, height=420)


def segment_bookings_chart(df_segments: pd.DataFrame) -> go.Figure:
    plot_df = df_segments.copy()
    plot_df["segment"] = plot_df["segment_name"].map(segment_short_label)
    plot_df = plot_df.sort_values("total_bookings")
    colors = [SEGMENT_COLORS.get(name, COLORS["blue"]) for name in plot_df["segment"]]

    fig = go.Figure(
        go.Bar(
            x=plot_df["total_bookings"],
            y=plot_df["segment"],
            orientation="h",
            marker_color=colors,
            text=plot_df["total_bookings"],
            textposition="outside",
        )
    )
    fig.update_xaxes(title="Бронирований")
    fig.update_yaxes(title=None)
    return base_layout(fig, height=360)


def model_metrics_chart(model_comparison: dict) -> go.Figure:
    rows = [
        {"Модель": model, "MAPE": values["mape_pct"], "MAE": values["mae"], "RMSE": values["rmse"]}
        for model, values in model_comparison.items()
    ]
    df = pd.DataFrame(rows).sort_values("MAPE")
    fig = px.bar(
        df,
        x="MAPE",
        y="Модель",
        orientation="h",
        text="MAPE",
        color="MAPE",
        color_continuous_scale=["#2ECC71", "#F39C12", "#E74C3C"],
    )
    fig.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
    fig.update_layout(coloraxis_showscale=False)
    return base_layout(fig, height=340)
