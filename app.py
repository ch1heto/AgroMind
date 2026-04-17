from __future__ import annotations

from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st

from agromind.config import DEFAULT_HISTORY_DAYS
from agromind.database import init_db
from agromind.services import (
    get_crop_filters,
    get_latest_prices_frame,
    get_price_history_frame,
    get_recent_news,
    refresh_data,
)


fragment_decorator = getattr(st, "fragment", getattr(st, "experimental_fragment", None))
if fragment_decorator is None:  # pragma: no cover
    def fragment_decorator(*args, **kwargs):
        def wrapper(func):
            return func
        return wrapper


st.set_page_config(
    page_title="AgroMind Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
        html, body, [data-testid="stAppViewContainer"] {
            color-scheme: dark;
        }
        [data-testid="stAppViewContainer"] {
            background:
                radial-gradient(circle at top left, rgba(46, 125, 50, 0.18), transparent 28%),
                radial-gradient(circle at bottom right, rgba(27, 94, 32, 0.16), transparent 24%),
                #0f1116;
        }
        [data-testid="stHeader"] {
            background: rgba(15, 17, 22, 0.65);
        }
        .news-card {
            background: rgba(255, 255, 255, 0.03);
            border: 1px solid rgba(255, 255, 255, 0.08);
            border-radius: 14px;
            padding: 14px 16px;
            margin-bottom: 12px;
        }
        .news-card a {
            color: #98f5a6;
            text-decoration: none;
            font-weight: 600;
        }
        .news-meta {
            color: #b7c0c8;
            font-size: 0.9rem;
            margin-top: 8px;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


def render_news_feed(news_items: list[dict[str, object]]) -> None:
    if not news_items:
        st.info("В SQLite пока нет новостей. Запустите обновление вручную или дождитесь следующего цикла.")
        return

    for item in news_items:
        published_at = item["published_at"].strftime("%Y-%m-%d %H:%M:%S")
        st.markdown(
            f"""
            <div class="news-card">
                <a href="{item["url"]}" target="_blank">{item["title"]}</a>
                <div class="news-meta">Дата публикации: {published_at}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def build_price_figure(history_frame: pd.DataFrame):
    chart_frame = history_frame.copy()
    chart_frame["series"] = chart_frame["crop_name"] + " | " + chart_frame["region"]

    figure = px.line(
        chart_frame,
        x="timestamp",
        y="wholesale_price",
        color="series",
        markers=True,
        title="Динамика оптовых цен",
        labels={
            "timestamp": "Дата",
            "wholesale_price": "Цена, руб.",
            "series": "Культура и регион",
        },
        hover_data={
            "crop_name": True,
            "region": True,
            "wholesale_price": ":.2f",
            "timestamp": True,
            "series": False,
        },
    )
    figure.update_layout(
        template="plotly_dark",
        legend_title_text="Культура и регион",
        margin=dict(l=20, r=20, t=60, b=20),
        hovermode="x unified",
    )
    return figure


@fragment_decorator(run_every="30s")
def render_dashboard_tabs() -> None:
    try:
        crop_options = get_crop_filters()
        news_items = get_recent_news()
        latest_prices = get_latest_prices_frame()
    except Exception as exc:
        st.error(f"Не удалось прочитать данные из SQLite: {exc}")
        return

    if "selected_crops" not in st.session_state:
        st.session_state.selected_crops = crop_options[:3]
    if "history_days" not in st.session_state:
        st.session_state.history_days = DEFAULT_HISTORY_DAYS

    st.session_state.selected_crops = [
        crop_name for crop_name in st.session_state.selected_crops if crop_name in crop_options
    ]
    if not st.session_state.selected_crops and crop_options:
        st.session_state.selected_crops = crop_options[:3]

    tab1, tab2 = st.tabs(["Сводка и Новости", "Аналитика и Графики"])

    with tab1:
        col1, col2 = st.columns((1.1, 1), gap="large")

        with col1:
            st.subheader("Актуальный ценовой срез")
            if latest_prices.empty:
                st.info("Свежий ценовой срез пока пуст.")
            else:
                summary_frame = latest_prices.rename(
                    columns={
                        "crop_name": "Культура",
                        "region": "Регион",
                        "published_at": "Дата публикации",
                        "wholesale_price": "Оптовая цена, руб.",
                    }
                )
                st.dataframe(summary_frame, use_container_width=True, hide_index=True)

        with col2:
            st.subheader("Свежие новости")
            render_news_feed(news_items)

    with tab2:
        st.subheader("Аналитика и Графики")

        controls_col1, controls_col2 = st.columns((1.3, 1), gap="large")
        with controls_col1:
            if "selected_crops_filter" not in st.session_state:
                st.session_state["selected_crops_filter"] = crop_options[:3] if crop_options else []

            selected_crops = st.multiselect(
                "Культуры для сравнения",
                options=crop_options,
                key="selected_crops_filter",
                help="Можно сравнить одну культуру в разных регионах, например базилик в Москве и Самаре.",
            )
        with controls_col2:
            history_days = st.slider(
                "Глубина истории, дней",
                min_value=7,
                max_value=180,
                value=st.session_state.history_days,
                key="history_days",
            )

        try:
            history_frame = get_price_history_frame(
                days=history_days,
                crop_names=selected_crops or None,
            )
            analytics_snapshot = get_latest_prices_frame(crop_names=selected_crops or None)
        except Exception as exc:
            st.error(f"Не удалось прочитать аналитические данные из SQLite: {exc}")
            return

        metrics_col1, metrics_col2, metrics_col3 = st.columns((1, 1, 1.2))
        with metrics_col1:
            st.metric("Активных культур", int(analytics_snapshot["crop_name"].nunique()) if not analytics_snapshot.empty else 0)
        with metrics_col2:
            st.metric("Активных регионов", int(analytics_snapshot["region"].nunique()) if not analytics_snapshot.empty else 0)
        with metrics_col3:
            st.caption(
                f"Автообновление вкладок: каждые 30 секунд. Последний рендер: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )

        if history_frame.empty:
            st.info("По выбранным культурам пока нет исторических данных.")
        else:
            st.plotly_chart(build_price_figure(history_frame), use_container_width=True)


def main() -> None:
    init_db()

    st.title("AgroMind")
    st.caption("Дашборд по ценам на гидропонную зелень и профильным агроновостям.")

    with st.sidebar:
        st.title("AgroMind")
        if st.button("Обновить данные сейчас", use_container_width=True, type="primary"):
            with st.spinner("Парсинг новостей и ценовых площадок..."):
                result = refresh_data()

            if result["news_added"] or result["prices_added"]:
                st.success(
                    "Обновление завершено: "
                    f"новостей добавлено {result['news_added']}, "
                    f"ценовых записей добавлено {result['prices_added']}."
                )
            if result["errors"]:
                for error in result["errors"]:
                    st.error(error)

    render_dashboard_tabs()


if __name__ == "__main__":
    main()
