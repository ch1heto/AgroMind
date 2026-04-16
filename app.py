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
if fragment_decorator is None:  # pragma: no cover - fallback for very old Streamlit
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
        st.info("В SQLite пока нет новостей. Запустите обновление вручную или дождитесь следующего цикла воркера.")
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
def render_prices_fragment(selected_crops: list[str], history_days: int) -> None:
    try:
        history_frame = get_price_history_frame(days=history_days, crop_names=selected_crops or None)
        latest_prices = get_latest_prices_frame(crop_names=selected_crops or None)
    except Exception as exc:
        st.error(f"Не удалось прочитать ценовые данные из SQLite: {exc}")
        return

    st.subheader("Цены на зелень")
    st.caption(
        f"Автообновление блока: каждые 30 секунд. Последний рендер: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

    if history_frame.empty:
        st.info("В SQLite пока нет ценовых записей. Запустите обновление данных.")
        return

    top_left, top_right = st.columns((2, 1), gap="large")

    with top_left:
        st.plotly_chart(build_price_figure(history_frame), use_container_width=True)

    with top_right:
        stats_frame = latest_prices.copy()
        if stats_frame.empty:
            st.info("Актуальный ценовой срез пока пуст.")
        else:
            st.metric("Активных культур", int(stats_frame["crop_name"].nunique()))
            st.metric("Активных регионов", int(stats_frame["region"].nunique()))
            st.metric("Последних записей", len(stats_frame))

    st.subheader("Актуальный ценовой срез")
    table_frame = latest_prices.rename(
        columns={
            "crop_name": "Культура",
            "region": "Регион",
            "published_at": "Дата публикации",
            "wholesale_price": "Оптовая цена, руб.",
        }
    )
    st.dataframe(table_frame, use_container_width=True, hide_index=True)


def main() -> None:
    init_db()

    st.title("AgroMind")
    st.caption("Дашборд хранит новости и ценовые срезы в SQLite и динамически обновляет блок цен.")

    with st.sidebar:
        st.subheader("Управление")
        history_days = st.slider(
            "Глубина истории, дней",
            min_value=7,
            max_value=180,
            value=DEFAULT_HISTORY_DAYS,
        )

        try:
            crop_options = get_crop_filters()
        except Exception as exc:
            crop_options = []
            st.warning(f"Не удалось загрузить список культур из SQLite: {exc}")
        selected_crops = st.multiselect(
            "Фильтр по культурам",
            options=crop_options,
            default=crop_options[:3],
            help="Можно сравнить одну и ту же культуру по разным регионам, например базилик в Москве и Самаре.",
        )

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
                    st.warning(error)

        st.caption("Ценовой блок ниже обновляется автоматически без полной перезагрузки страницы.")

    render_prices_fragment(selected_crops=selected_crops, history_days=history_days)

    st.subheader("Свежие новости")
    try:
        render_news_feed(get_recent_news())
    except Exception as exc:
        st.error(f"Не удалось прочитать новости из SQLite: {exc}")


if __name__ == "__main__":
    main()
