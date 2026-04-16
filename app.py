from __future__ import annotations

import pandas as pd
import streamlit as st

from agromind.database import init_db
from agromind.services import get_latest_prices, get_recent_news, refresh_data


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
        st.info("В базе пока нет сохранённых новостей. Нажмите 'Обновить данные'.")
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


def main() -> None:
    init_db()

    st.title("AgroMind")
    st.caption("Локальный дашборд для агропромышленных новостей и оптовых цен на зелень.")

    with st.sidebar:
        st.subheader("Управление")
        if st.button("Обновить данные", use_container_width=True, type="primary"):
            with st.spinner("Сбор данных из RSS и HTML-источников..."):
                result = refresh_data()

            if result["news_added"] or result["prices_added"]:
                st.success(
                    "Данные обновлены: "
                    f"новостей добавлено {result['news_added']}, "
                    f"ценовых записей добавлено {result['prices_added']}."
                )
            if result["errors"]:
                for error in result["errors"]:
                    st.warning(error)

    try:
        prices = get_latest_prices()
        news = get_recent_news()
    except Exception as exc:
        st.error(f"Ошибка чтения локальной базы данных: {exc}")
        return

    prices_df = pd.DataFrame(prices)

    left_column, right_column = st.columns((1.1, 1), gap="large")

    with left_column:
        st.subheader("Актуальные оптовые цены на зелень")
        if prices_df.empty:
            st.info("В базе пока нет ценовых записей. Выполните обновление данных.")
        else:
            st.dataframe(prices_df, use_container_width=True, hide_index=True)

    with right_column:
        st.subheader("Свежие агропромышленные новости")
        render_news_feed(news)


if __name__ == "__main__":
    main()
