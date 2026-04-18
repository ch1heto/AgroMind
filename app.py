from __future__ import annotations

import json

import streamlit as st
from streamlit_autorefresh import st_autorefresh

from agromind.ai_analyzer import chat_with_ai
from agromind.config import DATA_DIR, WORKER_INTERVAL_MINUTES
from agromind.database import init_db
from agromind.services import (
    get_crop_filters,
    get_latest_demand_signals_frame,
    get_latest_prices_frame,
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


def render_chat_tab() -> None:
    st.subheader("ИИ-Агроном")
    user_region = st.text_input("Ваш регион", value="Москва", key="user_region_input")

    if "chat_messages" not in st.session_state:
        st.session_state.chat_messages = []

    for message in st.session_state.chat_messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    user_text = st.chat_input("Спросите про маржинальность, цены, погоду или выбор культуры")
    if not user_text:
        return

    history = list(st.session_state.chat_messages)
    user_message = {"role": "user", "content": user_text}
    st.session_state.chat_messages.append(user_message)

    with st.chat_message("user"):
        st.markdown(user_text)

    with st.chat_message("assistant"):
        with st.spinner("Анализирую данные и климат..."):
            answer = chat_with_ai(
                user_message=user_text,
                history=history,
                user_region=user_region,
            )
        st.markdown(answer)

    st.session_state.chat_messages.append({"role": "assistant", "content": answer})


def _render_worker_health() -> None:
    """Показывает статус фонового воркера в сайдбаре."""
    health_file = DATA_DIR / "worker_health.json"

    if not health_file.exists():
        st.sidebar.warning("Воркер не запущен или ещё не записал статус.")
        return

    try:
        payload = json.loads(health_file.read_text())
    except Exception:
        st.sidebar.error("Не удалось прочитать статус воркера.")
        return

    status = payload.get("status", "unknown")
    last_update_raw = payload.get("last_update", "")
    error = payload.get("error")

    try:
        from datetime import datetime

        last_update = datetime.fromisoformat(last_update_raw)
        age_minutes = int((datetime.utcnow() - last_update).total_seconds() / 60)
        age_str = f"{age_minutes} мин назад"
    except Exception:
        age_minutes = None
        age_str = last_update_raw

    if status == "ok":
        st.sidebar.success(f"Воркер: активен ({age_str})")
    elif status == "running":
        st.sidebar.info(f"Воркер: сбор данных... ({age_str})")
    elif status == "error":
        st.sidebar.error(f"Воркер: ошибка ({age_str})")
        if error:
            with st.sidebar.expander("Подробности ошибки"):
                st.code(error)
    elif status == "crash":
        st.sidebar.error(f"Воркер упал: {error}")
    elif status == "stopped":
        st.sidebar.warning("Воркер остановлен.")
    else:
        st.sidebar.warning(f"Воркер: статус неизвестен ({status})")

    try:
        if age_minutes is not None and age_minutes > (WORKER_INTERVAL_MINUTES * 3):
            st.sidebar.warning(
                f"Воркер не обновлялся {age_minutes} мин. "
                "Возможно, процесс завис или остановился."
            )
    except Exception:
        pass


@fragment_decorator(run_every="30s")
def render_dashboard_tabs() -> None:
    st_autorefresh(interval=5000, limit=100, key="data_refresh")

    try:
        get_crop_filters()
        latest_prices = get_latest_prices_frame()
        demand_signals = get_latest_demand_signals_frame()
        news_items = get_recent_news()
    except Exception as exc:
        st.error(f"Не удалось прочитать данные из SQLite: {exc}")
        return

    tab1, tab2, tab3, tab4 = st.tabs(["Цены", "Госзакупки (Тендеры)", "Новости", "ИИ-Агроном"])

    with tab1:
        st.subheader("Актуальный ценовой срез")
        if latest_prices.empty:
            st.info("Свежий ценовой срез пока пуст.")
        else:
            prices_frame = latest_prices.rename(
                columns={
                    "crop_name": "Культура",
                    "region": "Регион",
                    "published_at": "Дата публикации",
                    "wholesale_price": "Оптовая цена, руб.",
                }
            )
            st.dataframe(prices_frame, use_container_width=True, hide_index=True)

    with tab2:
        st.subheader("Госзакупки")
        if demand_signals.empty:
            st.info("Тендеров пока нет")
        else:
            demand_frame = demand_signals.rename(
                columns={
                    "crop_name": "Культура",
                    "region": "Регион",
                    "contract_price": "Сумма контракта, руб.",
                    "published_at": "Дата",
                    "url": "Ссылка",
                }
            )
            st.dataframe(demand_frame, use_container_width=True, hide_index=True)

    with tab3:
        st.subheader("Новости")
        render_news_feed(news_items)

    with tab4:
        render_chat_tab()


def main() -> None:
    init_db()

    st.title("AgroMind")
    st.caption("Дашборд по ценам на гидропонную зелень, агроновостям, госзакупкам и AI-аналитике.")

    with st.sidebar:
        st.title("AgroMind")
        if st.button("Обновить данные сейчас", use_container_width=True, type="primary"):
            with st.spinner("Парсинг новостей, тендеров и ценовых площадок..."):
                result = refresh_data()

            if result["news_added"] or result["prices_added"] or result["demand_added"]:
                st.success(
                    "Обновление завершено: "
                    f"новостей добавлено {result['news_added']}, "
                    f"ценовых записей добавлено {result['prices_added']}, "
                    f"тендеров добавлено {result['demand_added']}."
                )
            if result["errors"]:
                for error in result["errors"]:
                    st.error(error)

        _render_worker_health()

    render_dashboard_tabs()


if __name__ == "__main__":
    main()
