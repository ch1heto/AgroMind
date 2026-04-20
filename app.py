from __future__ import annotations

from datetime import datetime

import plotly.graph_objects as go
import streamlit as st
from streamlit_autorefresh import st_autorefresh

import agromind.calculator as calculator_module
from agromind.ai_analyzer import AGRO_HANDBOOK, chat_with_ai
from agromind.database import (
    add_active_plant,
    get_active_plant,
    harvest_active_plant,
    init_db,
)
from agromind.services import (
    get_crop_filters,
    get_farm_profile,
    get_latest_demand_signals_frame,
    get_latest_prices_frame,
    get_price_history_frame,
    get_recent_news,
    save_farm_profile,
)


fragment_decorator = getattr(st, "fragment", getattr(st, "experimental_fragment", None))
if fragment_decorator is None:
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
        html, body, [data-testid="stAppViewContainer"] { color-scheme: dark; }
        [data-testid="stAppViewContainer"] {
            background:
                radial-gradient(circle at top left, rgba(46,125,50,0.18), transparent 28%),
                radial-gradient(circle at bottom right, rgba(27,94,32,0.16), transparent 24%),
                #0f1116;
        }
        [data-testid="stHeader"] { background: rgba(15,17,22,0.65); }
        .news-card {
            background: rgba(255,255,255,0.03);
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 14px; padding: 14px 16px; margin-bottom: 12px;
        }
        .news-card a { color: #98f5a6; text-decoration: none; font-weight: 600; }
        .news-meta { color: #b7c0c8; font-size: 0.9rem; margin-top: 8px; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Вкладка: Графики цен
# ---------------------------------------------------------------------------

def render_price_charts_tab() -> None:
    st.subheader("Динамика оптовых цен")

    crop_filters = get_crop_filters()
    all_cultures = sorted(AGRO_HANDBOOK.keys())

    col1, col2 = st.columns([2, 1])
    with col1:
        # Культуры из БД + из справочника (объединяем, убираем дубли)
        available = sorted(set(crop_filters) | set(all_cultures))
        selected_crops = st.multiselect(
            "Культуры",
            options=available,
            default=available[:3] if available else [],
            key="chart_crops",
        )
    with col2:
        days = st.selectbox(
            "Период",
            options=[7, 14, 30, 60, 90],
            index=2,
            format_func=lambda d: f"{d} дней",
            key="chart_days",
        )

    if not selected_crops:
        st.info("Выберите хотя бы одну культуру.")
        return

    df = get_price_history_frame(days=days, crop_names=selected_crops)

    if df.empty:
        st.info("Данных за выбранный период пока нет.")
        return

    # График медианной цены по дням
    fig = go.Figure()
    colors = [
        "#7ccf8a", "#4a9eff", "#ff7c4a", "#c97cff",
        "#ffcc4a", "#4affcc", "#ff4a9e", "#ccff4a",
    ]

    for i, crop in enumerate(selected_crops):
        crop_df = df[df["crop_name"] == crop].copy()
        if crop_df.empty:
            continue

        # Агрегируем по дате — медиана за день
        crop_df["date"] = crop_df["timestamp"].dt.date
        daily = (
            crop_df.groupby("date")["wholesale_price"]
            .agg(median="median", count="count")
            .reset_index()
        )

        color = colors[i % len(colors)]
        fig.add_trace(go.Scatter(
            x=daily["date"],
            y=daily["median"],
            mode="lines+markers",
            name=crop,
            line=dict(color=color, width=2),
            marker=dict(size=5),
            hovertemplate=(
                f"<b>{crop}</b><br>"
                "Дата: %{x}<br>"
                "Медиана: %{y:.0f} руб/кг<br>"
                "Лотов: %{customdata}<extra></extra>"
            ),
            customdata=daily["count"],
        ))

    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#f0f3f5", size=13),
        legend=dict(
            bgcolor="rgba(24,28,36,0.8)",
            bordercolor="rgba(255,255,255,0.1)",
            borderwidth=1,
        ),
        xaxis=dict(
            gridcolor="rgba(255,255,255,0.07)",
            title="Дата",
        ),
        yaxis=dict(
            gridcolor="rgba(255,255,255,0.07)",
            title="Оптовая цена, руб/кг",
            tickformat=",.0f",
        ),
        hovermode="x unified",
        margin=dict(l=10, r=10, t=30, b=10),
        height=420,
    )

    st.plotly_chart(fig, width="stretch")

    # Сводная таблица по последней цене
    st.markdown("**Последние зафиксированные цены**")
    latest = get_latest_prices_frame(crop_names=selected_crops)
    if not latest.empty:
        summary = (
            latest.groupby("crop_name")["wholesale_price"]
            .agg(median="median", min="min", max="max", count="count")
            .round(0)
            .astype(int)
            .reset_index()
            .rename(columns={
                "crop_name": "Культура",
                "median": "Медиана руб/кг",
                "min": "Мин",
                "max": "Макс",
                "count": "Лотов",
            })
            .sort_values("Медиана руб/кг", ascending=False)
        )
        st.dataframe(summary, width="stretch", hide_index=True)


# ---------------------------------------------------------------------------
# Остальные вкладки
# ---------------------------------------------------------------------------

def render_news_feed(news_items: list[dict]) -> None:
    if not news_items:
        st.info("Новостей пока нет. Запустите обновление вручную.")
        return
    for item in news_items:
        published_at = item["published_at"].strftime("%Y-%m-%d %H:%M")
        st.markdown(
            f"""
            <div class="news-card">
                <a href="{item['url']}" target="_blank">{item['title']}</a>
                <div class="news-meta">{published_at}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_chat_tab(farm_profile: dict[str, float]) -> None:
    st.subheader("ИИ-Агроном")
    user_region = st.text_input("Ваш регион", value="Москва", key="user_region_input")

    if "chat_messages" not in st.session_state:
        st.session_state.chat_messages = []

    for message in st.session_state.chat_messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    user_text = st.chat_input("Спросите про цены, культуру, уход или экономику")
    if not user_text:
        return

    history = list(st.session_state.chat_messages)
    st.session_state.chat_messages.append({"role": "user", "content": user_text})

    with st.chat_message("user"):
        st.markdown(user_text)

    with st.chat_message("assistant"):
        with st.spinner("Анализирую..."):
            current_profile = get_farm_profile()
            answer = chat_with_ai(
                user_message=user_text,
                history=history,
                user_region=user_region,
                farm_profile=current_profile,
            )
        st.markdown(answer)

    st.session_state.chat_messages.append({"role": "assistant", "content": answer})



# ---------------------------------------------------------------------------
# Основной дашборд
# ---------------------------------------------------------------------------

@fragment_decorator(run_every="30s")
def render_dashboard_tabs(farm_profile: dict[str, float]) -> None:
    st_autorefresh(interval=5000, limit=100, key="data_refresh")

    try:
        latest_prices = get_latest_prices_frame()
        demand_signals = get_latest_demand_signals_frame()
        news_items = get_recent_news()
    except Exception as exc:
        st.error(f"Не удалось прочитать данные из SQLite: {exc}")
        return

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Цены", "Графики", "Госзакупки", "Новости", "ИИ-Агроном"
    ])

    with tab1:
        st.subheader("Актуальный ценовой срез")
        if latest_prices.empty:
            st.info("Цен пока нет.")
        else:
            st.dataframe(
                latest_prices.rename(columns={
                    "crop_name": "Культура",
                    "region": "Регион",
                    "published_at": "Дата",
                    "wholesale_price": "Цена, руб/кг",
                }),
                width="stretch",
                hide_index=True,
            )

    with tab2:
        render_price_charts_tab()

    with tab3:
        st.subheader("Госзакупки (тендеры)")
        if demand_signals.empty:
            st.info("Тендеров пока нет.")
        else:
            st.dataframe(
                demand_signals.rename(columns={
                    "crop_name": "Культура",
                    "region": "Регион",
                    "contract_price": "Сумма, руб.",
                    "published_at": "Дата",
                    "url": "Ссылка",
                }),
                width="stretch",
                hide_index=True,
            )

    with tab4:
        st.subheader("Новости")
        render_news_feed(news_items)

    with tab5:
        render_chat_tab(farm_profile)


def main() -> None:
    init_db()
    farm_profile = get_farm_profile()
    active_plant = get_active_plant()
    available_cultures = sorted(
        getattr(calculator_module, "AGRO_HANDBOOK", AGRO_HANDBOOK).keys()
    )

    st.title("AgroMind")
    st.caption("Цены · Тендеры · Новости · AI-агроном")

    with st.sidebar:
        st.title("AgroMind")

        with st.expander("Моя сити-ферма", expanded=False):
            farm_area = st.number_input(
                "Площадь посадки (м²)",
                min_value=0.0,
                value=float(farm_profile["total_area_sqm"]),
                step=1.0,
                key="farm_area_input",
            )
            energy_price = st.number_input(
                "Тариф электроэнергии (руб/кВт·ч)",
                min_value=0.0,
                value=float(farm_profile["energy_price_kwh"]),
                step=0.1,
                key="farm_energy_input",
            )
            if st.button("Сохранить профиль", width="stretch"):
                save_farm_profile(area=farm_area, energy_price=energy_price)
                farm_profile = get_farm_profile()
                st.success("Сохранено.")


        st.header("🌱 Моя ферма")
        if active_plant is None:
            selected_culture = st.selectbox(
                "Культура для новой посадки",
                options=available_cultures,
                key="active_plant_culture",
            )
            if st.button("Посадить сегодня", width="stretch"):
                add_active_plant(selected_culture)
                st.rerun()
        else:
            st.success(
                f"Растет: {active_plant['culture_name']} | День: {active_plant['days_active']}"
            )
            if st.button("Собрать урожай", width="stretch"):
                harvest_active_plant()
                st.rerun()

    render_dashboard_tabs(farm_profile)


if __name__ == "__main__":
    main()
