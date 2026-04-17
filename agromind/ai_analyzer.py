from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import requests

from agromind.services import get_latest_prices_frame, get_recent_news


OLLAMA_URL = "http://localhost:11434/api/chat"
OLLAMA_MODEL = "qwen3.5:9b"
WEATHER_ERROR_TEXT = "Не удалось получить данные о погоде"

AGRO_HANDBOOK = {
    "Базилик": {
        "cycle_days": (30, 40),
        "ph": "5.5-6.5",
        "ec": "1.0-1.6",
        "temperature_c": "20-25",
        "humidity_pct": "40-60",
    },
    "Кинза": {
        "cycle_days": (30, 40),
        "ph": "6.0-6.7",
        "ec": "1.2-1.8",
        "temperature_c": "15-20",
        "humidity_pct": "50-70",
    },
    "Лук зеленый": {
        "cycle_days": (20, 30),
        "ph": "6.0-7.0",
        "ec": "1.4-1.8",
        "temperature_c": "18-22",
        "humidity_pct": "60-70",
    },
    "Микрозелень": {
        "cycle_days": (7, 10),
        "ph": "5.5-6.5",
        "ec": "0.8-1.2",
        "temperature_c": "20-24",
        "humidity_pct": "50-60",
    },
    "Мята": {
        "cycle_days": (45, 60),
        "ph": "5.5-6.0",
        "ec": "1.6-2.0",
        "temperature_c": "18-22",
        "humidity_pct": "70-80",
    },
    "Петрушка": {
        "cycle_days": (40, 60),
        "ph": "5.5-6.0",
        "ec": "1.2-1.8",
        "temperature_c": "15-20",
        "humidity_pct": "50-70",
    },
    "Руккола": {
        "cycle_days": (25, 35),
        "ph": "6.0-6.5",
        "ec": "1.2-1.8",
        "temperature_c": "15-20",
        "humidity_pct": "50-70",
    },
    "Салат": {
        "cycle_days": (35, 45),
        "ph": "5.5-6.0",
        "ec": "0.8-1.2",
        "temperature_c": "16-20",
        "humidity_pct": "60-70",
    },
    "Салат айсберг": {
        "cycle_days": (45, 60),
        "ph": "5.5-6.5",
        "ec": "1.0-1.5",
        "temperature_c": "15-18",
        "humidity_pct": "60-70",
    },
    "Укроп": {
        "cycle_days": (30, 40),
        "ph": "5.5-6.5",
        "ec": "1.0-1.6",
        "temperature_c": "15-20",
        "humidity_pct": "50-70",
    },
    "Шпинат": {
        "cycle_days": (30, 45),
        "ph": "6.0-7.0",
        "ec": "1.6-2.0",
        "temperature_c": "15-20",
        "humidity_pct": "50-70",
    },
}


def _normalize_region(value: str) -> str:
    return " ".join(str(value or "").strip().lower().replace("ё", "е").split())


def _has_local_region_match(region_value: str, user_region: str) -> bool:
    normalized_region = _normalize_region(region_value)
    normalized_user_region = _normalize_region(user_region)
    if not normalized_region or not normalized_user_region:
        return False
    return normalized_user_region in normalized_region


def _format_price_table(title: str, df: pd.DataFrame) -> str:
    if df.empty:
        return f"{title}:\n- Нет актуальных цен."

    aggregated = (
        df.groupby("crop_name", as_index=False)["wholesale_price"]
        .agg(median="median", min="min", max="max", lots="count")
        .sort_values(["median", "lots"], ascending=[False, False])
    )

    lines = [f"{title}:"]
    for row in aggregated.itertuples(index=False):
        lines.append(
            f"- {row.crop_name}: медиана {row.median:.2f} руб., "
            f"мин {row.min:.2f} руб., макс {row.max:.2f} руб., "
            f"лотов {int(row.lots)}."
        )
    return "\n".join(lines)


def _build_price_context(df: pd.DataFrame, user_region: str) -> str:
    if df.empty:
        return (
            "Локальные цены:\n- Нет актуальных цен.\n\n"
            "Средняя выборка по РФ:\n- Нет актуальных цен."
        )

    local_mask = df["region"].fillna("").apply(
        lambda region_value: _has_local_region_match(region_value, user_region)
    )
    local_df = df.loc[local_mask].copy()
    national_df = df.copy()

    local_title = f"Локальные цены ({(user_region or '').strip() or 'Москва'})"
    local_prices = _format_price_table(local_title, local_df)
    national_prices = _format_price_table("Средняя выборка по РФ", national_df)
    return f"{local_prices}\n\n{national_prices}"


def _build_news_summary() -> str:
    news_items = get_recent_news(limit=5)
    if not news_items:
        return "- Свежих новостей пока нет."

    lines: list[str] = []
    for item in news_items:
        published_at = item["published_at"].strftime("%Y-%m-%d %H:%M")
        lines.append(f"- {item['title']} ({published_at})")
    return "\n".join(lines)


def _build_agro_handbook_reference(today: datetime) -> str:
    lines: list[str] = []
    for crop_name, params in AGRO_HANDBOOK.items():
        min_days, max_days = params["cycle_days"]
        harvest_from = (today + timedelta(days=min_days)).strftime("%Y-%m-%d")
        harvest_to = (today + timedelta(days=max_days)).strftime("%Y-%m-%d")
        lines.append(
            f"- {crop_name}: цикл {min_days}-{max_days} дней, "
            f"если сеять сегодня, сбор ориентировочно {harvest_from}-{harvest_to}; "
            f"pH {params['ph']}, EC {params['ec']}, "
            f"t {params['temperature_c']}°C, H {params['humidity_pct']}%."
        )
    return "\n".join(lines)


def _get_weather_forecast(region: str) -> str:
    normalized_region = (region or "").strip() or "Москва"

    try:
        response = requests.get(
            f"https://wttr.in/{normalized_region}?format=j1",
            timeout=5,
        )
        response.raise_for_status()
        payload = response.json()

        current_condition = payload.get("current_condition", [{}])[0]
        current_temp = current_condition.get("temp_C")
        current_humidity = current_condition.get("humidity")
        if current_temp in (None, "") or current_humidity in (None, ""):
            raise ValueError("missing current weather data")

        forecast_parts: list[str] = []
        for day in payload.get("weather", [])[:3]:
            date_value = day.get("date")
            avg_temp = day.get("avgtempC")
            if not date_value or avg_temp in (None, ""):
                continue
            forecast_parts.append(f"{date_value}: {avg_temp}°C")

        if not forecast_parts:
            raise ValueError("missing forecast")

        return (
            f"Регион: {normalized_region}. "
            f"Сейчас {current_temp}°C, влажность {current_humidity}%. "
            f"Прогноз температуры на 3 дня: {', '.join(forecast_parts)}."
        )
    except (requests.exceptions.RequestException, ValueError, KeyError, IndexError):
        return WEATHER_ERROR_TEXT


def chat_with_ai(user_message: str, history: list[dict], user_region: str) -> str:
    now = datetime.now()
    normalized_region = (user_region or "").strip() or "Москва"

    price_df = get_latest_prices_frame()
    weather_summary = _get_weather_forecast(normalized_region)
    agro_handbook_summary = _build_agro_handbook_reference(now)
    price_context = _build_price_context(price_df, normalized_region)
    news_summary = _build_news_summary()

    system_prompt = (
        "Ты — опытный агроном-консультант B2B. "
        "ЗАПРЕЩЕНО использовать эмодзи. "
        "Говори простым, человеческим, профессиональным языком. "
        "Никаких роботизированных фраз вроде 'Данные отсутствуют'.\n\n"
        "АНАЛИЗ ПОМЕЩЕНИЯ: Если пользователь пишет 'сити-ферма', 'в помещении' — "
        "внешняя погода не важна, просто назови идеальные условия ВНУТРИ "
        "(из справочника AGRO_HANDBOOK). Если 'теплица' — сопоставляй с погодой на улице. "
        "Если тип не указан — вежливо уточни.\n\n"
        "АГРО-ДАННЫЕ: Опирайся на переданный AGRO_HANDBOOK "
        "(циклы, pH, EC, температура, влажность). "
        "Считай дату сбора урожая от сегодняшней даты.\n\n"
        "РЫНОК: Если есть 'Локальные цены', опирайся на них. "
        "Если их нет, используй 'Среднюю выборку по РФ' как ориентир. "
        "Рекомендуй маржинальные и быстрые культуры."
    )

    user_prompt = (
        f"Текущая дата: {now.strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"Регион пользователя: {normalized_region}\n\n"
        f"Погода:\n{weather_summary}\n\n"
        f"AGRO_HANDBOOK:\n{agro_handbook_summary}\n\n"
        f"{price_context}\n\n"
        f"Свежие новости:\n{news_summary}\n\n"
        f"Запрос пользователя:\n{user_message}"
    )

    messages: list[dict[str, str]] = [{"role": "system", "content": system_prompt}]

    for item in history:
        role = str(item.get("role", "")).strip()
        content = str(item.get("content", "")).strip()
        if role in {"user", "assistant"} and content:
            messages.append({"role": role, "content": content})

    messages.append({"role": "user", "content": user_prompt})

    try:
        response = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "messages": messages,
                "stream": False,
            },
            timeout=None,
        )
        response.raise_for_status()
        payload = response.json()
        return payload.get("message", {}).get("content") or "LLM не вернула ответ."
    except requests.exceptions.RequestException as exc:
        return f"Не удалось получить ответ от Ollama: {exc}"
    except ValueError as exc:
        return f"Не удалось разобрать ответ Ollama: {exc}"
