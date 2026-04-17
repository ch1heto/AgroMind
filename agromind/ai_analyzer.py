from __future__ import annotations

from datetime import datetime

import pandas as pd
import requests

from agromind.services import get_latest_prices_frame, get_recent_news


OLLAMA_URL = "http://localhost:11434/api/chat"
OLLAMA_MODEL = "qwen3.5:9b"
WEATHER_ERROR_TEXT = "Не удалось получить данные о погоде"

VEGETATION_CYCLES = {
    "Микрозелень": "7-10 дней",
    "Лук зеленый": "20-30 дней",
    "Руккола": "25-35 дней",
    "Укроп": "30-40 дней",
    "Кинза": "30-40 дней",
    "Базилик": "30-40 дней",
    "Салат": "35-45 дней",
    "Шпинат": "30-45 дней",
    "Салат айсберг": "45-60 дней",
    "Петрушка": "40-60 дней",
    "Мята": "45-60 дней",
}


def _build_price_summary(df: pd.DataFrame) -> str:
    if df.empty:
        return "- В базе нет актуальных цен."

    grouped = (
        df.groupby("crop_name", as_index=False)["wholesale_price"]
        .agg(median="median", lots="count", min="min", max="max")
        .sort_values(["median", "lots"], ascending=[False, True])
    )

    lines: list[str] = []
    for row in grouped.itertuples(index=False):
        lines.append(
            f"- {row.crop_name}: медиана {row.median:.2f} руб., "
            f"активных лотов {int(row.lots)}, "
            f"диапазон {row.min:.2f}-{row.max:.2f} руб."
        )

    return "\n".join(lines)


def _build_news_summary() -> str:
    news_items = get_recent_news(limit=5)
    if not news_items:
        return "- Свежих новостей в базе нет."

    lines: list[str] = []
    for item in news_items:
        published_at = item["published_at"].strftime("%Y-%m-%d %H:%M")
        lines.append(f"- {item['title']} ({published_at})")

    return "\n".join(lines)


def _build_vegetation_reference() -> str:
    return "\n".join(
        f"- {crop}: {cycle}"
        for crop, cycle in VEGETATION_CYCLES.items()
    )


def _get_weather_forecast(region: str) -> str:
    normalized_region = (region or "").strip() or "Москва"

    try:
        response = requests.get(
            f"https://wttr.in/{normalized_region}?format=j1",
            timeout=5,
        )
        response.raise_for_status()
        payload = response.json()

        current_temp = (
            payload.get("current_condition", [{}])[0].get("temp_C")
        )
        if current_temp in (None, ""):
            raise ValueError("missing current temperature")

        forecast_parts: list[str] = []
        for day in payload.get("weather", [])[:3]:
            date_value = day.get("date")
            avg_temp = day.get("avgtempC")
            if not date_value or avg_temp in (None, ""):
                continue
            forecast_parts.append(f"{date_value}: {avg_temp}°C")

        if not forecast_parts:
            raise ValueError("missing forecast")

        return f"Текущая: {current_temp}°C, Прогноз: " + ", ".join(forecast_parts)
    except (requests.exceptions.RequestException, ValueError, KeyError, IndexError):
        return WEATHER_ERROR_TEXT


def chat_with_ai(user_message: str, history: list[dict], user_region: str) -> str:
    current_date = datetime.now().strftime("%Y-%m-%d")
    normalized_region = (user_region or "").strip() or "Москва"

    df = get_latest_prices_frame()
    price_summary = _build_price_summary(df)
    news_summary = _build_news_summary()
    weather_summary = _get_weather_forecast(normalized_region)
    vegetation_reference = _build_vegetation_reference()

    system_prompt = (
        "Ты — профессиональная экспертная система в роли ИИ-агронома и B2B бизнес-аналитика. "
        "Стиль ответов: официально-деловой, аналитический, профессиональный. "
        "Использование любых эмодзи, смайликов и декоративных символов запрещено. "
        "Ты обязан опираться только на переданный контекст: календарную дату, регион, погоду, цены и новости. "
        "Текущая дата служит точкой отсчета для календаря посадок, сроков посева и даты выхода на срез. "
        "При расчете маржинальности строго учитывай циклы вегетации и дни до среза:\n"
        f"{vegetation_reference}\n"
        "Если пользователь использует термины 'сити-ферма', 'городская ферма', 'подвал', 'гараж' или 'помещение', "
        "объясняй, что внешняя погода влияет прежде всего на энергозатраты климатического оборудования. "
        "Если пользователь использует термин 'теплица', указывай на прямые риски для вегетации от внешней температуры. "
        "Если тип помещения не ясен, ты обязан вежливо уточнить условия выращивания "
        "(температура, влажность, тип освещения), прежде чем давать финальный совет. "
        "Анализируй погодные условия относительно целевого диапазона 18-22°C. "
        "Если в регионе холодно, обязательно напоминай о затратах на отопление теплиц или климатического оборудования и их влиянии на маржу. "
        "Если данных недостаточно, прямо укажи, чего не хватает, и не придумывай факты."
    )

    context_message = (
        f"Текущая дата: {current_date}\n"
        f"Регион пользователя: {normalized_region}\n"
        f"Погода и прогноз: {weather_summary}\n\n"
        "Агрегированная таблица цен из БД:\n"
        f"{price_summary}\n\n"
        "Свежие новости рынка:\n"
        f"{news_summary}"
    )

    messages: list[dict[str, str]] = [
        {"role": "system", "content": system_prompt},
        {"role": "system", "content": context_message},
    ]

    for item in history:
        role = str(item.get("role", "")).strip()
        content = str(item.get("content", "")).strip()
        if role in {"user", "assistant"} and content:
            messages.append({"role": role, "content": content})

    messages.append({"role": "user", "content": user_message})

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
