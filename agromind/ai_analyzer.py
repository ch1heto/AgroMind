from __future__ import annotations

import pandas as pd
import requests

from agromind.services import get_latest_prices_frame, get_recent_news


OLLAMA_URL = "http://localhost:11434/api/chat"
OLLAMA_MODEL = "qwen3.5:9b"
WEATHER_ERROR_TEXT = "Не удалось получить данные о погоде"


def _build_price_summary(df: pd.DataFrame) -> str:
    grouped = (
        df.groupby("crop_name", as_index=False)["wholesale_price"]
        .agg(median="median", lots="count", min="min", max="max")
        .sort_values(["median", "lots"], ascending=[False, True])
    )

    lines: list[str] = []
    for row in grouped.itertuples(index=False):
        lines.append(
            f"- {row.crop_name}: медиана {row.median:.2f} руб., "
            f"активных лотов {int(row.lots)}, диапазон {row.min:.2f}-{row.max:.2f} руб."
        )

    return "\n".join(lines) if lines else "- В базе нет актуальных цен."


def _build_news_summary() -> str:
    news_items = get_recent_news(limit=5)
    if not news_items:
        return "- Свежих новостей в базе нет."

    lines: list[str] = []
    for item in news_items:
        published_at = item["published_at"].strftime("%Y-%m-%d %H:%M")
        lines.append(f"- {item['title']} ({published_at})")

    return "\n".join(lines)


def _get_weather(region: str) -> str:
    normalized_region = (region or "").strip() or "Москва"
    try:
        response = requests.get(
            f"https://wttr.in/{normalized_region}?format=%t,+влажность+%h",
            timeout=5,
        )
        if response.status_code == 200:
            return response.text.strip()
    except requests.exceptions.RequestException:
        return WEATHER_ERROR_TEXT

    return WEATHER_ERROR_TEXT


def chat_with_ai(user_message: str, history: list[dict], user_region: str) -> str:
    df = get_latest_prices_frame()
    price_summary = _build_price_summary(df) if not df.empty else "- В базе нет цен."
    news_summary = _build_news_summary()
    weather_summary = _get_weather(user_region)
    normalized_region = (user_region or "").strip() or "Москва"

    system_prompt = (
        "Ты — ИИ-агроном и бизнес-аналитик B2B для фермеров. "
        "Ты обязан опираться только на переданный контекст: цены, новости, регион и текущую погоду. "
        "Строго учитывай цикл выращивания при оценке маржинальности: "
        "микрозелень 7-10 дней, базилик 30-40 дней. "
        "Если обсуждаются другие культуры, делай осторожные выводы и явно отмечай неопределенность. "
        "Ты обязан анализировать переданную погоду: идеальный диапазон для тепличной работы 18-22°C. "
        "Если в регионе холодно, обязательно напоминай о затратах на отопление теплиц и влиянии этого фактора на маржу. "
        "Отвечай структурно, конкретно и без воды."
    )

    context_message = (
        f"Регион пользователя: {normalized_region}\n"
        f"Текущая погода: {weather_summary}\n\n"
        "Актуальные рыночные цены:\n"
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
            timeout=300,
        )
        response.raise_for_status()
        payload = response.json()
        return payload.get("message", {}).get("content") or "LLM не вернула ответ."
    except requests.exceptions.RequestException as exc:
        return f"Не удалось получить ответ от Ollama: {exc}"
    except ValueError as exc:
        return f"Не удалось разобрать ответ Ollama: {exc}"
