from __future__ import annotations

import pandas as pd
import requests

from agromind.services import get_latest_prices_frame, get_recent_news


OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "qwen3.5:9b"


def _build_price_summary(df: pd.DataFrame) -> str:
    grouped = (
        df.groupby("crop_name", as_index=False)["wholesale_price"]
        .agg(min="min", max="max", median="median", count="count")
        .sort_values(["median", "count"], ascending=[False, True])
    )

    lines: list[str] = []
    for row in grouped.itertuples(index=False):
        lines.append(
            f"- {row.crop_name}: Медиана {row.median:.2f} руб. "
            f"(От {row.min:.2f} до {row.max:.2f} руб). "
            f"Объявлений: {int(row.count)}"
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


def generate_market_strategy() -> str:
    df = get_latest_prices_frame()
    if df.empty:
        return "В базе нет цен."

    price_summary = _build_price_summary(df)
    news_summary = _build_news_summary()

    system_prompt = (
        "Ты — бизнес-аналитик в сфере агробизнеса B2B. "
        "Помоги фермеру выбрать самую маржинальную культуру для старта. "
        "Опирайся только на переданные цифры и новости. "
        "Правило: меньше конкурентов (объявлений) и выше медианная цена = выгоднее ниша. "
        "Отвечай структурно, без воды."
    )

    user_prompt = (
        "Вот актуальная статистика по рынку из SQLite.\n\n"
        "Цены по культурам:\n"
        f"{price_summary}\n\n"
        "Последние новости рынка:\n"
        f"{news_summary}\n\n"
        "Сделай вывод: топ-3 культуры для старта, рекомендуемая цена продажи для каждой и почему именно они?"
    )

    try:
        response = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "system": system_prompt,
                "prompt": user_prompt,
                "stream": False,
            },
            timeout=120,
        )
        response.raise_for_status()
        return response.json().get("response") or "LLM не вернула ответ."
    except requests.exceptions.RequestException as exc:
        return f"Не удалось получить ответ от Ollama: {exc}"


if __name__ == "__main__":
    print("=" * 80)
    print("AgroMind AI Analyzer")
    print("=" * 80)
    print()
    print(generate_market_strategy())
    print()
