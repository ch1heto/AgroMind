from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import requests

from agromind.services import (
    get_latest_demand_signals_frame,
    get_latest_prices_frame,
    get_price_history_frame,
    get_recent_news,
)


OLLAMA_URL = "http://localhost:11434/api/chat"
OLLAMA_MODEL = "qwen3.5:9b"
WEATHER_ERROR_TEXT = "Данные о погоде недоступны"

# ---------------------------------------------------------------------------
# Справочник культур — расширен полями для ежедневных инструкций
# ---------------------------------------------------------------------------
AGRO_HANDBOOK: dict[str, dict] = {
    "Базилик": {
        "cycle_days": (30, 40),
        "ph": "5.5–6.5",
        "ec": "1.0–1.6",
        "temperature_c": "20–25",
        "humidity_pct": "40–60",
        "light_hours": 16,
        "water_l_per_sqm_day": 2.5,
        "solution_change_days": 7,
        "germination_days": (5, 7),
        "daily_checks": "цвет листьев (пожелтение = дефицит N), запах раствора, pH и EC каждые 2 дня",
        "harvest_sign": "высота 15–20 см, до начала цветения",
        "margin_note": "высокомаржинальная, быстрый оборот",
        "yield_kg_per_sqm": 2.2,
        "power_kw_per_sqm": 24.0,
        "seed_cost_per_sqm": 180.0,
        "nutrition_cost_per_sqm": 95.0,
    },
    "Кинза": {
        "cycle_days": (30, 40),
        "ph": "6.0–6.7",
        "ec": "1.2–1.8",
        "temperature_c": "15–20",
        "humidity_pct": "50–70",
        "light_hours": 12,
        "water_l_per_sqm_day": 2.0,
        "solution_change_days": 7,
        "germination_days": (7, 10),
        "daily_checks": "влажность субстрата, отсутствие полегания, ровный рост",
        "harvest_sign": "высота 20–25 см, до стрелкования",
        "margin_note": "стабильный спрос, средняя маржа",
        "yield_kg_per_sqm": 1.8,
        "power_kw_per_sqm": 18.0,
        "seed_cost_per_sqm": 120.0,
        "nutrition_cost_per_sqm": 80.0,
    },
    "Лук зеленый": {
        "cycle_days": (20, 30),
        "ph": "6.0–7.0",
        "ec": "1.4–1.8",
        "temperature_c": "18–22",
        "humidity_pct": "60–70",
        "light_hours": 14,
        "water_l_per_sqm_day": 2.0,
        "solution_change_days": 10,
        "germination_days": (3, 5),
        "daily_checks": "пожелтение кончиков (избыток соли), равномерность роста",
        "harvest_sign": "высота 25–30 см",
        "margin_note": "быстрый цикл, хорошая оборачиваемость",
        "yield_kg_per_sqm": 3.4,
        "power_kw_per_sqm": 16.0,
        "seed_cost_per_sqm": 140.0,
        "nutrition_cost_per_sqm": 75.0,
    },
    "Микрозелень": {
        "cycle_days": (7, 10),
        "ph": "5.5–6.5",
        "ec": "0.8–1.2",
        "temperature_c": "20–24",
        "humidity_pct": "50–60",
        "light_hours": 12,
        "water_l_per_sqm_day": 1.5,
        "solution_change_days": 0,
        "germination_days": (2, 3),
        "daily_checks": "влажность мата, отсутствие плесени, равномерность проростков",
        "harvest_sign": "семядольные листья раскрыты, первая пара настоящих листьев только начинается",
        "margin_note": "самый короткий цикл, максимальная оборачиваемость",
        "yield_kg_per_sqm": 1.4,
        "power_kw_per_sqm": 7.0,
        "seed_cost_per_sqm": 260.0,
        "nutrition_cost_per_sqm": 35.0,
    },
    "Мята": {
        "cycle_days": (45, 60),
        "ph": "5.5–6.0",
        "ec": "1.6–2.0",
        "temperature_c": "18–22",
        "humidity_pct": "70–80",
        "light_hours": 14,
        "water_l_per_sqm_day": 3.0,
        "solution_change_days": 7,
        "germination_days": (10, 14),
        "daily_checks": "влажность воздуха, состояние корней (гниль при застое), аромат",
        "harvest_sign": "побеги 15–20 см, срез над 2-м узлом",
        "margin_note": "длинный цикл, нишевый спрос, высокая цена",
        "yield_kg_per_sqm": 2.6,
        "power_kw_per_sqm": 32.0,
        "seed_cost_per_sqm": 210.0,
        "nutrition_cost_per_sqm": 120.0,
    },
    "Петрушка": {
        "cycle_days": (40, 60),
        "ph": "5.5–6.0",
        "ec": "1.2–1.8",
        "temperature_c": "15–20",
        "humidity_pct": "50–70",
        "light_hours": 14,
        "water_l_per_sqm_day": 2.0,
        "solution_change_days": 7,
        "germination_days": (14, 21),
        "daily_checks": "цвет листьев, равномерность роста, EC раствора",
        "harvest_sign": "высота 20–25 см, листья насыщенно-зелёные",
        "margin_note": "долгое прорастание, но стабильный спрос",
        "yield_kg_per_sqm": 2.0,
        "power_kw_per_sqm": 26.0,
        "seed_cost_per_sqm": 130.0,
        "nutrition_cost_per_sqm": 90.0,
    },
    "Руккола": {
        "cycle_days": (25, 35),
        "ph": "6.0–6.5",
        "ec": "1.2–1.8",
        "temperature_c": "15–20",
        "humidity_pct": "50–70",
        "light_hours": 14,
        "water_l_per_sqm_day": 2.0,
        "solution_change_days": 7,
        "germination_days": (4, 6),
        "daily_checks": "горечь листьев нарастает при перегреве — следи за t°, аромат",
        "harvest_sign": "высота 10–15 см, до появления стрелки",
        "margin_note": "ресторанный сегмент, премиальная цена",
        "yield_kg_per_sqm": 1.7,
        "power_kw_per_sqm": 18.0,
        "seed_cost_per_sqm": 150.0,
        "nutrition_cost_per_sqm": 70.0,
    },
    "Салат": {
        "cycle_days": (35, 45),
        "ph": "5.5–6.0",
        "ec": "0.8–1.2",
        "temperature_c": "16–20",
        "humidity_pct": "60–70",
        "light_hours": 16,
        "water_l_per_sqm_day": 2.5,
        "solution_change_days": 7,
        "germination_days": (3, 5),
        "daily_checks": "краевой ожог листьев (недостаток Ca), равномерность розетки",
        "harvest_sign": "розетка диаметром 20–25 см",
        "margin_note": "высокий объёмный спрос, госзакупки",
        "yield_kg_per_sqm": 3.1,
        "power_kw_per_sqm": 26.0,
        "seed_cost_per_sqm": 110.0,
        "nutrition_cost_per_sqm": 85.0,
    },
    "Салат айсберг": {
        "cycle_days": (45, 60),
        "ph": "5.5–6.5",
        "ec": "1.0–1.5",
        "temperature_c": "15–18",
        "humidity_pct": "60–70",
        "light_hours": 16,
        "water_l_per_sqm_day": 2.5,
        "solution_change_days": 7,
        "germination_days": (3, 5),
        "daily_checks": "формирование кочана, краевой ожог",
        "harvest_sign": "кочан плотный, диаметр 15–20 см",
        "margin_note": "крупные контракты HoReCa и сети",
        "yield_kg_per_sqm": 4.2,
        "power_kw_per_sqm": 30.0,
        "seed_cost_per_sqm": 135.0,
        "nutrition_cost_per_sqm": 100.0,
    },
    "Укроп": {
        "cycle_days": (30, 40),
        "ph": "5.5–6.5",
        "ec": "1.0–1.6",
        "temperature_c": "15–20",
        "humidity_pct": "50–70",
        "light_hours": 14,
        "water_l_per_sqm_day": 2.0,
        "solution_change_days": 7,
        "germination_days": (7, 14),
        "daily_checks": "пожелтение (дефицит Mg), равномерность всходов",
        "harvest_sign": "высота 25–30 см, до зонтика",
        "margin_note": "массовый спрос, конкурентная цена",
        "yield_kg_per_sqm": 2.3,
        "power_kw_per_sqm": 20.0,
        "seed_cost_per_sqm": 105.0,
        "nutrition_cost_per_sqm": 72.0,
    },
    "Шпинат": {
        "cycle_days": (30, 45),
        "ph": "6.0–7.0",
        "ec": "1.6–2.0",
        "temperature_c": "15–20",
        "humidity_pct": "50–70",
        "light_hours": 14,
        "water_l_per_sqm_day": 2.5,
        "solution_change_days": 7,
        "germination_days": (7, 10),
        "daily_checks": "горечь нарастает при длинном дне >14 ч — строго соблюдай световой режим",
        "harvest_sign": "5–6 настоящих листьев, высота 15–20 см",
        "margin_note": "спрос со стороны HoReCa, диетпитание",
        "yield_kg_per_sqm": 2.8,
        "power_kw_per_sqm": 22.0,
        "seed_cost_per_sqm": 145.0,
        "nutrition_cost_per_sqm": 88.0,
    },
}


# ---------------------------------------------------------------------------
# Погода
# ---------------------------------------------------------------------------

def _get_weather_forecast(region: str) -> str:
    normalized = (region or "").strip() or "Москва"
    try:
        geo = requests.get(
            "https://geocoding-api.open-meteo.com/v1/search",
            params={"name": normalized, "count": 1, "language": "ru", "format": "json"},
            timeout=5,
        )
        geo.raise_for_status()
        results = geo.json().get("results") or []
    except requests.exceptions.RequestException:
        return WEATHER_ERROR_TEXT

    if not results:
        return f"Координаты для «{normalized}» не найдены"

    loc = results[0]
    lat, lon = loc.get("latitude"), loc.get("longitude")
    if lat is None or lon is None:
        return WEATHER_ERROR_TEXT

    try:
        wx = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat, "longitude": lon,
                "current": "temperature_2m,relative_humidity_2m",
                "daily": "temperature_2m_max,temperature_2m_min",
                "timezone": "auto",
            },
            timeout=5,
        )
        wx.raise_for_status()
        payload = wx.json()
    except requests.exceptions.RequestException:
        return WEATHER_ERROR_TEXT

    cur = payload.get("current", {})
    t, h = cur.get("temperature_2m"), cur.get("relative_humidity_2m")
    if t is None or h is None:
        return WEATHER_ERROR_TEXT

    daily = payload.get("daily", {})
    dates = daily.get("time", [])
    mx = daily.get("temperature_2m_max", [])
    mn = daily.get("temperature_2m_min", [])
    forecast = ", ".join(
        f"{d}: {lo}…{hi}°C"
        for d, lo, hi in list(zip(dates, mn, mx))[:3]
        if None not in (d, lo, hi)
    )
    name = loc.get("name", normalized)
    return f"{name}: сейчас {t}°C, влажность {h}%. Прогноз: {forecast}"


# ---------------------------------------------------------------------------
# Построение контекстных блоков
# ---------------------------------------------------------------------------

def _normalize_region(value: str) -> str:
    return " ".join(str(value or "").strip().lower().replace("ё", "е").split())


def _region_match(region_value: str, user_region: str) -> bool:
    rv = _normalize_region(region_value)
    ur = _normalize_region(user_region)
    return bool(rv and ur and ur in rv)


def _build_price_trend_context(user_region: str) -> str:
    """Формирует блок с текущими ценами + трендом за 7 и 30 дней."""
    now_df = get_latest_prices_frame()
    hist7_df = get_price_history_frame(days=7)
    hist30_df = get_price_history_frame(days=30)

    if now_df.empty:
        return "<PRICES>НЕТ ДАННЫХ</PRICES>"

    local_mask = now_df["region"].fillna("").apply(
        lambda v: _region_match(v, user_region)
    )
    local_df = now_df[local_mask]
    national_df = now_df

    def _median(df: pd.DataFrame, crop: str) -> float | None:
        sub = df[df["crop_name"] == crop]["wholesale_price"]
        return float(sub.median()) if not sub.empty else None

    lines = ["<PRICES>"]
    for scope_label, df in [
        (f"Локальные ({user_region or 'Москва'})", local_df),
        ("По РФ", national_df),
    ]:
        if df.empty:
            lines.append(f"  [{scope_label}]: нет данных")
            continue
        agg = (
            df.groupby("crop_name")["wholesale_price"]
            .agg(median="median", count="count")
            .sort_values("median", ascending=False)
        )
        lines.append(f"  [{scope_label}]:")
        for crop, row in agg.iterrows():
            m7 = _median(hist7_df, crop)
            m30 = _median(hist30_df, crop)

            if m7 and row["median"] and m7 > 0:
                delta7 = ((row["median"] - m7) / m7) * 100
                trend7 = f"{delta7:+.0f}% за 7 дн"
            else:
                trend7 = "нет истории"

            if m30 and row["median"] and m30 > 0:
                delta30 = ((row["median"] - m30) / m30) * 100
                trend30 = f"{delta30:+.0f}% за 30 дн"
            else:
                trend30 = "нет истории"

            lines.append(
                f"    - {crop}: {row['median']:.0f} руб/кг "
                f"({trend7}, {trend30}), лотов: {int(row['count'])}"
            )
    lines.append("</PRICES>")
    return "\n".join(lines)


def _build_demand_context(user_region: str) -> str:
    df = get_latest_demand_signals_frame()
    if df.empty:
        return "<TENDERS>НЕТ АКТИВНЫХ ТЕНДЕРОВ</TENDERS>"

    local_mask = df["region"].fillna("").apply(lambda v: _region_match(v, user_region))
    local = df[local_mask].sort_values(
        ["published_at", "contract_price"], ascending=[False, False]
    ).head(10)

    if local.empty:
        return "<TENDERS>Тендеров в регионе не найдено</TENDERS>"

    lines = ["<TENDERS>"]
    for row in local.itertuples(index=False):
        lines.append(
            f"  - Культура: {row.crop_name} | "
            f"Сумма: {row.contract_price:.0f} руб. | "
            f"Дата: {row.published_at.strftime('%Y-%m-%d')} | "
            f"URL: {row.url}"
        )
    lines.append("</TENDERS>")
    return "\n".join(lines)


def _build_news_context() -> str:
    items = get_recent_news(limit=5)
    if not items:
        return "<NEWS>НЕТ СВЕЖИХ НОВОСТЕЙ</NEWS>"
    lines = ["<NEWS>"]
    for item in items:
        lines.append(f"  - {item['published_at'].strftime('%Y-%m-%d')} | {item['title']}")
    lines.append("</NEWS>")
    return "\n".join(lines)


def _build_handbook_context(today: datetime) -> str:
    lines = ["<AGRO_HANDBOOK>"]
    for crop, p in AGRO_HANDBOOK.items():
        lo, hi = p["cycle_days"]
        harvest_from = (today + timedelta(days=lo)).strftime("%Y-%m-%d")
        harvest_to = (today + timedelta(days=hi)).strftime("%Y-%m-%d")
        glo, ghi = p["germination_days"]
        lines.append(
            f"  [{crop}]\n"
            f"    Цикл: {lo}–{hi} дн. | Прорастание: {glo}–{ghi} дн.\n"
            f"    Если посеять сегодня, сбор: {harvest_from} — {harvest_to}\n"
            f"    pH: {p['ph']} | EC: {p['ec']} мСм/см\n"
            f"    Температура: {p['temperature_c']}°C | Влажность: {p['humidity_pct']}%\n"
            f"    Освещение: {p['light_hours']} ч/сутки\n"
            f"    Полив: {p['water_l_per_sqm_day']} л/м²/сутки\n"
            f"    Смена раствора: каждые {p['solution_change_days']} дней (0 = не применимо)\n"
            f"    Урожайность: {p['yield_kg_per_sqm']} кг/м² за цикл\n"
            f"    Электроэнергия: {p['power_kw_per_sqm']} кВт*ч/м² за цикл\n"
            f"    Семена и субстрат: {p['seed_cost_per_sqm']} руб/м² за цикл\n"
            f"    Питание: {p['nutrition_cost_per_sqm']} руб/м² за цикл\n"
            f"    Ежедневный контроль: {p['daily_checks']}\n"
            f"    Признак готовности к срезу: {p['harvest_sign']}\n"
            f"    Коммерческая заметка: {p['margin_note']}"
        )
    lines.append("</AGRO_HANDBOOK>")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Промпт — системный
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """/no_think
Ты — опытный агроном-технолог сити-ферм (гидропоника, аэропоника). 
Твоя задача — давать КОНКРЕТНЫЕ, ПРОВЕРЯЕМЫЕ инструкции на основе ТОЛЬКО переданных данных.

ЖЁСТКИЕ ПРАВИЛА:
1. Используй ТОЛЬКО данные из блоков <AGRO_HANDBOOK>, <PRICES>, <TENDERS>, <NEWS>, <WEATHER>.
2. Если данных нет — пиши «данных нет», НЕ придумывай цифры.
3. Никаких эмодзи, никаких общих фраз («важно следить», «рекомендуется»).
4. Отвечай конкретными числами: pH 5.8, EC 1.4, t 22°C — не диапазонами, если не нужно.
5. При рекомендации культуры ВСЕГДА указывай: дату сбора, текущую цену, тренд, наличие тендера.
6. Если пользователь спрашивает про ежедневный уход — давай пошаговый чек-лист с конкретными действиями.
7. Никогда не повторяй вопрос пользователя.
8. Ты работаешь в двух режимах.
РЕЖИМ КАЛЬКУЛЯТОРА: Если пользователь говорит 'У меня есть X площади', рассчитай потенциальную выручку, вычти операционные расходы (OPEX = свет + семена + питание из AGRO_HANDBOOK) и назови чистую прибыль. Учитывай стоимость кВт/ч из FARM_STATE.
РЕЖИМ СОВЕТНИКА: Если пользователь говорит 'Хочу заработать Y рублей', иди от обратного. Вычисли маржинальность 1 кв.м для самых выгодных культур по текущим рыночным ценам. Раздели желаемую прибыль на маржу с 1 кв.м, чтобы сказать пользователю, СКОЛЬКО квадратных метров ему нужно засеять для достижения цели. Если это больше его текущей площади из FARM_STATE, предложи разбить цель на несколько циклов.

ФОРМАТ ОТВЕТА (строго):
— Короткий вывод (1–2 предложения, главная мысль)
— Затем детали: по пунктам или таблицей, без лирики
— В конце: «Следующий шаг» — одно конкретное действие прямо сейчас

ПРОЦЕСС РАССУЖДЕНИЯ (внутренний, не показывай пользователю):
Перед ответом мысленно пройди шаги:
  А) Что спрашивает пользователь — тип вопроса (выбор культуры / уход / цены / тендеры)?
  Б) Какие культуры подходят по условиям (t°, pH, EC из справочника)?
  В) Какая цена сейчас и какой тренд (рост/падение)?
  Г) Есть ли тендер — это приоритет?
  Д) Посчитать экономику 1 кв.м и всей фермы, если хватает данных из FARM_STATE и AGRO_HANDBOOK.
  Е) Сформировать конкретный ответ с датами и числами.
"""


# ---------------------------------------------------------------------------
# Основная функция чата
# ---------------------------------------------------------------------------

def chat_with_ai(user_message: str, history: list[dict], user_region: str, farm_profile: dict) -> str:
    now = datetime.now()
    region = (user_region or "").strip() or "Москва"
    farm_area = float((farm_profile or {}).get("total_area_sqm", 0.0) or 0.0)
    energy_price = float((farm_profile or {}).get("energy_price_kwh", 0.0) or 0.0)

    weather = _get_weather_forecast(region)
    prices_block = _build_price_trend_context(region)
    demand_block = _build_demand_context(region)
    news_block = _build_news_context()
    handbook_block = _build_handbook_context(now)
    farm_state_block = (
        "<FARM_STATE>\n"
        f"  total_area_sqm: {farm_area}\n"
        f"  energy_price_kwh: {energy_price}\n"
        "</FARM_STATE>"
    )
    system_prompt = f"{SYSTEM_PROMPT}\n\n{farm_state_block}"

    # Пользовательский промпт — структурированный контекст + вопрос
    user_prompt = (
        f"<CONTEXT>\n"
        f"Дата и время: {now.strftime('%Y-%m-%d %H:%M')}\n"
        f"Регион: {region}\n"
        f"<WEATHER>{weather}</WEATHER>\n\n"
        f"{farm_state_block}\n\n"
        f"{handbook_block}\n\n"
        f"{prices_block}\n\n"
        f"{demand_block}\n\n"
        f"{news_block}\n"
        f"</CONTEXT>\n\n"
        f"<QUESTION>{user_message}</QUESTION>"
    )

    messages: list[dict] = [{"role": "system", "content": system_prompt}]

    # История — только последние 6 сообщений чтобы не раздувать контекст
    for item in history[-6:]:
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
                "keep_alive": "1h",
                "options": {
                    # Снижаем температуру — меньше «творчества», больше точности
                    "temperature": 0.3,
                    # Nucleus sampling — отсекаем маловероятные токены
                    "top_p": 0.85,
                    # Штраф за повторение одних и тех же фраз
                    "repeat_penalty": 1.15,
                    # Контекстное окно Qwen 3.5:9b
                    "num_ctx": 8192,
                },
                "stop": ["<|im_end|>", "<|im_start|>", "<|endoftext|>", "</s>"],
            },
            timeout=None,
        )
        response.raise_for_status()
        content = response.json().get("message", {}).get("content") or ""

        # Убираем блок <think>...</think> если модель его генерирует
        if "<think>" in content:
            import re
            content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL).strip()

        return content or "Модель не вернула ответ."

    except requests.exceptions.RequestException as exc:
        return f"Ошибка соединения с Ollama: {exc}"
    except ValueError as exc:
        return f"Ошибка разбора ответа Ollama: {exc}"
