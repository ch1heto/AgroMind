from __future__ import annotations

import re
from datetime import datetime, timedelta

import requests

from agromind.calculator import EconomicsCalculator
from agromind.rag_retriever import DataRetriever
from agromind.services import (
    get_latest_demand_signals_frame,
    get_recent_news,
)


OLLAMA_URL = "http://localhost:11434/api/chat"
OLLAMA_MODEL = "qwen3.5:9b"
WEATHER_ERROR_TEXT = "Данные о погоде недоступны"

AGRO_HANDBOOK: dict[str, dict] = {
    "Базилик": {
        "cycle_days": (30, 40), "ph": "5.5–6.5", "ec": "1.0–1.6",
        "temperature_c": "20–25", "humidity_pct": "40–60", "light_hours": 16,
        "water_l_per_sqm_day": 2.5, "solution_change_days": 7, "germination_days": (5, 7),
        "daily_checks": "цвет листьев (пожелтение = дефицит N), запах раствора, pH и EC каждые 2 дня",
        "harvest_sign": "высота 15–20 см, до начала цветения",
        "margin_note": "высокомаржинальная, быстрый оборот",
        "yield_kg_per_sqm": 2.2, "power_kw_per_sqm": 24.0,
        "seed_cost_per_sqm": 180.0, "nutrition_cost_per_sqm": 95.0,
    },
    "Кинза": {
        "cycle_days": (30, 40), "ph": "6.0–6.7", "ec": "1.2–1.8",
        "temperature_c": "15–20", "humidity_pct": "50–70", "light_hours": 12,
        "water_l_per_sqm_day": 2.0, "solution_change_days": 7, "germination_days": (7, 10),
        "daily_checks": "влажность субстрата, отсутствие полегания, ровный рост",
        "harvest_sign": "высота 20–25 см, до стрелкования",
        "margin_note": "стабильный спрос, средняя маржа",
        "yield_kg_per_sqm": 1.8, "power_kw_per_sqm": 18.0,
        "seed_cost_per_sqm": 120.0, "nutrition_cost_per_sqm": 80.0,
    },
    "Лук зеленый": {
        "cycle_days": (20, 30), "ph": "6.0–7.0", "ec": "1.4–1.8",
        "temperature_c": "18–22", "humidity_pct": "60–70", "light_hours": 14,
        "water_l_per_sqm_day": 2.0, "solution_change_days": 10, "germination_days": (3, 5),
        "daily_checks": "пожелтение кончиков (избыток соли), равномерность роста",
        "harvest_sign": "высота 25–30 см",
        "margin_note": "быстрый цикл, хорошая оборачиваемость",
        "yield_kg_per_sqm": 3.4, "power_kw_per_sqm": 16.0,
        "seed_cost_per_sqm": 140.0, "nutrition_cost_per_sqm": 75.0,
    },
    "Микрозелень": {
        "cycle_days": (7, 10), "ph": "5.5–6.5", "ec": "0.8–1.2",
        "temperature_c": "20–24", "humidity_pct": "50–60", "light_hours": 12,
        "water_l_per_sqm_day": 1.5, "solution_change_days": 0, "germination_days": (2, 3),
        "daily_checks": "влажность мата, отсутствие плесени, равномерность проростков",
        "harvest_sign": "семядольные листья раскрыты, первая пара настоящих листьев только начинается",
        "margin_note": "самый короткий цикл, максимальная оборачиваемость",
        "yield_kg_per_sqm": 1.4, "power_kw_per_sqm": 7.0,
        "seed_cost_per_sqm": 260.0, "nutrition_cost_per_sqm": 35.0,
    },
    "Мята": {
        "cycle_days": (45, 60), "ph": "5.5–6.0", "ec": "1.6–2.0",
        "temperature_c": "18–22", "humidity_pct": "70–80", "light_hours": 14,
        "water_l_per_sqm_day": 3.0, "solution_change_days": 7, "germination_days": (10, 14),
        "daily_checks": "влажность воздуха, состояние корней (гниль при застое), аромат",
        "harvest_sign": "побеги 15–20 см, срез над 2-м узлом",
        "margin_note": "длинный цикл, нишевый спрос, высокая цена",
        "yield_kg_per_sqm": 2.6, "power_kw_per_sqm": 32.0,
        "seed_cost_per_sqm": 210.0, "nutrition_cost_per_sqm": 120.0,
    },
    "Петрушка": {
        "cycle_days": (40, 60), "ph": "5.5–6.0", "ec": "1.2–1.8",
        "temperature_c": "15–20", "humidity_pct": "50–70", "light_hours": 14,
        "water_l_per_sqm_day": 2.0, "solution_change_days": 7, "germination_days": (14, 21),
        "daily_checks": "цвет листьев, равномерность роста, EC раствора",
        "harvest_sign": "высота 20–25 см, листья насыщенно-зелёные",
        "margin_note": "долгое прорастание, но стабильный спрос",
        "yield_kg_per_sqm": 2.0, "power_kw_per_sqm": 26.0,
        "seed_cost_per_sqm": 130.0, "nutrition_cost_per_sqm": 90.0,
    },
    "Руккола": {
        "cycle_days": (25, 35), "ph": "6.0–6.5", "ec": "1.2–1.8",
        "temperature_c": "15–20", "humidity_pct": "50–70", "light_hours": 14,
        "water_l_per_sqm_day": 2.0, "solution_change_days": 7, "germination_days": (4, 6),
        "daily_checks": "горечь листьев нарастает при перегреве — следи за t°, аромат",
        "harvest_sign": "высота 10–15 см, до появления стрелки",
        "margin_note": "ресторанный сегмент, премиальная цена",
        "yield_kg_per_sqm": 1.7, "power_kw_per_sqm": 18.0,
        "seed_cost_per_sqm": 150.0, "nutrition_cost_per_sqm": 70.0,
    },
    "Салат": {
        "cycle_days": (35, 45), "ph": "5.5–6.0", "ec": "0.8–1.2",
        "temperature_c": "16–20", "humidity_pct": "60–70", "light_hours": 16,
        "water_l_per_sqm_day": 2.5, "solution_change_days": 7, "germination_days": (3, 5),
        "daily_checks": "краевой ожог листьев (недостаток Ca), равномерность розетки",
        "harvest_sign": "розетка диаметром 20–25 см",
        "margin_note": "высокий объёмный спрос, госзакупки",
        "yield_kg_per_sqm": 3.1, "power_kw_per_sqm": 26.0,
        "seed_cost_per_sqm": 110.0, "nutrition_cost_per_sqm": 85.0,
    },
    "Салат айсберг": {
        "cycle_days": (45, 60), "ph": "5.5–6.5", "ec": "1.0–1.5",
        "temperature_c": "15–18", "humidity_pct": "60–70", "light_hours": 16,
        "water_l_per_sqm_day": 2.5, "solution_change_days": 7, "germination_days": (3, 5),
        "daily_checks": "формирование кочана, краевой ожог",
        "harvest_sign": "кочан плотный, диаметр 15–20 см",
        "margin_note": "крупные контракты HoReCa и сети",
        "yield_kg_per_sqm": 4.2, "power_kw_per_sqm": 30.0,
        "seed_cost_per_sqm": 135.0, "nutrition_cost_per_sqm": 100.0,
    },
    "Укроп": {
        "cycle_days": (30, 40), "ph": "5.5–6.5", "ec": "1.0–1.6",
        "temperature_c": "15–20", "humidity_pct": "50–70", "light_hours": 14,
        "water_l_per_sqm_day": 2.0, "solution_change_days": 7, "germination_days": (7, 14),
        "daily_checks": "пожелтение (дефицит Mg), равномерность всходов",
        "harvest_sign": "высота 25–30 см, до зонтика",
        "margin_note": "массовый спрос, конкурентная цена",
        "yield_kg_per_sqm": 2.3, "power_kw_per_sqm": 20.0,
        "seed_cost_per_sqm": 105.0, "nutrition_cost_per_sqm": 72.0,
    },
    "Шпинат": {
        "cycle_days": (30, 45), "ph": "6.0–7.0", "ec": "1.6–2.0",
        "temperature_c": "15–20", "humidity_pct": "50–70", "light_hours": 14,
        "water_l_per_sqm_day": 2.5, "solution_change_days": 7, "germination_days": (7, 10),
        "daily_checks": "горечь нарастает при длинном дне >14 ч — строго соблюдай световой режим",
        "harvest_sign": "5–6 настоящих листьев, высота 15–20 см",
        "margin_note": "спрос со стороны HoReCa, диетпитание",
        "yield_kg_per_sqm": 2.8, "power_kw_per_sqm": 22.0,
        "seed_cost_per_sqm": 145.0, "nutrition_cost_per_sqm": 88.0,
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
# Вспомогательные функции
# ---------------------------------------------------------------------------

def _normalize_region(value: str) -> str:
    return " ".join(str(value or "").strip().lower().replace("ё", "е").split())


def _region_match(region_value: str, user_region: str) -> bool:
    rv = _normalize_region(region_value)
    ur = _normalize_region(user_region)
    return bool(rv and ur and ur in rv)


def _detect_requested_culture(user_message: str, history: list[dict]) -> str:
    searchable = " ".join(
        [*(str(item.get("content", "")) for item in history[-4:]), user_message]
    ).lower()
    for name in sorted(AGRO_HANDBOOK.keys(), key=len, reverse=True):
        if name.lower() in searchable:
            return name
    return ""


def _extract_numbers_from_message(
    user_message: str, farm_profile: dict
) -> tuple[float, float]:
    """Извлекает площадь и тариф из сообщения, фолбэк — из farm_profile."""
    farm_area = float((farm_profile or {}).get("total_area_sqm", 0.0) or 0.0)
    energy_price = float((farm_profile or {}).get("energy_price_kwh", 0.0) or 0.0)

    area_patterns = [
        r"(\d+[\.,]?\d*)\s*(?:кв\.?\s*м|м2|m2|квадрат\w*)",
        r"(?:площадь|площади)[^\d]*(\d+[\.,]?\d*)",
        r"(\d+[\.,]?\d*)\s*(?:кв|sq)\b",
    ]
    for pat in area_patterns:
        m = re.search(pat, user_message, re.IGNORECASE)
        if m:
            try:
                farm_area = float(m.group(1).replace(",", "."))
                break
            except ValueError:
                pass

    tariff_patterns = [
        r"тариф[^\d]*(\d+[\.,]?\d*)",
        r"(\d+[\.,]?\d*)\s*руб[^\w]*(?:за\s*)?(?:квт|кВт|kw)",
        r"электр\w*[^\d]*(\d+[\.,]?\d*)",
        r"(?:свет|электро)[^\d]*(\d+[\.,]?\d*)",
    ]
    for pat in tariff_patterns:
        m = re.search(pat, user_message, re.IGNORECASE)
        if m:
            try:
                val = float(m.group(1).replace(",", "."))
                if 1.0 <= val <= 30.0:
                    energy_price = val
                    break
            except ValueError:
                pass

    return farm_area, energy_price


def _extract_average_price(market_summary: str) -> float | None:
    m = re.search(r"Средняя цена:\s*([\d.]+)", market_summary)
    if not m:
        return None
    try:
        return float(m.group(1))
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Контекстные блоки
# ---------------------------------------------------------------------------

def _build_handbook_context_filtered(today: datetime, culture: str) -> str:
    """
    Полная карточка — только для запрошенной культуры.
    Без культуры — компактная таблица всех (~300 токенов вместо ~1800).
    """
    lines = ["<AGRO_HANDBOOK>"]

    if culture and culture in AGRO_HANDBOOK:
        p = AGRO_HANDBOOK[culture]
        lo, hi = p["cycle_days"]
        glo, ghi = p["germination_days"]
        harvest_from = (today + timedelta(days=lo)).strftime("%Y-%m-%d")
        harvest_to = (today + timedelta(days=hi)).strftime("%Y-%m-%d")
        lines.append(
            f"  [{culture}]\n"
            f"    Цикл: {lo}–{hi} дн. | Прорастание: {glo}–{ghi} дн.\n"
            f"    Посев сегодня → сбор: {harvest_from} — {harvest_to}\n"
            f"    pH: {p['ph']} | EC: {p['ec']} мСм/см\n"
            f"    t°: {p['temperature_c']}°C | Влажность: {p['humidity_pct']}%\n"
            f"    Свет: {p['light_hours']} ч/сут | Вода: {p['water_l_per_sqm_day']} л/м²/сут\n"
            f"    Смена раствора: каждые {p['solution_change_days']} дн.\n"
            f"    Урожай: {p['yield_kg_per_sqm']} кг/м² | Свет-расход: {p['power_kw_per_sqm']} кВт·ч/м²\n"
            f"    Семена+субстрат: {p['seed_cost_per_sqm']} руб/м² | Питание: {p['nutrition_cost_per_sqm']} руб/м²\n"
            f"    Контроль: {p['daily_checks']}\n"
            f"    Готовность: {p['harvest_sign']}\n"
            f"    Коммерция: {p['margin_note']}"
        )
    else:
        lines.append("  Культура | цикл дн | pH | EC | урожай кг/м² | затраты руб/м²")
        for name, p in AGRO_HANDBOOK.items():
            lo, hi = p["cycle_days"]
            hf = (today + timedelta(days=lo)).strftime("%d.%m")
            ht = (today + timedelta(days=hi)).strftime("%d.%m")
            costs = p["seed_cost_per_sqm"] + p["nutrition_cost_per_sqm"]
            lines.append(
                f"  {name}: {lo}–{hi} дн ({hf}–{ht}) | "
                f"pH {p['ph']} | EC {p['ec']} | "
                f"{p['yield_kg_per_sqm']} кг/м² | ~{costs:.0f} руб/м²"
            )

    lines.append("</AGRO_HANDBOOK>")
    return "\n".join(lines)


def _build_demand_context(user_region: str) -> str:
    df = get_latest_demand_signals_frame()
    if df.empty:
        return "<TENDERS>НЕТ АКТИВНЫХ ТЕНДЕРОВ</TENDERS>"

    local_mask = df["region"].fillna("").apply(lambda v: _region_match(v, user_region))
    local = df[local_mask].sort_values(
        ["published_at", "contract_price"], ascending=[False, False]
    ).head(5)

    if local.empty:
        return "<TENDERS>Тендеров в регионе не найдено</TENDERS>"

    lines = ["<TENDERS>"]
    for row in local.itertuples(index=False):
        lines.append(
            f"  - {row.crop_name} | {row.contract_price:.0f} руб. | "
            f"{row.published_at.strftime('%Y-%m-%d')} | {row.url}"
        )
    lines.append("</TENDERS>")
    return "\n".join(lines)


def _build_news_context() -> str:
    items = get_recent_news(limit=3)
    if not items:
        return "<NEWS>НЕТ СВЕЖИХ НОВОСТЕЙ</NEWS>"
    lines = ["<NEWS>"]
    for item in items:
        lines.append(f"  - {item['published_at'].strftime('%Y-%m-%d')} | {item['title']}")
    lines.append("</NEWS>")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Системный промпт
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """/no_think
Ты — агроном-технолог сити-фермы (гидропоника, аэропоника). Даёшь КОНКРЕТНЫЕ инструкции только на основе переданных данных.

ПРАВИЛА:
1. Только данные из <AGRO_HANDBOOK>, <PRICES>, <TENDERS>, <NEWS>, <WEATHER>, <FARM_STATE>, <CALCULATED_ECONOMICS>.
2. Нет данных — пиши «данных нет». Не придумывай цифры.
3. Никаких эмодзи. Конкретные числа вместо диапазонов.
4. При рекомендации культуры: дата сбора + цена + тенденция + тендер.
5. Финансы — только из <CALCULATED_ECONOMICS>, сам не считай.
6. Уход — пошаговый чек-лист с числами.

РЕЖИМ КАЛЬКУЛЯТОРА: есть площадь → выручка и прибыль из CALCULATED_ECONOMICS.
РЕЖИМ СОВЕТНИКА: «хочу заработать Y» → площадь = Y / (прибыль на 1 м²) из CALCULATED_ECONOMICS.

ФОРМАТ:
— Вывод (1–2 предложения)
— Детали по пунктам или таблицей
— «Следующий шаг» — одно действие прямо сейчас
"""


# ---------------------------------------------------------------------------
# Основная функция
# ---------------------------------------------------------------------------

def chat_with_ai(
    user_message: str,
    history: list[dict],
    user_region: str,
    farm_profile: dict,
) -> str:
    now = datetime.now()
    region = (user_region or "").strip() or "Москва"

    farm_area, energy_price = _extract_numbers_from_message(user_message, farm_profile)
    requested_culture = _detect_requested_culture(user_message, history)

    retriever = DataRetriever()
    market_summary = retriever.get_aggregated_context(requested_culture, region)
    market_price_per_kg = _extract_average_price(market_summary)
    if market_price_per_kg is None:
        market_price_per_kg = 0.0

    weather = _get_weather_forecast(region)
    handbook_block = _build_handbook_context_filtered(now, requested_culture)
    demand_block = _build_demand_context(region)
    news_block = _build_news_context()

    farm_state_block = (
        "<FARM_STATE>\n"
        f"  Площадь: {farm_area} м²\n"
        f"  Тариф: {energy_price} руб/кВт·ч\n"
        "</FARM_STATE>"
    )
    prices_block = f"<PRICES>{market_summary}</PRICES>"

    if (
        requested_culture and requested_culture in AGRO_HANDBOOK
        and farm_area > 0 and energy_price > 0 and market_price_per_kg > 0
    ):
        economics = EconomicsCalculator.calculate_cycle_economics(
            area_sqm=farm_area,
            energy_price_kwh=energy_price,
            market_price_per_kg=market_price_per_kg,
            culture_data=AGRO_HANDBOOK[requested_culture],
        )
        per_sqm = economics["net_profit"] / farm_area
        economics_block = (
            "<CALCULATED_ECONOMICS>\n"
            f"  {requested_culture} | {farm_area} м²\n"
            f"  Свет: {economics['energy_cost']:.0f} руб | "
            f"Материалы: {economics['materials_cost']:.0f} руб | "
            f"Расходы: {economics['total_expenses']:.0f} руб\n"
            f"  Урожай: {economics['expected_yield_kg']:.1f} кг | "
            f"Выручка: {economics['expected_revenue']:.0f} руб | "
            f"Прибыль: {economics['net_profit']:.0f} руб\n"
            f"  Прибыль/м²: {per_sqm:.0f} руб\n"
            "</CALCULATED_ECONOMICS>"
        )
    else:
        missing = []
        if not requested_culture:
            missing.append("культура не определена")
        if farm_area <= 0:
            missing.append("площадь не указана")
        if energy_price <= 0:
            missing.append("тариф не указан")
        if market_price_per_kg <= 0:
            missing.append("рыночная цена недоступна")
        economics_block = (
            f"<CALCULATED_ECONOMICS>Расчёт невозможен: {', '.join(missing)}.</CALCULATED_ECONOMICS>"
        )

    system_prompt = (
        f"{SYSTEM_PROMPT}\n\n"
        f"{farm_state_block}\n\n"
        f"{prices_block}\n\n"
        f"{economics_block}"
    )

    user_prompt = (
        f"<CONTEXT>\n"
        f"Дата: {now.strftime('%Y-%m-%d %H:%M')} | Регион: {region}\n"
        f"<WEATHER>{weather}</WEATHER>\n\n"
        f"{handbook_block}\n\n"
        f"{demand_block}\n\n"
        f"{news_block}\n"
        f"</CONTEXT>\n\n"
        f"<QUESTION>{user_message}</QUESTION>"
    )

    messages: list[dict] = [{"role": "system", "content": system_prompt}]

    for item in history[-4:]:
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
                    "temperature": 0.3,
                    "top_p": 0.85,
                    "repeat_penalty": 1.15,
                    "num_ctx": 4096,    # Было 8192 — вдвое меньше VRAM под KV-cache
                    "num_gpu": 99,       # Все слои на GPU принудительно
                    "num_predict": 512,  # Ограничение длины ответа
                },
                "stop": ["<|im_end|>", "<|im_start|>", "<|endoftext|>", "</s>"],
            },
            timeout=None,
        )
        response.raise_for_status()
        content = response.json().get("message", {}).get("content") or ""

        if "<think>" in content:
            content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL).strip()

        return content or "Модель не вернула ответ."

    except requests.exceptions.RequestException as exc:
        return f"Ошибка соединения с Ollama: {exc}"
    except ValueError as exc:
        return f"Ошибка разбора ответа Ollama: {exc}"
