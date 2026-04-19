from __future__ import annotations

from difflib import get_close_matches
from datetime import datetime, timedelta
import math
import re
from typing import Any

import requests

from agromind.calculator import EconomicsCalculator
from agromind.database import get_active_plant
from agromind.influx_client import get_aggregated_prices


OLLAMA_URL = "http://localhost:11434/api/chat"
OLLAMA_MODEL = "qwen2.5:3b"
WEATHER_ERROR_TEXT = "Температура +5°C, пасмурно, высокая влажность."
DEFAULT_REGION = "Москва"
EASIEST_CULTURE = "Салат"
HIGH_MARGIN_CULTURE = "Базилик"

AGRO_HANDBOOK: dict[str, dict[str, Any]] = {
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
        "daily_checks": "цвет листьев, запах раствора, pH и EC каждые 2 дня",
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
        "daily_checks": "пожелтение кончиков, равномерность роста",
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
        "harvest_sign": "семядольные листья раскрыты, первые настоящие листья только появляются",
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
        "daily_checks": "влажность воздуха, состояние корней, аромат",
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
        "daily_checks": "контроль перегрева, горечи и аромата",
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
        "daily_checks": "краевой ожог листьев, равномерность розетки",
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
        "daily_checks": "пожелтение, равномерность всходов",
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
        "daily_checks": "контроль длины светового дня и горечи",
        "harvest_sign": "5–6 настоящих листьев, высота 15–20 см",
        "margin_note": "спрос со стороны HoReCa и диетического питания",
        "yield_kg_per_sqm": 2.8,
        "power_kw_per_sqm": 22.0,
        "seed_cost_per_sqm": 145.0,
        "nutrition_cost_per_sqm": 88.0,
    },
}

SYSTEM_PROMPT = """
Ты — агроном-консультант для сити-фермы. Твоя задача — спокойно, понятно и доброжелательно объяснить только те факты, которые уже переданы в блоке <CALCULATED_ECONOMICS>.
Строго запрещено самостоятельно считать, пересчитывать, оценивать или выводить новые цифры. Используй только те числа, которые явно указаны в контексте.
СТРОГО ЗАПРЕЩЕНО советовать, упоминать или предлагать культуры, которых нет в блоке <CALCULATED_ECONOMICS>. Никаких огурцов, томатов или клубники. Ты работаешь только с теми зеленными культурами, что передал скрипт.
Если в контексте нет точного финансового расчета или прямо сказано, что данных недостаточно, в конце ответа обязательно задай один вежливый уточняющий вопрос.
Если в контексте есть указание попросить пользователя заполнить тариф в настройках дашборда, сформулируй это естественно и по-человечески, без упоминания внутренней логики скрипта.
Дай ровно одну рекомендацию по уходу на основе блока <WEATHER>.
Пиши кратко, фактически и на языке пользователя. Не придумывай цены, прибыль, площадь, урожайность, расходы или длительность цикла.
Структура ответа:
1. Короткий вывод в 1-2 предложениях.
2. 2-4 пункта с фактами из <CALCULATED_ECONOMICS>.
3. Ровно одна рекомендация по уходу на основе <WEATHER>.
4. Один уточняющий вопрос только если в контексте не хватает данных для точного финансового ответа.
""".strip()

SYSTEM_PROMPT = """
Ты — агроном-консультант для сити-фермы. Твоя задача — спокойно, понятно и доброжелательно объяснить только те факты, которые уже переданы в блоке <CALCULATED_ECONOMICS>.
Правила:
1. Строго запрещено самостоятельно считать, пересчитывать, оценивать или выводить новые цифры. Используй только те числа, которые явно указаны в контексте.
2. СТРОГО ЗАПРЕЩЕНО советовать, упоминать или предлагать культуры, которых нет в блоке <CALCULATED_ECONOMICS>. Никаких огурцов, томатов или клубники. Ты работаешь только с теми зелёными культурами, что передал скрипт.
3. Если в контексте нет точного финансового расчёта или прямо сказано, что данных недостаточно, в конце ответа обязательно задай один вежливый уточняющий вопрос.
4. Если в контексте есть указание попросить пользователя заполнить тариф в настройках дашборда, сформулируй это естественно и по-человечески, без упоминания внутренней логики скрипта.
5. Если в контексте есть блок <FARM_STATE>, используй эту информацию для персонализации ответа. Например, если пользователь жалуется на проблему, свяжи её с текущим днём цикла: стадия всходов, вегетация или сбор.
6. Дай ровно одну рекомендацию по уходу на основе блока <WEATHER>.
Пиши кратко, фактически и на языке пользователя. Не придумывай цены, прибыль, площадь, урожайность, расходы или длительность цикла.
Структура ответа:
1. Короткий вывод в 1-2 предложениях.
2. 2-4 пункта с фактами из <CALCULATED_ECONOMICS>.
3. Ровно одна рекомендация по уходу на основе <WEATHER>.
4. Один уточняющий вопрос только если в контексте не хватает данных для точного финансового ответа.
""".strip()


def _normalize_text(value: str) -> str:
    cleaned = str(value or "").strip().lower().replace("ё", "е")
    number_word_replacements = {
        "один": "1",
        "два": "2",
        "три": "3",
        "пять": "5",
        "десять": "10",
        "пятьдесят": "50",
        "сто": "100",
    }
    for word, number in number_word_replacements.items():
        cleaned = cleaned.replace(word, number)
    cleaned = re.sub(r"[^\w\s.%/+:-]", " ", cleaned)
    return " ".join(cleaned.split())


def _normalize_region(value: str) -> str:
    normalized = _normalize_text(value)
    return normalized.title() if normalized else DEFAULT_REGION


def _parse_number(value_text: str, suffix: str = "") -> float | None:
    raw_value = (value_text or "").replace(" ", "").replace(",", ".")
    if not raw_value:
        return None

    try:
        value = float(raw_value)
    except ValueError:
        return None

    normalized_suffix = _normalize_text(suffix)
    if normalized_suffix in {"к", "тыщ", "тыща", "тыщи", "тыс", "тысяч", "тысяча"}:
        value *= 1_000
    elif normalized_suffix in {"м", "млн", "миллион", "миллиона", "миллионов"}:
        value *= 1_000_000
    return value


def _extract_area_sqm(normalized_message: str) -> float | None:
    trays_match = re.search(
        r"(?P<trays>\d+)\s*поддон\w*.*?(?P<cups>\d+)\s*стакан\w*",
        normalized_message,
        flags=re.IGNORECASE,
    )
    if trays_match:
        trays = _parse_number(trays_match.group("trays"))
        cups = _parse_number(trays_match.group("cups"))
        if trays and cups and trays > 0 and cups > 0:
            return (trays * cups) / 50.0

    patterns = (
        r"(?P<value>\d+(?:[.,]\d+)?)\s*(?:м2|м²|кв\.?\s*м|кв\b|квадрат(?:а|ов)?|метр(?:а|ов)?(?:\s+квадрат\w+)?)",
        r"(?:площад[ья]|помещени[ея]|комнат[аы])[^0-9]{0,20}(?P<value>\d+(?:[.,]\d+)?)\s*(?:м2|м²|кв\.?\s*м|кв\b|квадрат(?:а|ов)?)?",
    )
    for pattern in patterns:
        match = re.search(pattern, normalized_message, flags=re.IGNORECASE)
        if not match:
            continue
        value = _parse_number(match.group("value"))
        if value and value > 0:
            return value
    return None


def _extract_target_budget(normalized_message: str) -> float | None:
    patterns = (
        r"(?:бюджет|влож(?:ить|ения|усь|ение)?|капитал|инвест(?:ировать|иций|иции)?)[^0-9]{0,20}(?P<value>\d+(?:[.,]\d+)?)\s*(?P<suffix>к|тыщ(?:а|и)?|тыс(?:яч)?|млн|м)?",
        r"(?:хочу\s+заработать|заработать|доход|выручк[аи]|прибыл[ьи])[^0-9]{0,20}(?P<value>\d+(?:[.,]\d+)?)\s*(?P<suffix>к|тыщ(?:а|и)?|тыс(?:яч)?|млн|м)?",
    )
    for pattern in patterns:
        match = re.search(pattern, normalized_message, flags=re.IGNORECASE)
        if not match:
            continue
        value = _parse_number(match.group("value"), match.group("suffix") or "")
        if value and value > 0:
            return value
    return None


def _extract_culture(normalized_message: str) -> str | None:
    normalized_lookup = {_normalize_text(name): name for name in AGRO_HANDBOOK}

    for normalized_name, original_name in normalized_lookup.items():
        if normalized_name in normalized_message:
            return original_name

    tokens = re.findall(r"[a-zа-я0-9]+", normalized_message)
    candidates: set[str] = {normalized_message}
    max_window = min(3, len(tokens))
    for size in range(1, max_window + 1):
        for idx in range(len(tokens) - size + 1):
            candidates.add(" ".join(tokens[idx : idx + size]))

    for candidate in sorted(candidates, key=len, reverse=True):
        matches = get_close_matches(candidate, list(normalized_lookup.keys()), n=1, cutoff=0.72)
        if matches:
            return normalized_lookup[matches[0]]

    return None


def extract_user_intent(user_message: str) -> dict[str, str | float | None]:
    normalized_message = _normalize_text(user_message)
    return {
        "culture": _extract_culture(normalized_message),
        "area_sqm": _extract_area_sqm(normalized_message),
        "target_budget": _extract_target_budget(normalized_message),
    }


def _extract_energy_price(user_message: str, farm_profile: dict[str, Any]) -> float | None:
    normalized_message = _normalize_text(user_message)
    patterns = (
        r"тариф[^0-9]{0,20}(?P<value>\d+(?:[.,]\d+)?)",
        r"электр\w*[^0-9]{0,20}(?P<value>\d+(?:[.,]\d+)?)",
        r"(?P<value>\d+(?:[.,]\d+)?)\s*руб[^a-zа-я0-9]{0,8}(?:за\s*)?(?:квт|квтч|квт\*ч|kw)",
    )
    for pattern in patterns:
        match = re.search(pattern, normalized_message, flags=re.IGNORECASE)
        if not match:
            continue
        value = _parse_number(match.group("value"))
        if value and 0 < value <= 100:
            return value

    fallback = float((farm_profile or {}).get("energy_price_kwh", 0.0) or 0.0)
    return fallback if fallback > 0 else None


def _get_market_snapshot(culture: str, region: str) -> tuple[dict[str, Any] | None, str]:
    normalized_region = _normalize_region(region)
    try:
        region_snapshot = get_aggregated_prices(culture, normalized_region)
    except Exception:
        region_snapshot = None

    if region_snapshot:
        return region_snapshot, normalized_region

    try:
        global_snapshot = get_aggregated_prices(culture, "")
    except Exception:
        global_snapshot = None

    if global_snapshot:
        return global_snapshot, "Россия"

    return None, normalized_region


def _format_currency(value: float) -> str:
    return f"{value:,.0f}".replace(",", " ")


def _format_crop_conditions(culture: str, today: datetime) -> str:
    culture_data = AGRO_HANDBOOK[culture]
    cycle_from, cycle_to = culture_data["cycle_days"]
    harvest_from = (today + timedelta(days=cycle_from)).strftime("%Y-%m-%d")
    harvest_to = (today + timedelta(days=cycle_to)).strftime("%Y-%m-%d")
    return (
        f"{culture}: цикл {cycle_from}-{cycle_to} дней, pH {culture_data['ph']}, "
        f"EC {culture_data['ec']}, температура {culture_data['temperature_c']}°C, "
        f"влажность {culture_data['humidity_pct']}%, свет {culture_data['light_hours']} ч/сут, "
        f"ориентир по сбору при посеве сегодня {harvest_from} - {harvest_to}."
    )


def _build_exact_match_context(
    culture: str,
    area_sqm: float,
    region: str,
    energy_price_kwh: float | None,
    target_budget: float | None,
    today: datetime,
) -> str:
    snapshot, price_scope = _get_market_snapshot(culture, region)
    culture_data = AGRO_HANDBOOK[culture]
    lines = [
        "<CALCULATED_ECONOMICS>",
        "scenario: exact_match",
        "clarification_required: no",
        f"Культура: {culture}",
        f"Площадь: {area_sqm:.1f} м²",
    ]

    if target_budget:
        lines.append(f"Целевая сумма пользователя: {_format_currency(target_budget)} руб.")

    lines.append(_format_crop_conditions(culture, today))

    if energy_price_kwh is None:
        lines[2] = "clarification_required: yes"
        lines.append("Финансовый расчёт не завершён: не указан тариф на электроэнергию.")
        lines.append("</CALCULATED_ECONOMICS>")
        return "\n".join(lines)

    lines.append(f"Тариф электроэнергии: {energy_price_kwh:.2f} руб/кВт·ч")

    if not snapshot or snapshot.get("avg") is None:
        lines[2] = "clarification_required: yes"
        lines.append("Финансовый расчёт не завершён: нет рыночной цены за последние 7 дней.")
        lines.append("</CALCULATED_ECONOMICS>")
        return "\n".join(lines)

    economics = EconomicsCalculator.calculate_cycle_economics(
        area_sqm=area_sqm,
        energy_price_kwh=energy_price_kwh,
        market_price_per_kg=float(snapshot["avg"]),
        culture_data=culture_data,
    )
    cycle_from, cycle_to = culture_data["cycle_days"]
    lines.extend(
        [
            f"Источник цены: {price_scope}",
            f"Средняя рыночная цена: {float(snapshot['avg']):.2f} руб/кг",
            f"Диапазон цены: {float(snapshot['min']):.2f}-{float(snapshot['max']):.2f} руб/кг",
            f"Наблюдений за 7 дней: {int(snapshot['count'])}",
            f"Длительность цикла: {cycle_from}-{cycle_to} дней",
            f"Расходы на свет: {_format_currency(economics['energy_cost'])} руб.",
            f"Расходы на материалы: {_format_currency(economics['materials_cost'])} руб.",
            f"Общие расходы: {_format_currency(economics['total_expenses'])} руб.",
            f"Ожидаемый урожай: {economics['expected_yield_kg']:.1f} кг",
            f"Ожидаемая выручка: {_format_currency(economics['expected_revenue'])} руб.",
            f"Ожидаемая чистая прибыль: {_format_currency(economics['net_profit'])} руб.",
        ]
    )

    if target_budget and 0 < economics["net_profit"] < target_budget:
        cycles_needed = math.ceil(target_budget / economics["net_profit"])
        area_needed = (target_budget / economics["net_profit"]) * area_sqm
        lines.extend(
            [
                "Альтернативный путь до целевой суммы:",
                f"Путь 1 — циклы: чтобы выйти на {_format_currency(target_budget)} руб., нужно примерно {cycles_needed} циклов при той же площади.",
                f"Путь 2 — площадь: чтобы выйти на {_format_currency(target_budget)} руб. за один цикл, нужна площадь около {area_needed:.1f} м².",
            ]
        )

    lines.append("</CALCULATED_ECONOMICS>")
    return "\n".join(lines)


def _build_profit_hunt_context(
    area_sqm: float,
    region: str,
    energy_price_kwh: float | None,
    target_budget: float | None,
    today: datetime,
) -> str:
    lines = [
        "<CALCULATED_ECONOMICS>",
        "scenario: profit_hunt",
        "clarification_required: yes",
        f"Площадь: {area_sqm:.1f} м²",
        "Формат запроса: нужна лучшая культура по прибыли, но культура явно не указана.",
    ]

    if target_budget:
        lines.append(f"Целевая сумма пользователя: {_format_currency(target_budget)} руб.")

    if energy_price_kwh is None:
        lines.append("Финансовое ранжирование недоступно: не указан тариф на электроэнергию.")
        lines.append("</CALCULATED_ECONOMICS>")
        return "\n".join(lines)

    lines.append(f"Тариф электроэнергии: {energy_price_kwh:.2f} руб/кВт·ч")

    scored_cultures: list[dict[str, Any]] = []
    for culture, culture_data in AGRO_HANDBOOK.items():
        snapshot, price_scope = _get_market_snapshot(culture, region)
        if not snapshot or snapshot.get("avg") is None:
            continue

        economics = EconomicsCalculator.calculate_cycle_economics(
            area_sqm=area_sqm,
            energy_price_kwh=energy_price_kwh,
            market_price_per_kg=float(snapshot["avg"]),
            culture_data=culture_data,
        )
        scored_cultures.append(
            {
                "culture": culture,
                "price_scope": price_scope,
                "market_avg": float(snapshot["avg"]),
                "net_profit": float(economics["net_profit"]),
                "total_expenses": float(economics["total_expenses"]),
                "expected_revenue": float(economics["expected_revenue"]),
                "cycle_days": culture_data["cycle_days"],
            }
        )

    if not scored_cultures:
        lines.append("Финансовое ранжирование недоступно: в базе нет актуальных рыночных цен по культурам.")
        lines.append("</CALCULATED_ECONOMICS>")
        return "\n".join(lines)

    top_cultures = sorted(scored_cultures, key=lambda item: item["net_profit"], reverse=True)[:2]
    lines.append("Топ-2 культуры по ожидаемой чистой прибыли для указанной площади:")
    for index, item in enumerate(top_cultures, start=1):
        cycle_from, cycle_to = item["cycle_days"]
        lines.append(
            f"{index}. {item['culture']} — прибыль {_format_currency(item['net_profit'])} руб., "
            f"выручка {_format_currency(item['expected_revenue'])} руб., расходы {_format_currency(item['total_expenses'])} руб., "
            f"средняя цена {item['market_avg']:.2f} руб/кг, цикл {cycle_from}-{cycle_to} дней, источник цены {item['price_scope']}."
        )

    lines.append(f"Быстрый старт для новичка: {_format_crop_conditions(EASIEST_CULTURE, today)}")
    lines.append("</CALCULATED_ECONOMICS>")
    return "\n".join(lines)


def _build_beginner_context(
    intent: dict[str, str | float | None],
    target_budget: float | None,
    today: datetime,
) -> str:
    detected_culture = intent["culture"]
    lines = [
        "<CALCULATED_ECONOMICS>",
        "scenario: beginner",
        "clarification_required: yes",
        "Финансовый расчёт не выполнялся: площадь помещения не указана.",
    ]

    if detected_culture:
        lines.append(f"Распознан интерес к культуре: {detected_culture}.")

    if target_budget:
        lines.append(f"Целевая сумма пользователя: {_format_currency(target_budget)} руб.")

    lines.append(f"Лёгкая культура для старта: {_format_crop_conditions(EASIEST_CULTURE, today)}")
    lines.append(f"Высокомаржинальная культура: {_format_crop_conditions(HIGH_MARGIN_CULTURE, today)}")
    lines.append("</CALCULATED_ECONOMICS>")
    return "\n".join(lines)


def _build_missing_tariff_context(
    intent: dict[str, str | float | None],
    target_budget: float | None,
    today: datetime,
) -> str:
    culture = intent["culture"]
    area_sqm = intent["area_sqm"]
    lines = [
        "<CALCULATED_ECONOMICS>",
        "scenario: missing_tariff",
        "clarification_required: yes",
    ]

    if culture:
        lines.append(f"Культура: {culture}")
        lines.append(_format_crop_conditions(culture, today))
    else:
        lines.append("Культура не определена явно.")
        lines.append(f"Лёгкая культура для старта: {_format_crop_conditions(EASIEST_CULTURE, today)}")
        lines.append(f"Высокомаржинальная культура: {_format_crop_conditions(HIGH_MARGIN_CULTURE, today)}")

    if area_sqm:
        lines.append(f"Площадь: {float(area_sqm):.1f} м²")

    if target_budget:
        lines.append(f"Целевая сумма пользователя: {_format_currency(target_budget)} руб.")

    lines.append(
        "Внимание: Тариф на электроэнергию неизвестен (равен 0). Финансовый расчет невозможен. "
        "Объясни пользователю базовые характеристики культуры (цикл, климат) и ОБЯЗАТЕЛЬНО попроси его "
        "ввести свой тариф в панели настроек дашборда ('Тариф электроэнергии') и сохранить профиль для "
        "получения точного бизнес-плана."
    )
    lines.append("</CALCULATED_ECONOMICS>")
    return "\n".join(lines)


def build_economics_context(
    intent: dict[str, str | float | None],
    region: str,
    energy_price_kwh: float | None,
) -> str:
    today = datetime.now()
    culture = intent["culture"]
    area_sqm = intent["area_sqm"]
    target_budget = intent["target_budget"]
    normalized_target_budget = float(target_budget) if target_budget else None

    if area_sqm and (energy_price_kwh is None or energy_price_kwh <= 0):
        return _build_missing_tariff_context(
            intent=intent,
            target_budget=normalized_target_budget,
            today=today,
        )

    if culture and area_sqm:
        return _build_exact_match_context(
            culture=culture,
            area_sqm=float(area_sqm),
            region=region,
            energy_price_kwh=energy_price_kwh,
            target_budget=normalized_target_budget,
            today=today,
        )

    if area_sqm:
        return _build_profit_hunt_context(
            area_sqm=float(area_sqm),
            region=region,
            energy_price_kwh=energy_price_kwh,
            target_budget=normalized_target_budget,
            today=today,
        )

    return _build_beginner_context(
        intent=intent,
        target_budget=normalized_target_budget,
        today=today,
    )


def get_weather_context(region: str) -> str:
    normalized_region = _normalize_region(region)
    return f"{normalized_region}: температура +5°C, пасмурно, высокая влажность воздуха."


def chat_with_ai(
    user_message: str,
    history: list[dict[str, Any]],
    user_region: str,
    farm_profile: dict[str, Any],
) -> str:
    region = _normalize_region(user_region)
    intent = extract_user_intent(user_message)
    energy_price_kwh = _extract_energy_price(user_message, farm_profile)
    economics_block = build_economics_context(intent, region, energy_price_kwh)
    weather_block = get_weather_context(region) or WEATHER_ERROR_TEXT
    active_plant = get_active_plant()
    farm_state_block = ""
    if active_plant:
        farm_state_block = (
            "<FARM_STATE>\n"
            f"В данный момент у пользователя растет: {active_plant['culture_name']}. "
            f"Идет {active_plant['days_active']}-й день цикла.\n"
            "</FARM_STATE>\n"
        )

    user_prompt = (
        f"<CONTEXT>\n"
        f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M')} | Регион: {region}\n"
        f"<WEATHER>{weather_block}</WEATHER>\n"
        f"{farm_state_block}"
        f"{economics_block}\n"
        f"</CONTEXT>\n\n"
        f"<QUESTION>{user_message}</QUESTION>"
    )

    messages: list[dict[str, str]] = [{"role": "system", "content": SYSTEM_PROMPT}]
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
                    "temperature": 0.2,
                    "top_p": 0.85,
                    "repeat_penalty": 1.1,
                    "num_ctx": 4096,
                    "num_gpu": 99,
                    "num_predict": 2048,
                },
                "stop": ["<|im_end|>", "<|im_start|>", "<|endoftext|>", "</s>"],
            },
            timeout=None,
        )
        response.raise_for_status()
        content = response.json().get("message", {}).get("content") or ""

        print(f"RAW OLLAMA RESPONSE: {content}")

        if "<think>" in content:
            content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL).strip()

        return content or "Модель не вернула ответ."
    except requests.exceptions.RequestException as exc:
        return f"Ошибка соединения с Ollama: {exc}"
    except ValueError as exc:
        return f"Ошибка разбора ответа Ollama: {exc}"
