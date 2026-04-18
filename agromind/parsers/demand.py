from __future__ import annotations

import html
import logging
import re
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from time import struct_time
from typing import Any
from urllib.parse import urlencode

import feedparser
import requests

from agromind.config import DEFAULT_USER_AGENT


logger = logging.getLogger(__name__)

DEMAND_RSS_ENDPOINT = "https://zakupki.gov.ru/epz/order/extendedsearch/rss.html"
DEMAND_HEADERS = {
    "User-Agent": DEFAULT_USER_AGENT,
    "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
}

# Ограничиваем время ожидания, чтобы парсер не подвисал на каждой культуре
DEMAND_REQUEST_TIMEOUT = 8

# Защита от слишком большого тела ответа (RSS с мегабайтами мусора)
MAX_RESPONSE_BYTES = 5 * 1024 * 1024  # 5 MB

PRICE_PATTERNS = (
    r"Начальная\s*\(максимальная\)\s*цена\s*контракта[:\s]*([\d\s]+(?:[.,]\d+)?)",
    r"Начальная цена контракта[:\s]*([\d\s]+(?:[.,]\d+)?)",
    r"НМЦК[:\s]*([\d\s]+(?:[.,]\d+)?)",
    r"Цена контракта[:\s]*([\d\s]+(?:[.,]\d+)?)",
)
REGION_MAPPING = {
    "мск": "Москва", "москв": "Москва",
    "спб": "Санкт-Петербург", "петербург": "Санкт-Петербург", "ленинград": "Ленинградская область",
    "екб": "Свердловская область", "екатеринбург": "Свердловская область", "свердловск": "Свердловская область",
    "члб": "Челябинская область", "челябинск": "Челябинская область",
    "краснодар": "Краснодарский край", "кубань": "Краснодарский край",
    "ростов": "Ростовская область",
    "татарстан": "Республика Татарстан", "казань": "Республика Татарстан",
    "новосиб": "Новосибирская область",
    "нижегород": "Нижегородская область", "нижний новгород": "Нижегородская область",
    "самар": "Самарская область",
    "крым": "Республика Крым",
    "воронеж": "Воронежская область",
    "волгоград": "Волгоградская область",
    "уфа": "Республика Башкортостан", "башкир": "Республика Башкортостан",
    "перм": "Пермский край",
    "омск": "Омская область",
    "красноярск": "Красноярский край",
    "новосибирск": "Новосибирская область",
    "саратов": "Саратовская область",
    "тюмень": "Тюменская область",
    "россия": "Россия (Федеральный)",
    "рф": "Россия (Федеральный)",
}


def _normalize_datetime(raw_value: Any) -> datetime | None:
    if raw_value is None:
        return None

    if isinstance(raw_value, struct_time):
        return datetime(
            raw_value.tm_year,
            raw_value.tm_mon,
            raw_value.tm_mday,
            raw_value.tm_hour,
            raw_value.tm_min,
            raw_value.tm_sec,
            tzinfo=timezone.utc,
        ).replace(tzinfo=None)

    if isinstance(raw_value, str):
        try:
            parsed = parsedate_to_datetime(raw_value)
        except (TypeError, ValueError, IndexError):
            try:
                parsed = datetime.fromisoformat(raw_value)
            except ValueError:
                return None
        if parsed.tzinfo is not None:
            return parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed

    return None


def _clean_text(value: str) -> str:
    raw_text = html.unescape(value or "")
    without_tags = re.sub(r"<[^>]+>", " ", raw_text)
    return re.sub(r"\s+", " ", without_tags).strip()


def _extract_contract_price(text: str) -> float:
    for pattern in PRICE_PATTERNS:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue

        normalized = match.group(1).replace(" ", "").replace("\xa0", "").replace(",", ".")
        try:
            return float(normalized)
        except ValueError:
            continue

    return 0.0


def _extract_region(text: str) -> str:
    if not text:
        return "Россия (Регион не указан)"

    text_lower = text.lower()

    words = re.findall(r"\b\w+\b", text_lower)
    for word in words:
        if word in REGION_MAPPING:
            return REGION_MAPPING[word]

    for key, normalized_name in REGION_MAPPING.items():
        if len(key) > 4 and key in text_lower:
            return normalized_name

    return "Россия (Регион не указан)"


def fetch_demand_signals(crop_names: list[str]) -> list[dict[str, Any]]:
    collected: list[dict[str, Any]] = []
    seen_urls: set[str] = set()

    for crop_name in crop_names:
        rss_url = f"{DEMAND_RSS_ENDPOINT}?{urlencode({'searchString': crop_name})}"

        try:
            response = requests.get(
                rss_url,
                headers=DEMAND_HEADERS,
                timeout=DEMAND_REQUEST_TIMEOUT,
                stream=True,
            )
            response.raise_for_status()

            # Защита от огромных ответов
            content = b""
            for chunk in response.iter_content(chunk_size=65536):
                content += chunk
                if len(content) > MAX_RESPONSE_BYTES:
                    logger.warning("RSS response too large for %s, truncating", crop_name)
                    break

            parsed_feed = feedparser.parse(content)
        except requests.exceptions.Timeout:
            logger.warning("Demand RSS timeout (%ss) for %s", DEMAND_REQUEST_TIMEOUT, crop_name)
            continue
        except requests.exceptions.RequestException as exc:
            logger.warning("Demand RSS request failed for %s: %s", crop_name, exc)
            continue
        except Exception as exc:
            logger.warning("Demand RSS parsing bootstrap failed for %s: %s", crop_name, exc)
            continue

        if getattr(parsed_feed, "bozo", False) and not parsed_feed.entries:
            error = getattr(parsed_feed, "bozo_exception", "unknown RSS parsing error")
            logger.warning("Demand RSS feed is unavailable for %s: %s", crop_name, error)
            continue

        for entry in parsed_feed.entries:
            url = (entry.get("link") or "").strip()
            if not url or url in seen_urls:
                continue

            title = _clean_text(entry.get("title", ""))
            description = _clean_text(entry.get("summary", "") or entry.get("description", ""))
            published_at = (
                _normalize_datetime(entry.get("published_parsed"))
                or _normalize_datetime(entry.get("updated_parsed"))
                or _normalize_datetime(entry.get("published"))
                or _normalize_datetime(entry.get("updated"))
                or datetime.now(timezone.utc)
            )

            region = _extract_region(f"{title} {description}")
            contract_price = _extract_contract_price(description)

            collected.append(
                {
                    "crop_name": crop_name,
                    "region": region,
                    "contract_price": contract_price,
                    "published_at": published_at,
                    "url": url,
                }
            )
            seen_urls.add(url)

    return collected
