from __future__ import annotations

import re
from datetime import datetime
from typing import Any

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from agromind.config import (
    AGROBAZAR_HERBS_URL,
    DEFAULT_REGION,
    REQUEST_HEADERS,
    REQUEST_TIMEOUT,
)


HERB_KEYWORDS = (
    "укроп",
    "лук",
    "лук зеленый",
    "петрушка",
    "кинза",
    "базилик",
    "шпинат",
    "салат",
    "руккола",
    "щавель",
    "мята",
    "розмарин",
    "сельдерей",
    "тархун",
    "эстрагон",
)

PRICE_RE = re.compile(r"(?P<price>\d+(?:[.,]\d+)?)\s*(?:₽|руб(?:\.|ля|лей)?)", re.IGNORECASE)
DATE_RE = re.compile(r"\d{2}\.\d{2}\.\d{4}(?:\s+\d{2}:\d{2}:\d{2})?")
AD_NUMBER_RE = re.compile(r"^№\s*\d+$")


def _normalize_lines(text: str) -> list[str]:
    lines = []
    for raw_line in text.replace("\xa0", " ").splitlines():
        cleaned = " ".join(raw_line.strip().split())
        if cleaned:
            lines.append(cleaned)
    return lines


def _split_blocks(lines: list[str]) -> list[list[str]]:
    blocks: list[list[str]] = []
    current_block: list[str] = []
    capture = False

    for line in lines:
        if AD_NUMBER_RE.match(line):
            if current_block:
                blocks.append(current_block)
            current_block = [line]
            capture = True
            continue

        if capture:
            current_block.append(line)

    if current_block:
        blocks.append(current_block)

    return blocks


def _parse_datetime(lines: list[str]) -> datetime | None:
    for line in lines:
        matched = DATE_RE.search(line)
        if not matched:
            continue

        raw_value = matched.group(0)
        for date_format in ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y"):
            try:
                return datetime.strptime(raw_value, date_format)
            except ValueError:
                continue

    return None


def _parse_crop_name(lines: list[str]) -> str | None:
    for index, line in enumerate(lines):
        lowered = line.lower()
        if lowered in {"продать", "купить"} and index + 1 < len(lines):
            candidate = lines[index + 1].strip()
            if len(candidate) <= 120:
                return candidate

    for line in lines:
        lowered = line.lower()
        if any(keyword in lowered for keyword in HERB_KEYWORDS) and len(line) <= 120:
            return line

    return None


def _parse_price(lines: list[str]) -> float | None:
    for line in lines:
        matched = PRICE_RE.search(line)
        if matched:
            return float(matched.group("price").replace(",", "."))
    return None


def _parse_region(lines: list[str]) -> str:
    for line in lines:
        if line.lower().startswith("место продажи:"):
            return line.split(":", maxsplit=1)[1].strip() or DEFAULT_REGION

    for line in lines:
        if "россия" in line.lower() and len(line) <= 120:
            return line

    return DEFAULT_REGION


def _block_contains_herbs(lines: list[str]) -> bool:
    joined = " ".join(lines).lower()
    return any(keyword in joined for keyword in HERB_KEYWORDS)


def _parse_block(lines: list[str]) -> dict[str, Any] | None:
    if not _block_contains_herbs(lines):
        return None

    crop_name = _parse_crop_name(lines)
    wholesale_price = _parse_price(lines)

    if not crop_name or wholesale_price is None:
        return None

    return {
        "crop_name": crop_name,
        "wholesale_price": wholesale_price,
        "published_at": _parse_datetime(lines) or datetime.utcnow(),
        "region": _parse_region(lines),
    }


def _parse_card_datetime(raw_value: str) -> datetime | None:
    raw_value = " ".join(raw_value.split())
    for date_format in ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y"):
        try:
            return datetime.strptime(raw_value, date_format)
        except ValueError:
            continue
    return None


def _parse_price_from_text(raw_value: str) -> float | None:
    matched = PRICE_RE.search(raw_value)
    if matched:
        return float(matched.group("price").replace(",", "."))
    return None


def _extract_prices_from_cards(soup: BeautifulSoup) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    seen_snapshots: set[tuple[str, float, datetime, str]] = set()

    for card in soup.select(".pl-item"):
        if not isinstance(card, Tag):
            continue

        title_element = card.select_one(".pl-title")
        price_element = card.select_one(".pl-price")
        date_element = card.select_one(".pl-date")
        region_element = card.select_one(".pl-sale-place span") or card.select_one(".pl-descr")

        crop_name = title_element.get_text(" ", strip=True) if title_element else ""
        raw_price = price_element.get_text(" ", strip=True) if price_element else ""
        raw_date = date_element.get_text(" ", strip=True) if date_element else ""
        region = (
            region_element.get_text(" ", strip=True)
            if region_element
            else DEFAULT_REGION
        )

        wholesale_price = _parse_price_from_text(raw_price)
        published_at = _parse_card_datetime(raw_date) or datetime.utcnow()

        if not crop_name or wholesale_price is None:
            continue

        snapshot_key = (crop_name, wholesale_price, published_at, region)
        if snapshot_key in seen_snapshots:
            continue

        seen_snapshots.add(snapshot_key)
        results.append(
            {
                "crop_name": crop_name,
                "wholesale_price": wholesale_price,
                "published_at": published_at,
                "region": region or DEFAULT_REGION,
            }
        )

    return results


def fetch_wholesale_herb_prices() -> list[dict[str, Any]]:
    last_error: requests.RequestException | None = None
    response = None

    for _ in range(2):
        try:
            response = requests.get(
                AGROBAZAR_HERBS_URL,
                headers=REQUEST_HEADERS,
                timeout=(10, REQUEST_TIMEOUT),
            )
            response.raise_for_status()
            break
        except requests.RequestException as exc:
            last_error = exc

    if response is None:
        raise RuntimeError(f"Failed to load Agrobazar prices page: {last_error}") from last_error

    if not response.encoding or response.encoding.lower() == "iso-8859-1":
        response.encoding = response.apparent_encoding or response.encoding

    soup = BeautifulSoup(response.text, "html.parser")
    results = _extract_prices_from_cards(soup)

    if results:
        return results

    lines = _normalize_lines(soup.get_text("\n", strip=True))
    blocks = _split_blocks(lines)

    results = []
    seen_snapshots: set[tuple[str, float, datetime, str]] = set()

    for block in blocks:
        parsed = _parse_block(block)
        if not parsed:
            continue

        snapshot_key = (
            parsed["crop_name"],
            parsed["wholesale_price"],
            parsed["published_at"],
            parsed["region"],
        )
        if snapshot_key in seen_snapshots:
            continue

        seen_snapshots.add(snapshot_key)
        results.append(parsed)

    if not results:
        raise RuntimeError("Agrobazar page was loaded, but no herb price listings were parsed.")

    return results
