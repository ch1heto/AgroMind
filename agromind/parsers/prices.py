from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Callable, Iterable
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
from requests import Response
from urllib3.exceptions import InsecureRequestWarning

from agromind.config import (
    AGRORU_URLS,
    AGROBAZAR_URLS,
    B2B_TRADE_URLS,
    DEFAULT_REGION,
    REQUEST_HEADERS,
    REQUEST_TIMEOUT,
)


requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

HERB_KEYWORDS = (
    "зелень",
    "микрозелень",
    "салат",
    "руккола",
    "рукола",
    "айсберг",
    "романо",
    "лолло",
    "корн",
    "кресс",
    "укроп",
    "петрушка",
    "кинза",
    "базилик",
    "шпинат",
    "щавель",
    "лук зеленый",
    "лук зелёный",
    "лук-перо",
    "шнитт-лук",
    "черемша",
    "сельдерей",
    "тархун",
    "эстрагон",
    "розмарин",
    "тимьян",
    "мята",
    "мелисса",
    "шалфей",
    "душица",
    "орегано",
    "майоран",
    "любисток",
    "мангольд",
    "мизуна",
)

PRICE_RE = re.compile(
    r"(?P<price>\d[\d\s]*(?:[.,]\d+)?)\s*(?:₽|руб(?:\.|ля|лей)?|р(?:\.|/кг|/шт)?)",
    re.IGNORECASE,
)
DATE_RE = re.compile(r"\d{2}\.\d{2}\.\d{4}(?:\s+\d{2}:\d{2}(?::\d{2})?)?")
RELATIVE_DATE_RE = re.compile(r"(?P<hours>\d+)\s+час")

RUSSIAN_MONTHS = {
    "янв": 1,
    "января": 1,
    "фев": 2,
    "февраля": 2,
    "мар": 3,
    "марта": 3,
    "апр": 4,
    "апреля": 4,
    "май": 5,
    "мая": 5,
    "июн": 6,
    "июня": 6,
    "июл": 7,
    "июля": 7,
    "авг": 8,
    "августа": 8,
    "сен": 9,
    "сент": 9,
    "сентября": 9,
    "окт": 10,
    "октября": 10,
    "ноя": 11,
    "ноября": 11,
    "дек": 12,
    "декабря": 12,
}

CANONICAL_CROP_NAMES = (
    ("лук зелёный", "Лук зеленый"),
    ("лук зеленый", "Лук зеленый"),
    ("лук-перо", "Лук зеленый"),
    ("шнитт-лук", "Шнитт-лук"),
    ("укроп", "Укроп"),
    ("петрушка", "Петрушка"),
    ("кинза", "Кинза"),
    ("базилик", "Базилик"),
    ("шпинат", "Шпинат"),
    ("руккола", "Руккола"),
    ("рукола", "Руккола"),
    ("салат", "Салат"),
    ("айсберг", "Салат айсберг"),
    ("романо", "Салат романо"),
    ("лолло", "Салат лолло"),
    ("корн", "Корн"),
    ("кресс", "Кресс-салат"),
    ("щавель", "Щавель"),
    ("мята", "Мята"),
    ("мелисса", "Мелисса"),
    ("розмарин", "Розмарин"),
    ("тимьян", "Тимьян"),
    ("тархун", "Тархун"),
    ("эстрагон", "Эстрагон"),
    ("сельдерей", "Сельдерей"),
    ("черемша", "Черемша"),
    ("шалфей", "Шалфей"),
    ("душица", "Душица"),
    ("орегано", "Орегано"),
    ("майоран", "Майоран"),
    ("любисток", "Любисток"),
    ("мангольд", "Мангольд"),
    ("мизуна", "Мизуна"),
    ("микрозелень", "Микрозелень"),
    ("зелень", "Зелень"),
)


def _normalize_text(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split())


def _contains_herb_keyword(value: str) -> bool:
    lowered = _normalize_text(value).lower()
    return any(keyword in lowered for keyword in HERB_KEYWORDS)


def _extract_canonical_crop_name(raw_value: str) -> str:
    normalized = _normalize_text(raw_value)
    lowered = normalized.lower()

    for keyword, canonical_name in CANONICAL_CROP_NAMES:
        if keyword in lowered:
            return canonical_name

    return normalized


def _parse_price(raw_value: str) -> float | None:
    matched = PRICE_RE.search(_normalize_text(raw_value))
    if not matched:
        return None

    normalized = matched.group("price").replace(" ", "").replace(",", ".")
    try:
        return float(normalized)
    except ValueError:
        return None


def _normalize_region(raw_value: str | None) -> str:
    if not raw_value:
        return DEFAULT_REGION

    region = _normalize_text(raw_value.strip(" ,.-"))
    return region or DEFAULT_REGION


def _parse_numeric_date(raw_value: str) -> datetime | None:
    matched = DATE_RE.search(_normalize_text(raw_value))
    if not matched:
        return None

    for date_format in ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M", "%d.%m.%Y"):
        try:
            return datetime.strptime(matched.group(0), date_format)
        except ValueError:
            continue

    return None


def _parse_human_date(raw_value: str) -> datetime | None:
    normalized = _normalize_text(raw_value).lower()
    now = datetime.utcnow()

    if not normalized:
        return None

    time_match = re.search(r"(\d{1,2}):(\d{2})(?::(\d{2}))?", normalized)
    hours = int(time_match.group(1)) if time_match else 0
    minutes = int(time_match.group(2)) if time_match else 0
    seconds = int(time_match.group(3) or 0) if time_match else 0

    if "сегодня" in normalized:
        return now.replace(hour=hours, minute=minutes, second=seconds, microsecond=0)

    if "вчера" in normalized:
        yesterday = now - timedelta(days=1)
        return yesterday.replace(hour=hours, minute=minutes, second=seconds, microsecond=0)

    relative_match = RELATIVE_DATE_RE.search(normalized)
    if relative_match:
        return (now - timedelta(hours=int(relative_match.group("hours")))).replace(microsecond=0)

    month_match = re.search(
        r"(?P<day>\d{1,2})\s+(?P<month>[а-яё]+)(?:\s+(?P<year>\d{4}))?",
        normalized,
    )
    if not month_match:
        return None

    month = RUSSIAN_MONTHS.get(month_match.group("month"))
    if month is None:
        return None

    year = int(month_match.group("year") or now.year)
    day = int(month_match.group("day"))

    try:
        return datetime(year, month, day, hours, minutes, seconds)
    except ValueError:
        return None


def _parse_datetime(raw_value: str | None) -> datetime:
    if raw_value:
        parsed = _parse_numeric_date(raw_value) or _parse_human_date(raw_value)
        if parsed is not None:
            return parsed
    return datetime.utcnow()


def _build_price_item(
    *,
    crop_name: str,
    wholesale_price: float | None,
    published_at: datetime,
    region: str | None,
    source: str,
) -> dict[str, object] | None:
    if not crop_name or wholesale_price is None:
        return None

    normalized_name = _extract_canonical_crop_name(crop_name)
    if not _contains_herb_keyword(normalized_name):
        return None

    return {
        "crop_name": normalized_name,
        "wholesale_price": wholesale_price,
        "published_at": published_at,
        "region": _normalize_region(region),
        "source": source,
    }


def _request_url(url: str, *, allow_404: bool = False) -> Response:
    last_error: Exception | None = None

    for verify in (True, False):
        for _ in range(2):
            try:
                response = requests.get(
                    url,
                    headers=REQUEST_HEADERS,
                    timeout=(15, max(REQUEST_TIMEOUT, 60)),
                    verify=verify,
                )
                if allow_404 and response.status_code == 404:
                    return response
                response.raise_for_status()
                if not response.encoding or response.encoding.lower() == "iso-8859-1":
                    response.encoding = response.apparent_encoding or response.encoding
                return response
            except requests.exceptions.SSLError as exc:
                last_error = exc
                if verify:
                    break
            except requests.RequestException as exc:
                last_error = exc

        if last_error and verify and isinstance(last_error, requests.exceptions.SSLError):
            continue

    raise RuntimeError(f"Failed to load page {url}: {last_error}") from last_error


def _build_query_page_url(base_url: str, page: int) -> str:
    if page <= 1:
        return base_url

    parsed = urlparse(base_url)
    query_params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query_params["page"] = str(page)
    return urlunparse(parsed._replace(query=urlencode(query_params, doseq=True)))


def _build_agroru_page_url(base_url: str, page: int) -> str:
    if page <= 1:
        return base_url

    normalized = base_url.rstrip("/")
    return f"{normalized}/ct-0-p{page}.htm"


def _find_listing_container(node: Tag, *, max_depth: int = 5) -> Tag:
    current: Tag = node
    best = node

    for _ in range(max_depth):
        parent = current.parent
        if not isinstance(parent, Tag):
            break

        parent_text = _normalize_text(parent.get_text(" ", strip=True))
        if 30 <= len(parent_text) <= 800:
            best = parent
        if _parse_price(parent_text) is not None and len(parent_text) <= 800:
            return parent

        current = parent

    return best


def _deduplicate(items: Iterable[dict[str, object]]) -> list[dict[str, object]]:
    unique_items: list[dict[str, object]] = []
    seen: set[tuple[str, float, datetime, str]] = set()

    for item in items:
        snapshot_key = (
            str(item["crop_name"]),
            float(item["wholesale_price"]),
            item["published_at"],
            str(item["region"]),
        )
        if snapshot_key in seen:
            continue

        seen.add(snapshot_key)
        unique_items.append(item)

    return unique_items


def _paginate_source(
    source_name: str,
    source_urls: list[str],
    page_builder: Callable[[str, int], str],
    parser: Callable[[BeautifulSoup, str], list[dict[str, object]]],
) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []

    for base_url in source_urls:
        page = 1
        while True:
            page_url = page_builder(base_url, page)
            response = _request_url(page_url, allow_404=True)
            if response.status_code == 404:
                break

            soup = BeautifulSoup(response.text, "html.parser")
            page_items = parser(soup, page_url)
            if not page_items:
                break

            results.extend(page_items)
            page += 1

    if not results:
        raise RuntimeError(f"{source_name} pages were loaded, but no herb listings were parsed.")

    return _deduplicate(results)


def _parse_agrobazar_page(soup: BeautifulSoup, page_url: str) -> list[dict[str, object]]:
    page_items: list[dict[str, object]] = []

    for card in soup.select(".pl-item"):
        if not isinstance(card, Tag):
            continue

        title_element = card.select_one(".pl-title")
        price_element = card.select_one(".pl-price")
        date_element = card.select_one(".pl-date")
        region_element = card.select_one(".pl-sale-place span") or card.select_one(".pl-descr")

        crop_name = title_element.get_text(" ", strip=True) if title_element else ""
        price_value = _parse_price(price_element.get_text(" ", strip=True) if price_element else "")
        published_at = _parse_datetime(date_element.get_text(" ", strip=True) if date_element else "")
        region = region_element.get_text(" ", strip=True) if region_element else DEFAULT_REGION

        item = _build_price_item(
            crop_name=crop_name,
            wholesale_price=price_value,
            published_at=published_at,
            region=region,
            source="agrobazar",
        )
        if item:
            page_items.append(item)

    return page_items


def fetch_wholesale_herb_prices() -> list[dict[str, object]]:
    return _paginate_source(
        source_name="Agrobazar",
        source_urls=AGROBAZAR_URLS,
        page_builder=_build_query_page_url,
        parser=_parse_agrobazar_page,
    )


def _parse_b2b_trade_page(soup: BeautifulSoup, page_url: str) -> list[dict[str, object]]:
    page_items: list[dict[str, object]] = []
    seen_urls: set[str] = set()

    for anchor in soup.select("a[href*='/ru/product/']"):
        if not isinstance(anchor, Tag):
            continue

        href = anchor.get("href", "").strip()
        absolute_url = urljoin(page_url, href)
        if absolute_url in seen_urls:
            continue

        title = _normalize_text(anchor.get_text(" ", strip=True))
        if not _contains_herb_keyword(title):
            continue

        container = _find_listing_container(anchor, max_depth=6)
        container_text = _normalize_text(container.get_text(" ", strip=True))
        price_value = _parse_price(container_text)
        if price_value is None:
            continue

        region = DEFAULT_REGION
        date_text = container_text

        item = _build_price_item(
            crop_name=title,
            wholesale_price=price_value,
            published_at=_parse_datetime(date_text),
            region=region,
            source="b2b.trade",
        )
        if item:
            page_items.append(item)
            seen_urls.add(absolute_url)

    return page_items


def fetch_b2b_trade_prices() -> list[dict[str, object]]:
    return _paginate_source(
        source_name="B2B.TRADE",
        source_urls=B2B_TRADE_URLS,
        page_builder=_build_query_page_url,
        parser=_parse_b2b_trade_page,
    )


def _parse_agroru_page(soup: BeautifulSoup, page_url: str) -> list[dict[str, object]]:
    page_items: list[dict[str, object]] = []
    seen_urls: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        if not isinstance(anchor, Tag):
            continue

        href = anchor.get("href", "").strip()
        absolute_url = urljoin(page_url, href)
        path = urlparse(absolute_url).path
        title = _normalize_text(anchor.get_text(" ", strip=True))

        if absolute_url in seen_urls:
            continue
        if not re.search(r"/doska/.+-\d+\.htm$", path):
            continue
        if not _contains_herb_keyword(title):
            continue

        container = _find_listing_container(anchor, max_depth=6)
        container_text = _normalize_text(container.get_text(" ", strip=True))
        price_value = _parse_price(container_text)
        if price_value is None:
            continue

        region = DEFAULT_REGION
        for region_anchor in container.find_all("a", href=True):
            region_href = region_anchor.get("href", "")
            region_text = _normalize_text(region_anchor.get_text(" ", strip=True))
            if any(marker in region_href for marker in ("/city-", "/region-", "/respublika-")) and region_text:
                region = region_text
                break

        item = _build_price_item(
            crop_name=title,
            wholesale_price=price_value,
            published_at=_parse_datetime(container_text),
            region=region,
            source="agroru",
        )
        if item:
            page_items.append(item)
            seen_urls.add(absolute_url)

    return page_items


def fetch_agroru_prices() -> list[dict[str, object]]:
    return _paginate_source(
        source_name="Agroru",
        source_urls=AGRORU_URLS,
        page_builder=_build_agroru_page_url,
        parser=_parse_agroru_page,
    )


def fetch_all_prices() -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    errors: list[str] = []

    for parser in (
        fetch_wholesale_herb_prices,
        fetch_b2b_trade_prices,
        fetch_agroru_prices,
    ):
        try:
            results.extend(parser())
        except Exception as exc:
            errors.append(f"{parser.__name__}: {exc}")

    deduplicated = _deduplicate(results)
    if deduplicated:
        return deduplicated

    raise RuntimeError("; ".join(errors) if errors else "No price data was collected.")
