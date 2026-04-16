from __future__ import annotations

import re
from datetime import datetime, timedelta
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag
from requests import Response
from urllib3.exceptions import InsecureRequestWarning

from agromind.config import (
    AGROBAZAR_URLS,
    AGROSERVER_URLS,
    DEFAULT_REGION,
    FRUITINFO_URLS,
    MAX_PAGES,
    REQUEST_HEADERS,
    REQUEST_TIMEOUT,
)


requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

HERB_KEYWORDS = (
    "зелень",
    "микрозелень",
    "укроп",
    "лук зеленый",
    "лук зелёный",
    "лук-перо",
    "шнитт-лук",
    "петрушка",
    "кинза",
    "базилик",
    "шпинат",
    "салат",
    "айсберг",
    "романо",
    "лолло",
    "руккола",
    "рукола",
    "щавель",
    "мята",
    "мелисса",
    "розмарин",
    "сельдерей",
    "тархун",
    "эстрагон",
    "черемша",
    "кресс-салат",
    "корн",
    "мангольд",
    "мизуна",
    "тимьян",
    "шалфей",
    "душица",
    "майоран",
    "любисток",
)

PRICE_RE = re.compile(
    r"(?P<price>\d[\d\s]*(?:[.,]\d+)?)\s*(?:₽|руб(?:\.|ля|лей)?)",
    re.IGNORECASE,
)
DATE_RE = re.compile(r"\d{2}\.\d{2}\.\d{4}(?:\s+\d{2}:\d{2}:\d{2})?")

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
    ("лук зеленый", "Лук зеленый"),
    ("лук зелёный", "Лук зеленый"),
    ("лук-перо", "Лук зеленый"),
    ("шнитт-лук", "Шнитт-лук"),
    ("микрозелень", "Микрозелень"),
    ("укроп", "Укроп"),
    ("петрушка", "Петрушка"),
    ("кинза", "Кинза"),
    ("базилик", "Базилик"),
    ("шпинат", "Шпинат"),
    ("руккола", "Руккола"),
    ("рукола", "Руккола"),
    ("салат", "Салат"),
    ("щавель", "Щавель"),
    ("мята", "Мята"),
    ("мелисса", "Мелисса"),
    ("розмарин", "Розмарин"),
    ("сельдерей", "Сельдерей"),
    ("тархун", "Тархун"),
    ("эстрагон", "Эстрагон"),
    ("черемша", "Черемша"),
    ("кресс-салат", "Кресс-салат"),
    ("корн", "Корн"),
    ("мангольд", "Мангольд"),
    ("мизуна", "Мизуна"),
    ("тимьян", "Тимьян"),
    ("шалфей", "Шалфей"),
    ("душица", "Душица"),
    ("майоран", "Майоран"),
    ("любисток", "Любисток"),
    ("зелень", "Зелень"),
)


def _normalize_text(value: str) -> str:
    return " ".join(value.replace("\xa0", " ").split())


def _contains_herb_keyword(value: str) -> bool:
    lowered = _normalize_text(value).lower()
    return any(keyword in lowered for keyword in HERB_KEYWORDS)


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
    normalized = _normalize_text(raw_value)
    matched = DATE_RE.search(normalized)
    if not matched:
        return None

    for date_format in ("%d.%m.%Y %H:%M:%S", "%d.%m.%Y"):
        try:
            return datetime.strptime(matched.group(0), date_format)
        except ValueError:
            continue

    return None


def _parse_human_date(raw_value: str) -> datetime | None:
    normalized = _normalize_text(raw_value).lower()
    now = datetime.now()

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
    crop_name: str,
    wholesale_price: float | None,
    published_at: datetime,
    region: str | None,
) -> dict[str, object] | None:
    if not crop_name or wholesale_price is None:
        return None

    normalized_name = _normalize_text(crop_name)
    if not _contains_herb_keyword(normalized_name):
        return None

    return {
        "crop_name": normalized_name,
        "wholesale_price": wholesale_price,
        "published_at": published_at,
        "region": _normalize_region(region),
    }


def _extract_canonical_crop_name(raw_value: str) -> str:
    normalized = _normalize_text(raw_value)
    lowered = normalized.lower()

    for keyword, canonical_name in CANONICAL_CROP_NAMES:
        if keyword in lowered:
            return canonical_name

    return normalized


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


def _response_text(url: str) -> str:
    return _request_url(url).text


def _build_page_url(base_url: str, page: int) -> str:
    if page <= 1:
        return base_url

    parsed = urlparse(base_url)
    query_params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query_params["page"] = str(page)

    return urlunparse(
        parsed._replace(query=urlencode(query_params, doseq=True))
    )


def _deduplicate(items: list[dict[str, object]]) -> list[dict[str, object]]:
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


def fetch_wholesale_herb_prices() -> list[dict[str, object]]:
    results: list[dict[str, object]] = []

    for url in AGROBAZAR_URLS:
        for page in range(1, MAX_PAGES + 1):
            page_url = _build_page_url(url, page)
            response = _request_url(page_url, allow_404=True)
            if response.status_code == 404:
                break

            soup = BeautifulSoup(response.text, "html.parser")
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
                published_at = _parse_datetime(
                    date_element.get_text(" ", strip=True) if date_element else ""
                )
                region = region_element.get_text(" ", strip=True) if region_element else DEFAULT_REGION

                item = _build_price_item(crop_name, price_value, published_at, region)
                if item:
                    page_items.append(item)

            if not page_items:
                break

            results.extend(page_items)

    if not results:
        raise RuntimeError("Agrobazar page was loaded, but no herb listings were parsed.")

    return _deduplicate(results)


def fetch_agroserver_prices() -> list[dict[str, object]]:
    results: list[dict[str, object]] = []

    for url in AGROSERVER_URLS:
        for page in range(1, MAX_PAGES + 1):
            page_url = _build_page_url(url, page)
            response = _request_url(page_url, allow_404=True)
            if response.status_code == 404:
                break

            soup = BeautifulSoup(response.text, "html.parser")
            page_items: list[dict[str, object]] = []

            for card in soup.select(".line"):
                if not isinstance(card, Tag):
                    continue

                title_element = card.select_one(".th a")
                price_element = card.select_one(".price")
                region_element = card.select_one(".geo")
                date_element = card.select_one(".date")

                crop_name = title_element.get_text(" ", strip=True) if title_element else ""
                price_value = _parse_price(price_element.get_text(" ", strip=True) if price_element else "")
                published_at = _parse_datetime(
                    date_element.get_text(" ", strip=True) if date_element else ""
                )
                region = region_element.get_text(" ", strip=True) if region_element else DEFAULT_REGION

                item = _build_price_item(crop_name, price_value, published_at, region)
                if item:
                    page_items.append(item)

                for nested_item in card.select(".list li.for_stream_stat"):
                    if not isinstance(nested_item, Tag):
                        continue

                    nested_title = nested_item.select_one("a")
                    nested_price = nested_item.select_one("span")

                    nested_crop_name = (
                        nested_title.get_text(" ", strip=True) if nested_title else ""
                    )
                    nested_price_value = _parse_price(
                        nested_price.get_text(" ", strip=True) if nested_price else ""
                    )

                    item = _build_price_item(
                        nested_crop_name,
                        nested_price_value,
                        published_at,
                        region,
                    )
                    if item:
                        page_items.append(item)

            if not page_items:
                break

            results.extend(page_items)

    if not results:
        raise RuntimeError("Agroserver page was loaded, but no herb listings were parsed.")

    return _deduplicate(results)


def _collect_fruitinfo_offer_urls(listing_url: str, soup: BeautifulSoup) -> list[str]:
    listing_path = urlparse(listing_url).path.rstrip("/")
    seen: set[str] = set()
    offer_urls: list[str] = []

    for anchor in soup.find_all("a", href=True):
        href = anchor["href"].strip()
        absolute_url = urljoin(listing_url, href)
        path = urlparse(absolute_url).path.rstrip("/")
        title = _normalize_text(anchor.get_text(" ", strip=True))

        if not path or path == listing_path:
            continue
        if "/trade/" not in path:
            continue
        if path.endswith("/sale") or path.endswith("/buy"):
            continue
        if path.endswith("/trade/newOffer") or path.endswith("/trade/offersMap"):
            continue
        if not _contains_herb_keyword(f"{title} {path}"):
            continue
        if absolute_url in seen:
            continue

        seen.add(absolute_url)
        offer_urls.append(absolute_url)

    return offer_urls


def _parse_fruitinfo_offer(offer_url: str) -> dict[str, object] | None:
    soup = BeautifulSoup(_response_text(offer_url), "html.parser")

    crop_name = ""
    for candidate in (
        soup.select_one(".details__breadcrumbs"),
        soup.select_one(".content-wrap__title"),
        soup.select_one("h1"),
    ):
        if not candidate:
            continue
        candidate_text = candidate.get_text(" ", strip=True)
        if _contains_herb_keyword(candidate_text):
            crop_name = candidate_text
            break

    if not crop_name:
        crop_name = urlparse(offer_url).path.rsplit("/", maxsplit=1)[-1].replace("-", " ")
    crop_name = _extract_canonical_crop_name(crop_name)

    price_value = None
    for candidate in (
        soup.select_one(".contacts__price"),
        soup.select_one(".contacts__price-wrap"),
    ):
        if not candidate:
            continue
        price_value = _parse_price(candidate.get_text(" ", strip=True))
        if price_value is not None:
            break

    date_text = ""
    for candidate in (
        soup.select_one(".details__date"),
        soup.select_one(".details__top-wrap"),
    ):
        if candidate:
            date_text = candidate.get_text(" ", strip=True)
            if date_text:
                break

    region_text = ""
    for candidate in (
        soup.select_one(".info__address"),
        soup.select_one(".info__address-details"),
    ):
        if candidate:
            region_text = candidate.get_text(" ", strip=True)
            if region_text:
                break

    return _build_price_item(
        crop_name=crop_name,
        wholesale_price=price_value,
        published_at=_parse_datetime(date_text),
        region=region_text,
    )


def fetch_fruitinfo_prices() -> list[dict[str, object]]:
    results: list[dict[str, object]] = []

    for listing_url in FRUITINFO_URLS:
        for page in range(1, MAX_PAGES + 1):
            page_url = _build_page_url(listing_url, page)
            response = _request_url(page_url, allow_404=True)
            if response.status_code == 404:
                break

            soup = BeautifulSoup(response.text, "html.parser")
            offer_urls = _collect_fruitinfo_offer_urls(page_url, soup)
            if not offer_urls:
                break

            page_items: list[dict[str, object]] = []
            for offer_url in offer_urls:
                item = _parse_fruitinfo_offer(offer_url)
                if item:
                    page_items.append(item)

            if not page_items:
                break

            results.extend(page_items)

    if not results:
        raise RuntimeError("Fruitinfo pages were loaded, but no herb listings were parsed.")

    return _deduplicate(results)


def fetch_all_prices() -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    errors: list[str] = []

    for parser in (
        fetch_wholesale_herb_prices,
        fetch_agroserver_prices,
        fetch_fruitinfo_prices,
    ):
        try:
            results.extend(parser())
        except Exception as exc:
            errors.append(f"{parser.__name__}: {exc}")

    deduplicated = _deduplicate(results)
    if deduplicated:
        return deduplicated

    raise RuntimeError("; ".join(errors) if errors else "No price data was collected.")
