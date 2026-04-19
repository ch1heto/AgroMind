from __future__ import annotations

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from time import struct_time
from typing import Any

import feedparser
import requests
import urllib3

from agromind.config import NEWS_FEEDS, REQUEST_HEADERS


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _normalize_datetime(raw_value: Any) -> datetime | None:
    if raw_value is None:
        return None

    if isinstance(raw_value, struct_time):
        return datetime(
            raw_value.tm_year, raw_value.tm_mon, raw_value.tm_mday,
            raw_value.tm_hour, raw_value.tm_min, raw_value.tm_sec,
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


def fetch_news_from_feeds(
    feeds: tuple[str, ...] | list[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Парсит RSS-ленты новостей.
    
    Параметр feeds позволяет services.py передавать только рабочие ленты,
    исключая битые (403, timeout). По умолчанию берёт NEWS_FEEDS из config.
    """
    active_feeds = feeds if feeds is not None else NEWS_FEEDS

    collected: list[dict[str, Any]] = []
    errors: list[str] = []
    seen_urls: set[str] = set()

    for feed_url in active_feeds:
        try:
            response = requests.get(
                feed_url,
                headers=REQUEST_HEADERS,
                timeout=15,  # Было 20 — сокращаем чтобы не подвисать
                verify=False,
            )
            response.raise_for_status()
            parsed_feed = feedparser.parse(response.content)
        except Exception as req_exc:
            errors.append(f"Failed to download RSS feed {feed_url}: {req_exc}")
            continue

        if getattr(parsed_feed, "bozo", False) and not parsed_feed.entries:
            error = getattr(parsed_feed, "bozo_exception", "Unknown RSS parsing error")
            errors.append(f"Failed to read RSS feed {feed_url}: {error}")
            continue

        for entry in parsed_feed.entries:
            title = (entry.get("title") or "").strip()
            url = (entry.get("link") or "").strip()

            if not title or not url or url in seen_urls:
                continue

            published_at = (
                _normalize_datetime(entry.get("published_parsed"))
                or _normalize_datetime(entry.get("updated_parsed"))
                or _normalize_datetime(entry.get("published"))
                or _normalize_datetime(entry.get("updated"))
                or datetime.utcnow()
            )

            collected.append({"title": title, "published_at": published_at, "url": url})
            seen_urls.add(url)

    if not collected and errors:
        raise RuntimeError("; ".join(errors))

    return collected
