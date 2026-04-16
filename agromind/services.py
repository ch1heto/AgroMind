from __future__ import annotations

from typing import Any

from sqlalchemy import Select, and_, desc, func, select
from sqlalchemy.orm import Session

from agromind.database import init_db, session_scope
from agromind.models import News, PriceSummary
from agromind.parsers.news import fetch_news_from_feeds
from agromind.parsers.prices import fetch_wholesale_herb_prices


def save_news(session: Session, items: list[dict[str, Any]]) -> int:
    added = 0

    for item in items:
        exists = session.scalar(select(News.id).where(News.url == item["url"]))
        if exists:
            continue

        session.add(
            News(
                title=item["title"],
                published_at=item["published_at"],
                url=item["url"],
            )
        )
        added += 1

    return added


def save_price_summaries(session: Session, items: list[dict[str, Any]]) -> int:
    added = 0

    for item in items:
        exists_stmt: Select[tuple[int]] = select(PriceSummary.id).where(
            and_(
                PriceSummary.crop_name == item["crop_name"],
                PriceSummary.wholesale_price == item["wholesale_price"],
                PriceSummary.published_at == item["published_at"],
                PriceSummary.region == item["region"],
            )
        )
        exists = session.scalar(exists_stmt)
        if exists:
            continue

        session.add(
            PriceSummary(
                crop_name=item["crop_name"],
                wholesale_price=item["wholesale_price"],
                published_at=item["published_at"],
                region=item["region"],
            )
        )
        added += 1

    return added


def refresh_data() -> dict[str, Any]:
    init_db()
    result: dict[str, Any] = {
        "news_added": 0,
        "prices_added": 0,
        "errors": [],
    }

    news_items: list[dict[str, Any]] = []
    price_items: list[dict[str, Any]] = []

    try:
        news_items = fetch_news_from_feeds()
    except Exception as exc:
        result["errors"].append(f"News parsing error: {exc}")

    try:
        price_items = fetch_wholesale_herb_prices()
    except Exception as exc:
        result["errors"].append(f"Price parsing error: {exc}")

    with session_scope() as session:
        if news_items:
            result["news_added"] = save_news(session, news_items)
        if price_items:
            result["prices_added"] = save_price_summaries(session, price_items)

    return result


def get_recent_news(limit: int = 20) -> list[dict[str, Any]]:
    init_db()
    with session_scope() as session:
        stmt = select(News).order_by(desc(News.published_at)).limit(limit)
        rows = session.scalars(stmt).all()

    return [
        {
            "title": row.title,
            "published_at": row.published_at,
            "url": row.url,
        }
        for row in rows
    ]


def get_latest_prices() -> list[dict[str, Any]]:
    init_db()
    with session_scope() as session:
        latest_subquery = (
            select(
                PriceSummary.crop_name.label("crop_name"),
                PriceSummary.region.label("region"),
                func.max(PriceSummary.published_at).label("latest_published_at"),
            )
            .group_by(PriceSummary.crop_name, PriceSummary.region)
            .subquery()
        )

        stmt = (
            select(PriceSummary)
            .join(
                latest_subquery,
                and_(
                    PriceSummary.crop_name == latest_subquery.c.crop_name,
                    PriceSummary.region == latest_subquery.c.region,
                    PriceSummary.published_at == latest_subquery.c.latest_published_at,
                ),
            )
            .order_by(PriceSummary.crop_name.asc(), PriceSummary.region.asc())
        )
        rows = session.scalars(stmt).all()

    return [
        {
            "Культура": row.crop_name,
            "Оптовая цена, руб.": row.wholesale_price,
            "Дата публикации": row.published_at.strftime("%Y-%m-%d %H:%M:%S"),
            "Регион": row.region,
        }
        for row in rows
    ]
