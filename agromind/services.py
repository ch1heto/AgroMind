from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import pandas as pd
from sqlalchemy import Select, and_, desc, func, select
from sqlalchemy.orm import Session

from agromind.database import init_db, session_scope
from agromind.models import News, PriceSummary
from agromind.parsers.news import fetch_news_from_feeds
from agromind.parsers.prices import fetch_all_prices


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

    try:
        news_items = fetch_news_from_feeds()
    except Exception as exc:
        result["errors"].append(f"News parsing error: {exc}")

    with session_scope() as session:
        if news_items:
            result["news_added"] = save_news(session, news_items)
            session.commit()

        try:
            for batch in fetch_all_prices():
                if not batch:
                    continue
                result["prices_added"] += save_price_summaries(session, batch)
                session.commit()
        except Exception as exc:
            result["errors"].append(f"Price parsing error: {exc}")

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


def get_price_history_frame(days: int, crop_names: list[str] | None = None) -> pd.DataFrame:
    init_db()
    with session_scope() as session:
        stmt = select(PriceSummary).order_by(
            PriceSummary.published_at.asc(),
            PriceSummary.crop_name.asc(),
            PriceSummary.region.asc(),
        )

        if crop_names:
            stmt = stmt.where(PriceSummary.crop_name.in_(crop_names))

        if days > 0:
            stmt = stmt.where(
                PriceSummary.published_at >= datetime.utcnow() - timedelta(days=days)
            )

        rows = session.scalars(stmt).all()

    if not rows:
        return pd.DataFrame(
            columns=["timestamp", "crop_name", "region", "wholesale_price"]
        )

    return pd.DataFrame(
        [
            {
                "timestamp": row.published_at,
                "crop_name": row.crop_name,
                "region": row.region,
                "wholesale_price": row.wholesale_price,
            }
            for row in rows
        ]
    )


def get_latest_prices_frame(crop_names: list[str] | None = None) -> pd.DataFrame:
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

        if crop_names:
            stmt = stmt.where(PriceSummary.crop_name.in_(crop_names))

        rows = session.scalars(stmt).all()

    if not rows:
        return pd.DataFrame(
            columns=["crop_name", "region", "published_at", "wholesale_price"]
        )

    return pd.DataFrame(
        [
            {
                "crop_name": row.crop_name,
                "region": row.region,
                "published_at": row.published_at,
                "wholesale_price": row.wholesale_price,
            }
            for row in rows
        ]
    )


def get_crop_filters() -> list[str]:
    init_db()
    with session_scope() as session:
        stmt = select(PriceSummary.crop_name).distinct().order_by(PriceSummary.crop_name.asc())
        rows = session.scalars(stmt).all()

    return [crop_name for crop_name in rows if crop_name]
