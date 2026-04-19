from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
from sqlalchemy import Select, and_, desc, func, select
from sqlalchemy import text
from sqlalchemy.orm import Session

from agromind.database import init_db, session_scope
from agromind.models import DemandSignal, FarmProfile, News, PriceSummary
from agromind.parsers.demand import fetch_demand_signals
from agromind.parsers.news import fetch_news_from_feeds
from agromind.parsers.prices import fetch_all_prices


logger = logging.getLogger(__name__)


class DialogueManager:
    SHORT_CONFIRMATIONS = {"да", "ок", "окей", "продолжай", "yes", "continue"}

    def __init__(self) -> None:
        init_db()

    def load_state(self) -> dict[str, Any]:
        with session_scope() as session:
            row = session.execute(
                text(
                    """
                    SELECT last_topic, awaiting_confirmation, farm_type
                    FROM dialogue_state
                    WHERE id = 1
                    """
                )
            ).mappings().first()

        if row is None:
            return {
                "last_topic": None,
                "awaiting_confirmation": False,
                "farm_type": None,
            }

        return {
            "last_topic": row["last_topic"],
            "awaiting_confirmation": bool(row["awaiting_confirmation"]),
            "farm_type": row["farm_type"],
        }

    def save_state(
        self,
        *,
        last_topic: str | None,
        awaiting_confirmation: bool,
        farm_type: str | None,
    ) -> None:
        with session_scope() as session:
            session.execute(
                text(
                    """
                    INSERT INTO dialogue_state (id, last_topic, awaiting_confirmation, farm_type, updated_at)
                    VALUES (1, :last_topic, :awaiting_confirmation, :farm_type, :updated_at)
                    ON CONFLICT(id) DO UPDATE SET
                        last_topic = excluded.last_topic,
                        awaiting_confirmation = excluded.awaiting_confirmation,
                        farm_type = excluded.farm_type,
                        updated_at = excluded.updated_at
                    """
                ),
                {
                    "last_topic": last_topic,
                    "awaiting_confirmation": int(awaiting_confirmation),
                    "farm_type": farm_type,
                    "updated_at": datetime.utcnow(),
                },
            )

    def get_context_filter(self, farm_profile: dict[str, Any] | None) -> dict[str, Any]:
        state = self.load_state()
        farm_type = str((farm_profile or {}).get("type") or state.get("farm_type") or "").strip().lower()
        return {
            "farm_type": farm_type or None,
            "include_weather": farm_type != "indoor",
        }

    def handle_short_answers(
        self,
        user_message: str,
        state: dict[str, Any],
    ) -> dict[str, Any] | None:
        normalized = re.sub(r"\s+", " ", str(user_message or "").strip().lower())
        if normalized not in self.SHORT_CONFIRMATIONS:
            return None

        last_topic = str(state.get("last_topic") or "").strip()
        if not last_topic:
            return None

        semantic_map = {
            "economics": "Продолжи финансовый разбор и точный бизнес-план по текущей культуре.",
            "rag_care": "Продолжи диагностику проблемы растения и рекомендации по уходу для текущей культуры.",
            "cultivation": "Продолжи рекомендации по выращиванию текущей культуры и следующему шагу по циклу.",
            "harvest": "Продолжи рекомендации по сбору урожая и завершению текущего цикла.",
        }
        resolved_message = semantic_map.get(
            last_topic,
            f"Продолжи последнюю тему разговора: {last_topic}.",
        )
        return {
            "resolved_message": resolved_message,
            "resolved_topic": last_topic,
        }

# ---------------------------------------------------------------------------
# Рабочие RSS-ленты новостей (проверены на 2026-04)
# ---------------------------------------------------------------------------
NEWS_FEEDS_ACTIVE = (
    "https://www.agroinvestor.ru/rss/public-agronews.xml",
    "https://www.agroinvestor.ru/rss/public-agroanalytics.xml",
    "https://glavagronom.ru/feed/",
    # fermer.ru отключён — отдаёт 403/timeout стабильно
)


def _try_write_influx(crop_name: str, region: str, price: float) -> None:
    """Пишет цену в InfluxDB. Ошибку логирует, не бросает."""
    try:
        from agromind.influx_client import write_price
        write_price(crop_name, region, price)
    except Exception as exc:
        logger.debug("InfluxDB write skipped: %s", exc)


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
        # Параллельно пишем в InfluxDB для графиков и RAG
        _try_write_influx(
            item["crop_name"],
            item["region"],
            float(item["wholesale_price"]),
        )
        added += 1
    return added


def save_demand_signals(session: Session, items: list[dict[str, Any]]) -> int:
    added = 0
    for item in items:
        exists = session.scalar(
            select(DemandSignal.id).where(DemandSignal.url == item["url"])
        )
        if exists:
            continue
        session.add(
            DemandSignal(
                crop_name=item["crop_name"],
                region=item["region"],
                contract_price=item["contract_price"],
                published_at=item["published_at"],
                url=item["url"],
            )
        )
        added += 1
    return added


def refresh_data() -> dict[str, Any]:
    init_db()
    result: dict[str, Any] = {
        "news_added": 0,
        "prices_added": 0,
        "demand_added": 0,
        "errors": [],
    }

    news_items: list[dict[str, Any]] = []
    demand_items: list[dict[str, Any]] = []

    try:
        news_items = fetch_news_from_feeds(feeds=NEWS_FEEDS_ACTIVE)
    except Exception as exc:
        result["errors"].append(f"News parsing error: {exc}")

    try:
        from agromind.ai_analyzer import AGRO_HANDBOOK
        demand_items = fetch_demand_signals(list(AGRO_HANDBOOK.keys()))
    except Exception as exc:
        result["errors"].append(f"Demand parsing error: {exc}")

    with session_scope() as session:
        if news_items:
            result["news_added"] = save_news(session, news_items)
            session.commit()

        if demand_items:
            try:
                result["demand_added"] = save_demand_signals(session, demand_items)
                session.commit()
            except Exception as exc:
                result["errors"].append(f"Demand save error: {exc}")

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
    return [{"title": r.title, "published_at": r.published_at, "url": r.url} for r in rows]


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
        return pd.DataFrame(columns=["timestamp", "crop_name", "region", "wholesale_price"])

    return pd.DataFrame([
        {
            "timestamp": r.published_at,
            "crop_name": r.crop_name,
            "region": r.region,
            "wholesale_price": r.wholesale_price,
        }
        for r in rows
    ])


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
        return pd.DataFrame(columns=["crop_name", "region", "published_at", "wholesale_price"])

    return pd.DataFrame([
        {
            "crop_name": r.crop_name,
            "region": r.region,
            "published_at": r.published_at,
            "wholesale_price": r.wholesale_price,
        }
        for r in rows
    ])


def get_latest_demand_signals_frame() -> pd.DataFrame:
    init_db()
    with session_scope() as session:
        stmt = select(DemandSignal).order_by(
            DemandSignal.published_at.desc(),
            DemandSignal.contract_price.desc(),
            DemandSignal.crop_name.asc(),
        )
        rows = session.scalars(stmt).all()

    if not rows:
        return pd.DataFrame(columns=["crop_name", "region", "contract_price", "published_at", "url"])

    return pd.DataFrame([
        {
            "crop_name": r.crop_name,
            "region": r.region,
            "contract_price": r.contract_price,
            "published_at": r.published_at,
            "url": r.url,
        }
        for r in rows
    ])


def get_farm_profile() -> dict[str, float]:
    init_db()
    with session_scope() as session:
        profile = session.get(FarmProfile, 1)
    if profile is None:
        return {"total_area_sqm": 0.0, "energy_price_kwh": 0.0}
    return {
        "total_area_sqm": float(profile.total_area_sqm or 0.0),
        "energy_price_kwh": float(profile.energy_price_kwh or 0.0),
    }


def save_farm_profile(area: float, energy_price: float) -> None:
    init_db()
    with session_scope() as session:
        profile = session.get(FarmProfile, 1)
        if profile is None:
            profile = FarmProfile(
                id=1,
                total_area_sqm=float(area or 0.0),
                energy_price_kwh=float(energy_price or 0.0),
                updated_at=datetime.utcnow(),
            )
            session.add(profile)
        else:
            profile.total_area_sqm = float(area or 0.0)
            profile.energy_price_kwh = float(energy_price or 0.0)
            profile.updated_at = datetime.utcnow()


def get_crop_filters() -> list[str]:
    init_db()
    with session_scope() as session:
        stmt = select(PriceSummary.crop_name).distinct().order_by(PriceSummary.crop_name.asc())
        rows = session.scalars(stmt).all()
    return [r for r in rows if r]
