from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Float, Integer, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class News(Base):
    __tablename__ = "news"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    published_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    url: Mapped[str] = mapped_column(String(1000), nullable=False, unique=True)


class PriceSummary(Base):
    __tablename__ = "price_summaries"
    __table_args__ = (
        UniqueConstraint(
            "crop_name",
            "wholesale_price",
            "published_at",
            "region",
            name="uq_price_summary_snapshot",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    crop_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    wholesale_price: Mapped[float] = mapped_column(Float, nullable=False)
    published_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    region: Mapped[str] = mapped_column(String(255), nullable=False, index=True)


class DemandSignal(Base):
    __tablename__ = "demand_signals"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    crop_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    region: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    contract_price: Mapped[float] = mapped_column(Float, nullable=False)
    published_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    url: Mapped[str] = mapped_column(String(1000), nullable=False, unique=True)
