from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from typing import Iterator

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from agromind.config import DATA_DIR, DB_PATH
from agromind.models import Base


DATA_DIR.mkdir(parents=True, exist_ok=True)


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    # Ждём до 30 сек если БД заблокирована воркером
    cursor.execute("PRAGMA busy_timeout=30000")
    cursor.close()


engine = create_engine(
    f"sqlite:///{DB_PATH.as_posix()}",
    future=True,
    echo=False,
    # Таймаут на уровне SQLAlchemy (дополнительная защита)
    # Для SQLite — один поток пишет, остальные ждут; check_same_thread=False
    # нужен т.к. Streamlit и worker в разных потоках
    connect_args={"timeout": 30, "check_same_thread": False},
)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,
    future=True,
)


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS user_plants (
                    id INTEGER PRIMARY KEY,
                    culture_name TEXT,
                    plant_date DATE,
                    is_active BOOLEAN DEFAULT 1
                )
                """
            )
        )


@contextmanager
def session_scope() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def add_active_plant(culture_name: str) -> None:
    init_db()
    with session_scope() as session:
        session.execute(
            text("UPDATE user_plants SET is_active = 0 WHERE is_active = 1")
        )
        session.execute(
            text(
                """
                INSERT INTO user_plants (culture_name, plant_date, is_active)
                VALUES (:culture_name, :plant_date, 1)
                """
            ),
            {
                "culture_name": (culture_name or "").strip(),
                "plant_date": date.today(),
            },
        )


def get_active_plant() -> dict | None:
    init_db()
    with session_scope() as session:
        result = session.execute(
            text(
                """
                SELECT culture_name, plant_date
                FROM user_plants
                WHERE is_active = 1
                ORDER BY id DESC
                LIMIT 1
                """
            )
        ).mappings().first()

    if result is None:
        return None

    plant_date_value = result["plant_date"]
    if isinstance(plant_date_value, str):
        plant_date_parsed = date.fromisoformat(plant_date_value)
    elif isinstance(plant_date_value, datetime):
        plant_date_parsed = plant_date_value.date()
    else:
        plant_date_parsed = plant_date_value

    days_active = (date.today() - plant_date_parsed).days
    return {
        "culture_name": result["culture_name"],
        "plant_date": plant_date_parsed,
        "days_active": days_active,
    }


def harvest_active_plant() -> None:
    init_db()
    with session_scope() as session:
        session.execute(
            text("UPDATE user_plants SET is_active = 0 WHERE is_active = 1")
        )


# ---------------------------------------------------------------------------
# TTL-очистка — вызывать из worker.py раз в сутки
# ---------------------------------------------------------------------------

def purge_old_records(
    *,
    price_ttl_days: int = 90,
    demand_ttl_days: int = 30,
    news_ttl_days: int = 60,
) -> dict[str, int]:
    """
    Удаляет устаревшие записи из всех таблиц.

    - price_ttl_days: цены старше N дней удаляются (дефолт 90)
    - demand_ttl_days: тендеры старше N дней удаляются (дефолт 30)
    - news_ttl_days:   новости старше N дней удаляются (дефолт 60)

    Возвращает словарь с количеством удалённых строк по каждой таблице.
    """
    now = datetime.now(timezone.utc)
    deleted: dict[str, int] = {}

    with session_scope() as session:
        price_cutoff = now - timedelta(days=price_ttl_days)
        result = session.execute(
            text("DELETE FROM price_summaries WHERE published_at < :cutoff"),
            {"cutoff": price_cutoff},
        )
        deleted["price_summaries"] = result.rowcount

        demand_cutoff = now - timedelta(days=demand_ttl_days)
        result = session.execute(
            text("DELETE FROM demand_signals WHERE published_at < :cutoff"),
            {"cutoff": demand_cutoff},
        )
        deleted["demand_signals"] = result.rowcount

        news_cutoff = now - timedelta(days=news_ttl_days)
        result = session.execute(
            text("DELETE FROM news WHERE published_at < :cutoff"),
            {"cutoff": news_cutoff},
        )
        deleted["news"] = result.rowcount

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text("PRAGMA wal_checkpoint(TRUNCATE)"))

    return deleted
