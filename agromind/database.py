from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta
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
    now = datetime.utcnow()
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

        # VACUUM освобождает место на диске после массового удаления
        # Выполняется вне транзакции
        session.execute(text("PRAGMA wal_checkpoint(TRUNCATE)"))

    return deleted
