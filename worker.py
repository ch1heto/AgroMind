from __future__ import annotations

import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.schedulers.background import BackgroundScheduler

from agromind.config import DATA_DIR, WORKER_INTERVAL_MINUTES
from agromind.database import init_db, purge_old_records
from agromind.services import refresh_data


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("agromind.worker")

# Файл-замок: если существует — другой экземпляр воркера уже запущен
LOCK_FILE = DATA_DIR / "worker.lock"
# Файл healthcheck: Streamlit может читать его чтобы показать статус воркера
HEALTH_FILE = DATA_DIR / "worker_health.json"

# Раз в N циклов запускать TTL-очистку (1 раз в сутки при интервале 30 мин = каждые 48 циклов)
PURGE_EVERY_N_CYCLES = max(1, int(1440 / max(WORKER_INTERVAL_MINUTES, 1)))

_cycle_counter = 0


def _write_health(status: str, error: str | None = None) -> None:
    import json

    payload = {
        "status": status,
        "last_update": datetime.now(timezone.utc).isoformat(),
        "error": error,
    }
    try:
        HEALTH_FILE.write_text(json.dumps(payload, ensure_ascii=False))
    except OSError:
        pass


def _acquire_lock() -> bool:
    """Создаёт lock-файл с PID. Возвращает False если уже заблокировано."""
    if LOCK_FILE.exists():
        try:
            pid = int(LOCK_FILE.read_text().strip())
            # Проверяем жив ли процесс с таким PID
            os.kill(pid, 0)
            logger.warning("Worker already running with PID %s. Exiting.", pid)
            return False
        except ValueError:
            # Процесс мёртв — зачищаем старый замок
            logger.warning("Stale lock file found. Removing.")
            LOCK_FILE.unlink(missing_ok=True)
        except OSError:
            logger.warning("Worker already running or lock is owned by another process. Exiting.")
            return False

    LOCK_FILE.write_text(str(os.getpid()))
    return True


def _release_lock() -> None:
    LOCK_FILE.unlink(missing_ok=True)


def job() -> None:
    global _cycle_counter
    _cycle_counter += 1

    logger.info("Cycle #%s started.", _cycle_counter)
    _write_health("running")

    # --- Основной сбор данных ---
    max_retries = 3
    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            result = refresh_data()
            logger.info(
                "Cycle #%s done: news=%s prices=%s demand=%s errors=%s",
                _cycle_counter,
                result["news_added"],
                result["prices_added"],
                result["demand_added"],
                result["errors"],
            )
            _write_health("ok")
            last_error = None
            break
        except Exception as exc:
            last_error = exc
            logger.warning("Cycle #%s attempt %s/%s failed: %s", _cycle_counter, attempt, max_retries, exc)
            if attempt < max_retries:
                time.sleep(10 * attempt)  # экспоненциальная пауза: 10с, 20с

    if last_error:
        logger.error("Cycle #%s failed after %s attempts: %s", _cycle_counter, max_retries, last_error)
        _write_health("error", str(last_error))

    # --- TTL-очистка раз в сутки ---
    if _cycle_counter % PURGE_EVERY_N_CYCLES == 0:
        try:
            deleted = purge_old_records()
            logger.info("TTL purge: %s", deleted)
        except Exception as exc:
            logger.warning("TTL purge failed: %s", exc)


def _on_job_event(event) -> None:
    if event.exception:
        logger.error("Scheduler job crashed: %s", event.exception)
        _write_health("crash", str(event.exception))


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if not _acquire_lock():
        sys.exit(1)

    init_db()
    _write_health("starting")

    # Первый запуск немедленно
    logger.info("Worker starting. Running first collection cycle...")
    job()

    interval = max(WORKER_INTERVAL_MINUTES, 1)
    scheduler = BackgroundScheduler(
        job_defaults={"max_instances": 1, "coalesce": True, "misfire_grace_time": 300},
    )
    scheduler.add_listener(_on_job_event, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
    scheduler.add_job(
        job,
        trigger="interval",
        minutes=interval,
        id="agromind-refresh",
        replace_existing=True,
        next_run_time=datetime.now() + timedelta(minutes=interval),
    )

    logger.info("Scheduler started. Interval: %s min. Purge every %s cycles.", interval, PURGE_EVERY_N_CYCLES)

    try:
        scheduler.start()
        # Держим главный поток живым
        while True:
            time.sleep(30)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Worker stopping...")
        scheduler.shutdown(wait=False)
    finally:
        _release_lock()
        _write_health("stopped")
        logger.info("Worker stopped.")


if __name__ == "__main__":
    main()
