from __future__ import annotations

import logging

from apscheduler.schedulers.blocking import BlockingScheduler

from agromind.config import WORKER_INTERVAL_MINUTES
from agromind.database import init_db
from agromind.services import refresh_data


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("agromind.worker")


def collect_once() -> None:
    result = refresh_data()
    logger.info(
        "Cycle finished: news_added=%s, prices_added=%s, errors=%s",
        result["news_added"],
        result["prices_added"],
        result["errors"],
    )


def main() -> None:
    init_db()
    collect_once()

    scheduler = BlockingScheduler()
    scheduler.add_job(
        collect_once,
        trigger="interval",
        minutes=max(WORKER_INTERVAL_MINUTES, 1),
        id="agromind-refresh",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )

    logger.info("Worker started. Interval: %s minute(s).", max(WORKER_INTERVAL_MINUTES, 1))
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Worker stopped.")


if __name__ == "__main__":
    main()
