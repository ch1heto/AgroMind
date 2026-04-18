from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from agromind.database import engine


class DataRetriever:
    def get_aggregated_context(self, culture: str, region: str) -> str:
        normalized_culture = (culture or "").strip()
        normalized_region = (region or "").strip()

        if not normalized_culture:
            return "Сводка по рынку: культура не определена, агрегированная статистика не рассчитана."

        cutoff = datetime.now(timezone.utc) - timedelta(days=7)

        query = text(
            """
            SELECT
                AVG(wholesale_price) AS avg_price,
                MIN(wholesale_price) AS min_price,
                MAX(wholesale_price) AS max_price,
                COUNT(*) AS total_count
            FROM price_summaries
            WHERE crop_name = :culture
              AND published_at >= :cutoff
              AND (:region = '' OR instr(region, :region) > 0 OR instr(lower(region), lower(:region)) > 0)
            """
        )

        with engine.connect() as conn:
            row = conn.execute(
                query,
                {
                    "culture": normalized_culture,
                    "region": normalized_region,
                    "cutoff": cutoff,
                },
            ).mappings().first()

        if not row or int(row["total_count"] or 0) == 0:
            return (
                f"Сводка по рынку: Культура {normalized_culture}, Регион {normalized_region or 'не указан'}. "
                "Записей за последние 7 дней не найдено."
            )

        return (
            f"Сводка по рынку: Культура {normalized_culture}, Регион {normalized_region or 'не указан'}. "
            f"Средняя цена: {float(row['avg_price']):.2f} руб, "
            f"Мин: {float(row['min_price']):.2f} руб, "
            f"Макс: {float(row['max_price']):.2f} руб. "
            f"Найдено записей за неделю: {int(row['total_count'])}."
        )
