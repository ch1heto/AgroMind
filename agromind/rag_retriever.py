from __future__ import annotations

from agromind.influx_client import get_aggregated_prices


class DataRetriever:
    def get_aggregated_context(self, culture: str, region: str) -> str:
        normalized_culture = (culture or "").strip()
        normalized_region = (region or "").strip()

        if not normalized_culture:
            return "Сводка по рынку: культура не определена, агрегированная статистика не рассчитана."

        aggregated = get_aggregated_prices(normalized_culture, normalized_region)
        if not aggregated:
            return (
                f"Сводка по рынку: Культура {normalized_culture}, Регион {normalized_region or 'не указан'}. "
                "Записей за последние 7 дней не найдено."
            )

        return (
            f"Сводка по рынку: Культура {normalized_culture}, Регион {normalized_region or 'не указан'}. "
            f"Средняя цена: {float(aggregated['avg']):.2f} руб, "
            f"Мин: {float(aggregated['min']):.2f} руб, "
            f"Макс: {float(aggregated['max']):.2f} руб. "
            f"Найдено записей за неделю: {int(aggregated['count'])}."
        )
