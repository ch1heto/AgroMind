"""
rag_retriever.py — поиск по локальной базе знаний (ChromaDB + nomic-embed-text).

Содержит:
1. Умный триггер — определяет нужен ли RAG для данного запроса.
2. Функцию поиска — возвращает топ-N релевантных чанков.
3. Обратная совместимость — класс DataRetriever для InfluxDB-запросов сохранён.
"""
from __future__ import annotations

import logging
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

CHROMA_DIR = Path("data/chroma")
COLLECTION_NAME = "agromind_knowledge"
EMBED_MODEL = "nomic-embed-text"
OLLAMA_URL = "http://localhost:11434"
TOP_K = 3  # Топ-3 чанка достаточно для контекста в 4096 токенов


# ---------------------------------------------------------------------------
# Умный триггер — маршрутизация запросов
# ---------------------------------------------------------------------------

# Ключевые слова, при которых нужно искать в справочнике.
# Разбиты на категории для простой отладки.
_RAG_TRIGGER_PATTERNS: dict[str, list[str]] = {
    "болезни": [
        r"болезн\w*", r"заболева\w*", r"инфекц\w*", r"гриб\w*", r"плесен\w*",
        r"гниль", r"гни\w*", r"фитофтор\w*", r"мучнист\w*", r"пероноспор\w*",
        r"бактери\w*", r"вирус\w*", r"хлороз\w*", r"некроз\w*",
    ],
    "симптомы": [
        r"желте\w*", r"жёлте\w*", r"желтые\s+листья", r"жёлтые\s+листья",
        r"пятн\w*", r"бурые?\s+пятна", r"коричнев\w*\s+пятна",
        r"вян\w*", r"увяда\w*", r"скручива\w*", r"чернеют?",
        r"гибнет?\b", r"засыха\w*", r"отмира\w*",
        r"слабый\s+рост", r"плохо\s+растёт", r"не\s+растёт",
        r"бледн\w*", r"деформ\w*", r"трескает\w*",
    ],
    "вредители": [
        r"вредител\w*", r"тля\b", r"тли\b", r"клещ\w*", r"паутинный",
        r"трипс\w*", r"белокрылк\w*", r"мошк\w*", r"личинк\w*",
        r"нематод\w*", r"слизн\w*",
    ],
    "химия_раствора": [
        r"ph\b", r"рн\b", r"ec\b", r"эс\b",
        r"питательн\w*\s+раствор", r"раствор\s+питан\w*",
        r"концентрац\w*\s+раствор",
        r"дефицит\w*\s+(?:азот|фосфор|калий|кальций|магний|железо|цинк|марганец)",
        r"нехватк\w*\s+(?:азот|фосфор|калий|кальций|магний|железо)",
        r"избыток\w*\s+(?:азот|фосфор|калий|соли)",
        r"засолен\w*", r"жёсткость\s+воды", r"жесткость\s+воды",
        r"хлороз\w*", r"некроз\w*",
        r"азот\w*", r"фосфор\w*", r"калий\w*", r"кальций\w*", r"магний\w*",
    ],
    "диагностика": [
        r"почему\s+(?:желте|вян|чернее|сохн|гибн)\w*",
        r"что\s+(?:случил|происход|не\s+так)\w*",
        r"как\s+(?:лечить|спасти|исправить)\w*",
        r"диагноз\w*", r"диагностик\w*", r"определ\w*\s+болезн",
        r"помог\w*\s+(?:растени|листь|корн)\w*",
    ],
}

# Компилируем все паттерны в один regex для скорости
_ALL_TRIGGER_RE = re.compile(
    "|".join(
        p for patterns in _RAG_TRIGGER_PATTERNS.values() for p in patterns
    ),
    re.IGNORECASE,
)


def needs_rag_search(user_message: str) -> bool:
    """
    Возвращает True если запрос касается болезней, симптомов, вредителей
    или химии раствора — то есть нужен поиск в справочнике.

    Работает за O(len(message)) без обращений к GPU/CPU-моделям.
    """
    return bool(_ALL_TRIGGER_RE.search(user_message))


def get_triggered_category(user_message: str) -> str | None:
    """Возвращает название категории триггера для отладки (или None)."""
    for category, patterns in _RAG_TRIGGER_PATTERNS.items():
        for p in patterns:
            if re.search(p, user_message, re.IGNORECASE):
                return category
    return None


# ---------------------------------------------------------------------------
# Эмбеддинг запроса
# ---------------------------------------------------------------------------

def _embed_query(query: str) -> list[float]:
    """Получает вектор запроса от Ollama. Быстро — один текст, нет батча."""
    import requests

    response = requests.post(
        f"{OLLAMA_URL}/api/embed",
        json={"model": EMBED_MODEL, "input": [query]},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["embeddings"][0]


# ---------------------------------------------------------------------------
# Инициализация ChromaDB (ленивая, при первом запросе)
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _get_collection():
    """
    Открывает ChromaDB коллекцию. Кешируется — клиент создаётся один раз.
    Возвращает None если база не существует (RAG не проиндексирован).
    """
    if not CHROMA_DIR.exists():
        logger.warning(
            "ChromaDB не найдена (%s). "
            "Запустите: python rag_ingest.py для индексации справочников.",
            CHROMA_DIR,
        )
        return None

    try:
        import chromadb
        from chromadb.config import Settings

        client = chromadb.PersistentClient(
            path=str(CHROMA_DIR),
            settings=Settings(anonymized_telemetry=False),
        )

        collection = client.get_collection(name=COLLECTION_NAME)
        count = collection.count()

        if count == 0:
            logger.warning("ChromaDB коллекция пуста. Запустите python rag_ingest.py")
            return None

        logger.info("ChromaDB подключена: %d чанков в коллекции", count)
        return collection

    except Exception as exc:
        logger.warning("Не удалось подключиться к ChromaDB: %s", exc)
        return None


# ---------------------------------------------------------------------------
# Основная функция поиска
# ---------------------------------------------------------------------------

def search_knowledge_base(
    query: str,
    top_k: int = TOP_K,
    min_relevance: float = 0.25,  # Минимальный порог релевантности (косинусное расстояние)
) -> list[dict[str, Any]]:
    """
    Ищет в базе знаний топ-K наиболее релевантных чанков.

    Возвращает список словарей:
        [{"text": str, "source": str, "score": float}, ...]

    Возвращает [] если:
    - ChromaDB не проиндексирована
    - Ollama недоступна
    - Ни один чанк не прошёл порог релевантности
    """
    collection = _get_collection()
    if collection is None:
        return []

    try:
        query_embedding = _embed_query(query)
    except Exception as exc:
        logger.warning("Не удалось получить эмбеддинг запроса: %s", exc)
        return []

    try:
        results = collection.query(
            query_embeddings=[query_embedding],
            n_results=min(top_k, collection.count()),
            include=["documents", "metadatas", "distances"],
        )
    except Exception as exc:
        logger.warning("Ошибка запроса к ChromaDB: %s", exc)
        return []

    docs = results.get("documents", [[]])[0]
    metas = results.get("metadatas", [[]])[0]
    distances = results.get("distances", [[]])[0]

    output: list[dict[str, Any]] = []
    for doc, meta, dist in zip(docs, metas, distances):
        # ChromaDB с косинусным расстоянием: 0 = идентично, 2 = противоположно
        # Нормализуем в score: 1 = идентично, 0 = противоположно
        score = 1.0 - (dist / 2.0)

        if score < min_relevance:
            logger.debug("Чанк отфильтрован (score=%.3f < %.3f): %s...", score, min_relevance, doc[:60])
            continue

        output.append({
            "text": doc,
            "source": meta.get("source", "unknown"),
            "score": round(score, 3),
        })

    if output:
        logger.info(
            "RAG нашёл %d релевантных чанков (лучший score=%.3f)",
            len(output), output[0]["score"],
        )
    else:
        logger.info("RAG: релевантных чанков не найдено для запроса: %s...", query[:60])

    return output


def format_rag_context(chunks: list[dict[str, Any]]) -> str:
    """
    Форматирует найденные чанки в блок для системного промпта.
    Готов к вставке в user_prompt рядом с <CALCULATED_ECONOMICS>.
    """
    if not chunks:
        return ""

    lines = ["<KNOWLEDGE_BASE>"]
    lines.append("Найдено в справочнике по гидропонике:")
    for i, chunk in enumerate(chunks, 1):
        lines.append(f"\n[Источник {i}: {chunk['source']} | релевантность {chunk['score']:.2f}]")
        lines.append(chunk["text"])
    lines.append("</KNOWLEDGE_BASE>")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Обратная совместимость — DataRetriever для InfluxDB (не трогаем)
# ---------------------------------------------------------------------------

class DataRetriever:
    """
    Получает агрегированные ценовые данные из InfluxDB.
    Этот класс НЕ связан с ChromaDB RAG — это отдельный источник данных.
    """

    def get_aggregated_context(self, culture: str, region: str) -> str:
        from agromind.influx_client import get_aggregated_prices

        normalized_culture = (culture or "").strip()
        normalized_region = (region or "").strip()

        if not normalized_culture:
            return "Сводка по рынку: культура не определена."

        try:
            aggregated = get_aggregated_prices(normalized_culture, normalized_region)
        except Exception as exc:
            logger.warning("InfluxDB недоступна: %s", exc)
            return f"Сводка по рынку: InfluxDB недоступна ({exc})."

        if not aggregated:
            return (
                f"Сводка по рынку: {normalized_culture}, "
                f"регион {normalized_region or 'не указан'}. "
                "Данных за последние 7 дней нет."
            )

        return (
            f"Сводка по рынку: {normalized_culture}, "
            f"регион {normalized_region or 'не указан'}. "
            f"Средняя цена: {float(aggregated['avg']):.2f} руб, "
            f"мин: {float(aggregated['min']):.2f}, "
            f"макс: {float(aggregated['max']):.2f}. "
            f"Записей за неделю: {int(aggregated['count'])}."
        )
