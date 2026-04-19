"""
rag_ingest.py — индексация справочников по гидропонике в ChromaDB.

Запуск:
    python rag_ingest.py                    # индексирует всё из папки docs/
    python rag_ingest.py --reset            # очищает базу и переиндексирует
    python rag_ingest.py docs/my_book.pdf  # индексирует один файл

Поддерживаемые форматы: .txt, .md, .pdf
"""
from __future__ import annotations

import argparse
import hashlib
import logging
from pathlib import Path

import chromadb
from chromadb.config import Settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("agromind.rag_ingest")

# ---------------------------------------------------------------------------
# Конфигурация
# ---------------------------------------------------------------------------

DOCS_DIR = Path("docs")
CHROMA_DIR = Path("data/chroma")
COLLECTION_NAME = "agromind_knowledge"

# Оптимальные параметры чанкинга для агрономических справочников:
# — 400 символов: достаточно для одного агрономического совета или описания болезни.
#   Меньше (200) — теряем контекст "симптом → причина → лечение" внутри чанка.
#   Больше (1000) — один чанк содержит несколько разных болезней,
#   топ-3 RAG будут "размытыми", без фокуса.
# — 80 символов перекрытия: сохраняем переходный контекст между чанками.
CHUNK_SIZE = 400
CHUNK_OVERLAP = 80

EMBED_MODEL = "nomic-embed-text"  # 274M параметров, ~550 МБ VRAM, запускается через Ollama CPU
OLLAMA_URL = "http://localhost:11434"


# ---------------------------------------------------------------------------
# Эмбеддинг через Ollama REST (без LangChain — меньше зависимостей)
# ---------------------------------------------------------------------------

def _embed_texts(texts: list[str]) -> list[list[float]]:
    """Получает эмбеддинги от Ollama батчами по 32 для экономии памяти."""
    import requests

    embeddings: list[list[float]] = []
    batch_size = 32

    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        response = requests.post(
            f"{OLLAMA_URL}/api/embed",
            json={"model": EMBED_MODEL, "input": batch},
            timeout=120,
        )
        response.raise_for_status()
        embeddings.extend(response.json()["embeddings"])

    return embeddings


# ---------------------------------------------------------------------------
# Чанкинг
# ---------------------------------------------------------------------------

def _chunk_text(text: str, source: str) -> list[dict]:
    """
    Нарезает текст на чанки с перекрытием.
    Старается резать по концу абзаца или предложения, а не по середине слова.
    """
    chunks: list[dict] = []
    start = 0
    text_len = len(text)

    while start < text_len:
        end = min(start + CHUNK_SIZE, text_len)

        if end < text_len:
            # Предпочитаем рвать по концу абзаца
            p = text.rfind("\n\n", max(0, end - 80), end)
            if p > start:
                end = p
            else:
                # Затем по концу предложения
                s = max(
                    text.rfind(". ", max(0, end - 60), end),
                    text.rfind(".\n", max(0, end - 60), end),
                )
                if s > start:
                    end = s + 1

        chunk_text = text[start:end].strip()

        if len(chunk_text) > 50:
            chunk_id = hashlib.md5(
                f"{source}:{start}:{chunk_text[:50]}".encode()
            ).hexdigest()
            chunks.append({
                "id": chunk_id,
                "text": chunk_text,
                "source": source,
                "chunk_start": start,
            })

        start = max(start + 1, end - CHUNK_OVERLAP)

    return chunks


# ---------------------------------------------------------------------------
# Загрузка файлов
# ---------------------------------------------------------------------------

def _load_txt(path: Path) -> str:
    for enc in ("utf-8", "cp1251", "latin-1"):
        try:
            return path.read_text(encoding=enc)
        except UnicodeDecodeError:
            continue
    raise RuntimeError(f"Не удалось прочитать {path}")


def _load_pdf(path: Path) -> str:
    try:
        from pdfminer.high_level import extract_text
        return extract_text(str(path))
    except ImportError:
        pass
    try:
        import PyPDF2
        parts: list[str] = []
        with open(path, "rb") as f:
            for page in PyPDF2.PdfReader(f).pages:
                parts.append(page.extract_text() or "")
        return "\n".join(parts)
    except ImportError:
        raise RuntimeError("Установите: pip install pdfminer.six")


def _load_file(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".txt", ".md"}:
        return _load_txt(path)
    elif suffix == ".pdf":
        return _load_pdf(path)
    raise ValueError(f"Неподдерживаемый формат: {suffix}")


# ---------------------------------------------------------------------------
# ChromaDB
# ---------------------------------------------------------------------------

def get_chroma_client() -> chromadb.ClientAPI:
    CHROMA_DIR.mkdir(parents=True, exist_ok=True)
    return chromadb.PersistentClient(
        path=str(CHROMA_DIR),
        settings=Settings(anonymized_telemetry=False),
    )


def get_or_create_collection(client: chromadb.ClientAPI) -> chromadb.Collection:
    return client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"},
    )


def ingest_file(path: Path, collection: chromadb.Collection, force: bool = False) -> int:
    logger.info("Обрабатываю: %s", path.name)

    try:
        raw_text = _load_file(path)
    except Exception as exc:
        logger.error("Не удалось загрузить %s: %s", path, exc)
        return 0

    chunks = _chunk_text(raw_text, source=path.name)
    if not chunks:
        logger.warning("Файл %s не дал ни одного чанка", path.name)
        return 0

    logger.info("  Нарезано %d чанков", len(chunks))

    # Инкрементальное обновление — пропускаем уже проиндексированные
    if not force:
        existing = set(collection.get(include=[])["ids"])
        chunks = [c for c in chunks if c["id"] not in existing]
        if not chunks:
            logger.info("  Все чанки уже проиндексированы")
            return 0

    logger.info("  Получаю эмбеддинги для %d новых чанков...", len(chunks))
    embeddings = _embed_texts([c["text"] for c in chunks])

    # Добавляем батчами по 100
    for i in range(0, len(chunks), 100):
        b = chunks[i : i + 100]
        e = embeddings[i : i + 100]
        collection.add(
            ids=[c["id"] for c in b],
            embeddings=e,
            documents=[c["text"] for c in b],
            metadatas=[{"source": c["source"], "chunk_start": c["chunk_start"]} for c in b],
        )

    logger.info("  Добавлено %d чанков", len(chunks))
    return len(chunks)


def ingest_directory(docs_dir: Path, collection: chromadb.Collection, force: bool = False) -> None:
    files = [
        f for f in docs_dir.iterdir()
        if f.is_file() and f.suffix.lower() in {".txt", ".md", ".pdf"}
    ]
    if not files:
        logger.warning("В папке %s нет поддерживаемых файлов", docs_dir)
        return

    total = 0
    for fp in sorted(files):
        total += ingest_file(fp, collection, force=force)
    logger.info("Индексация завершена. Всего добавлено: %d чанков", total)


def main() -> None:
    parser = argparse.ArgumentParser(description="AgroMind RAG Ingest")
    parser.add_argument("paths", nargs="*", help="Файлы для индексации (по умолчанию — вся папка docs/)")
    parser.add_argument("--reset", action="store_true", help="Сбросить и переиндексировать")
    args = parser.parse_args()

    client = get_chroma_client()

    if args.reset:
        logger.info("Сброс коллекции %s...", COLLECTION_NAME)
        try:
            client.delete_collection(COLLECTION_NAME)
        except Exception:
            pass

    collection = get_or_create_collection(client)
    logger.info("Чанков в базе: %d", collection.count())

    if args.paths:
        for p in args.paths:
            fp = Path(p)
            if fp.is_file():
                ingest_file(fp, collection, force=args.reset)
            else:
                logger.error("Файл не найден: %s", p)
    else:
        DOCS_DIR.mkdir(exist_ok=True)
        ingest_directory(DOCS_DIR, collection, force=args.reset)

    logger.info("Итого чанков в базе: %d", collection.count())


if __name__ == "__main__":
    main()
