from __future__ import annotations

from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "agromind.db"

AGROINVESTOR_RSS_URL = "https://www.agroinvestor.ru/feed/public-agronews.xml"
AGRODAY_RSS_URL = "https://agroday.ru/rss/news/"
AGROBAZAR_HERBS_URL = "https://agrobazar.ru/herb/wholesale/all/moskva_rossiya/"

REQUEST_TIMEOUT = 30
DEFAULT_REGION = "Москва, Россия"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)
REQUEST_HEADERS = {
    "User-Agent": DEFAULT_USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}
