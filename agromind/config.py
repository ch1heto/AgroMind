from __future__ import annotations

from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "agromind.db"

AGROBAZAR_URLS = [
    "https://agrobazar.ru/herb/wholesale/all/",
]

AGROSERVER_URLS = [
    "https://agroserver.ru/zelen/",
]

FRUITINFO_URLS = [
    "https://fruitinfo.biz/ru/trade/veget/ukrop",
    "https://fruitinfo.biz/ru/trade/veget/luk-zeleniy/sale",
]

NEWS_FEEDS = (
    "https://www.agroinvestor.ru/feed/public-agronews.xml",
    "https://agroday.ru/rss/news/",
    "https://agri-news.ru/rss.php",
)

REQUEST_TIMEOUT = 30
MAX_PAGES = 3
DEFAULT_REGION = "Россия"
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
