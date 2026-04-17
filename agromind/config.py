from __future__ import annotations

import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "agromind.db"

AGROBAZAR_URLS = [
    "https://agrobazar.ru/herb/wholesale/all/",
    "https://agrobazar.ru/herb/wholesale/mikrozelen/",
]

B2B_TRADE_URLS = [
    "https://b2b.trade/ru/catalog/all/food/fruits-vegetables-berries/greens-microeraine-salads-seedlings",
]

AGRORU_URLS = [
    "https://agroru.com/doska/salaty-i-zelen-optom/",
]

FRUITINFO_URLS = [
    "https://fruitinfo.biz/Russia/trade/veget/bazilik",
    "https://fruitinfo.biz/Russia/trade/veget/shpinat/sale",
    "https://fruitinfo.biz/all/trade/veget/zeleniy-salat/sale",
    "https://fruitinfo.biz/moskva/trade/veget/mikrozelen",
    "https://fruitinfo.biz/chelyabinsk/trade/veget/mikrozelen",
    "https://fruitinfo.biz/chelyabinsk/trade/veget/bazilik",
    "https://fruitinfo.biz/ekaterinburg/trade/veget/mikrozelen",
]

ORDERBRIDGE_URLS = [
    "https://orderbridge.ru/catalog/ovoschi_i_zelen/svezhie_ovoschi_pomidory_ogurtsy_kapusta_i_td/svezhaya_zelen_126",
]

NEWS_FEEDS = (
    "https://www.agroinvestor.ru/rss/public-agronews.xml",
    "https://www.agroinvestor.ru/rss/public-agroanalytics.xml",
    "https://fermer.ru/feed/forum/new",
    "https://glavagronom.ru/feed/",
)

DEFAULT_REGION = "Россия"
REQUEST_TIMEOUT = int(os.getenv("AGROMIND_REQUEST_TIMEOUT", "30"))
WORKER_INTERVAL_MINUTES = int(os.getenv("AGROMIND_WORKER_INTERVAL_MINUTES", "30"))
AUTO_REFRESH_INTERVAL_MS = int(os.getenv("AGROMIND_AUTO_REFRESH_MS", "60000"))
DEFAULT_HISTORY_DAYS = int(os.getenv("AGROMIND_HISTORY_DAYS", "30"))

INFLUXDB_URL = os.getenv("AGROMIND_INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("AGROMIND_INFLUXDB_TOKEN", "agromind-token")
INFLUXDB_ORG = os.getenv("AGROMIND_INFLUXDB_ORG", "agromind")
INFLUXDB_BUCKET = os.getenv("AGROMIND_INFLUXDB_BUCKET", "agromind_prices")
INFLUXDB_PRICE_MEASUREMENT = "wholesale_prices"

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
