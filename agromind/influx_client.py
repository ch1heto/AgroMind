from __future__ import annotations

import os
from typing import Any

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


INFLUX_URL = os.getenv("AGROMIND_INFLUXDB_URL", "http://localhost:8086")
INFLUX_TOKEN = os.getenv("AGROMIND_INFLUXDB_TOKEN", "agromind_super_secret_token")
INFLUX_ORG = os.getenv("AGROMIND_INFLUXDB_ORG", "agromind_org")
INFLUX_BUCKET = os.getenv("AGROMIND_INFLUXDB_BUCKET", "agromind_bucket")
INFLUX_MEASUREMENT = "market_prices"

_client: InfluxDBClient | None = None


def _get_client() -> InfluxDBClient:
    global _client
    if _client is None:
        _client = InfluxDBClient(
            url=INFLUX_URL,
            token=INFLUX_TOKEN,
            org=INFLUX_ORG,
        )
    return _client


def write_price(culture: str, region: str, price: float) -> None:
    point = (
        Point(INFLUX_MEASUREMENT)
        .tag("culture", (culture or "").strip())
        .tag("region", (region or "").strip())
        .field("price", float(price))
        .time(None, WritePrecision.NS)
    )

    client = _get_client()
    write_api = client.write_api(write_options=SYNCHRONOUS)
    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)


def get_aggregated_prices(culture: str, region: str) -> dict[str, Any] | None:
    normalized_culture = (culture or "").strip()
    normalized_region = (region or "").strip()

    if not normalized_culture:
        return None

    escaped_culture = normalized_culture.replace("\\", "\\\\").replace('"', '\\"')
    escaped_region = normalized_region.replace("\\", "\\\\").replace('"', '\\"')

    flux_query = f"""
base =
    from(bucket: "{INFLUX_BUCKET}")
        |> range(start: -7d)
        |> filter(fn: (r) => r._measurement == "{INFLUX_MEASUREMENT}")
        |> filter(fn: (r) => r._field == "price")
        |> filter(fn: (r) => r.culture == "{escaped_culture}")
        |> filter(fn: (r) => "{escaped_region}" == "" or r.region == "{escaped_region}")

min_t = base |> min() |> set(key: "metric", value: "min")
max_t = base |> max() |> set(key: "metric", value: "max")
mean_t = base |> mean() |> set(key: "metric", value: "avg")
count_t = base |> count() |> set(key: "metric", value: "count")

union(tables: [min_t, max_t, mean_t, count_t])
    |> keep(columns: ["metric", "_value"])
"""

    client = _get_client()
    tables = client.query_api().query(flux_query, org=INFLUX_ORG)

    aggregated = {"avg": None, "min": None, "max": None, "count": 0}

    for table in tables:
        for record in table.records:
            metric = record.values.get("metric")
            value = record.get_value()
            if metric == "count":
                aggregated["count"] = int(value or 0)
            elif metric in {"avg", "min", "max"} and value is not None:
                aggregated[metric] = float(value)

    if aggregated["count"] == 0:
        return None

    return aggregated
