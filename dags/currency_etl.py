import os
import requests
from datetime import datetime, timezone
from pymongo import MongoClient, ReplaceOne

try:  # Airflow 3 (newer Astro Runtime)
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2
    from airflow.decorators import dag, task

API_URL = "https://api.frankfurter.dev/v2/rates"


@dag(
    dag_id="currency_exchange_etl",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "currency"],
)
def currency_exchange_etl():

    @task
    def extract_exchange_rates(ti=None):
        params = {"base": "EUR", "quotes": "USD,GBP,JPY"}
        resp = requests.get(API_URL, params=params, timeout=30)
        resp.raise_for_status()
        ti.xcom_push(key="raw_exchange_rates", value=resp.json())

    @task
    def transform_exchange_rates(ti=None):
        raw = ti.xcom_pull(task_ids="extract_exchange_rates", key="raw_exchange_rates")
        retrieved_at = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0).isoformat()
        records = []

        if isinstance(raw, list):
            # v2 format: [{"date":..., "base":..., "quote":..., "rate":...}, ...]
            for item in raw:
                records.append({
                    "base_currency": item["base"],
                    "target_currency": item["quote"],
                    "exchange_rate": float(item["rate"]),
                    "rate_date": item["date"],
                    "source": "Frankfurter API",
                    "retrieved_at": retrieved_at,
                })
        else:
            # fallback format: {"base":..., "date":..., "rates": {"USD": 1.15, ...}}
            for quote, rate in raw["rates"].items():
                records.append({
                    "base_currency": raw["base"],
                    "target_currency": quote,
                    "exchange_rate": float(rate),
                    "rate_date": raw["date"],
                    "source": "Frankfurter API",
                    "retrieved_at": retrieved_at,
                })

        ti.xcom_push(key="transformed_exchange_rates", value=records)

    @task
    def load_to_mongodb(ti=None):
        records = ti.xcom_pull(task_ids="transform_exchange_rates", key="transformed_exchange_rates")
        client = MongoClient(os.environ["MONGO_URI"])
        col = client[os.environ["MONGO_DB"]][os.environ["MONGO_COLLECTION"]]
        ops = [
            ReplaceOne(
                {"base_currency": r["base_currency"],
                 "target_currency": r["target_currency"],
                 "rate_date": r["rate_date"]},
                r, upsert=True,
            )
            for r in records
        ]
        if ops:
            result = col.bulk_write(ops)
            print(f"Upserted: {result.upserted_count}, modified: {result.modified_count}")
        client.close()

    extract_exchange_rates() >> transform_exchange_rates() >> load_to_mongodb()


currency_exchange_etl()