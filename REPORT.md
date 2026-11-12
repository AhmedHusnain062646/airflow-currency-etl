# Airflow ETL Pipeline — Currency Exchange Rates

## Report: Building Airflow ETL Pipeline with ASTRO CLI

**Student:** Ahmed Husnain  
**Date:** November 12, 2026  
**Repository:** [https://github.com/AhmedHusnain062646/airflow-currency-etl](https://github.com/AhmedHusnain062646/airflow-currency-etl)

---

## 1. Project Overview

This project implements a daily ETL (Extract, Transform, Load) pipeline using **Apache Airflow** (via the **Astro CLI**) that:
1. **Extracts** live currency exchange rates from the [Frankfurter API](https://frankfurter.dev/)
2. **Transforms** the raw API response into a structured format
3. **Loads** the transformed data into **MongoDB**

The infrastructure includes **Mongo Express** as a web-based administration interface and uses **Docker networking** and **persistent storage**.

---

## 2. Project Structure

```
airflow_currency_etl/
├── dags/
│   └── currency_etl.py          # Main DAG with 3 ETL tasks
├── screenshots/                  # Report screenshots
├── docker-compose.override.yml   # MongoDB & Mongo Express services
├── Dockerfile                    # Astro Runtime base image
├── requirements.txt              # Python dependencies (requests, pymongo)
├── .env                          # MongoDB connection config
├── airflow_settings.yaml         # Airflow local settings
├── packages.txt                  # System packages
└── tests/
    └── dags/
        └── test_dag_example.py   # DAG validation tests
```

---

## 3. Code Implementation

### 3.1 DAG Code — `dags/currency_etl.py`

The DAG defines three tasks using the TaskFlow API (`@task` decorator) and uses **Named XCom** to transfer data between tasks.

```python
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
```

**Key Points:**
- Uses **Named XCom** (`xcom_push`/`xcom_pull` with `key` parameter) to transfer data between tasks
- Task chain: `extract → transform → load`
- Handles both Frankfurter API v2 list format and fallback dict format
- Uses `ReplaceOne` with `upsert=True` to avoid duplicate records

### 3.2 Docker Compose Override — `docker-compose.override.yml`

```yaml
services:
  mongo:
    image: mongo:7
    container_name: mongo
    restart: unless-stopped
    environment:
      MONGO_INITDB_ROOT_USERNAME: admin
      MONGO_INITDB_ROOT_PASSWORD: admin123
    ports:
      - "27017:27017"
    volumes:
      - mongo_data:/data/db          # persistent storage
    networks:
      - airflow                      # docker networking
    labels:
      lab.network: "airflow (shared bridge network with Airflow services)"
      lab.storage: "named volume mongo_data -> /data/db"
    healthcheck:
      test: ["CMD", "mongosh", "--quiet", "--eval", "db.adminCommand('ping')"]
      interval: 10s
      timeout: 5s
      retries: 10

  mongo-express:
    image: mongo-express:latest
    container_name: mongo-express
    restart: unless-stopped
    depends_on:
      mongo:
        condition: service_healthy
    environment:
      ME_CONFIG_MONGODB_URL: mongodb://admin:admin123@mongo:27017/
      ME_CONFIG_BASICAUTH_USERNAME: admin
      ME_CONFIG_BASICAUTH_PASSWORD: admin123
    ports:
      - "8081:8081"
    networks:
      - airflow
    labels:
      lab.network: "airflow (reaches mongo by service name)"
      lab.storage: "stateless; data lives in mongo_data"

volumes:
  mongo_data:
```

**Meta Details:**
- **Docker Networking:** All services (Airflow, MongoDB, Mongo Express) share the `airflow` bridge network
- **Persistent Storage:** Named volume `mongo_data` is mounted to `/data/db` for MongoDB data persistence

### 3.3 Dockerfile

```dockerfile
FROM astrocrpublic.azurecr.io/runtime:3.3-12
```

### 3.4 Requirements

```
requests
pymongo
```

### 3.5 Environment Variables (`.env`)

```
MONGO_URI=mongodb://admin:admin123@mongo:27017/?authSource=admin
MONGO_DB=currency_db
MONGO_COLLECTION=exchange_rates
```

---

## 4. Screenshots & Output

### 4.1 Airflow Home — Dashboard Overview

The Airflow home page shows system health (all components healthy), DAG run statistics, and task instance summaries.

![Airflow Home Dashboard](screenshots/01_airflow_home.png)

### 4.2 DAGs List

Shows the `currency_exchange_etl` DAG with tags `currency, etl`, scheduled daily, and the latest successful run.

![DAGs List](screenshots/02_airflow_dags_list.png)

### 4.3 DAG Overview

Shows the DAG overview with run history chart, task status sidebar (extract_exchange_rates ✅, transform_exchange_rates ✅, load_to_mongodb showing execution attempts).

![DAG Overview](screenshots/03_dag_overview.png)

### 4.4 DAG Details

Shows DAG metadata including Dag ID, file location (`/usr/local/airflow/dags/currency_etl.py`), schedule, start date, and version information.

![DAG Details](screenshots/04_dag_details.png)

### 4.5 DAG Runs

Shows all DAG runs with their states. Multiple successful runs (green "Success" badges) confirm the pipeline works correctly end-to-end.

![DAG Runs](screenshots/05_dag_runs.png)

### 4.6 Tasks Definition

Shows the 3 tasks defined in the DAG: `extract_exchange_rates`, `load_to_mongodb`, and `transform_exchange_rates` — all using the `@task` operator.

![DAG Tasks](screenshots/06_dag_tasks.png)

### 4.7 DAG Code View

The Airflow Code tab displays the full source code of the DAG file as parsed by Airflow.

![DAG Code](screenshots/07_dag_code.png)

### 4.8 Mongo Express — Home Page

Mongo Express web interface showing available databases including `currency_db`, MongoDB version 7.0.48, and server status.

![Mongo Express Home](screenshots/08_mongo_express_home.png)

### 4.9 Mongo Express — currency_db Database

Shows the `currency_db` database with the `exchange_rates` collection, 3 objects (documents), and database statistics.

![Mongo Express currency_db](screenshots/09_mongo_express_currency_db.png)

### 4.10 Mongo Express — exchange_rates Collection

Shows the 3 exchange rate documents stored in MongoDB with the transformed data structure:

| Field | EUR→GBP | EUR→JPY | EUR→USD |
|-------|---------|---------|---------|
| `base_currency` | EUR | EUR | EUR |
| `target_currency` | GBP | JPY | USD |
| `exchange_rate` | 0.83559 | 177.28 | 1.1122 |
| `rate_date` | 2026-11-12 | 2026-11-12 | 2026-11-12 |
| `source` | Frankfurter API | Frankfurter API | Frankfurter API |
| `retrieved_at` | 2026-11-12T16:43:05 | 2026-11-12T16:43:05 | 2026-11-12T16:43:05 |

![Mongo Express exchange_rates](screenshots/10_mongo_express_exchange_rates.png)

---

## 5. Docker Infrastructure

### 5.1 Running Containers

```
NAMES                                         STATUS                  PORTS
airflow-currency-etl_ee0a87-triggerer-1       Up About an hour
airflow-currency-etl_ee0a87-dag-processor-1   Up About an hour
airflow-currency-etl_ee0a87-api-server-1      Up About an hour        127.0.0.1:8080->8080/tcp
airflow-currency-etl_ee0a87-scheduler-1       Up About an hour
mongo                                         Up About an hour        0.0.0.0:27017->27017/tcp
mongo-express                                 Up About an hour        0.0.0.0:8081->8081/tcp
airflow-currency-etl_ee0a87-postgres-1        Up About an hour        127.0.0.1:5432->5432/tcp
```

### 5.2 Docker Networking

All services are connected to the shared bridge network:

```
NETWORK NAME                              DRIVER
airflow-currency-etl_ee0a87_airflow       bridge
```

Both `mongo` and `mongo-express` containers are connected to the `airflow-currency-etl_ee0a87_airflow` network, enabling service-name-based DNS resolution (e.g., Airflow tasks connect to MongoDB using `mongo:27017`).

### 5.3 Persistent Storage

```
VOLUME NAME                                     DRIVER
airflow-currency-etl_ee0a87_mongo_data           local
```

MongoDB data is stored in the named volume `mongo_data` (mapped to `/data/db` inside the container), ensuring data persists across container restarts.

### 5.4 MongoDB Data Verification

```json
[
  {
    "_id": "ObjectId('6af5e8c7dcaadca8ce775fc5')",
    "source": "Frankfurter API",
    "rate_date": "2026-11-12",
    "retrieved_at": "2026-11-12T16:43:05",
    "base_currency": "EUR",
    "exchange_rate": 0.83559,
    "target_currency": "GBP"
  },
  {
    "_id": "ObjectId('6af5e8c7dcaadca8ce775fc6')",
    "source": "Frankfurter API",
    "rate_date": "2026-11-12",
    "retrieved_at": "2026-11-12T16:43:05",
    "base_currency": "EUR",
    "exchange_rate": 177.28,
    "target_currency": "JPY"
  },
  {
    "_id": "ObjectId('6af5e8c7dcaadca8ce775fc7')",
    "source": "Frankfurter API",
    "rate_date": "2026-11-12",
    "retrieved_at": "2026-11-12T16:43:05",
    "base_currency": "EUR",
    "exchange_rate": 1.1122,
    "target_currency": "USD"
  }
]
```

---

## 6. Named XCom Usage

Data is transferred between tasks using **Named XCom** with explicit keys:

1. **Extract → Transform:** `ti.xcom_push(key="raw_exchange_rates", value=resp.json())` and `ti.xcom_pull(task_ids="extract_exchange_rates", key="raw_exchange_rates")`
2. **Transform → Load:** `ti.xcom_push(key="transformed_exchange_rates", value=records)` and `ti.xcom_pull(task_ids="transform_exchange_rates", key="transformed_exchange_rates")`

---

## 7. Transformed Data Format

Each record follows the required structure:

```json
{
    "base_currency": "EUR",
    "target_currency": "USD",
    "exchange_rate": 1.1122,
    "rate_date": "2026-11-12",
    "source": "Frankfurter API",
    "retrieved_at": "2026-11-12T16:43:05"
}
```

---

## 8. Conclusion

The Airflow ETL pipeline was successfully implemented and deployed using the Astro CLI with:
- ✅ Three tasks: Extract, Transform, Load
- ✅ Frankfurter API integration for currency exchange rates (EUR → USD, GBP, JPY)
- ✅ Named XCom for inter-task data transfer
- ✅ MongoDB as the target database with Mongo Express admin interface
- ✅ Docker networking (shared `airflow` bridge network)
- ✅ Persistent storage (named volume `mongo_data`)
- ✅ Daily schedule with successful execution verified
