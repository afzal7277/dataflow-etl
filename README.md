# dataflow-etl

![CI](https://github.com/afzal7277/dataflow-etl/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.11-blue)
![dbt](https://img.shields.io/badge/dbt-1.7-orange)
![Postgres](https://img.shields.io/badge/postgres-15-blue)
![Docker](https://img.shields.io/badge/docker-ready-2496ED)

End-to-end ETL pipeline that extracts data, loads it into PostgreSQL,
transforms it with dbt, and validates quality at every layer using
Great Expectations. Fully containerised with Docker.

---

## Architecture
```
Faker (extract)
    ↓
PostgreSQL raw_layer (load)
    ↓
Great Expectations checkpoint 1 (validate raw)
    ↓
dbt models — staging → marts (transform)
    ↓
Great Expectations checkpoint 2 (validate marts)
```

---

## Tech Stack

| Layer        | Tool                          |
|--------------|-------------------------------|
| Extract      | Python, Faker                 |
| Load         | psycopg2, UPSERT              |
| Transform    | dbt-core 1.7, PostgreSQL      |
| Validate     | Great Expectations 0.18       |
| Test         | pytest, pytest-mock           |
| Container    | Docker, Docker Compose        |
| CI/CD        | GitHub Actions                |

---

## Project Structure
```
dataflow-etl/
├── extract/
│   └── generator.py          # generates fake customers + orders
├── load/
│   └── postgres_loader.py    # UPSERT into PostgreSQL
├── dbt/
│   ├── models/
│   │   ├── staging/          # cleans raw data (views)
│   │   └── marts/            # business tables (tables)
│   └── tests/                # custom singular dbt tests
├── great_expectations/
│   └── ge_validate.py        # GE checkpoints
├── tests/
│   ├── unit/                 # 20 unit tests (no DB needed)
│   └── integration/          # 8 integration tests (real DB)
├── docker/
│   ├── Dockerfile
│   └── init.sql              # creates schemas + tables
├── docker-compose.yml
├── pipeline.py               # runs full pipeline end to end
└── .github/workflows/ci.yml  # GitHub Actions CI/CD
```

---

## Quick Start

### Prerequisites
- Docker Desktop running
- Git

### Run the full pipeline
```bash
git clone https://github.com/afzal7277/dataflow-etl.git
cd dataflow-etl
cp .env.example .env        # fill in your values

# Start PostgreSQL
docker compose up db -d

# Run extract + load + GE validation
docker compose run --rm -e POSTGRES_HOST=db -e POSTGRES_PORT=5432 pipeline python pipeline.py

# Run dbt models + tests
docker compose run --rm dbt run
docker compose run --rm dbt test
```

---

## Running Tests
```bash
# Create virtual environment
python -m venv .venv
.venv\Scripts\Activate.ps1    # Windows
source .venv/bin/activate      # Mac/Linux

# Install dependencies
pip install -r requirements.txt

# Start test database
docker compose up db-test -d

# Unit tests (no DB needed)
pytest tests/unit/ -v

# Integration tests (needs db-test running)
pytest tests/integration/ -v -m integration

# All tests
pytest tests/ -v
```

---

## Test Coverage

| Layer            | Tool               | Count  | Status |
|------------------|--------------------|--------|--------|
| Unit tests       | pytest             | 20     | ✅     |
| Integration tests| pytest             | 8      | ✅     |
| dbt tests        | dbt test           | 16     | ✅     |
| GE validations   | Great Expectations | 18     | ✅     |
| **Total**        |                    | **62** | ✅     |

---

## Data Flow

**Extract** — `generator.py` uses Faker to generate realistic
e-commerce data: 50 customers and 200 orders per run.

**Load** — `postgres_loader.py` uses UPSERT (INSERT ON CONFLICT)
so re-running the pipeline never creates duplicate rows.

**Transform** — dbt builds 4 models in two layers:
- `stg_customers`, `stg_orders` — clean and type-cast raw data
- `mart_customer_orders` — orders joined with customer details
- `mart_daily_revenue` — daily revenue aggregated by country and product

**Validate** — Great Expectations runs 18 checks across two
checkpoints: one on raw data after load, one on marts after dbt.

---

## CI/CD

Every push to `main` triggers two GitHub Actions jobs:

- **test** — unit tests + integration tests + data load
- **dbt-test** — dbt models + dbt tests + GE validation

Both must pass before merge.

---

## Author

**afzal7277** — [GitHub](https://github.com/afzal7277) · [LinkedIn](https://linkedin.com/in/zafarafzal) · [Portfolio](https://afzal7277.github.io)