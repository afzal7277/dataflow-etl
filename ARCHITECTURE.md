# Architecture — dataflow-etl

## Overview

dataflow-etl is a production-grade ETL pipeline that extracts e-commerce data,
loads it into PostgreSQL, transforms it with dbt, and validates quality at every
layer using Great Expectations. Everything runs inside Docker.

---

## Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                        EXTRACT                                  │
│   generator.py — Faker generates 50 customers + 200 orders      │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                         LOAD                                    │
│   postgres_loader.py — UPSERT into raw_layer schema             │
│   raw_layer.customers     raw_layer.orders                      │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│              VALIDATE — Checkpoint 1 (11 checks)                │
│   Great Expectations validates raw data before transformation   │
│   Checks: nulls, uniqueness, value ranges, row counts           │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                       TRANSFORM                                 │
│   dbt runs 4 models in 2 layers                                 │
│                                                                 │
│   staging/                                                      │
│   ├── stg_customers   (view) — clean, type-cast customers       │
│   └── stg_orders      (view) — clean, type-cast orders          │
│                                                                 │
│   marts/                                                        │
│   ├── mart_customer_orders  (table) — orders + customer details │
│   └── mart_daily_revenue    (table) — revenue by date/country   │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│              VALIDATE — Checkpoint 2 (7 checks)                 │
│   Great Expectations validates mart tables after dbt            │
│   Checks: revenue positive, row counts, valid countries         │
└─────────────────────────────────────────────────────────────────┘
```

---

## Database Schema

Three schemas keep data separated by layer:

```
dataflow_db
├── raw_layer/            ← raw data, never modified after load
│   ├── customers
│   └── orders
│
└── staging/              ← dbt output (views + tables)
    ├── stg_customers     (view)
    ├── stg_orders        (view)
    ├── mart_customer_orders  (table)
    └── mart_daily_revenue    (table)
```

---

## Tech Stack

| Layer        | Tool                     | Why                                                  |
|--------------|--------------------------|------------------------------------------------------|
| Extract      | Python, Faker            | Realistic synthetic data, fully controlled           |
| Load         | psycopg2, UPSERT         | Idempotent loads — safe to retry on failure          |
| Transform    | dbt-core 1.7             | Version-controlled SQL, built-in lineage + tests     |
| Validate     | Great Expectations 0.18  | Business rule checks at raw and mart layers          |
| Database     | PostgreSQL 15            | Reliable, well-supported, works locally and in cloud |
| Container    | Docker + Compose         | Reproducible, no local dependency conflicts          |
| Test         | pytest, pytest-mock      | Fast unit tests + real DB integration tests          |
| CI/CD        | GitHub Actions           | Automated test runs on every push                   |

---

## Docker Services

```
docker-compose.yml
├── db              postgres:15  — main DB, port 5435
├── db-test         postgres:15  — isolated test DB, port 5436
├── pipeline        python:3.11  — runs extract + load + GE
└── dbt             dbt-postgres:1.7.11 — runs transformations
```

All services connect via Docker's internal network.
`db` and `db-test` use the same `init.sql` to create identical schemas.

---

## dbt Model Layers

### Staging layer (views)
- One model per raw table — no joins allowed
- Responsibilities: rename columns, cast types, standardise strings
- `stg_customers` — lowercases email, initcap country, casts signup_date to timestamp
- `stg_orders` — casts quantity/price/amount to correct numerics, lowercases status

### Marts layer (tables)
- Consume from staging only — never touch raw tables directly
- Materialised as tables for query performance
- `mart_customer_orders` — wide table joining orders + customer details (used by BI tools)
- `mart_daily_revenue` — daily aggregation by country + product, excludes cancelled orders

---

## Testing Strategy

Four layers of testing, 62 automated checks total:

```
┌─────────────────────┬────────┬──────────────────────────────────┐
│ Layer               │ Count  │ What it checks                   │
├─────────────────────┼────────┼──────────────────────────────────┤
│ Unit tests          │  20    │ Python logic, no DB required      │
│ Integration tests   │   8    │ Full load flow, real Postgres     │
│ dbt tests           │  16    │ Schema integrity, relationships   │
│ GE validations      │  18    │ Business rules, value ranges      │
├─────────────────────┼────────┼──────────────────────────────────┤
│ Total               │  62    │ All passing                       │
└─────────────────────┴────────┴──────────────────────────────────┘
```

### Unit tests (`tests/unit/`)
- No database, no Docker needed
- Uses `pytest-mock` to patch `psycopg2.connect`
- Tests: record counts, field validation, total amount calculation, status values

### Integration tests (`tests/integration/`)
- Runs against real `db-test` container
- `clean_tables` fixture truncates all tables before and after each test
- Tests: actual row counts in DB, data correctness, idempotency (load twice = same count)

### dbt tests (`dbt/models/*/schema.yml`)
- `unique` and `not_null` on all primary keys
- `accepted_values` on status column
- `not_null` on all critical mart columns

### Great Expectations (`great_expectations/ge_validate.py`)
- Checkpoint 1 on `raw_layer.orders` — 11 checks
- Checkpoint 2 on `staging.mart_daily_revenue` — 7 checks
- Script exits with code 1 on failure — blocks CI/CD pipeline

---

## CI/CD Pipeline

Two GitHub Actions jobs run on every push to `main`:

```
Push to main
│
├── Job 1: test
│   ├── Start postgres (main + test DB)
│   ├── Install Python 3.11 dependencies
│   ├── Init DB schemas (init.sql)
│   ├── Run unit tests (20 tests)
│   ├── Run integration tests (8 tests)
│   └── Load data into main DB
│
└── Job 2: dbt-test  (only runs if Job 1 passes)
    ├── Start fresh postgres
    ├── Init schema + load data
    ├── Install dbt-postgres==1.7.4
    ├── dbt deps
    ├── dbt run  (builds 4 models)
    ├── dbt test (16 tests)
    └── Great Expectations validation (18 checks)
```

---

## Key Engineering Decisions

**Why UPSERT instead of truncate + reload?**
UPSERT is atomic per row. If the pipeline crashes mid-run, existing data is untouched.
Truncate + reload leaves the table empty on crash — dangerous in production.

**Why separate test database?**
Integration tests truncate tables to guarantee clean state.
Running against the main DB would destroy real data. `db-test` on port 5436 is fully isolated.

**Why both dbt tests AND Great Expectations?**
dbt tests check structural integrity after transformation.
GE checks business rules — and crucially, checks raw data *before* dbt runs,
catching source problems before they corrupt the pipeline.

**Why Docker for everything?**
Local Python is 3.14 which dbt and GE don't yet support.
Docker pins the environment to Python 3.11, making the project
reproducible on any machine with just Docker Desktop installed.

**Why staging → marts layering?**
Staging models are 1:1 with raw tables. If a raw table changes,
only one staging model needs updating. Mart models consume only
from staging — never from raw — keeping transformations clean and debuggable.

---

## Project Structure

```
dataflow-etl/
├── extract/
│   ├── __init__.py
│   └── generator.py          # Faker data generation
├── load/
│   ├── __init__.py
│   └── postgres_loader.py    # UPSERT into PostgreSQL
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_orders.sql
│   │   │   └── schema.yml
│   │   └── marts/
│   │       ├── mart_customer_orders.sql
│   │       ├── mart_daily_revenue.sql
│   │       └── schema.yml
│   └── tests/
│       └── assert_revenue_positive.sql
├── great_expectations/
│   └── ge_validate.py        # GE checkpoints
├── tests/
│   ├── conftest.py
│   ├── unit/
│   │   ├── test_generator.py
│   │   └── test_loader.py
│   └── integration/
│       └── test_full_pipeline.py
├── docker/
│   ├── Dockerfile
│   └── init.sql
├── docs/
│   └── ARCHITECTURE.md       # this file
├── .github/workflows/
│   └── ci.yml
├── docker-compose.yml
├── pipeline.py
├── requirements.txt
├── pyproject.toml
└── README.md
```


---

## Author

**afzal7277** —
[GitHub](https://github.com/afzal7277) ·
[LinkedIn](https://linkedin.com/in/zafarafzal) ·
[Portfolio](https://afzal7277.github.io)
