import os
import pytest
import psycopg2
import psycopg2.extras
from unittest.mock import MagicMock


# ── Test DB connection string ────────────────────────────────────────────────

TEST_DB = "postgresql://zafff:dataflow123@localhost:5436/dataflow_test_db"


# ── Sample data fixtures ─────────────────────────────────────────────────────

@pytest.fixture
def sample_customers():
    return [
        {
            "customer_id": 1,
            "name":        "zafff",
            "email":       "zafff@test.com",
            "country":     "India",
            "signup_date": "2024-01-15T10:30:00",
        },
        {
            "customer_id": 2,
            "name":        "John Doe",
            "email":       "john@test.com",
            "country":     "USA",
            "signup_date": "2024-03-20T08:00:00",
        },
    ]


@pytest.fixture
def sample_orders():
    return [
        {
            "customer_id":   1,
            "customer_name": "zafff",
            "product":       "Laptop",
            "quantity":      1,
            "unit_price":    999.99,
            "total_amount":  999.99,
            "status":        "shipped",
            "order_date":    "2024-06-01T12:00:00",
            "country":       "India",
        },
        {
            "customer_id":   2,
            "customer_name": "John Doe",
            "product":       "Mouse",
            "quantity":      2,
            "unit_price":    29.99,
            "total_amount":  59.98,
            "status":        "delivered",
            "order_date":    "2024-06-02T14:00:00",
            "country":       "USA",
        },
    ]


# ── Mock DB fixture (unit tests) ─────────────────────────────────────────────

@pytest.fixture
def mock_db(mocker):
    """Patches psycopg2.connect — no real DB needed."""
    mock_cur  = MagicMock()
    mock_conn = MagicMock()

    mock_conn.__enter__ = lambda s: mock_conn
    mock_conn.__exit__  = MagicMock(return_value=False)
    mock_conn.cursor.return_value.__enter__ = lambda s: mock_cur
    mock_conn.cursor.return_value.__exit__  = MagicMock(return_value=False)

    mocker.patch("psycopg2.connect", return_value=mock_conn)
    mocker.patch("psycopg2.extras.execute_batch")

    return mock_cur


# ── Real DB fixtures (integration tests) ────────────────────────────────────

@pytest.fixture(scope="session")
def db_conn():
    """Real connection to test DB. Requires docker compose up db-test."""
    conn = psycopg2.connect(TEST_DB)
    yield conn
    conn.close()


@pytest.fixture(autouse=False)
def clean_tables(db_conn):
    """Wipes tables before each integration test — fresh start every time."""
    with db_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE raw_layer.orders RESTART IDENTITY CASCADE")
        cur.execute("TRUNCATE TABLE raw_layer.customers RESTART IDENTITY CASCADE")
    db_conn.commit()
    yield
    # clean up after test too
    with db_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE raw_layer.orders RESTART IDENTITY CASCADE")
        cur.execute("TRUNCATE TABLE raw_layer.customers RESTART IDENTITY CASCADE")
    db_conn.commit()