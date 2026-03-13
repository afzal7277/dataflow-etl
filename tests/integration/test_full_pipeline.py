import pytest
import psycopg2
from load.postgres_loader import load_customers, load_orders
from extract.generator import generate_customers, generate_orders

TEST_DB = "postgresql://zafff:dataflow123@localhost:5436/dataflow_test_db"

pytestmark = pytest.mark.integration


# ── Helper ───────────────────────────────────────────────────────────────────

def query(db_conn, sql):
    """Run a query and return all rows."""
    with db_conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


# ── Customer integration tests ───────────────────────────────────────────────

def test_customers_load_into_db(db_conn, clean_tables, sample_customers, monkeypatch):
    """Customers should land in raw_layer.customers."""
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5436")
    monkeypatch.setenv("POSTGRES_DB",   "dataflow_test_db")

    load_customers(sample_customers)

    rows = query(db_conn, "SELECT COUNT(*) FROM raw_layer.customers")
    assert rows[0][0] == 2


def test_customers_data_is_correct(db_conn, clean_tables, sample_customers, monkeypatch):
    """Customer names and emails should match what was loaded."""
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5436")
    monkeypatch.setenv("POSTGRES_DB",   "dataflow_test_db")

    load_customers(sample_customers)

    rows = query(db_conn, "SELECT name, email FROM raw_layer.customers ORDER BY customer_id")
    assert rows[0] == ("zafff", "zafff@test.com")
    assert rows[1] == ("John Doe", "john@test.com")


def test_customers_load_is_idempotent(db_conn, clean_tables, sample_customers, monkeypatch):
    """Loading same customers twice should not create duplicates."""
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5436")
    monkeypatch.setenv("POSTGRES_DB",   "dataflow_test_db")

    load_customers(sample_customers)
    load_customers(sample_customers)  # second run

    rows = query(db_conn, "SELECT COUNT(*) FROM raw_layer.customers")
    assert rows[0][0] == 2  # still 2, not 4


# ── Order integration tests ───────────────────────────────────────────────────

def test_orders_load_into_db(db_conn, clean_tables, sample_orders, monkeypatch):
    """Orders should land in raw_layer.orders."""
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5436")
    monkeypatch.setenv("POSTGRES_DB",   "dataflow_test_db")

    load_orders(sample_orders)

    rows = query(db_conn, "SELECT COUNT(*) FROM raw_layer.orders")
    assert rows[0][0] == 2


def test_orders_data_is_correct(db_conn, clean_tables, sample_orders, monkeypatch):
    """Order product and amount should match what was loaded."""
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5436")
    monkeypatch.setenv("POSTGRES_DB",   "dataflow_test_db")

    load_orders(sample_orders)

    rows = query(db_conn, "SELECT product, total_amount FROM raw_layer.orders ORDER BY total_amount")
    assert rows[0][0] == "Mouse"
    assert float(rows[0][1]) == 59.98
    assert rows[1][0] == "Laptop"
    assert float(rows[1][1]) == 999.99


def test_orders_status_values_are_valid(db_conn, clean_tables, monkeypatch):
    """All generated orders should have valid status values."""
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5436")
    monkeypatch.setenv("POSTGRES_DB",   "dataflow_test_db")

    orders = generate_orders(50)
    load_orders(orders)

    rows = query(db_conn, """
        SELECT COUNT(*) FROM raw_layer.orders
        WHERE status NOT IN ('pending','shipped','delivered','cancelled')
    """)
    assert rows[0][0] == 0  # no invalid statuses


def test_orders_total_amount_never_negative(db_conn, clean_tables, monkeypatch):
    """No order should have a negative total amount."""
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5436")
    monkeypatch.setenv("POSTGRES_DB",   "dataflow_test_db")

    orders = generate_orders(100)
    load_orders(orders)

    rows = query(db_conn, "SELECT COUNT(*) FROM raw_layer.orders WHERE total_amount < 0")
    assert rows[0][0] == 0


def test_full_pipeline_row_counts(db_conn, clean_tables, monkeypatch):
    """Full extract + load should produce correct row counts."""
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5436")
    monkeypatch.setenv("POSTGRES_DB",   "dataflow_test_db")

    customers = generate_customers(30)
    orders    = generate_orders(100)

    load_customers(customers)
    load_orders(orders)

    c_rows = query(db_conn, "SELECT COUNT(*) FROM raw_layer.customers")
    o_rows = query(db_conn, "SELECT COUNT(*) FROM raw_layer.orders")

    assert c_rows[0][0] == 30
    assert o_rows[0][0] == 100