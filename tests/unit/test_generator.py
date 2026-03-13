import pytest
from extract.generator import generate_customers, generate_orders

VALID_STATUSES  = {"pending", "shipped", "delivered", "cancelled"}
VALID_COUNTRIES = {"India", "USA", "UK", "Germany", "Australia", "Canada", "Singapore"}


# ── Customer tests ──────────────────────────────────────────────────────────

def test_generate_customers_count():
    result = generate_customers(10)
    assert len(result) == 10


def test_generate_customers_default_count():
    result = generate_customers()
    assert len(result) == 50


def test_customer_has_required_fields():
    result = generate_customers(1)
    record = result[0]
    assert "customer_id" in record
    assert "name"        in record
    assert "email"       in record
    assert "country"     in record
    assert "signup_date" in record


def test_customer_ids_are_unique():
    result = generate_customers(20)
    ids = [r["customer_id"] for r in result]
    assert len(ids) == len(set(ids))


def test_customer_country_is_valid():
    result = generate_customers(20)
    for r in result:
        assert r["country"] in VALID_COUNTRIES


def test_customer_email_contains_at():
    result = generate_customers(10)
    for r in result:
        assert "@" in r["email"]


# ── Order tests ─────────────────────────────────────────────────────────────

def test_generate_orders_count():
    result = generate_orders(15)
    assert len(result) == 15


def test_generate_orders_default_count():
    result = generate_orders()
    assert len(result) == 200


def test_order_has_required_fields():
    result = generate_orders(1)
    record = result[0]
    for field in ["customer_id", "product", "quantity",
                  "unit_price", "total_amount", "status",
                  "order_date", "country"]:
        assert field in record


def test_order_total_equals_quantity_times_price():
    result = generate_orders(50)
    for r in result:
        expected = round(r["unit_price"] * r["quantity"], 2)
        assert r["total_amount"] == expected


def test_order_status_is_valid():
    result = generate_orders(50)
    for r in result:
        assert r["status"] in VALID_STATUSES


def test_order_quantity_is_positive():
    result = generate_orders(50)
    for r in result:
        assert r["quantity"] >= 1


def test_order_unit_price_is_positive():
    result = generate_orders(50)
    for r in result:
        assert r["unit_price"] > 0


def test_order_country_is_valid():
    result = generate_orders(50)
    for r in result:
        assert r["country"] in VALID_COUNTRIES