import pytest
from unittest.mock import MagicMock, patch


# ── Sample data fixtures ────────────────────────────────────────────────────

@pytest.fixture
def sample_customers():
    return [
        {
            "customer_id": 1,
            "name":        "Zafff",
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
            "customer_name": "Zafff",
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


# ── Mock DB connection fixture ──────────────────────────────────────────────

@pytest.fixture
def mock_db(mocker):
    """
    Patches psycopg2.connect so no real DB is needed in unit tests.
    Returns the mock cursor so tests can assert calls on it.
    """
    mock_cur  = MagicMock()
    mock_conn = MagicMock()

    mock_conn.__enter__ = lambda s: mock_conn
    mock_conn.__exit__  = MagicMock(return_value=False)
    mock_conn.cursor.return_value.__enter__ = lambda s: mock_cur
    mock_conn.cursor.return_value.__exit__  = MagicMock(return_value=False)

    mocker.patch("psycopg2.connect", return_value=mock_conn)
    mocker.patch("psycopg2.extras.execute_batch")

    return mock_cur