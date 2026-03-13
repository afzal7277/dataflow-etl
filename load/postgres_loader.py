import os
import logging
import psycopg2
import psycopg2.extras
from typing import List, Dict, Any
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


def get_connection():
    """Return a psycopg2 connection using env vars."""
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", 5435),
        dbname=os.getenv("POSTGRES_DB", "dataflow_db"),
        user=os.getenv("POSTGRES_USER", "zafff"),
        password=os.getenv("POSTGRES_PASSWORD", "dataflow123"),
    )


UPSERT_CUSTOMERS = """
    INSERT INTO raw_layer.customers
        (customer_id, name, email, country, signup_date)
    VALUES
        (%(customer_id)s, %(name)s, %(email)s, %(country)s, %(signup_date)s)
    ON CONFLICT (customer_id) DO UPDATE SET
        name        = EXCLUDED.name,
        email       = EXCLUDED.email,
        country     = EXCLUDED.country
"""

UPSERT_ORDERS = """
    INSERT INTO raw_layer.orders
        (customer_id, customer_name, product, quantity,
         unit_price, total_amount, status, order_date, country)
    VALUES
        (%(customer_id)s, %(customer_name)s, %(product)s, %(quantity)s,
         %(unit_price)s, %(total_amount)s, %(status)s, %(order_date)s, %(country)s)
"""


def load_customers(records: List[Dict[str, Any]]) -> int:
    """Upsert customers into raw_layer.customers."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, UPSERT_CUSTOMERS, records)
            count = cur.rowcount
    logger.info(f"Loaded {len(records)} customers")
    return len(records)


def load_orders(records: List[Dict[str, Any]]) -> int:
    """Insert orders into raw_layer.orders."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, UPSERT_ORDERS, records)
    logger.info(f"Loaded {len(records)} orders")
    return len(records)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from extract.generator import generate_customers, generate_orders
    customers = generate_customers(50)
    orders    = generate_orders(200)
    load_customers(customers)
    load_orders(orders)
    print("Load complete.")