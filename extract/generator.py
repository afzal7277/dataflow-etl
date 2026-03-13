import random
import logging
from faker import Faker
from datetime import datetime, timedelta
from typing import List, Dict, Any

fake = Faker()
logger = logging.getLogger(__name__)

PRODUCTS = [
    ("Laptop",      999.99),
    ("Mouse",        29.99),
    ("Keyboard",     79.99),
    ("Monitor",     399.99),
    ("Headphones",  149.99),
    ("Webcam",       89.99),
    ("USB Hub",      39.99),
    ("SSD Drive",   119.99),
]

STATUSES = ["pending", "shipped", "delivered", "cancelled"]

COUNTRIES = ["India", "USA", "UK", "Germany", "Australia", "Canada", "Singapore"]


def generate_customers(n: int = 50) -> List[Dict[str, Any]]:
    """Generate n fake customer records."""
    customers = []
    for i in range(1, n + 1):
        customers.append({
            "customer_id": i,
            "name":        fake.name(),
            "email":       fake.email(),
            "country":     random.choice(COUNTRIES),
            "signup_date": fake.date_time_between(
                start_date="-2y", end_date="now"
            ).isoformat(),
        })
    logger.info(f"Generated {len(customers)} customers")
    return customers


def generate_orders(n: int = 200, num_customers: int = 50) -> List[Dict[str, Any]]:
    """Generate n fake order records."""
    orders = []
    for _ in range(n):
        product, unit_price = random.choice(PRODUCTS)
        quantity    = random.randint(1, 5)
        total       = round(unit_price * quantity, 2)
        order_date  = fake.date_time_between(
            start_date="-1y", end_date="now"
        )
        orders.append({
            "customer_id":  random.randint(1, num_customers),
            "customer_name": fake.name(),
            "product":      product,
            "quantity":     quantity,
            "unit_price":   unit_price,
            "total_amount": total,
            "status":       random.choice(STATUSES),
            "order_date":   order_date.isoformat(),
            "country":      random.choice(COUNTRIES),
        })
    logger.info(f"Generated {len(orders)} orders")
    return orders


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    customers = generate_customers(50)
    orders    = generate_orders(200)
    print(f"Sample customer: {customers[0]}")
    print(f"Sample order:    {orders[0]}")