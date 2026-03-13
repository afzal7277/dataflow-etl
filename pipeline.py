import logging
import subprocess
import sys
from extract.generator import generate_customers, generate_orders
from load.postgres_loader import load_customers, load_orders

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)


def run_dbt():
    """Run dbt models inside the dbt container."""
    logger.info("Running dbt models...")
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", "/usr/app/dbt"],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        logger.error("dbt run failed")
        logger.error(result.stderr)
        sys.exit(1)
    logger.info("dbt run complete")


def run_dbt_tests():
    """Run dbt tests."""
    logger.info("Running dbt tests...")
    result = subprocess.run(
        ["dbt", "test", "--profiles-dir", "/usr/app/dbt"],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        logger.error("dbt tests failed")
        logger.error(result.stderr)
        sys.exit(1)
    logger.info("dbt tests complete")


def main():
    logger.info("========== DATAFLOW-ETL PIPELINE START ==========")

    # ── Step 1: Extract ──────────────────────────────────────────────────
    logger.info("STEP 1: Extracting data...")
    customers = generate_customers(50)
    orders    = generate_orders(200)
    logger.info(f"Generated {len(customers)} customers, {len(orders)} orders")

    # ── Step 2: Load ─────────────────────────────────────────────────────
    logger.info("STEP 2: Loading into PostgreSQL...")
    load_customers(customers)
    load_orders(orders)
    logger.info("Load complete")

# ── Step 3: Validate raw data ────────────────────────────────────────
    logger.info("STEP 3: Validating raw data with Great Expectations...")
    import importlib.util, os
    spec = importlib.util.spec_from_file_location(
        "ge_validate",
        os.path.join(os.path.dirname(__file__), "great_expectations", "ge_validate.py")
    )
    ge_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ge_mod)
    ge_mod.main()

    logger.info("========== PIPELINE COMPLETE ==========")


if __name__ == "__main__":
    main()