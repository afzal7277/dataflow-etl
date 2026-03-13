import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import great_expectations as ge
from great_expectations.core import ExpectationSuite, ExpectationConfiguration

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_engine():
    """Create SQLAlchemy engine from env vars."""
    user     = os.getenv("POSTGRES_USER", "zafff")
    password = os.getenv("POSTGRES_PASSWORD", "dataflow123")
    host     = os.getenv("POSTGRES_HOST", "localhost")
    port     = os.getenv("POSTGRES_PORT", "5432")
    db       = os.getenv("POSTGRES_DB", "dataflow_db")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")


def build_raw_orders_suite() -> ExpectationSuite:
    """Expectations for raw_layer.orders — runs after load."""
    suite = ExpectationSuite(expectation_suite_name="raw_orders_suite")

    # Table level
    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_table_row_count_to_be_between",
        kwargs={"min_value": 1, "max_value": 10_000_000}
    ))
    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_table_columns_to_match_set",
        kwargs={"column_set": [
            "order_id", "customer_id", "customer_name",
            "product", "quantity", "unit_price",
            "total_amount", "status", "order_date", "country"
        ], "exact_match": False}
    ))

    # Column level
    for col in ["order_id", "customer_id", "product", "status", "order_date"]:
        suite.add_expectation(ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": col}
        ))

    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_unique",
        kwargs={"column": "order_id"}
    ))

    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "status",
            "value_set": ["pending", "shipped", "delivered", "cancelled"]
        }
    ))

    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "total_amount", "min_value": 0}
    ))

    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "quantity", "min_value": 1}
    ))

    return suite


def build_mart_revenue_suite() -> ExpectationSuite:
    """Expectations for staging.mart_daily_revenue — runs after dbt."""
    suite = ExpectationSuite(expectation_suite_name="mart_revenue_suite")

    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_table_row_count_to_be_between",
        kwargs={"min_value": 1, "max_value": 10_000_000}
    ))

    for col in ["order_date", "total_revenue", "total_orders"]:
        suite.add_expectation(ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": col}
        ))

    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "total_revenue", "min_value": 0}
    ))

    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "total_orders", "min_value": 1}
    ))

    suite.add_expectation(ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "country",
            "value_set": [
                "India", "Usa", "Uk", "Germany",
                "Australia", "Canada", "Singapore"
            ]
        }
    ))

    return suite


def run_validation(suite: ExpectationSuite, df: pd.DataFrame) -> bool:
    """Run a GE suite against a dataframe. Returns True if all pass."""
    ge_df    = ge.from_pandas(df)
    results  = ge_df.validate(expectation_suite=suite, result_format="SUMMARY")

    suite_name = suite.expectation_suite_name
    total      = results["statistics"]["evaluated_expectations"]
    passed     = results["statistics"]["successful_expectations"]
    failed     = results["statistics"]["unsuccessful_expectations"]

    logger.info(f"--- {suite_name} ---")
    logger.info(f"Total: {total} | Passed: {passed} | Failed: {failed}")

    if not results["success"]:
        logger.error(f"VALIDATION FAILED for {suite_name}")
        for r in results["results"]:
            if not r["success"]:
                logger.error(f"  FAIL: {r['expectation_config']['expectation_type']} "
                             f"on column '{r['expectation_config']['kwargs'].get('column', 'table')}'")
        return False

    logger.info(f"VALIDATION PASSED for {suite_name}")
    return True


def main():
    engine = get_engine()

    # ── Checkpoint 1: raw orders ─────────────────────────────────────────
    logger.info("Running checkpoint 1: raw_layer.orders")
    df_raw = pd.read_sql("SELECT * FROM raw_layer.orders", engine)
    suite1  = build_raw_orders_suite()
    result1 = run_validation(suite1, df_raw)

    # ── Checkpoint 2: mart daily revenue ────────────────────────────────
    logger.info("Running checkpoint 2: staging.mart_daily_revenue")
    df_mart = pd.read_sql("SELECT * FROM staging.mart_daily_revenue", engine)
    suite2  = build_mart_revenue_suite()
    result2 = run_validation(suite2, df_mart)

    # ── Final result ─────────────────────────────────────────────────────
    if result1 and result2:
        logger.info("ALL VALIDATIONS PASSED")
    else:
        logger.error("SOME VALIDATIONS FAILED — check logs above")
        raise SystemExit(1)


if __name__ == "__main__":
    main()