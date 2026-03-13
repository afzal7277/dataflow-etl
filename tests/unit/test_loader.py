import pytest
from unittest.mock import patch, MagicMock
import psycopg2.extras
from load.postgres_loader import load_customers, load_orders


def test_load_customers_returns_count(mock_db, sample_customers):
    result = load_customers(sample_customers)
    assert result == 2


def test_load_orders_returns_count(mock_db, sample_orders):
    result = load_orders(sample_orders)
    assert result == 2


def test_load_customers_calls_execute_batch(mock_db, sample_customers, mocker):
    mock_batch = mocker.patch("psycopg2.extras.execute_batch")
    load_customers(sample_customers)
    assert mock_batch.called


def test_load_orders_calls_execute_batch(mock_db, sample_orders, mocker):
    mock_batch = mocker.patch("psycopg2.extras.execute_batch")
    load_orders(sample_orders)
    assert mock_batch.called


def test_load_customers_empty_list(mock_db):
    result = load_customers([])
    assert result == 0


def test_load_orders_empty_list(mock_db):
    result = load_orders([])
    assert result == 0