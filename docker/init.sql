-- Create schemas
CREATE SCHEMA IF NOT EXISTS raw_layer;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

-- Raw orders table
CREATE TABLE IF NOT EXISTS raw_layer.orders (
    order_id        SERIAL PRIMARY KEY,
    customer_id     INTEGER NOT NULL,
    customer_name   VARCHAR(100),
    product         VARCHAR(100),
    quantity        INTEGER,
    unit_price      NUMERIC(10, 2),
    total_amount    NUMERIC(10, 2),
    status          VARCHAR(20),
    order_date      TIMESTAMP,
    country         VARCHAR(50),
    _loaded_at      TIMESTAMP DEFAULT NOW()
);

-- Raw customers table
CREATE TABLE IF NOT EXISTS raw_layer.customers (
    customer_id     SERIAL PRIMARY KEY,
    name            VARCHAR(100),
    email           VARCHAR(150),
    country         VARCHAR(50),
    signup_date     TIMESTAMP,
    _loaded_at      TIMESTAMP DEFAULT NOW()
);