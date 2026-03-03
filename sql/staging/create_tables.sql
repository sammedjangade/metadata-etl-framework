CREATE SCHEMA staging;
CREATE SCHEMA dwh;
CREATE SCHEMA datamart;

CREATE TABLE staging.customers (
    customer_id   INT,
    name          VARCHAR(100),
    email         VARCHAR(100),
    city          VARCHAR(100),
    created_date  DATE
);

CREATE TABLE staging.orders (
    order_id      INT,
    customer_id   INT,
    product       VARCHAR(100),
    amount        DECIMAL(10,2),
    order_date    DATE
);