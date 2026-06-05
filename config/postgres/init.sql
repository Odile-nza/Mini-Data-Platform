-- Runs once on fresh volume as postgres superuser.
-- Creates all application users and databases.

CREATE USER airflow WITH PASSWORD 'airflow';
CREATE USER platform_user WITH PASSWORD 'platform_pass';
CREATE USER metabase_user WITH PASSWORD 'metabase_pass';

CREATE DATABASE airflow OWNER airflow;
CREATE DATABASE platform OWNER platform_user;
CREATE DATABASE metabase_db OWNER metabase_user;

\connect platform

CREATE TABLE IF NOT EXISTS public.sales (
    id           SERIAL PRIMARY KEY,
    sale_id      VARCHAR(36)    NOT NULL UNIQUE,
    order_date   DATE           NOT NULL,
    customer_id  VARCHAR(20)    NOT NULL,
    product_sku  VARCHAR(50)    NOT NULL,
    product_name VARCHAR(200)   NOT NULL DEFAULT 'Unknown Product',
    category     VARCHAR(100)   NOT NULL,
    quantity     INTEGER        NOT NULL CHECK (quantity > 0),
    unit_price   NUMERIC(10, 2) NOT NULL CHECK (unit_price >= 0),
    total_amount NUMERIC(12, 2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    region       VARCHAR(100),
    sales_rep    VARCHAR(100),
    source_file  VARCHAR(255)   NOT NULL,
    loaded_at    TIMESTAMPTZ    DEFAULT NOW(),
    updated_at   TIMESTAMPTZ    DEFAULT NOW()
);

CREATE INDEX idx_sales_order_date  ON public.sales (order_date);
CREATE INDEX idx_sales_category    ON public.sales (category);
CREATE INDEX idx_sales_customer_id ON public.sales (customer_id);
CREATE INDEX idx_sales_region      ON public.sales (region);

GRANT ALL ON ALL TABLES    IN SCHEMA public TO platform_user;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO platform_user;
GRANT USAGE ON SCHEMA public TO platform_user;

\connect metabase_db
GRANT ALL ON SCHEMA public TO metabase_user;
