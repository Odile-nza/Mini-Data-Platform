"""
ETL pipeline: MinIO (raw CSV) → clean/transform → PostgreSQL.

Flow:
  list_new_files → process_and_load → archive_processed_files
"""
import io
import logging
import os
from datetime import timedelta
from typing import List, Optional

import pendulum

import boto3
import pandas as pd
import psycopg2
import psycopg2.extras
from botocore.client import Config

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator

log = logging.getLogger(__name__)

# ----- connection helpers -------------------------------------------------- #

def _s3():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _pg():
    return psycopg2.connect(
        host=os.environ["PLATFORM_DB_HOST"],
        database=os.environ["PLATFORM_DB_NAME"],
        user=os.environ["PLATFORM_DB_USER"],
        password=os.environ["PLATFORM_DB_PASSWORD"],
    )


# ----- task callables ------------------------------------------------------- #

def list_new_files(**context) -> bool:
    """Return True (continue pipeline) when there are files to process."""
    bucket = os.environ["MINIO_BUCKET"]
    response = _s3().list_objects_v2(Bucket=bucket, Prefix="raw/")
    files = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".csv")
    ]
    log.info("Found %d CSV files in raw/: %s", len(files), files)
    context["ti"].xcom_push(key="file_list", value=files)
    return bool(files)


def _clean_dataframe(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    required = ["sale_id", "order_date", "customer_id",
                "product_sku", "quantity", "unit_price"]
    df.dropna(subset=required, inplace=True)

    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df.dropna(subset=["order_date"], inplace=True)
    df["order_date"] = df["order_date"].dt.date

    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df.dropna(subset=["quantity", "unit_price"], inplace=True)

    df = df[(df["quantity"] > 0) & (df["unit_price"] >= 0)].copy()

    df["product_name"] = df["product_name"].fillna("Unknown Product").str.strip()
    df["region"] = df["region"].fillna("Unknown").str.strip().str.title()
    df["sales_rep"] = df["sales_rep"].fillna("").str.strip()
    df["sale_id"] = df["sale_id"].astype(str).str.strip()
    df["customer_id"] = df["customer_id"].astype(str).str.strip()
    df["product_sku"] = df["product_sku"].astype(str).str.strip()
    df["category"] = df["category"].fillna("Other").str.strip()
    df["source_file"] = source_file

    return df


UPSERT_SQL = """
    INSERT INTO public.sales
        (sale_id, order_date, customer_id, product_sku, product_name,
         category, quantity, unit_price, region, sales_rep, source_file)
    VALUES %s
    ON CONFLICT (sale_id) DO UPDATE SET
        quantity    = EXCLUDED.quantity,
        unit_price  = EXCLUDED.unit_price,
        region      = EXCLUDED.region,
        sales_rep   = EXCLUDED.sales_rep,
        source_file = EXCLUDED.source_file,
        updated_at  = NOW()
"""


def process_and_load(**context) -> None:
    files: List[str] = context["ti"].xcom_pull(key="file_list",
                                                task_ids="list_new_files")
    s3 = _s3()
    bucket = os.environ["MINIO_BUCKET"]
    total_loaded, total_dropped, processed = 0, 0, []

    for key in files:
        log.info("Processing %s", key)
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        df_raw = pd.read_csv(io.BytesIO(body))
        df = _clean_dataframe(df_raw.copy(), key)

        dropped = len(df_raw) - len(df)
        total_dropped += dropped
        log.info("%s: %d rows kept, %d dropped", key, len(df), dropped)

        if df.empty:
            processed.append(key)
            continue

        records = [
            (
                row.sale_id, row.order_date, row.customer_id,
                row.product_sku, row.product_name, row.category,
                int(row.quantity), float(row.unit_price),
                row.region, row.sales_rep, row.source_file,
            )
            for row in df.itertuples()
        ]

        conn = _pg()
        try:
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, UPSERT_SQL, records,
                                               page_size=500)
            conn.commit()
            total_loaded += len(records)
            processed.append(key)
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    log.info("Pipeline run: loaded=%d dropped=%d files=%d",
             total_loaded, total_dropped, len(processed))
    context["ti"].xcom_push(key="processed_files", value=processed)


def archive_processed_files(**context) -> None:
    files: Optional[List[str]] = context["ti"].xcom_pull(
        key="processed_files", task_ids="process_and_load"
    )
    if not files:
        return
    s3 = _s3()
    bucket = os.environ["MINIO_BUCKET"]
    for src in files:
        dst = src.replace("raw/", "processed/", 1)
        s3.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": src},
            Key=dst,
        )
        s3.delete_object(Bucket=bucket, Key=src)
        log.info("Archived %s → %s", src, dst)


# ----- DAG definition ------------------------------------------------------- #

default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="sales_pipeline",
    description="MinIO CSV → clean → PostgreSQL",
    default_args=default_args,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["etl", "sales"],
) as dag:

    check = ShortCircuitOperator(
        task_id="list_new_files",
        python_callable=list_new_files,
    )

    load = PythonOperator(
        task_id="process_and_load",
        python_callable=process_and_load,
    )

    archive = PythonOperator(
        task_id="archive_processed_files",
        python_callable=archive_processed_files,
    )

    check >> load >> archive
