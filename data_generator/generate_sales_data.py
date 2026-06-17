"""
Generates synthetic sales CSV files and uploads them to MinIO.
Intentionally injects ~15% dirty rows to exercise the Airflow cleaning step.

Usage:
    MINIO_ENDPOINT=http://localhost:9000 python generate_sales_data.py
    python generate_sales_data.py --rows 500
"""
import argparse
from typing import List, Dict
import os
import random
import uuid
from datetime import date, timedelta

import boto3
from botocore.client import Config
from faker import Faker

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "sales-data")

PRODUCTS = {
    "Electronics": [
        ("SKU-E001", "Laptop Pro 15"),
        ("SKU-E002", "Wireless Mouse"),
        ("SKU-E003", "Mechanical Keyboard"),
        ("SKU-E004", "27-inch Monitor"),
        ("SKU-E005", "USB-C Hub"),
    ],
    "Clothing": [
        ("SKU-C001", "Running Jacket"),
        ("SKU-C002", "Office Trousers"),
        ("SKU-C003", "Casual T-Shirt"),
    ],
    "Books": [
        ("SKU-B001", "Data Engineering Handbook"),
        ("SKU-B002", "Python for Data Science"),
        ("SKU-B003", "Cloud Architecture Patterns"),
    ],
    "Home & Garden": [
        ("SKU-H001", "Standing Desk"),
        ("SKU-H002", "Ergonomic Chair"),
        ("SKU-H003", "Desk Lamp"),
    ],
    "Sports": [
        ("SKU-S001", "Yoga Mat"),
        ("SKU-S002", "Resistance Bands Set"),
        ("SKU-S003", "Water Bottle 1L"),
    ],
}

REGIONS = ["North", "South", "East", "West", "Central"]
CATEGORIES = list(PRODUCTS.keys())
UNIT_PRICE_RANGE = {
    "Electronics": (29.99, 1999.99),
    "Clothing": (9.99, 149.99),
    "Books": (12.99, 59.99),
    "Home & Garden": (29.99, 799.99),
    "Sports": (4.99, 99.99),
}

CSV_HEADER = (
    "sale_id,order_date,customer_id,product_sku,product_name,"
    "category,quantity,unit_price,region,sales_rep\n"
)


def random_date(days_back: int = 365) -> str:
    delta = random.randint(0, days_back)
    return (date.today() - timedelta(days=delta)).isoformat()


def make_dirty(row: dict, faker: Faker) -> dict:
    """Randomly corrupt a row to test the cleaning pipeline."""
    choice = random.random()
    if choice < 0.04:
        row["quantity"] = None
    elif choice < 0.07:
        row["quantity"] = random.choice([-5, 0])
    elif choice < 0.10:
        row["unit_price"] = None
    elif choice < 0.12:
        row["order_date"] = "not-a-date"
    elif choice < 0.15:
        row["product_name"] = None
    elif choice < 0.17:
        row["region"] = f"  {row['region'].lower()}  "
    return row


def generate_row(faker: Faker, dirty: bool = False) -> dict:
    category = random.choice(CATEGORIES)
    sku, name = random.choice(PRODUCTS[category])
    low, high = UNIT_PRICE_RANGE[category]
    row = {
        "sale_id": str(uuid.uuid4()),
        "order_date": random_date(),
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "product_sku": sku,
        "product_name": name,
        "category": category,
        "quantity": random.randint(1, 50),
        "unit_price": round(random.uniform(low, high), 2),
        "region": random.choice(REGIONS),
        "sales_rep": faker.name(),
    }
    if dirty:
        row = make_dirty(row, faker)
    return row


# rows_to_csv: converts list of row dicts to CSV string


def rows_to_csv(rows: List[Dict]) -> str:
    lines = [CSV_HEADER]
    for r in rows:
        lines.append(
            f"{r['sale_id']},{r['order_date']},{r['customer_id']},"
            f"{r['product_sku']},{r['product_name'] or ''},"
            f"{r['category']},{r['quantity'] if r['quantity'] is not None else ''},"
            f"{r['unit_price'] if r['unit_price'] is not None else ''},"
            f"{r['region']},{r['sales_rep']}\n"
        )
    return "".join(lines)


def upload_to_minio(csv_content: str, key: str) -> None:
    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    client.put_object(
        Bucket=MINIO_BUCKET,
        Key=key,
        Body=csv_content.encode("utf-8"),
        ContentType="text/csv",
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=200)
    args = parser.parse_args()

    faker = Faker()
    rows = [
        generate_row(faker, dirty=(random.random() < 0.15))
        for _ in range(args.rows)
    ]
    csv_content = rows_to_csv(rows)

    run_id = str(uuid.uuid4())[:8]
    key = f"raw/sales_{date.today().isoformat()}_{run_id}.csv"
    upload_to_minio(csv_content, key)
    print(f"Uploaded {args.rows} rows → s3://{MINIO_BUCKET}/{key}")


if __name__ == "__main__":
    main()
