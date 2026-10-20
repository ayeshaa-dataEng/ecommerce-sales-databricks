# Databricks notebook source


# COMMAND ----------

import logging
import os
import sys

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "ecommerce")
dbutils.widgets.text("volume", "raw_files")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
VOLUME = dbutils.widgets.get("volume").strip()

# The notebooks live in <repo>/notebooks; the library lives in <repo>/src.
REPO_ROOT = os.path.dirname(os.getcwd())
SRC_DIR = os.path.join(REPO_ROOT, "src")
REPO_DATA_DIR = os.path.join(REPO_ROOT, "data")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

SOURCE_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
SOURCE_FILES = {
    "customers": "Customer.xlsx",
    "products": "Products.csv",
    "orders": "Orders.json",
}


def table(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{name}"


TABLES = {
    "raw_customers": table("raw_customers"),
    "raw_products": table("raw_products"),
    "raw_orders": table("raw_orders"),
    "enriched_customers": table("enriched_customers"),
    "enriched_products": table("enriched_products"),
    "enriched_orders": table("enriched_orders"),
    "rejected_orders": table("rejected_orders"),
    "profit_aggregate": table("profit_aggregate"),
}


def write_table(df, full_name: str, comment: str = None) -> None:
    """Full refresh write to a managed Delta table (idempotent: safe to re-run)."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_name)
    )
    if comment:
        spark.sql(f"COMMENT ON TABLE {full_name} IS '{comment}'")
    print(f"wrote {full_name}: {spark.table(full_name).count():,} rows")


logging.basicConfig(level=logging.INFO)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
print(f"catalog={CATALOG} schema={SCHEMA} source_dir={SOURCE_DIR} repo={REPO_ROOT}")
