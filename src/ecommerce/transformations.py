"""PySpark transformations for the e-commerce sales pipeline.

Layers (medallion style):
    raw       -> source data as-is, columns renamed to snake_case, plus ingestion metadata
    enriched  -> cleaned / typed / de-duplicated customers, products and orders
    aggregate -> profit by year, category, sub-category and customer

Every function here is a pure DataFrame -> DataFrame transformation (readers excepted)
so it can be unit tested without touching any tables.

Spark notes:
    * Databricks serverless runs with ANSI mode on, where a bad CAST or to_date() raises
      instead of returning NULL. We use try_cast / try_to_timestamp so a single dirty
      value is rejected instead of failing the whole job.
    * The customer and product dimensions are tiny (< 2k rows), so they are broadcast
      into the order join to avoid shuffling the fact table.
"""
from __future__ import annotations

import logging
import os
import re
from typing import Iterable, List, Tuple

import pandas as pd
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

logger = logging.getLogger(__name__)

UNKNOWN = "Unknown"
DATE_FORMAT = "d/M/yyyy"  # source dates look like 21/8/2016 (day first, no zero padding)


class DataQualityError(Exception):
    """Raised when an input does not meet the minimum contract needed to process it."""


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

# Field names exactly as they appear in Orders.json. An explicit schema avoids a full
# inference pass over the file and makes type drift visible via _corrupt_record.
ORDERS_SOURCE_SCHEMA = T.StructType(
    [
        T.StructField("Row ID", T.LongType()),
        T.StructField("Order ID", T.StringType()),
        T.StructField("Order Date", T.StringType()),
        T.StructField("Ship Date", T.StringType()),
        T.StructField("Ship Mode", T.StringType()),
        T.StructField("Customer ID", T.StringType()),
        T.StructField("Product ID", T.StringType()),
        T.StructField("Quantity", T.LongType()),
        T.StructField("Price", T.DoubleType()),
        T.StructField("Discount", T.DoubleType()),
        T.StructField("Profit", T.DoubleType()),
        T.StructField("_corrupt_record", T.StringType()),
    ]
)

REQUIRED_RAW_CUSTOMER_COLUMNS = ["customer_id", "customer_name", "country"]
REQUIRED_RAW_PRODUCT_COLUMNS = ["product_id", "category", "sub_category"]
REQUIRED_RAW_ORDER_COLUMNS = [
    "row_id", "order_id", "order_date", "customer_id", "product_id", "quantity", "profit",
]


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------

def to_snake_case(name: str) -> str:
    """'Sub-Category' -> 'sub_category', 'Price per product' -> 'price_per_product'."""
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip())
    return name.strip("_").lower()


def standardize_columns(df: DataFrame) -> DataFrame:
    """Rename every column to snake_case (Delta does not allow spaces in column names)."""
    renamed = [to_snake_case(c) if not c.startswith("_") else c for c in df.columns]
    duplicates = {c for c in renamed if renamed.count(c) > 1}
    if duplicates:
        raise DataQualityError(f"Column names collide after standardisation: {sorted(duplicates)}")
    return df.toDF(*renamed)


def require_columns(df: DataFrame, required: Iterable[str], dataset: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise DataQualityError(f"{dataset}: missing required column(s) {missing}; got {df.columns}")


def trim_string_columns(df: DataFrame) -> DataFrame:
    """Trim leading/trailing whitespace on all string columns; empty strings become NULL."""
    for field in df.schema.fields:
        if isinstance(field.dataType, T.StringType):
            trimmed = F.trim(F.col(field.name))
            df = df.withColumn(field.name, F.when(trimmed == "", None).otherwise(trimmed))
    return df


def add_ingestion_metadata(df: DataFrame, source_file: Column) -> DataFrame:
    return df.withColumn("_source_file", source_file).withColumn(
        "_ingested_at", F.current_timestamp()
    )


def _parse_date(col_name: str) -> Column:
    # try_to_timestamp returns NULL instead of raising under ANSI mode.
    return F.expr(f"try_to_timestamp({col_name}, '{DATE_FORMAT}')").cast("date")


def _try_cast(col_name: str, data_type: str) -> Column:
    return F.expr(f"try_cast({col_name} AS {data_type})")


# ---------------------------------------------------------------------------
# Readers -> raw layer
# ---------------------------------------------------------------------------

def _check_exists(path: str) -> None:
    """Fail fast with a clear message for local / Volume / Workspace paths.

    Volumes (/Volumes/...) are FUSE mounted on Databricks, so os.path works for them.
    URIs such as s3://, abfss:// or dbfs: are left to Spark to resolve.
    """
    if "://" not in path and not path.startswith("dbfs:") and not os.path.exists(path):
        raise FileNotFoundError(f"Source file not found: {path}")


def read_raw_products(spark: SparkSession, path: str) -> DataFrame:
    """Products.csv: header row, quoted names containing commas and escaped quotes ("")."""
    _check_exists(path)
    df = (
        spark.read.option("header", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("multiLine", True)
        .option("mode", "PERMISSIVE")
        .option("inferSchema", False)  # raw layer keeps the source text untouched
        .csv(path)
    )
    df = standardize_columns(df)
    require_columns(df, REQUIRED_RAW_PRODUCT_COLUMNS, "products")
    return add_ingestion_metadata(df, F.col("_metadata.file_path"))


def read_raw_orders(spark: SparkSession, path: str) -> DataFrame:
    """Orders.json: a single JSON array spanning many lines."""
    _check_exists(path)
    df = (
        spark.read.schema(ORDERS_SOURCE_SCHEMA)
        .option("multiLine", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(path)
    )
    df = standardize_columns(df)
    require_columns(df, REQUIRED_RAW_ORDER_COLUMNS, "orders")
    return add_ingestion_metadata(df, F.col("_metadata.file_path"))


def read_raw_customers(spark: SparkSession, path: str, sheet_name=0) -> DataFrame:
    """Customer.xlsx: read with pandas (openpyxl), then hand to Spark.

    Spark has no built-in Excel reader, and the spark-excel Maven library cannot be
    installed on serverless compute. The file is small (~800 rows) so pandas is fine.
    Everything is read as text so values such as postal codes keep leading zeros.
    """
    _check_exists(path)
    try:
        pdf = pd.read_excel(path, sheet_name=sheet_name, dtype=str, engine="openpyxl")
    except ImportError as exc:  # pragma: no cover - environment specific
        raise ImportError("openpyxl is required to read Excel: %pip install openpyxl") from exc

    pdf.columns = [to_snake_case(str(c)) for c in pdf.columns]
    pdf = pdf.astype(object).where(pd.notna(pdf), None)  # NaN -> None so Spark sees NULL
    schema = T.StructType([T.StructField(c, T.StringType()) for c in pdf.columns])
    df = spark.createDataFrame(pdf, schema=schema)
    require_columns(df, REQUIRED_RAW_CUSTOMER_COLUMNS, "customers")
    return add_ingestion_metadata(df, F.lit(path))


# ---------------------------------------------------------------------------
# Enriched layer: customers
# ---------------------------------------------------------------------------

def clean_customer_name(col: Column) -> Column:
    """Remove the noise injected into names in the source file.

    Examples: 'Pete@#$ Takahito' -> 'Pete Takahito', 'Tam&^*ara Willing___)ham' ->
    'Tamara Willingham', "Mary O'Rourke" is kept as-is. Noise that added a space cannot
    be undone ('Kat rina Bavinger' stays as it is) - see README assumptions.

    Rules:
      1. keep only letters (any language, e.g. 'Bühler'), whitespace and apostrophes
      2. keep an apostrophe only in the O'Brien / D'Ascenzo pattern (letter ' Uppercase)
      3. collapse repeated whitespace and trim
      4. NULL / empty -> 'Unknown'
    """
    c = F.regexp_replace(col, r"[^\p{L}\s']", "")
    c = F.regexp_replace(c, r"(?<!\p{L})'|'(?!\p{Lu})", "")
    c = F.trim(F.regexp_replace(c, r"\s+", " "))
    return F.when(c.isNull() | (c == ""), F.lit(UNKNOWN)).otherwise(c)


def build_enriched_customers(raw_customers: DataFrame) -> DataFrame:
    require_columns(raw_customers, REQUIRED_RAW_CUSTOMER_COLUMNS, "customers")
    df = trim_string_columns(raw_customers.drop("_source_file", "_ingested_at"))
    df = (
        df.filter(F.col("customer_id").isNotNull())
        .withColumn("customer_id", F.upper("customer_id"))
        .withColumn("customer_name", clean_customer_name(F.col("customer_name")))
        .withColumn("country", F.coalesce(F.col("country"), F.lit(UNKNOWN)))
    )
    if "email" in df.columns:
        df = df.withColumn("email", F.lower("email"))

    # One row per customer; prefer the record that actually has a name.
    w = Window.partitionBy("customer_id").orderBy(
        F.when(F.col("customer_name") == UNKNOWN, 1).otherwise(0), F.col("customer_name")
    )
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


# ---------------------------------------------------------------------------
# Enriched layer: products
# ---------------------------------------------------------------------------

def build_enriched_products(raw_products: DataFrame) -> DataFrame:
    """Clean products and keep exactly one row per product_id.

    The source has 33 product_ids that appear twice (different name/state/price) but
    always with the same category & sub-category. Keeping duplicates would multiply
    order rows on the join, so we keep one deterministic row per id.
    """
    require_columns(raw_products, REQUIRED_RAW_PRODUCT_COLUMNS, "products")
    df = trim_string_columns(raw_products.drop("_source_file", "_ingested_at"))
    df = (
        df.filter(F.col("product_id").isNotNull())
        .withColumn("product_id", F.upper("product_id"))
        .withColumn("category", F.coalesce(F.col("category"), F.lit(UNKNOWN)))
        .withColumn("sub_category", F.coalesce(F.col("sub_category"), F.lit(UNKNOWN)))
    )
    if "price_per_product" in df.columns:
        df = df.withColumn("price_per_product", _try_cast("price_per_product", "DOUBLE"))

    order_cols = [F.col(c).asc_nulls_last() for c in ("product_name", "state") if c in df.columns]
    w = Window.partitionBy("product_id").orderBy(*(order_cols or [F.col("product_id")]))
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


# ---------------------------------------------------------------------------
# Enriched layer: orders
# ---------------------------------------------------------------------------

def clean_orders(raw_orders: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Type and validate orders.

    Returns (valid_orders, rejected_orders). Rejected rows keep their original values
    plus a `reject_reason`, so nothing is silently dropped.
    """
    require_columns(raw_orders, REQUIRED_RAW_ORDER_COLUMNS, "orders")
    df = trim_string_columns(raw_orders.drop("_source_file", "_ingested_at"))
    if "_corrupt_record" not in df.columns:
        df = df.withColumn("_corrupt_record", F.lit(None).cast("string"))

    typed = (
        df.withColumn("order_id", F.upper("order_id"))
        .withColumn("customer_id", F.upper("customer_id"))
        .withColumn("product_id", F.upper("product_id"))
        .withColumn("order_date_parsed", _parse_date("order_date"))
        .withColumn(
            "ship_date_parsed",
            _parse_date("ship_date") if "ship_date" in df.columns else F.lit(None).cast("date"),
        )
        .withColumn("quantity", _try_cast("quantity", "INT"))
        .withColumn("profit", _try_cast("profit", "DOUBLE"))
    )
    for optional in ("price", "discount"):
        if optional in typed.columns:
            typed = typed.withColumn(optional, _try_cast(optional, "DOUBLE"))

    reason = (
        F.when(F.col("_corrupt_record").isNotNull(), "corrupt record")
        .when(F.col("order_id").isNull(), "missing order_id")
        .when(F.col("customer_id").isNull(), "missing customer_id")
        .when(F.col("product_id").isNull(), "missing product_id")
        .when(F.col("order_date_parsed").isNull(), "invalid order_date")
        .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "invalid quantity")
        .when(F.col("profit").isNull(), "missing profit")
    )
    flagged = typed.withColumn("reject_reason", reason)

    rejected = flagged.filter(F.col("reject_reason").isNotNull()).drop(
        "order_date_parsed", "ship_date_parsed"
    )

    valid = (
        flagged.filter(F.col("reject_reason").isNull())
        .drop("reject_reason", "_corrupt_record", "order_date", "ship_date")
        .withColumnRenamed("order_date_parsed", "order_date")
        .withColumnRenamed("ship_date_parsed", "ship_date")
        .withColumn("order_year", F.year("order_date"))
        # row_id is the line-item key; the same row loaded twice must count once.
        .dropDuplicates(["row_id"] if "row_id" in typed.columns else None)
    )
    return valid, rejected


def build_enriched_orders(
    orders: DataFrame, customers: DataFrame, products: DataFrame
) -> DataFrame:
    """Order lines + customer name/country + product category/sub-category.

    * `orders` must be the valid output of `clean_orders`.
    * Left joins: an order is never lost because its customer/product is missing from
      the master data (204 source orders reference unknown product ids). Missing
      attributes become 'Unknown' so profit totals still reconcile.
    * Profit is rounded to 2 decimal places (HALF_UP).
    """
    require_columns(orders, ["order_id", "order_date", "customer_id", "product_id", "profit"], "orders")
    require_columns(customers, ["customer_id", "customer_name", "country"], "customers")
    require_columns(products, ["product_id", "category", "sub_category"], "products")

    if products.groupBy("product_id").count().filter("count > 1").limit(1).count() > 0:
        raise DataQualityError("products must be unique on product_id (use build_enriched_products)")
    if customers.groupBy("customer_id").count().filter("count > 1").limit(1).count() > 0:
        raise DataQualityError("customers must be unique on customer_id (use build_enriched_customers)")

    cust = customers.select("customer_id", "customer_name", "country")
    prod = products.select("product_id", "category", "sub_category")

    df = (
        orders.join(F.broadcast(cust), on="customer_id", how="left")
        .join(F.broadcast(prod), on="product_id", how="left")
        .withColumn("customer_name", F.coalesce("customer_name", F.lit(UNKNOWN)))
        .withColumn("country", F.coalesce("country", F.lit(UNKNOWN)))
        .withColumn("category", F.coalesce("category", F.lit(UNKNOWN)))
        .withColumn("sub_category", F.coalesce("sub_category", F.lit(UNKNOWN)))
        .withColumn("profit", F.round(F.col("profit"), 2))
    )
    if "order_year" not in df.columns:
        df = df.withColumn("order_year", F.year("order_date"))

    preferred = [
        "row_id", "order_id", "order_date", "order_year", "ship_date", "ship_mode",
        "customer_id", "customer_name", "country",
        "product_id", "category", "sub_category",
        "quantity", "price", "discount", "profit",
    ]
    return df.select([c for c in preferred if c in df.columns])


# ---------------------------------------------------------------------------
# Aggregate layer
# ---------------------------------------------------------------------------

AGGREGATE_KEYS = ["order_year", "category", "sub_category", "customer_id", "customer_name"]


def build_profit_aggregate(enriched_orders: DataFrame) -> DataFrame:
    """Profit by Year, Product Category, Product Sub Category and Customer.

    customer_id is part of the key because names are not unique and some are 'Unknown'.
    """
    require_columns(enriched_orders, AGGREGATE_KEYS + ["profit", "order_id"], "enriched_orders")
    return enriched_orders.groupBy(*AGGREGATE_KEYS).agg(
        F.round(F.sum("profit"), 2).alias("total_profit"),
        F.countDistinct("order_id").alias("order_count"),
    )


def data_quality_summary(rejected_orders: DataFrame) -> List[Tuple[str, int]]:
    rows = rejected_orders.groupBy("reject_reason").count().orderBy("reject_reason").collect()
    return [(r["reject_reason"], r["count"]) for r in rows]
