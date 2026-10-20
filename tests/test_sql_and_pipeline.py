"""Tests for the SQL reports, the file readers and an end-to-end run on the real data."""
import os

import pandas as pd
import pytest
from pyspark.sql import functions as F

from conftest import ON_DATABRICKS
from ecommerce import sql_queries as q
from ecommerce import transformations as tr

AGG_COLUMNS = "order_year int, category string, sub_category string, customer_id string, " \
              "customer_name string, total_profit double, order_count long"


@pytest.fixture
def agg_view(spark):
    rows = [
        (2016, "Furniture", "Chairs", "C1", "Alice", 15.67, 1),
        (2016, "Technology", "Phones", "C1", "Alice", -20.0, 1),
        (2016, "Technology", "Phones", "C2", "Bob", 4.33, 1),
        (2017, "Furniture", "Chairs", "C1", "Alice", 7.0, 1),
        (2017, "Technology", "Phones", "C2", "Bob", 100.0, 1),
    ]
    spark.createDataFrame(rows, AGG_COLUMNS).createOrReplaceTempView("test_profit_agg")
    return "test_profit_agg"


def as_tuples(df):
    return [tuple(r) for r in df.collect()]


def test_sql_profit_by_year(spark, agg_view):
    assert as_tuples(q.run_query(spark, q.PROFIT_BY_YEAR, agg_view)) == [(2016, 0.0), (2017, 107.0)]


def test_sql_profit_by_year_and_category(spark, agg_view):
    assert as_tuples(q.run_query(spark, q.PROFIT_BY_YEAR_AND_CATEGORY, agg_view)) == [
        (2016, "Furniture", 15.67),
        (2016, "Technology", -15.67),
        (2017, "Furniture", 7.0),
        (2017, "Technology", 100.0),
    ]


def test_sql_profit_by_customer_sorted_desc(spark, agg_view):
    assert as_tuples(q.run_query(spark, q.PROFIT_BY_CUSTOMER, agg_view)) == [
        ("C2", "Bob", 104.33),
        ("C1", "Alice", 2.67),
    ]


def test_sql_profit_by_customer_and_year(spark, agg_view):
    assert as_tuples(q.run_query(spark, q.PROFIT_BY_CUSTOMER_AND_YEAR, agg_view)) == [
        ("C1", "Alice", 2016, -4.33),
        ("C1", "Alice", 2017, 7.0),
        ("C2", "Bob", 2016, 4.33),
        ("C2", "Bob", 2017, 100.0),
    ]


def test_all_reports_reconcile_to_same_total(spark, agg_view):
    totals = {
        name: q.run_query(spark, sql, agg_view).agg(F.round(F.sum("total_profit"), 2)).first()[0]
        for name, sql in q.ALL_QUERIES.items()
    }
    assert len(set(totals.values())) == 1, totals


# --------------------------------------------------------------------------- readers

def test_read_raw_customers_from_excel_keeps_text_and_nulls(spark, tmp_path):
    path = str(tmp_path / "customers.xlsx")
    pd.DataFrame(
        {"Customer ID": ["C1", "C2"], "Customer Name": ["Ann", None],
         "Country": ["United States", "United States"], "Postal Code": ["02134", "10001"]}
    ).to_excel(path, index=False)

    df = tr.read_raw_customers(spark, path)
    rows = {r.customer_id: r for r in df.collect()}
    assert {"customer_id", "customer_name", "postal_code", "_source_file", "_ingested_at"} <= set(df.columns)
    assert rows["C1"].postal_code == "02134"  # leading zero preserved
    assert rows["C2"].customer_name is None


def test_readers_fail_fast_on_missing_file(spark, tmp_path):
    with pytest.raises(FileNotFoundError):
        tr.read_raw_orders(spark, str(tmp_path / "missing.json"))


# Spark on serverless cannot read the driver's local /tmp, so file-based Spark readers
# are exercised locally/CI; on Databricks they are covered by the pipeline notebooks.
local_only = pytest.mark.skipif(ON_DATABRICKS, reason="reads local files with Spark")


@local_only
def test_read_raw_products_handles_quoted_commas_and_quotes(spark, tmp_path):
    path = tmp_path / "products.csv"
    path.write_text(
        "Product ID,Category,Sub-Category,Product Name,State,Price per product\n"
        'P1,Office Supplies,Paper,"Message Book, 11"" X 5 1/4""",New York,5.5\n'
    )
    row = tr.read_raw_products(spark, str(path)).first()
    assert row.product_name == 'Message Book, 11" X 5 1/4"'
    assert row.sub_category == "Paper" and row.price_per_product == "5.5"


@local_only
def test_read_raw_orders_multiline_json(spark, tmp_path):
    path = tmp_path / "orders.json"
    path.write_text(
        '[\n {"Row ID": 1, "Order ID": "A", "Order Date": "21/8/2016", "Ship Date": "25/8/2016",'
        ' "Ship Mode": "Same Day", "Customer ID": "C1", "Product ID": "P1",'
        ' "Quantity": 2, "Price": 10.5, "Discount": 0, "Profit": 1.234}\n]'
    )
    row = tr.read_raw_orders(spark, str(path)).first()
    assert (row.row_id, row.order_id, row.quantity, row.profit) == (1, "A", 2, 1.234)


# --------------------------------------------------------------------------- end to end

@local_only
def test_end_to_end_on_source_files(spark, data_dir):
    if not os.path.exists(os.path.join(data_dir, "Orders.json")):
        pytest.skip("source files not present")

    raw_c = tr.read_raw_customers(spark, os.path.join(data_dir, "Customer.xlsx"))
    raw_p = tr.read_raw_products(spark, os.path.join(data_dir, "Products.csv"))
    raw_o = tr.read_raw_orders(spark, os.path.join(data_dir, "Orders.json"))

    customers = tr.build_enriched_customers(raw_c)
    products = tr.build_enriched_products(raw_p)
    orders, rejected = tr.clean_orders(raw_o)
    enriched = tr.build_enriched_orders(orders, customers, products)
    agg = tr.build_profit_aggregate(enriched)

    assert raw_o.count() == 9994 and rejected.count() == 0
    assert enriched.count() == raw_o.count()  # no rows lost or duplicated by joins
    assert products.count() == products.select("product_id").distinct().count()
    assert enriched.filter(F.col("order_year").isNull()).count() == 0

    raw_total = raw_o.agg(F.sum("profit")).first()[0]
    enriched_total = enriched.agg(F.sum("profit")).first()[0]
    agg_total = agg.agg(F.sum("total_profit")).first()[0]
    assert enriched_total == pytest.approx(raw_total, abs=0.01 * 9994)
    assert agg_total == pytest.approx(enriched_total, abs=0.01)
