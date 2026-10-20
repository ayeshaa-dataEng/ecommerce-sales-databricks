"""Unit tests for the e-commerce transformations (PySpark only).

Each test builds a tiny in-memory DataFrame that reproduces one scenario or data
quality issue seen in the real source files, so failures point at a single rule.
"""
import datetime as dt

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ecommerce import transformations as tr
from ecommerce.transformations import DataQualityError, UNKNOWN

# --------------------------------------------------------------------------- fixtures

CUSTOMER_SCHEMA = T.StructType(
    [T.StructField(c, T.StringType()) for c in ("customer_id", "customer_name", "country", "email")]
)
PRODUCT_SCHEMA = T.StructType(
    [
        T.StructField(c, T.StringType())
        for c in ("product_id", "category", "sub_category", "product_name", "state", "price_per_product")
    ]
)
RAW_ORDER_SCHEMA = T.StructType(
    [
        T.StructField("row_id", T.LongType()),
        T.StructField("order_id", T.StringType()),
        T.StructField("order_date", T.StringType()),
        T.StructField("ship_date", T.StringType()),
        T.StructField("ship_mode", T.StringType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("product_id", T.StringType()),
        T.StructField("quantity", T.LongType()),
        T.StructField("price", T.DoubleType()),
        T.StructField("discount", T.DoubleType()),
        T.StructField("profit", T.DoubleType()),
    ]
)


def raw_order(row_id, order_id="CA-2016-1", order_date="21/8/2016", customer_id="C1",
              product_id="P1", quantity=1, profit=10.0, ship_date="25/8/2016"):
    return (row_id, order_id, order_date, ship_date, "Standard Class", customer_id,
            product_id, quantity, 100.0, 0.0, profit)


@pytest.fixture
def customers(spark):
    return spark.createDataFrame(
        [
            ("C1", "Alice Smith", "United States", "a@x.com"),
            ("C2", "Bob Jones", "United States", "b@x.com"),
        ],
        CUSTOMER_SCHEMA,
    )


@pytest.fixture
def products(spark):
    return spark.createDataFrame(
        [
            ("P1", "Furniture", "Chairs", "Chair", "Texas", "10.5"),
            ("P2", "Technology", "Phones", "Phone", "Ohio", "99.99"),
        ],
        PRODUCT_SCHEMA,
    )


def make_orders(spark, rows):
    valid, _ = tr.clean_orders(spark.createDataFrame(rows, RAW_ORDER_SCHEMA))
    return valid


# --------------------------------------------------------------------------- helpers

@pytest.mark.parametrize(
    "source, expected",
    [
        ("Row ID", "row_id"),
        ("Sub-Category", "sub_category"),
        ("Price per product", "price_per_product"),
        ("  Postal Code ", "postal_code"),
        ("email", "email"),
    ],
)
def test_to_snake_case(source, expected):
    assert tr.to_snake_case(source) == expected


def test_standardize_columns_renames_and_detects_collisions(spark):
    df = spark.createDataFrame([("a", "b")], ["Customer ID", "Sub-Category"])
    assert tr.standardize_columns(df).columns == ["customer_id", "sub_category"]

    clash = spark.createDataFrame([("a", "b")], ["Customer ID", "customer_id"])
    with pytest.raises(DataQualityError):
        tr.standardize_columns(clash)


def test_require_columns_raises_clear_error(spark):
    df = spark.createDataFrame([("a",)], ["x"])
    with pytest.raises(DataQualityError, match="missing required column"):
        tr.require_columns(df, ["x", "y"], "dataset")


def test_trim_string_columns_turns_blank_into_null(spark):
    df = spark.createDataFrame([("  a ", "   ", 1)], ["s1", "s2", "n"])
    row = tr.trim_string_columns(df).first()
    assert row.s1 == "a" and row.s2 is None and row.n == 1


# --------------------------------------------------------------------------- customers

@pytest.mark.parametrize(
    "dirty, expected",
    [
        ("Pierre Wener", "Pierre Wener"),
        ("Gary567 Hansen", "Gary Hansen"),
        ("Pete@#$ Takahito", "Pete Takahito"),
        ("Tam&^*ara Willing___)ham", "Tamara Willingham"),
        ("       Kristi;'[]na Nunn", "Kristina Nunn"),
        ("John Hus<>><>ton", "John Huston"),
        (" &&Tracy Blumstein", "Tracy Blumstein"),
        ("Neil Knudson%^&*(", "Neil Knudson"),
        ("[]-=;''Becky Pak", "Becky Pak"),
        ("Mary O'Rourke", "Mary O'Rourke"),
        ("Russell D'Ascenzo", "Russell D'Ascenzo"),
        ("Peter Bühler", "Peter Bühler"),
        ("Paul  4567Van Hugh", "Paul Van Hugh"),
        # junk with spaces in the middle of a word
        ("Ad.       ..am Hart", "Adam Hart"),
        ("Anth 34789ony Witt", "Anthony Witt"),
        ("Tho   12 =-.mas Boland", "Thomas Boland"),
        ("Rick Wi 4567@#$lson", "Rick Wilson"),
        ("Fra9876nk Gasti  ;.,.,neau", "Frank Gastineau"),
        ("   _Mike Vitt 12313orini", "Mike Vittorini"),
        ("B         ecky Martin", "Becky Martin"),
        # look-alike characters
        ("Bi1l Donatelli", "Bill Donatelli"),
        ("Ji11 Stevenson", "Jill Stevenson"),
        ("N0ra Paige", "Nora Paige"),
        ("C@thy Armstrong", "Cathy Armstrong"),
        ("Fred Wa55erman", "Fred Wasserman"),
        ("Karen Dan!els", "Karen Daniels"),
        ("Maribeth 5chnelling", "Maribeth Schnelling"),
        # digits that are not look-alikes are still removed
        ("Mitch Willin0009gham", "Mitch Willingham"),
        ("Brosin34a Hoffman 09", "Brosina Hoffman"),
        ("Joy Bell-", "Joy Bell"),
        ("   ", UNKNOWN),
        ("@#$%", UNKNOWN),
        (None, UNKNOWN),
    ],
)
def test_clean_customer_name(spark, dirty, expected):
    df = spark.createDataFrame([(dirty,)], "name string")
    assert df.select(tr.clean_customer_name(F.col("name")).alias("n")).first().n == expected


def test_enriched_customers_cleans_dedupes_and_drops_missing_ids(spark):
    raw = spark.createDataFrame(
        [
            (" c1 ", "Ali#ce Smith", " United States ", " A@X.COM "),
            ("C1", None, "United States", "a@x.com"),  # duplicate id, no name
            (None, "Ghost", "United States", "g@x.com"),  # no id -> dropped
            ("C2", "Bob", None, "b@x.com"),  # missing country
        ],
        CUSTOMER_SCHEMA,
    )
    result = {r.customer_id: r for r in tr.build_enriched_customers(raw).collect()}

    assert set(result) == {"C1", "C2"}
    assert result["C1"].customer_name == "Alice Smith"  # named record wins over 'Unknown'
    assert result["C1"].country == "United States"
    assert result["C1"].email == "a@x.com"
    assert result["C2"].country == UNKNOWN


def test_enriched_customers_requires_columns(spark):
    with pytest.raises(DataQualityError):
        tr.build_enriched_customers(spark.createDataFrame([("C1",)], ["customer_id"]))


# --------------------------------------------------------------------------- products

def test_enriched_products_one_row_per_product(spark):
    raw = spark.createDataFrame(
        [
            ("P1", "Furniture", "Chairs", "B chair", "Texas", "81.882"),
            ("P1", "Furniture", "Chairs", "A chair", "Ohio", "63.686"),
            ("P2", " Technology ", "Phones", "Phone", "Ohio", "not-a-number"),
            (None, "Furniture", "Chairs", "orphan", "Ohio", "1"),
        ],
        PRODUCT_SCHEMA,
    )
    result = {r.product_id: r for r in tr.build_enriched_products(raw).collect()}

    assert set(result) == {"P1", "P2"}
    assert result["P1"].product_name == "A chair"  # deterministic choice
    assert result["P1"].price_per_product == pytest.approx(63.686)
    assert result["P2"].category == "Technology"
    assert result["P2"].price_per_product is None  # bad price does not fail the job


# --------------------------------------------------------------------------- orders

def test_clean_orders_parses_day_first_dates_and_year(spark):
    rows = [raw_order(1, order_date="21/8/2016", ship_date="5/9/2016"),
            raw_order(2, order_date="3/10/2014", ship_date="3/10/2014")]
    result = {r.row_id: r for r in make_orders(spark, rows).collect()}

    assert result[1].order_date == dt.date(2016, 8, 21)
    assert result[1].ship_date == dt.date(2016, 9, 5)
    assert result[1].order_year == 2016
    assert result[2].order_date == dt.date(2014, 10, 3)  # not March 10th
    assert result[2].order_year == 2014


def test_clean_orders_rejects_bad_rows_with_reason(spark):
    rows = [
        raw_order(1),
        raw_order(2, order_date="31/02/2016"),
        raw_order(3, order_date="not a date"),
        raw_order(4, customer_id=None),
        raw_order(5, product_id="  "),
        raw_order(6, quantity=0),
        raw_order(7, profit=None),
        raw_order(8, order_id=None),
    ]
    valid, rejected = tr.clean_orders(spark.createDataFrame(rows, RAW_ORDER_SCHEMA))

    assert [r.row_id for r in valid.collect()] == [1]
    reasons = {r.row_id: r.reject_reason for r in rejected.collect()}
    assert reasons == {
        2: "invalid order_date",
        3: "invalid order_date",
        4: "missing customer_id",
        5: "missing product_id",
        6: "invalid quantity",
        7: "missing profit",
        8: "missing order_id",
    }
    assert dict(tr.data_quality_summary(rejected))["invalid order_date"] == 2


def test_clean_orders_removes_duplicate_line_items(spark):
    rows = [raw_order(1), raw_order(1), raw_order(2)]
    assert make_orders(spark, rows).count() == 2


def test_clean_orders_flags_corrupt_json_records(spark):
    schema = T.StructType(RAW_ORDER_SCHEMA.fields + [T.StructField("_corrupt_record", T.StringType())])
    rows = [raw_order(1) + (None,), raw_order(2) + ('{"Row ID": "oops"',)]
    valid, rejected = tr.clean_orders(spark.createDataFrame(rows, schema))
    assert valid.count() == 1
    assert rejected.first().reject_reason == "corrupt record"


# --------------------------------------------------------------------------- enriched orders

def test_enriched_orders_has_required_columns(spark, customers, products):
    orders = make_orders(spark, [raw_order(1)])
    df = tr.build_enriched_orders(orders, customers, products)
    for col in ("order_id", "order_date", "order_year", "profit", "customer_name",
                "country", "category", "sub_category"):
        assert col in df.columns


def test_enriched_orders_joins_customer_and_product_attributes(spark, customers, products):
    orders = make_orders(spark, [raw_order(1, customer_id="C2", product_id="P2")])
    row = tr.build_enriched_orders(orders, customers, products).first()
    assert (row.customer_name, row.country) == ("Bob Jones", "United States")
    assert (row.category, row.sub_category) == ("Technology", "Phones")


@pytest.mark.parametrize(
    "profit, expected",
    [(63.6912, 63.69), (2.675, 2.68), (-14.925, -14.93), (102.19, 102.19), (5.0, 5.0), (0.004, 0.0)],
)
def test_enriched_orders_rounds_profit_to_two_decimals(spark, customers, products, profit, expected):
    orders = make_orders(spark, [raw_order(1, profit=profit)])
    assert tr.build_enriched_orders(orders, customers, products).first().profit == expected


def test_enriched_orders_keeps_orders_with_unknown_customer_or_product(spark, customers, products):
    orders = make_orders(spark, [raw_order(1, customer_id="NOPE"), raw_order(2, product_id="NOPE")])
    result = {r.row_id: r for r in tr.build_enriched_orders(orders, customers, products).collect()}

    assert len(result) == 2  # nothing dropped by the joins
    assert (result[1].customer_name, result[1].country) == (UNKNOWN, UNKNOWN)
    assert (result[2].category, result[2].sub_category) == (UNKNOWN, UNKNOWN)


def test_enriched_orders_rejects_duplicate_master_data(spark, customers):
    """Duplicate product ids would silently multiply profit, so we refuse them."""
    dup_products = spark.createDataFrame(
        [("P1", "Furniture", "Chairs", "a", "x", "1"), ("P1", "Furniture", "Chairs", "b", "y", "2")],
        PRODUCT_SCHEMA,
    )
    orders = make_orders(spark, [raw_order(1)])
    with pytest.raises(DataQualityError, match="unique on product_id"):
        tr.build_enriched_orders(orders, customers, dup_products)

    # ...and after enrichment the row count is unchanged.
    deduped = tr.build_enriched_products(dup_products)
    assert tr.build_enriched_orders(orders, customers, deduped).count() == 1


# --------------------------------------------------------------------------- aggregate

@pytest.fixture
def enriched(spark, customers, products):
    rows = [
        raw_order(1, order_id="O1", order_date="1/1/2016", customer_id="C1", product_id="P1", profit=10.111),
        raw_order(2, order_id="O1", order_date="1/1/2016", customer_id="C1", product_id="P1", profit=5.555),
        raw_order(3, order_id="O2", order_date="5/6/2016", customer_id="C1", product_id="P2", profit=-20.0),
        raw_order(4, order_id="O3", order_date="1/1/2017", customer_id="C1", product_id="P1", profit=7.0),
        raw_order(5, order_id="O4", order_date="9/9/2017", customer_id="C2", product_id="P2", profit=100.0),
    ]
    return tr.build_enriched_orders(make_orders(spark, rows), customers, products)


def test_profit_aggregate_groups_by_year_category_subcategory_customer(enriched):
    agg = tr.build_profit_aggregate(enriched)
    result = {
        (r.order_year, r.category, r.sub_category, r.customer_id): (r.total_profit, r.order_count)
        for r in agg.collect()
    }
    assert result == {
        (2016, "Furniture", "Chairs", "C1"): (15.67, 1),  # 10.11 + 5.56, same order
        (2016, "Technology", "Phones", "C1"): (-20.0, 1),
        (2017, "Furniture", "Chairs", "C1"): (7.0, 1),
        (2017, "Technology", "Phones", "C2"): (100.0, 1),
    }
    assert set(tr.AGGREGATE_KEYS + ["total_profit"]).issubset(agg.columns)


def test_profit_aggregate_preserves_total_profit(enriched):
    agg = tr.build_profit_aggregate(enriched)
    total_enriched = enriched.agg(F.round(F.sum("profit"), 2)).first()[0]
    total_agg = agg.agg(F.round(F.sum("total_profit"), 2)).first()[0]
    assert total_enriched == total_agg == pytest.approx(102.67)


def test_profit_aggregate_on_empty_input(spark, customers, products):
    empty = spark.createDataFrame([], RAW_ORDER_SCHEMA)
    orders, _ = tr.clean_orders(empty)
    agg = tr.build_profit_aggregate(tr.build_enriched_orders(orders, customers, products))
    assert agg.count() == 0
