# E-commerce Sales Data Processing on Databricks (PySpark)

A layered (raw → enriched → aggregate) PySpark pipeline that turns the raw customer, product and
order files into analysis-ready Delta tables, with unit tests and SQL reports.

## Project layout

```
data/                       source files (Customer.xlsx, Products.csv, Orders.json)
src/ecommerce/
    transformations.py      all PySpark logic: readers, cleaning, enrichment, aggregation
    sql_queries.py          the 4 SQL reports (shared by notebook 04 and the tests)
notebooks/                  Databricks notebooks (source format .py)
    00_config               widgets, table names, schema/volume creation, write helper
    01_raw_ingestion        source files -> raw_* tables
    02_enriched_tables      enriched_customers, enriched_products, enriched_orders, rejected_orders
    03_aggregate_table      profit_aggregate
    04_sql_analysis         SQL: profit by year / year+category / customer / customer+year
    05_run_unit_tests       runs the pytest suite on Databricks
    run_all                 runs everything in order
tests/                      pytest unit tests (54 tests)
```

## Tables produced (`workspace.ecommerce.*` by default)

| Layer | Table | Contents |
|---|---|---|
| Raw | `raw_customers`, `raw_products`, `raw_orders` | Source data unchanged, snake_case column names, `_source_file`, `_ingested_at` |
| Enriched | `enriched_customers` | Trimmed, one row per `customer_id`, names cleaned, email lower-cased |
| Enriched | `enriched_products` | Trimmed, price typed, one row per `product_id` |
| Enriched | `enriched_orders` | Order info + **profit rounded to 2 dp** + **customer name & country** + **category & sub-category** + `order_year` |
| Enriched | `rejected_orders` | Rows that failed validation, with `reject_reason` (quarantine) |
| Aggregate | `profit_aggregate` | `total_profit` and `order_count` by `order_year`, `category`, `sub_category`, `customer_id`, `customer_name` |

## How to run on Databricks

1. **Workspace → Create → Git folder** and paste this repository's URL (public repos need no token).
2. Open `notebooks/run_all` and attach it to **Serverless** (or any cluster running DBR 14.3+), then **Run all**.
   * Unit tests run first, then the pipeline, then the SQL reports.
   * The schema `workspace.ecommerce` and volume `raw_files` are created automatically, and the source
     files are copied from `data/` into the volume. To use other files, upload them to
     `/Volumes/workspace/ecommerce/raw_files/` first.
   * If your catalog isn't called `workspace`, change the `catalog` widget.
3. Open `notebooks/04_sql_analysis` to see the four SQL outputs.

## How to run the tests locally

Requires Python 3.9+ and Java 17.

```bash
pip install -r requirements-dev.txt
pytest tests -v
```

Locally the suite also runs the file readers and an end-to-end check on the real files in `data/`.

## Data quality findings and how they are handled

| Finding (in the source data) | Handling |
|---|---|
| 74 customer names contain digits/symbols (`Pete@#$ Takahito`, `Tam&^*ara Willing___)ham`) and extra whitespace | Keep only letters (Unicode, so `Bühler` survives), spaces and O'Brien-style apostrophes; collapse whitespace |
| 8 customers have no name | `customer_name = 'Unknown'` (the customer and their orders are kept) |
| 33 product IDs appear twice with different name/state/price (same category & sub-category) | Keep one deterministic row per `product_id` (first by product name). Otherwise the join would double those orders' profit. `build_enriched_orders` refuses non-unique master data |
| 204 orders reference product IDs not present in Products.csv | Left join; category/sub-category = `'Unknown'` so total profit still reconciles |
| Dates are day-first without padding (`3/10/2014` = 3 Oct 2014) | Parsed explicitly with `d/M/yyyy` |
| Possible dirty values (bad dates, non-numeric prices, missing keys, quantity ≤ 0, malformed JSON) | `try_cast` / `try_to_timestamp` (safe under ANSI mode) and the row goes to `rejected_orders` with a reason, instead of failing the job or being silently dropped |
| Duplicate order lines | De-duplicated on `row_id` |

The current source files produce **0 rejected orders**: all 9,994 orders reach `enriched_orders`.

## Assumptions

* **Profit** is the `Profit` field of each order line, rounded to 2 dp with HALF_UP (`2.675 → 2.68`).
  Aggregates sum the rounded line profits and round the result to 2 dp again.
* **Year** means the year of the **order date**.
* **Customer** in aggregates is `customer_id` + `customer_name`; the id is kept because names aren't
  unique and some are `Unknown`.
* Name cleaning can't undo noise that added a space inside a name (e.g. `Kat rina Bavinger`); about 20
  such names stay as-is rather than guessing. `customer_id` is the reliable key.
* Each run is a full refresh (`overwrite`), so re-running is idempotent. An incremental `MERGE` would be
  the next step for daily loads.
* Excel is read with pandas + openpyxl because Spark has no built-in Excel reader and the spark-excel
  Maven library can't be installed on serverless compute. The file is small (≈800 rows).

## Performance and scalability notes

* The dimensions (≈800 customers, ≈1.8k products) are **broadcast** in the order join, so the fact
  table is never shuffled for the join.
* Explicit schema for the JSON orders file (no inference pass over the data).
* All logic is DataFrame transformations, with no Python UDFs or `collect()` in the data path, so it runs
  on Photon/serverless and scales with the cluster.
* At this size (10k rows) partitioning would only create small files. At larger volumes, cluster
  `enriched_orders` by `order_year, customer_id` (liquid clustering) and switch the loads to incremental
  `MERGE`/Auto Loader.

## Sample results (from the provided data)

| order_year | total_profit |
|---|---|
| 2014 | 39,185.75 |
| 2015 | 63,073.09 |
| 2016 | 65,073.23 |
| 2017 | 111,084.96 |
