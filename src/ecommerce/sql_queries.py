"""SQL for the required profit reports, run against the aggregate table.

Kept in Python so the exact same statements are used by the 04_sql_analysis notebook
and by the unit tests. `{table}` is the (fully qualified) aggregate table or a temp view.
"""

PROFIT_BY_YEAR = """
SELECT order_year,
       ROUND(SUM(total_profit), 2) AS total_profit
FROM {table}
GROUP BY order_year
ORDER BY order_year
"""

PROFIT_BY_YEAR_AND_CATEGORY = """
SELECT order_year,
       category,
       ROUND(SUM(total_profit), 2) AS total_profit
FROM {table}
GROUP BY order_year, category
ORDER BY order_year, category
"""

PROFIT_BY_CUSTOMER = """
SELECT customer_id,
       customer_name,
       ROUND(SUM(total_profit), 2) AS total_profit
FROM {table}
GROUP BY customer_id, customer_name
ORDER BY total_profit DESC, customer_id
"""

PROFIT_BY_CUSTOMER_AND_YEAR = """
SELECT customer_id,
       customer_name,
       order_year,
       ROUND(SUM(total_profit), 2) AS total_profit
FROM {table}
GROUP BY customer_id, customer_name, order_year
ORDER BY customer_id, order_year
"""

ALL_QUERIES = {
    "profit_by_year": PROFIT_BY_YEAR,
    "profit_by_year_category": PROFIT_BY_YEAR_AND_CATEGORY,
    "profit_by_customer": PROFIT_BY_CUSTOMER,
    "profit_by_customer_year": PROFIT_BY_CUSTOMER_AND_YEAR,
}


def run_query(spark, query: str, table: str):
    return spark.sql(query.format(table=table))
