# Databricks notebook source


# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from ecommerce import transformations as tr

enriched_orders = spark.table(TABLES["enriched_orders"])
profit_aggregate = tr.build_profit_aggregate(enriched_orders)

write_table(profit_aggregate, TABLES["profit_aggregate"],
            "Profit by year, category, sub-category and customer")

# COMMAND ----------

# Reconciliation: the aggregate must add up to the enriched table.
enriched_total = enriched_orders.agg(F.round(F.sum("profit"), 2)).first()[0]
aggregate = spark.table(TABLES["profit_aggregate"])
aggregate_total = aggregate.agg(F.round(F.sum("total_profit"), 2)).first()[0]
print(f"enriched total profit={enriched_total:,.2f} aggregate total profit={aggregate_total:,.2f}")
# Each group is rounded to 2 dp, so allow at most half a cent of drift per group.
tolerance = 0.005 * aggregate.count() + 0.01
assert abs(enriched_total - aggregate_total) <= tolerance, "aggregate does not reconcile to enriched_orders"

# COMMAND ----------

display(spark.table(TABLES["profit_aggregate"]).orderBy("order_year", "category", "sub_category", "customer_id"))
