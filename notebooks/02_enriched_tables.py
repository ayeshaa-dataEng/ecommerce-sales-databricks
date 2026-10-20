# Databricks notebook source


# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from ecommerce import transformations as tr

raw_customers = spark.table(TABLES["raw_customers"])
raw_products = spark.table(TABLES["raw_products"])
raw_orders = spark.table(TABLES["raw_orders"])

# COMMAND ----------



# COMMAND ----------

enriched_customers = tr.build_enriched_customers(raw_customers)
enriched_products = tr.build_enriched_products(raw_products)

write_table(enriched_customers, TABLES["enriched_customers"], "Cleaned customers, one row per customer_id")
write_table(enriched_products, TABLES["enriched_products"], "Cleaned products, one row per product_id")

# COMMAND ----------



# COMMAND ----------

valid_orders, rejected_orders = tr.clean_orders(raw_orders)

enriched_orders = tr.build_enriched_orders(
    valid_orders,
    spark.table(TABLES["enriched_customers"]),
    spark.table(TABLES["enriched_products"]),
)

write_table(enriched_orders, TABLES["enriched_orders"],
            "Orders with profit (2 dp), customer name/country, product category/sub-category")
write_table(rejected_orders, TABLES["rejected_orders"], "Orders that failed validation")

# COMMAND ----------


# COMMAND ----------

raw_count = raw_orders.count()
enriched_count = spark.table(TABLES["enriched_orders"]).count()
rejected_count = spark.table(TABLES["rejected_orders"]).count()

print(f"raw={raw_count:,} enriched={enriched_count:,} rejected={rejected_count:,}")
print("reject reasons:", tr.data_quality_summary(spark.table(TABLES["rejected_orders"])))
display(
    spark.table(TABLES["enriched_orders"])
    .agg(
        F.sum(F.when(F.col("category") == tr.UNKNOWN, 1).otherwise(0)).alias("orders_with_unknown_product"),
        F.sum(F.when(F.col("customer_name") == tr.UNKNOWN, 1).otherwise(0)).alias("orders_with_unknown_customer_name"),
    )
)

assert enriched_count + rejected_count == raw_count, "row count mismatch between raw and enriched orders"

# COMMAND ----------

display(spark.table(TABLES["enriched_orders"]).limit(20))
