# Databricks notebook source


# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_year,
# MAGIC        ROUND(SUM(total_profit), 2) AS total_profit
# MAGIC FROM profit_aggregate
# MAGIC GROUP BY order_year
# MAGIC ORDER BY order_year

# COMMAND ----------


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_year,
# MAGIC        category,
# MAGIC        ROUND(SUM(total_profit), 2) AS total_profit
# MAGIC FROM profit_aggregate
# MAGIC GROUP BY order_year, category
# MAGIC ORDER BY order_year, category

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id,
# MAGIC        customer_name,
# MAGIC        ROUND(SUM(total_profit), 2) AS total_profit
# MAGIC FROM profit_aggregate
# MAGIC GROUP BY customer_id, customer_name
# MAGIC ORDER BY total_profit DESC, customer_id

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id,
# MAGIC        customer_name,
# MAGIC        order_year,
# MAGIC        ROUND(SUM(total_profit), 2) AS total_profit
# MAGIC FROM profit_aggregate
# MAGIC GROUP BY customer_id, customer_name, order_year
# MAGIC ORDER BY customer_id, order_year
