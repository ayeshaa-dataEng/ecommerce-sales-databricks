# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · SQL reports
# MAGIC Required aggregates, written in SQL on top of `profit_aggregate`.
# MAGIC The same statements live in `src/ecommerce/sql_queries.py`, where they are unit tested.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md ### 1. Profit by Year

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_year,
# MAGIC        ROUND(SUM(total_profit), 2) AS total_profit
# MAGIC FROM profit_aggregate
# MAGIC GROUP BY order_year
# MAGIC ORDER BY order_year

# COMMAND ----------

# MAGIC %md ### 2. Profit by Year + Product Category

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_year,
# MAGIC        category,
# MAGIC        ROUND(SUM(total_profit), 2) AS total_profit
# MAGIC FROM profit_aggregate
# MAGIC GROUP BY order_year, category
# MAGIC ORDER BY order_year, category

# COMMAND ----------

# MAGIC %md ### 3. Profit by Customer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id,
# MAGIC        customer_name,
# MAGIC        ROUND(SUM(total_profit), 2) AS total_profit
# MAGIC FROM profit_aggregate
# MAGIC GROUP BY customer_id, customer_name
# MAGIC ORDER BY total_profit DESC, customer_id

# COMMAND ----------

# MAGIC %md ### 4. Profit by Customer + Year

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id,
# MAGIC        customer_name,
# MAGIC        order_year,
# MAGIC        ROUND(SUM(total_profit), 2) AS total_profit
# MAGIC FROM profit_aggregate
# MAGIC GROUP BY customer_id, customer_name, order_year
# MAGIC ORDER BY customer_id, order_year
