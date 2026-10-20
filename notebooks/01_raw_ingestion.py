# Databricks notebook source

# COMMAND ----------

# MAGIC %pip install openpyxl --quiet

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------



# COMMAND ----------

import shutil

for key, file_name in SOURCE_FILES.items():
    target = os.path.join(SOURCE_DIR, file_name)
    if os.path.exists(target):
        print(f"found   {target}")
        continue
    repo_copy = os.path.join(REPO_DATA_DIR, file_name)
    if not os.path.exists(repo_copy):
        raise FileNotFoundError(
            f"{file_name} is neither in {SOURCE_DIR} nor in {REPO_DATA_DIR}. "
            "Upload it to the volume (Catalog > volume > Upload to this volume)."
        )
    shutil.copy(repo_copy, target)
    print(f"copied  {repo_copy} -> {target}")

# COMMAND ----------

from ecommerce import transformations as tr

raw_customers = tr.read_raw_customers(spark, os.path.join(SOURCE_DIR, SOURCE_FILES["customers"]))
raw_products = tr.read_raw_products(spark, os.path.join(SOURCE_DIR, SOURCE_FILES["products"]))
raw_orders = tr.read_raw_orders(spark, os.path.join(SOURCE_DIR, SOURCE_FILES["orders"]))

write_table(raw_customers, TABLES["raw_customers"], "Raw customers from Customer.xlsx")
write_table(raw_products, TABLES["raw_products"], "Raw products from Products.csv")
write_table(raw_orders, TABLES["raw_orders"], "Raw orders from Orders.json")

# COMMAND ----------

display(spark.table(TABLES["raw_orders"]).limit(10))
