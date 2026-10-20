# Databricks notebook source
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "ecommerce")
dbutils.widgets.text("volume", "raw_files")
params = {k: dbutils.widgets.get(k) for k in ("catalog", "schema", "volume")}

for notebook in ["05_run_unit_tests", "01_raw_ingestion", "02_enriched_tables",
                 "03_aggregate_table", "04_sql_analysis"]:
    print(f"running {notebook} ...")
    dbutils.notebook.run(f"./{notebook}", 1800, params)
print("pipeline finished")
