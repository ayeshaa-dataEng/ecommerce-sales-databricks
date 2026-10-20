# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Unit tests
# MAGIC Runs the pytest suite in `tests/` on the notebook's Spark session.
# MAGIC Tests that read local files with Spark are skipped here (serverless Spark cannot read the
# MAGIC driver's local disk); they run locally / in CI.

# COMMAND ----------

# MAGIC %pip install pytest openpyxl --quiet

# COMMAND ----------

import os
import sys

import pytest

repo_root = os.path.dirname(os.getcwd())
sys.dont_write_bytecode = True  # repo folders are read-only for .pyc files

exit_code = pytest.main([os.path.join(repo_root, "tests"), "-v", "-p", "no:cacheprovider", "-rs"])
assert exit_code == 0, f"unit tests failed (pytest exit code {exit_code}) - see output above"
