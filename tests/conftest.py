import os
import sys

import pytest
from pyspark.sql import SparkSession

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(ROOT, "src"))

ON_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ


@pytest.fixture(scope="session")
def spark():
    """Reuse the notebook's session on Databricks; start a small local one elsewhere."""
    if ON_DATABRICKS:
        # Works on classic and serverless (Spark Connect) compute.
        from databricks.sdk.runtime import spark as notebook_spark

        yield notebook_spark
        return
    active = SparkSession.getActiveSession()
    if active is not None:
        yield active
        return
    session = (
        SparkSession.builder.master("local[2]")
        .appName("ecommerce-unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "true")  # behave like Databricks serverless
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def data_dir():
    return os.path.join(ROOT, "data")
