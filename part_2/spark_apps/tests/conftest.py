from datetime import datetime

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    yield spark
    spark.stop()
