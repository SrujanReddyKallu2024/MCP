import json
from typing import Dict

import boto3
from pyspark.sql import SparkSession
import pytest


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Fixture for creating a spark context.

    Returns:
        SparkSession: the spark context
    """
    spark_session = (
        SparkSession.builder.master("local[1]")
        .appName("unit-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


@pytest.fixture(scope="module")
def s3_bucket(s3_mock: boto3.client) -> str:
    """Setup s3 bucket for testing."""
    test_bucket = "my-bucket"
    s3_mock.create_bucket(Bucket=test_bucket)
    return test_bucket
