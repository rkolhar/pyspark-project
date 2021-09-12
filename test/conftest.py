import pytest
from pyspark.sql import SparkSession
import logging


def quiet_py4j():
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[2]") \
        .appName('clue').getOrCreate()
    return spark