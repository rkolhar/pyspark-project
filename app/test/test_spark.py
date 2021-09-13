import pytest
from pyspark.sql import SparkSession
from app.src.sleep_duration import sleep_range


@pytest.fixture(scope="session")
def spark_test_session():
    return (
        SparkSession
            .builder
            .master('local[4]')
            .appName('clue')
            .getOrCreate()
    )


def test_fix_json_string():
    pass
    # bad_json = f.read('test.json')


@pytest.fixture(scope="function")
def test_sleep_data(spark):
    data = [["1", "2018-01-24T21:58:00.000+01:00", "2018-01-25T03:55:00.000+01:00"],
            ["2", "2018-01-13T20:47:00.000+01:00", "2018-01-14T03:03:00.000+01:00"],
            ["3", "2018-01-14T22:19:00.000+01:00", "2018-01-15T02:41:00.000+01:00"]]
    df = spark.createDataFrame(data, ['userId', 'startTime', 'endTime'])
    return df


def test_sleep_duration(spark, test_sleep_data):
    test_duration_df = sleep_range(test_sleep_data)
    test_df = test_duration_df.collect()
    expected_data = [("1", "3-6", "2018-01-25"), ("2", "6-9", "2018-01-14", ("3", "3-6", "2018-01-15"))]
    expected_df = spark.createDataFrame(expected_data, ["userId", "value", "date"])
    expected_df_collect = expected_df.collect()
    assert test_df == expected_df_collect
