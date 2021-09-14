#!/usr/bin/env python

import sys
from pyspark.sql import SparkSession
from pyspark import SparkConf
from config.logger import Log4j
from src.fix_json import fix_json, aggregate_model, read_parquet
from src.sleep_duration import read_source_2, sleep_range, write_sleep


def main():
    conf = SparkConf()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .appName("clue") \
        .master("local[2]") \
        .getOrCreate()

    logger = Log4j(spark)

    logger.info("Fixing Json")
    fix_json()
    logger.info("Converting to parquet")
    df = read_parquet()
    aggregate_model(df)
    logger.info("Aggregating users per device model")

    logger.info('Reading source 2 ...')
    read_df = read_source_2(spark)
    logger.info('Computing sleep range')
    sleep_df = sleep_range(read_df)
    logger.info('Done!')
    write_sleep(sleep_df)

    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except RuntimeError:
        sys.exit(1)
