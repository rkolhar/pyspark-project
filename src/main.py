#!/usr/bin/env python

from pyspark.sql import SparkSession
from pyspark import SparkConf
from config.logger import Log4j
from fix_json import fix_json, aggregate_model, read_parquet
from sleep_duration import read_source_2, sleep_range, write_sleep
from aggregate_events import  read_source_1


def main():
	conf = SparkConf()
	conf.set("spark.app.name", "clue")
	conf.set("spark.master", "local[2]")
	spark = SparkSession.builder\
		.config(conf=conf)\
		.appName("clue")\
		.master("local[2]")\
		.getOrCreate()

	logger = Log4j(spark)

#	fix = fix_json(spark)
#	df = read_parquet()
#	aggregate_model(df)

	read_df = read_source_2(spark)
	sleep_df = sleep_range(read_df)
	write_sleep(sleep_df)

#	read_2_df = read_source_1(spark)

	# TO DO
	# correct utc timestamp
	# model _name convention for apple
	# drop created_3?
	
	spark.stop()

if __name__ == "__main__":
 #   try:
	main()
 #   except RuntimeError:
 #       sys.exit(1)