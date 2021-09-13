from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql import functions as F
from datetime import datetime
from pyspark import StorageLevel


def read_source_2(spark):
    """
    Read data source 2 file
    :param spark:
    :return: df
    """
    sleep_file = "/opt/spark-input/data_source_2.csv"
    df = spark.read.option("header", "True").csv(sleep_file)

    return df


def compute_delta(y, x):
    """
    A udf to compute time difference between start and end time
    :param y:
    :param x:
    :return: sleep duration in hours
    """
    end = datetime.strptime(y, '%Y-%m-%dT%H:%M:%S.%f%z')
    start = datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%f%z')
    delta = (end-start).total_seconds()
    delta_hours = int((delta/3600))
    return delta_hours


# register as a UDF 
match_hour_udf = F.udf(compute_delta, IntegerType())


def sleep_range(df):
    """
    Apply udf function
    :param df:
    :return: df_hours
    """
    df_hours = df.withColumn('Duration', match_hour_udf(F.col('endTime'), F.col('startTime')))\
        .withColumn('value',
                    F.when((F.col('Duration').between(0, 3)), "0-3")
                     .when((F.col('Duration').between(4, 6)), "3-6")
                     .when((F.col('Duration').between(7, 9)), "6-9")
                     .when((F.col('Duration') > 9), ">9")
                     .otherwise('na'))\
        .withColumn('date', to_timestamp('endTime').cast('string'))

    drop_cols = ('startTime', 'endTime', 'Duration')
    df_hours = df_hours.drop(*drop_cols)
   # df_hours.show(5)
   # df_hours.printSchema()
    return df_hours


def write_sleep(df_hours):
    """
    write results in compressed form
    :param df_hours:
    :return:
    """

    df_hours.write\
        .option("header", "true") \
        .option("compression", "gzip")\
        .mode("overwrite")\
        .csv("/opt/spark-output/sleep_range.csv.gz")

    # df_hours.write \
    #     .option("header", "true") \
    #     .mode("overwrite") \
    #     .parquet("/opt/spark-output/sleep_range.parquet")\
    #     .createOrReplaceTempView("parquetTable")




