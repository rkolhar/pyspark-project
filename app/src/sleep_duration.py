from pyspark.sql.functions import to_date
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
from datetime import datetime


def read_source_2(spark):
    """
    Read data source 2 file
    :param spark:
    :return: df
    """
    sleep_file = "/opt/spark-data/data_source_2.csv"
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
    delta_hours = round((delta/3600),1)
    return delta_hours


# register as a UDF 
match_hour_udf = F.udf(compute_delta, DoubleType())


def sleep_range(df):
    """
    Apply udf function
    :param df:
    :return: df_hours
    """
    df_hours = df.withColumn('Duration', match_hour_udf(F.col('endTime'), F.col('startTime')))\
        .withColumn('value',
                    F.when((F.col('Duration').between(0.0, 3.0)), "0-3")
                     .when((F.col('Duration').between(3.1, 6.0)), "3-6")
                     .when((F.col('Duration').between(6.1, 9.0)), "6-9")
                     .when((F.col('Duration') > 9.0), ">9")
                     .otherwise('na'))\
        .withColumn('date', to_date('endTime').cast('string'))

    drop_cols = ('startTime', 'endTime', 'Duration')
    df_hours = df_hours.drop(*drop_cols)
 #   df_hours.show()
 #   df_hours.printSchema()
    return df_hours


def write_sleep(df_hours):
    """
    write results in compressed form
    :param df_hours:
    :return:
    """

    # df_hours.write.mode("overwrite")\
    #     .option("header", True)\
    #     .option("compression", "gzip")\
    #     .csv("/opt/spark-data/sleep.csv")

    df_hours.write \
        .option("header", True) \
        .mode("overwrite") \
        .format("parquet") \
        .save("/opt/spark-data/sleep_range.parquet")





