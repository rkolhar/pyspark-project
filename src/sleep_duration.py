from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql import functions as F
from datetime import datetime
from pyspark import StorageLevel


def read_source_2(spark):
   # df = spark.read.option("header", "True").csv("../input/data_source_2.tar.gz", sep=',')
    df = spark.read.option("header", "True").csv("../input/data_source_2.csv", sep=',')
    return df


def compute_delta(y, x):
    end = datetime.strptime(y, '%Y-%m-%dT%H:%M:%S.%f%z')
    start = datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%f%z')
    delta = (end-start).total_seconds()
    delta_hours = int((delta/3600))
    return delta_hours


# register as a UDF 
match_hour_udf = F.udf(compute_delta, IntegerType())


# Apply function
def sleep_range(df):
    df_hours = df.withColumn('Duration', match_hour_udf(F.col('endTime'), F.col('startTime')))\
        .withColumn('value',
                    F.when((F.col('Duration').between(0, 3)), "0-3")
                     .when((F.col('Duration').between(3, 6)), "3-6")
                     .when((F.col('Duration').between(6, 9)), "6-9")
                     .when((F.col('Duration') > 9), ">9")
                     .otherwise('na'))\
        .withColumn('date', to_timestamp('endTime').cast('string'))

    drop_cols = ('startTime', 'endTime', 'Duration')
    df_hours = df_hours.drop(*drop_cols)
   # df_hours.show(10)
    df_hours.printSchema()
    df_hours = df_hours.persist(StorageLevel.MEMORY_AND_DISK)
    return df_hours


def write_sleep(df_hours):
    df_hours.write\
        .option("header", "true") \
         .option("compression", "gzip")\
        .mode("overwrite")\
        .csv("../output/sleep_range1.csv")\


    # df_hours.write \
    #     .option("header", "true") \
    #     .mode("overwrite") \
    #     .parquet("../output/sleep_range.parquet")\
    #     .createOrReplaceTempView("parquetTable")