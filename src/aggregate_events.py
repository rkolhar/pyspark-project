from functools import reduce
from sleep_duration import  sleep_range


def read_source_1(spark):
    df = spark.read.option("header", "True").csv("../input/data_source_1.csv")
    return df


def unionAll(df, df_hours):
   # df_join = df_hours.join(df, df_hours.userId == df.userId, "left_outer")
    union_df = df.union(df_hours)
    union_df.show(5)


def write_join(df_join):
    df_join.write.option("header",True) \
        .partitionBy("date") \
        .mode("overwrite") \
        .csv("../output/results.csv")