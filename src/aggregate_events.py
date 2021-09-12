

def read_source_1(spark):
    df = spark.read.option("header", "True").csv("../input/data_source_1.tar.gz")
    return df

