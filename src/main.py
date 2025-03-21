from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("My Spark App") \
    .getOrCreate()

import reading
import transform
import loading

# код

spark.stop()
