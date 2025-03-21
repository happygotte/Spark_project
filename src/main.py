from pyspark.sql import SparkSession

spark = (SparkSession
    .builder
    .appName("spark_project")
    .getOrCreate()
        )

import Reading as R
import Transform as T
import Writing as W

# код
path = "path-to-our-data"

# Read
csv_strategy = R.CSVReadStrategy(spark, path)
csv_reader = R.DataReader(csv_strategy)
csv_df = csv_reader.read()

spark.stop()
