from pyspark.sql import SparkSession

# Создаём SparkSession
spark = SparkSession.builder \
    .appName("My Spark App") \
    .getOrCreate()

# Запускаем ваши скрипты
import reading
import transform
import loading

# Закрываем SparkSession
spark.stop()