from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame

class WriteStrategy(ABC):
    @abstractmethod
    def write_data(self, df: DataFrame):
        pass

class FileSystemWriteStrategy(WriteStrategy):
    def __init__(self, spark: SparkSession, path: str, format: str, mode: str = "overwrite"):
        self.spark = spark
        self.path = path
        self.format = format
        self.mode = mode
    def write_data(self, df: DataFrame):
        df.write.format(self.format).mode(self.mode).option('header', 'true').save(self.path)

class DataWriter:
    def __init__(self, write_strategy: WriteStrategy):
        self.write_strategy = write_strategy
    def write(self, df: DataFrame):
        self.write_strategy.write_data(df)
