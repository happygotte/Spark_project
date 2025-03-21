from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame

# Define the strategy interface
class ReadStrategy(ABC):
    @abstractmethod
    def read_data(self) -> DataFrame:
        pass

# File System Read Strategy - represents a file system reading
class FileSystemReadStrategy(ReadStrategy):
    def __init__(self, spark: SparkSession, path: str):
        self.spark = spark
        self.path = path

    @abstractmethod
    def read_data(self):
        pass

# Implement the strategy for CSV
class CSVReadStrategy(FileSystemReadStrategy):
    def read_data(self) -> DataFrame:
        return self.spark.read.format("csv").option("header", "true").load(self.path)


# Implement the strategy for Delta
class DeltaReadStrategy(FileSystemReadStrategy):
    def read_data(self) -> DataFrame:
        return self.spark.read.format("delta").load(self.path)


# Implement the strategy for Parquet
class ParquetReadStrategy(FileSystemReadStrategy):
    def read_data(self) -> DataFrame:
        return self.spark.read.format("parquet").load(self.path)

# Context class
class DataReader:
    def __init__(self, read_strategy: ReadStrategy):
        self.read_strategy = read_strategy

    def read(self) -> DataFrame:
        return self.read_strategy.read_data()