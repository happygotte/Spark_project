from loguru import logger
from pyspark.sql import SparkSession
import Reading as R
import Transform as T
import Writing as W


def main():
    spark = (SparkSession
             .builder
             .appName("spark_project")
             .getOrCreate()
             )

    try:
        path_to_articles = "path-to-our-data"
        path_to_customers = "path-to-our-data"
        path_to_transactions = "path-to-our-data"


        # Read
        scv_articles = R.DataReader(R.CSVReadStrategy(spark, path_to_articles))
        df_articles = scv_articles.read()
        logger.info("scv_articles files are read")

        scv_customers = R.DataReader(R.CSVReadStrategy(spark, path_to_customers))
        df_customers = scv_customers.read()
        logger.info("scv_customers files are read")

        scv_transactions = R.DataReader(R.CSVReadStrategy(spark, path_to_transactions))
        df_transactions = scv_transactions.read()
        logger.info("scv_transactions files are read")

        # Transform
        df_transactions_filtered = df_transactions.T.filter_date('2023-08-01', '2023-08-31')
        df_joined = df_transactions_filtered.T.join_df([df_transactions_filtered, df_customers, df_transactions])

        columns = [
            "customer_id",
            "customer_group_by_age",
            "transaction_amount",
            "most_exp_article_id",
            "number_of_articles",
            "number_of_product_groups"]

        df_transformed = (df_joined
                          .transform(T.filter_age(23))
                          .transform(T.aggregate_purchases)
                          .transform(T.select_columns(columns))
                          )

        # Writing
        path_to_write = "path-to-our-data"
        writer = W.DataWriter(W.FileSystemWriteStrategy(spark, path_to_write, "parquet"))
        writer.write(df_transformed)

    except FileNotFoundError as e:
        logger.error(f'File not found: {e}')
    except Exception as e:
        logger.error(f'An error occurred: {e}')
    finally:
        spark.stop()
        logger.info("SparkSession closed")

if __name__ == "__main__":
    main()
    logger.info(f'Job started')