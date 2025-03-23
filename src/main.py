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
    logger.info('SparkSession is created!!!')

    try:
        path_to_articles = "data/articles.csv"
        path_to_customers = "data/customers.csv"
        path_to_transactions = "data/transactions_train.csv"

        # READING
        scv_articles = R.DataReader(R.CSVReadStrategy(spark, path_to_articles))
        df_articles = scv_articles.read()
        logger.info("scv_articles files are read")

        scv_customers = R.DataReader(R.CSVReadStrategy(spark, path_to_customers))
        df_customers = scv_customers.read()
        logger.info("scv_customers files are read")

        scv_transactions = R.DataReader(R.CSVReadStrategy(spark, path_to_transactions))
        df_transactions = scv_transactions.read()
        logger.info("scv_transactions files are read")

        # TRANSFORM
        df_transactions_filtered = T.filter_date(df_transactions,'2020-09-01', '2023-09-30')
        df_joined = T.join_df([df_transactions_filtered, df_customers, df_articles])
        logger.info("df transformed")

        columns = [
            "customer_id",
            "customer_group_by_age",
            "transaction_amount",
            "max_price",
            "number_of_articles",
            "number_of_product_groups"]

        df_transformed = (df_joined
                          .transform(T.filter_age)
                          .transform(T.aggregate_purchases)
                          .transform(T.select_columns(columns))
                          )
        #df_transformed.show()
        logger.info("df_transformed created")

        # WRITING
        path_to_write = "data/df_transformed"
        writer = W.DataWriter(W.FileSystemWriteStrategy(spark, path_to_write, "csv"))
        writer.write(df_transformed)
        logger.info("df wrote")

    except FileNotFoundError as e:
        logger.error(f'File not found: {e}')

    except Exception as e:
        logger.error(f'An error occurred: {e}')
    finally:
        spark.stop()
        logger.info("SparkSession closed")


if __name__ == "__main__":
    logger.info(f'Job started!')
    main()
