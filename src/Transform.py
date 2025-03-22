import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def filter_date(df: DataFrame, date_start: str, date_end: str) -> DataFrame:
    df = df.withColumn("date", F.to_date("t_dat", "yyyy-MM-dd"))
    return df.filter((F.col("date") >= date_start) & (F.col("date") <= date_end))

def join_df(dfs: list) -> DataFrame:
    return dfs[0].join(dfs[1], dfs[0]["customer_id"] == dfs[1]["customer_id"], "inner") \
                .join(dfs[2], dfs[0]["article_id"] == dfs[2]["article_id"], "inner")

def filter_age(df: DataFrame, age: int) -> DataFrame:
    df = df.withColumn("customer_group_by_age",
            F.when(F.col("age") < age, "S")
            .F.when((F.col("age") >= age) & (F.col("age") <= 59), "A")
            .F.otherwise("R"))
    return df

def aggregate(df: DataFrame) -> DataFrame:
    df = (df.groupBy("customer_id", "customer_group_by_age")
            .agg(sum("price").alias("transaction_amount"),                              # Сумма всех покупок
            F.countDistinct("article_id").alias("number_of_articles"),                  # Количество покупок
            F.countDistinct("product_group_name").alias("number_of_product_groups"),    # Количество групп товаров
            max("price").alias("max_price"))                                            # Самая дорогая покупка
          )
    return df

def select_columns(df: DataFrame, columns: list) -> DataFrame:
    return df.select(columns).distinct()