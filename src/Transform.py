import pyspark.sql.functions as F
from pyspark.sql import DataFrame


# Фильтруем данные из самого большого файла по дате
def filter_date(df: DataFrame, date_start: str, date_end: str) -> DataFrame:
    df = df.withColumn("date", F.to_date("t_dat", "yyyy-MM-dd"))
    return df.filter((F.col("date") >= date_start) & (F.col("date") <= date_end))


# Объединяем все три файла по id
def join_df(dfs: list) -> DataFrame:
    return (dfs[0].join(dfs[1], dfs[0]["customer_id"] == dfs[1]["customer_id"], "inner")
            .join(dfs[2], dfs[0]["article_id"] == dfs[2]["article_id"], "inner")
            .drop(dfs[0]["customer_id"], dfs[0]["article_id"]))


# Распределяем на 3 группы по возрастам
def filter_age(df: DataFrame, age_1=23, age_2=59) -> DataFrame:
    # если мне надо передать другие возраста, то тоже надо делать функцию-обёртку
    df = df.withColumn("customer_group_by_age",
                       F.when(df["age"] < age_1, "S")
                       .when((df["age"] >= age_1) & (df["age"] <= age_2), "A")
                       .otherwise("R"))
    return df


# Агрегируем данные по различным категориям
def aggregate_purchases(df: DataFrame) -> DataFrame:
    df = (df.groupBy("customer_id", "customer_group_by_age")
          .agg(F.sum("price").alias("transaction_amount"),  # Сумма всех покупок
               F.countDistinct("article_id").alias("number_of_articles"),  # Количество покупок
               F.countDistinct("product_group_name").alias("number_of_product_groups"),  # Количество групп товаров
               F.max("price").alias("max_price"))  # Самая дорогая покупка
          )
    return df


# Отбираем нужные колонки
# Метод transform в PySpark принимает функцию, которая должна иметь только один аргумент — # DataFrame.
# Функция-обёртка принимает список колонок и возвращает внутреннюю функцию, которая уже имеет в качестве
# аргумента только df
def select_columns(columns: list):
    def inner(df: DataFrame):
        return df.select(columns).distinct()

    return inner
