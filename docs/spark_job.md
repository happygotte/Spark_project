## Из чего состоит Spark-джоба?

Spark-джоба — это приложение, которое выполняет задачи с использованием Apache Spark. Оно состоит из нескольких ключевых компонентов.

### 1. SparkSession
**SparkSession** — это точка входа для работы с Spark. Он управляет всеми ресурсами и настройками.

```python
from pyspark.sql import SparkSession

# Создаём SparkSession
spark = SparkSession.builder \
    .appName("My Spark App") \
    .getOrCreate()
```

### 2. Чтение данных
Данные могут быть прочитаны из различных источников (например, файлы, базы данных).

```Copy
# Чтение CSV-файла
df = spark.read.csv("data.csv", header=True, inferSchema=True)
```

### 3. Трансформации (Transformations)
Операции над данными, которые создают новый DataFrame. Они выполняются лениво (только при вызове действия).

```Copy
# Фильтрация данных
filtered_df = df.filter(df["age"] > 30)

# Агрегация данных
aggregated_df = df.groupBy("department").count()
```

### 4. Действия (Actions)
Действия запускают вычисления и возвращают результат (например, запись данных или сбор в память).

```Copy
# Запись данных в файл
filtered_df.write.csv("output.csv")

# Сбор данных в память
result = aggregated_df.collect()
```

### 5. Закрытие SparkSession
После завершения работы необходимо закрыть SparkSession, чтобы освободить ресурсы.

```spark.stop()```