# Используем базовый образ Spark
FROM bitnami/spark:3.4.1

# Копируем файлы проекта
COPY src/ /app/

# Указываем рабочую директорию
WORKDIR /app

# Команда по умолчанию (запуск main.py)
CMD ["spark-submit", "main.py"]