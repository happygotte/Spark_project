# Используем базовый образ Spark
FROM bitnami/spark:3.4.1

# Указываем рабочую директорию
WORKDIR /app

# Копируем requirements.txt в контейнер
COPY requirements.txt requirements.txt

# Устанавливаем зависимости
RUN pip install -r requirements.txt

# Копируем файлы проекта
COPY src/ /app

# Команда по умолчанию (запуск main.py)
CMD ["spark-submit", "--master", "local[*]", "/app/main.py"]