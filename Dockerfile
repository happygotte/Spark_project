# Используем базовый образ Spark
FROM bitnami/spark:3.4.1

# Копируем requirements.txt в контейнер
COPY requirements.txt /app/requirements.txt

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r /app/requirements.txt

# Копируем файлы проекта
COPY src/ /app/

# Указываем рабочую директорию
WORKDIR /app

# Команда по умолчанию (запуск main.py)
CMD ["spark-submit", "main.py"]