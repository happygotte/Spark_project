## Структура проекта
- `src/main.py`: Главный скрипт, который запускает `reading.py`, `transform.py` и `loading.py`.
- `src/reading.py`: Скрипт для чтения данных.
- `src/transform.py`: Скрипт для обработки данных.
- `src/loading.py`: Скрипт для загрузки данных.
- `docs`: полезные статьи

## Запуск проекта

### 1. Standalone-режим
Запуск Spark в локальном режиме:
```bash
docker-compose -f docker-compose-standalone.yml up
```

### 2. Cluster-режим
Запуск Spark в локальном режиме:
```bash
docker-compose -f docker-compose-cluster.yml up
```

### 3. Структура джобы
