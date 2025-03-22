## Структура проекта
- `src/main.py`: Главный скрипт, который запускает `Reading.py`, `Transform.py` и `Writing.py`.
- `src/Reading.py`: Скрипт для чтения данных.
- `src/Transform.py`: Скрипт для обработки данных.
- `src/Writing.py`: Скрипт для загрузки данных.
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

## Структура джобы
