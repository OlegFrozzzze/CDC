# Trigger-Based CDC Example

Этот пример демонстрирует реализацию CDC (Change Data Capture) с использованием **триггеров PostgreSQL**.

## Отличия от Log-Based CDC

| Аспект | Log-Based (основной пример) | Trigger-Based (этот пример) |
|--------|------------------------------|------------------------------|
| **Метод** | Чтение из WAL (Write-Ahead Log) | Триггеры на INSERT/UPDATE/DELETE |
| **Инструмент** | Debezium | Python скрипт (cdc_producer.py) |
| **Нагрузка на БД** | Минимальная | Средняя (триггеры выполняются при каждой операции) |
| **Производительность** | Высокая | Средняя |
| **Масштабируемость** | Отличная | Ограниченная |
| **Сложность настройки** | Средняя | Простая |

## Архитектура

1. **PostgreSQL** с триггерами на таблице `users`
2. **Таблица `cdc_changes_log`** для хранения изменений
3. **Python Producer** (`cdc_producer.py`) читает изменения из таблицы и отправляет в Kafka
4. **Kafka** для передачи сообщений
5. **Spark** для обработки данных и создания Delta таблиц

## Структура файлов

```
trigger_cdc/
├── docker-compose.yml          # Docker Compose конфигурация
├── sql/
│   └── init-db.sql            # SQL скрипты с триггерами
├── cdc_producer.py            # Python скрипт для чтения изменений и отправки в Kafka
├── spark_read_kafka.py        # Spark скрипт для чтения из Kafka
├── spark_process_raw.py       # Spark скрипт для обработки и создания Delta таблиц
├── requirements.txt           # Python зависимости
├── run_all.bat                # Скрипт для запуска всего pipeline
└── README.md                  # Этот файл
```

## Как работает

### 1. Триггеры в PostgreSQL

При каждой операции (INSERT/UPDATE/DELETE) на таблице `users`:
- Триггер автоматически записывает изменение в таблицу `cdc_changes_log`
- Сохраняются старые и новые значения (для UPDATE)
- Операция помечается как необработанная (`processed = FALSE`)

### 2. CDC Producer

Python скрипт `cdc_producer.py`:
- Периодически опрашивает таблицу `cdc_changes_log`
- Находит необработанные изменения (`processed = FALSE`)
- Отправляет их в Kafka в формате, совместимом с Debezium
- Помечает изменения как обработанные (`processed = TRUE`)

### 3. Spark обработка

Аналогично основному примеру:
- Чтение из Kafka и сохранение в `raw_store`
- Обработка и создание Delta таблиц

## Запуск

### Windows

```batch
cd trigger_cdc
run_all.bat
```

### Linux/Mac

```bash
cd trigger_cdc
docker-compose up -d
# Подождите пока сервисы запустятся
docker exec trigger-cdc-spark /opt/spark/bin/spark-submit ...
```

## Просмотр логов

### CDC Producer
```batch
docker logs trigger-cdc-producer
```

### PostgreSQL
```batch
docker logs trigger-cdc-postgres
```

### Kafka
```batch
docker logs trigger-cdc-kafka
```

## Проверка изменений в таблице CDC

```batch
docker exec trigger-cdc-postgres psql -U postgres -d testdb -c "SELECT * FROM cdc_changes_log ORDER BY changed_at DESC LIMIT 10;"
```

## Проверка необработанных изменений

```batch
docker exec trigger-cdc-postgres psql -U postgres -d testdb -c "SELECT COUNT(*) FROM cdc_changes_log WHERE processed = FALSE;"
```

## Остановка

```batch
docker-compose down
```

## Остановка с удалением данных

```batch
docker-compose down -v
```

## Портовая конфигурация

Чтобы не конфликтовать с основным примером, используются другие порты:

- PostgreSQL: `5433` (вместо 5432)
- Zookeeper: `2182` (вместо 2181)
- Kafka: `9094` (вместо 9092)
- Kafka UI: `8082` (вместо 8080)

## Преимущества Trigger-Based подхода

1. ✅ Простота настройки - не требует logical replication
2. ✅ Работает с любой версией PostgreSQL
3. ✅ Полный контроль над форматом данных
4. ✅ Легко добавить дополнительную логику

## Недостатки Trigger-Based подхода

1. ❌ Увеличивает нагрузку на БД (триггеры выполняются синхронно)
2. ❌ Может замедлить операции записи
3. ❌ Ограниченная масштабируемость при высокой нагрузке
4. ❌ Требует дополнительную таблицу для хранения изменений

## Когда использовать

**Trigger-Based CDC подходит для:**
- Небольших и средних проектов
- Систем с низкой/средней нагрузкой на запись
- Когда нужен полный контроль над процессом
- Когда нельзя использовать logical replication

**Log-Based CDC (основной пример) подходит для:**
- Высоконагруженных систем
- Когда нужна максимальная производительность
- Production окружений
- Когда важна минимальная нагрузка на БД

