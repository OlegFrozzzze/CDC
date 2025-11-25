# Шпаргалка для ручного запуска Polling-Based CDC Pipeline по шагам

Используйте эту шпаргалку для презентации - копируйте команды и запускайте их по отдельности.

---

## STEP 0: Очистка raw_store и DDS

```batch
if exist raw_store rmdir /s /q raw_store 2>nul
if exist DDS rmdir /s /q DDS 2>nul
if exist raw_store\_checkpoints rmdir /s /q raw_store\_checkpoints 2>nul
```

---

## STEP 1: Настройка контейнеров (остановка/создание/запуск)

### Проверка текущих контейнеров:
```batch
docker ps -a --filter "name=polling-cdc-"
```

### Остановка и удаление всех контейнеров:
```batch
docker-compose down -v
```

### Проверка наличия образа Spark (опционально):
```batch
docker images cdc-spark:latest
```

### Сборка образа Spark (только если его нет):
```batch
docker-compose build spark
```

### Запуск всех контейнеров:
```batch
docker-compose up -d
```

### Ожидание готовности сервисов:
```batch
docker exec polling-cdc-postgres pg_isready -U postgres
docker exec polling-cdc-kafka kafka-broker-api-versions --bootstrap-server kafka:29092
```

---

## STEP 2: Проверка, что сервисы запущены

```batch
docker ps --filter "name=polling-cdc-"
docker exec polling-cdc-postgres pg_isready -U postgres >nul 2>&1
docker exec polling-cdc-kafka kafka-broker-api-versions --bootstrap-server kafka:29092 >nul 2>&1
```

---

## STEP 3: Проверка начальных данных в БД

```batch
docker exec polling-cdc-postgres psql -U postgres -d testdb -c "SELECT * FROM users;"
```

---

## STEP 4: Проверка топиков Kafka (должны быть пустыми)

```batch
docker exec polling-cdc-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

---

## STEP 5: Запуск Polling Producer для обработки начальных данных

Producer проверяет БД один раз, находит записи с `updated_at > last_polled`, отправляет в Kafka и завершается.

```batch
docker exec polling-cdc-producer python polling_producer.py
```

---

## STEP 6: Проверка данных в топике Kafka

```batch
ping 127.0.0.1 -n 3 >nul 2>&1
docker exec polling-cdc-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic polling-cdc.users --from-beginning --max-messages 10 --timeout-ms 5000 2>nul
```

---

## STEP 7: Spark - Чтение из Kafka и сохранение в raw_store

```batch
docker exec polling-cdc-spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3 --driver-java-options "-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties -Dorg.apache.ivy.core.log.LogOptions=QUIET" --conf "spark.ui.showConsoleProgress=false" /app/spark_read_kafka.py 2>nul
```

---

## STEP 8: Spark - Обработка raw_store и создание Delta таблиц

```batch
docker exec polling-cdc-spark /opt/spark/bin/spark-submit --packages io.delta:delta-core_2.12:2.1.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --driver-java-options "-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties -Dorg.apache.ivy.core.log.LogOptions=QUIET" --conf "spark.ui.showConsoleProgress=false" /app/spark_process_raw.py 2>nul
```

---

## STEP 9: Добавление новых данных и повтор цикла

### Добавление новых данных:
```batch
docker exec polling-cdc-postgres psql -U postgres -d testdb -c "INSERT INTO users (ID, name, email, age) VALUES (1003, 'New User', 'newuser@example.com', 28);"
docker exec polling-cdc-postgres psql -U postgres -d testdb -c "UPDATE users SET email = 'john.updated@example.com', age = 31, updated_at = CURRENT_TIMESTAMP WHERE ID = 1001;"
docker exec polling-cdc-postgres psql -U postgres -d testdb -c "DELETE FROM users WHERE ID = 1002;"
```

**Важно:** При UPDATE нужно явно обновлять `updated_at = CURRENT_TIMESTAMP`, иначе polling не обнаружит изменение.

### Проверка изменений в БД:
```batch
timeout /t 5 /nobreak >nul
docker exec polling-cdc-postgres psql -U postgres -d testdb -c "SELECT * FROM users;"
```

### Запуск Polling Producer для обработки новых изменений:
```batch
docker exec polling-cdc-producer python polling_producer.py
```

Producer проверяет БД снова, находит только записи с `updated_at > last_polled` (т.е. новые изменения), отправляет в Kafka и завершается.

### Повтор STEP 7 (чтение из Kafka):
```batch
docker exec polling-cdc-spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3 --driver-java-options "-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties -Dorg.apache.ivy.core.log.LogOptions=QUIET" --conf "spark.ui.showConsoleProgress=false" /app/spark_read_kafka.py 2>nul
```

### Повтор STEP 8 (обработка и Delta таблицы):
```batch
docker exec polling-cdc-spark /opt/spark/bin/spark-submit --packages io.delta:delta-core_2.12:2.1.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --driver-java-options "-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties -Dorg.apache.ivy.core.log.LogOptions=QUIET" --conf "spark.ui.showConsoleProgress=false" /app/spark_process_raw.py 2>nul
```

---

## Полезные команды для отладки

### Просмотр логов контейнеров:
```batch
docker logs polling-cdc-postgres
docker logs polling-cdc-kafka
docker logs polling-cdc-producer
docker logs polling-cdc-spark
```

### Остановка всех контейнеров:
```batch
docker-compose down
```

### Остановка и удаление всех контейнеров с данными:
```batch
docker-compose down -v
```

### Просмотр всех топиков Kafka:
```batch
docker exec polling-cdc-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### Просмотр данных в топике (в реальном времени):
```batch
docker exec polling-cdc-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic polling-cdc.users --from-beginning
```

### Проверка данных в БД с сортировкой по updated_at:
```batch
docker exec polling-cdc-postgres psql -U postgres -d testdb -c "SELECT * FROM users ORDER BY updated_at DESC;"
```

### Проверка файла last_polled_timestamp (внутри контейнера producer):
```batch
docker exec polling-cdc-producer cat /app/last_polled_timestamp.txt
```

