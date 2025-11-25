# Шпаргалка для ручного запуска Trigger-Based CDC Pipeline по шагам

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
docker ps -a --filter "name=trigger-cdc-"
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
docker exec trigger-cdc-postgres pg_isready -U postgres
docker exec trigger-cdc-kafka kafka-broker-api-versions --bootstrap-server kafka:29092
```

---

## STEP 2: Проверка, что сервисы запущены

```batch
docker ps --filter "name=trigger-cdc-"
docker exec trigger-cdc-postgres pg_isready -U postgres >nul 2>&1
docker exec trigger-cdc-kafka kafka-broker-api-versions --bootstrap-server kafka:29092 >nul 2>&1
```

---

## STEP 3: Проверка начальных данных в БД

```batch
docker exec trigger-cdc-postgres psql -U postgres -d testdb -c "SELECT * FROM users;"
```

---

## STEP 4: Проверка CDC log таблицы

```batch
docker exec trigger-cdc-postgres psql -U postgres -d testdb -c "SELECT COUNT(*) as unprocessed_changes FROM cdc_changes_log WHERE processed = FALSE;"
```

---

## STEP 5: Ожидание обработки начальных данных producer'ом

CDC producer работает в фоне и автоматически обрабатывает записи из `cdc_changes_log`.

```batch
timeout /t 10 /nobreak >nul
```

---

## STEP 6: Проверка топиков Kafka

```batch
docker exec trigger-cdc-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

---

## STEP 7: Проверка данных в топике Kafka

```batch
docker exec trigger-cdc-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic trigger-cdc.users --from-beginning --max-messages 10 --timeout-ms 5000 2>nul
```

---

## STEP 8: Spark - Чтение из Kafka и сохранение в raw_store

```batch
docker exec trigger-cdc-spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3 --driver-java-options "-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties -Dorg.apache.ivy.core.log.LogOptions=QUIET" --conf "spark.ui.showConsoleProgress=false" /app/spark_read_kafka.py 2>nul
```

---

## STEP 9: Spark - Обработка raw_store и создание Delta таблиц

```batch
docker exec trigger-cdc-spark /opt/spark/bin/spark-submit --packages io.delta:delta-core_2.12:2.1.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --driver-java-options "-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties -Dorg.apache.ivy.core.log.LogOptions=QUIET" --conf "spark.ui.showConsoleProgress=false" /app/spark_process_raw.py 2>nul
```

---

## STEP 10: Добавление новых данных и повтор цикла

### Добавление новых данных:
```batch
docker exec trigger-cdc-postgres psql -U postgres -d testdb -c "INSERT INTO users (ID, name, email, age) VALUES (1003, 'New User', 'newuser@example.com', 28);"
docker exec trigger-cdc-postgres psql -U postgres -d testdb -c "UPDATE users SET email = 'john.updated@example.com', age = 31 WHERE ID = 1001;"
docker exec trigger-cdc-postgres psql -U postgres -d testdb -c "DELETE FROM users WHERE ID = 1002;"
```

### Проверка изменений в БД:
```batch
timeout /t 5 /nobreak >nul
docker exec trigger-cdc-postgres psql -U postgres -d testdb -c "SELECT * FROM users;"
```

### Проверка записей в CDC log:
```batch
docker exec trigger-cdc-postgres psql -U postgres -d testdb -c "SELECT * FROM cdc_changes_log WHERE processed = FALSE;"
```

### Ожидание обработки producer'ом:
```batch
timeout /t 10 /nobreak >nul
```

### Повтор STEP 8 (чтение из Kafka):
```batch
docker exec trigger-cdc-spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3 --driver-java-options "-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties -Dorg.apache.ivy.core.log.LogOptions=QUIET" --conf "spark.ui.showConsoleProgress=false" /app/spark_read_kafka.py 2>nul
```

### Повтор STEP 9 (обработка и Delta таблицы):
```batch
docker exec trigger-cdc-spark /opt/spark/bin/spark-submit --packages io.delta:delta-core_2.12:2.1.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --driver-java-options "-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties -Dorg.apache.ivy.core.log.LogOptions=QUIET" --conf "spark.ui.showConsoleProgress=false" /app/spark_process_raw.py 2>nul
```

---

## Полезные команды для отладки

### Просмотр логов контейнеров:
```batch
docker logs trigger-cdc-postgres
docker logs trigger-cdc-kafka
docker logs trigger-cdc-producer
docker logs trigger-cdc-spark
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
docker exec trigger-cdc-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### Просмотр данных в топике (в реальном времени):
```batch
docker exec trigger-cdc-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic trigger-cdc.users --from-beginning
```

### Проверка CDC log таблицы:
```batch
docker exec trigger-cdc-postgres psql -U postgres -d testdb -c "SELECT * FROM cdc_changes_log ORDER BY change_id DESC LIMIT 10;"
```

### Проверка обработанных записей:
```batch
docker exec trigger-cdc-postgres psql -U postgres -d testdb -c "SELECT COUNT(*) as processed_count FROM cdc_changes_log WHERE processed = TRUE;"
```

