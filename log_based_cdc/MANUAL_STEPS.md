# Шпаргалка для ручного запуска CDC Pipeline по шагам

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
docker ps -a --filter "name=cdc-"
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

### Ожидание готовности сервисов (выполняется автоматически в run_all.bat):
```batch
docker exec cdc-postgres pg_isready -U postgres
docker exec cdc-kafka kafka-broker-api-versions --bootstrap-server localhost:9092
curl.exe -s http://localhost:8083/connectors
```

---

## STEP 2: Проверка, что сервисы запущены

```batch
docker ps
docker exec cdc-postgres pg_isready -U postgres >nul 2>&1
docker exec cdc-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >nul 2>&1
curl.exe -s http://localhost:8083/connectors >nul 2>&1
```

---

## STEP 3: Проверка, что в БД нет данных

```batch
docker exec cdc-postgres psql -U postgres -d testdb -c "\dt"
```

---

## STEP 4: Добавление начальных данных в БД

```batch
powershell -Command "Get-Content sql\init-db.sql | docker exec -i cdc-postgres psql -U postgres -d testdb"
```

---

## STEP 5: Проверка добавленных данных

```batch
docker exec cdc-postgres psql -U postgres -d testdb -c "\dt"
docker exec cdc-postgres psql -U postgres -d testdb -c "SELECT * FROM users;"
```

---

## STEP 6: Создание публикации и слота репликации

```batch
docker exec cdc-postgres psql -U postgres -d testdb -c "DROP PUBLICATION IF EXISTS debezium_publication;"
docker exec cdc-postgres psql -U postgres -d testdb -c "CREATE PUBLICATION debezium_publication FOR TABLE users;"
docker exec cdc-postgres psql -U postgres -d testdb -c "SELECT * FROM pg_publication WHERE pubname = 'debezium_publication';"
```

---

## STEP 7: Проверка топиков Kafka (должны быть пустыми)

```batch
docker exec cdc-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

---

## STEP 8: Запуск Debezium коннектора

```batch
curl.exe -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @debezium-connector-config.json
timeout /t 10 /nobreak >nul
```

---

## STEP 9: Проверка конфигурации Debezium

```batch
curl.exe http://localhost:8083/connectors
curl.exe http://localhost:8083/connectors/postgres-connector/status
curl.exe http://localhost:8083/connectors/postgres-connector/config
```

---

## STEP 10: Проверка топиков Kafka (топик должен появиться)

```batch
docker exec cdc-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

---

## STEP 11: Проверка данных в топике Kafka

```batch
ping 127.0.0.1 -n 3 >nul 2>&1
docker exec cdc-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic postgres-server.public.users --from-beginning --max-messages 10 --timeout-ms 5000 2>nul
curl.exe -s http://localhost:8081/subjects
```

---

## STEP 12: Spark - Чтение из Kafka и сохранение в raw_store

```batch
docker exec cdc-spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3 --driver-java-options "-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties -Dorg.apache.ivy.core.log.LogOptions=QUIET" --conf "spark.ui.showConsoleProgress=false" /app/spark_read_kafka.py 2>nul
```

---

## STEP 13: Spark - Обработка raw_store и создание Delta таблиц

```batch
docker exec cdc-spark /opt/spark/bin/spark-submit --packages io.delta:delta-core_2.12:2.1.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --driver-java-options "-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties -Dorg.apache.ivy.core.log.LogOptions=QUIET" --conf "spark.ui.showConsoleProgress=false" /app/spark_process_raw.py 2>nul
```

---

## STEP 14: Добавление новых данных и повтор цикла

### Добавление новых данных:
```batch
docker exec cdc-postgres psql -U postgres -d testdb -c "INSERT INTO users (ID, name, email, age) VALUES (1003, 'New User', 'newuser@example.com', 28);"
docker exec cdc-postgres psql -U postgres -d testdb -c "UPDATE users SET email = 'john.updated@example.com', age = 31 WHERE ID = 1001;"
docker exec cdc-postgres psql -U postgres -d testdb -c "DELETE FROM users WHERE ID = 1002;"
```

### Проверка изменений в БД:
```batch
timeout /t 5 /nobreak >nul
docker exec cdc-postgres psql -U postgres -d testdb -c "SELECT * FROM users;"
```

### Повтор STEP 12 (чтение из Kafka):
```batch
docker exec cdc-spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3 --driver-java-options "-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties -Dorg.apache.ivy.core.log.LogOptions=QUIET" --conf "spark.ui.showConsoleProgress=false" /app/spark_read_kafka.py 2>nul
```

### Повтор STEP 13 (обработка и Delta таблицы):
```batch
docker exec cdc-spark /opt/spark/bin/spark-submit --packages io.delta:delta-core_2.12:2.1.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --driver-java-options "-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties -Dorg.apache.ivy.core.log.LogOptions=QUIET" --conf "spark.ui.showConsoleProgress=false" /app/spark_process_raw.py 2>nul
```

---

## Полезные команды для отладки

### Просмотр логов контейнеров:
```batch
docker logs cdc-postgres
docker logs cdc-kafka
docker logs cdc-debezium
docker logs cdc-spark
```

### Остановка всех контейнеров:
```batch
docker-compose down
```

### Остановка и удаление всех контейнеров с данными:
```batch
docker-compose down -v
```

### Проверка состояния Debezium:
```batch
curl.exe http://localhost:8083/connectors/postgres-connector/status
```

### Просмотр всех топиков Kafka:
```batch
docker exec cdc-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### Просмотр данных в топике (в реальном времени):
```batch
docker exec cdc-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic postgres-server.public.users --from-beginning
```

### Остановка Debezium коннектора:
```batch
curl.exe -X DELETE http://localhost:8083/connectors/postgres-connector
```

