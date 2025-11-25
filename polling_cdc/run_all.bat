@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

echo ========================================
echo   Polling-Based CDC Pipeline
echo ========================================
echo.

echo STEP 0: CLEAN RAW_STORE AND DDS
if exist raw_store rmdir /s /q raw_store 2>nul
if exist DDS rmdir /s /q DDS 2>nul
if exist raw_store\_checkpoints rmdir /s /q raw_store\_checkpoints 2>nul
echo.

echo STEP 1: STOP AND REMOVE EXISTING CONTAINERS
echo Stopping and removing ALL CDC-related containers...
for /f "tokens=*" %%i in ('docker ps -a -q --filter "name=cdc-"') do (docker rm -f %%i >nul 2>&1)
for /f "tokens=*" %%i in ('docker ps -a -q --filter "name=trigger-cdc-"') do (docker rm -f %%i >nul 2>&1)
for /f "tokens=*" %%i in ('docker ps -a -q --filter "name=polling-cdc-"') do (docker rm -f %%i >nul 2>&1)
docker-compose down -v
if errorlevel 1 goto :error
echo.

echo STEP 2: BUILD AND START CONTAINERS
docker images cdc-spark:latest 2>nul | findstr "cdc-spark" >nul
if errorlevel 1 (
    echo Building Spark image (first time only, downloading base image ~500MB)
    docker-compose build spark
    if errorlevel 1 goto :error
) else (
    echo Spark image already exists, skipping build
)

docker-compose up -d
if errorlevel 1 goto :error

echo Waiting for services to be ready...
set /a retries=0
:wait_postgres
docker exec polling-cdc-postgres sh -c "pg_isready -U postgres < /dev/null" >nul 2>&1
if errorlevel 1 (
    set /a retries+=1
    if !retries! geq 30 (
        echo PostgreSQL is not ready after 60 seconds
        goto :error
    )
    timeout /t 2 /nobreak >nul 2>&1
    goto :wait_postgres
)
set /a retries=0
:wait_kafka
docker exec polling-cdc-kafka sh -c "kafka-broker-api-versions --bootstrap-server kafka:29092 < /dev/null" >nul 2>&1
if errorlevel 1 (
    set /a retries+=1
    if !retries! geq 60 (
        echo Kafka is not ready after 120 seconds
        goto :error
    )
    timeout /t 2 /nobreak >nul 2>&1
    goto :wait_kafka
)
timeout /t 10 /nobreak >nul
echo.

echo STEP 3: CHECK SERVICES ARE UP
docker ps --filter "name=polling-cdc-"
echo.

echo STEP 4: CHECK INITIAL DATA IN DATABASE
docker exec polling-cdc-postgres psql -U postgres -d testdb -c "SELECT * FROM users;"
echo.

echo STEP 5: CHECK KAFKA TOPICS (SHOULD BE EMPTY INITIALLY)
set /a kafka_retries=0
:kafka_ready_check
docker exec polling-cdc-kafka sh -c "kafka-topics --bootstrap-server kafka:29092 --list < /dev/null" >nul 2>&1
if errorlevel 1 (
    set /a kafka_retries+=1
    if !kafka_retries! geq 30 (
        echo Kafka is not ready after 60 seconds
        goto :error
    )
    timeout /t 2 /nobreak >nul
    goto :kafka_ready_check
)
docker exec polling-cdc-kafka sh -c "kafka-topics --bootstrap-server kafka:29092 --list < /dev/null"
if errorlevel 1 (
    echo (No topics found - this is expected before producer runs)
) else (
    echo (Only system topics may be present)
)
echo.

echo STEP 6: RUN POLLING PRODUCER TO PROCESS INITIAL DATA
docker exec polling-cdc-producer python polling_producer.py >nul 2>&1
timeout /t 1 /nobreak >nul 2>&1
echo.

echo STEP 7: CHECK KAFKA TOPICS (Topic should appear after producer runs)
docker exec polling-cdc-kafka sh -c "kafka-topics --bootstrap-server kafka:29092 --list < /dev/null" 2>nul
echo.

echo STEP 8: CHECK DATA IN KAFKA TOPIC (Producer should have sent data)
timeout /t 2 /nobreak >nul 2>&1
echo Reading messages from topic (should show 2 messages):
docker exec polling-cdc-kafka sh -c "kafka-console-consumer --bootstrap-server kafka:29092 --topic polling-cdc.users --from-beginning --max-messages 2 --timeout-ms 10000 < /dev/null 2>/dev/null || true"
echo.

echo STEP 9: RUN SPARK - READ FROM KAFKA AND SAVE TO RAW_STORE
docker exec polling-cdc-spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3 --driver-java-options "-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties -Dorg.apache.ivy.core.log.LogOptions=QUIET" --conf "spark.ui.showConsoleProgress=false" /app/spark_read_kafka.py 2>nul
if errorlevel 1 goto :error
echo.

echo STEP 10: RUN SPARK - PROCESS RAW_STORE AND CREATE DELTA TABLES
docker exec polling-cdc-spark /opt/spark/bin/spark-submit --packages io.delta:delta-core_2.12:2.1.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --driver-java-options "-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties -Dorg.apache.ivy.core.log.LogOptions=QUIET" --conf "spark.ui.showConsoleProgress=false" /app/spark_process_raw.py 2>nul
if errorlevel 1 goto :error
echo.

echo STEP 11: ADD NEW DATA (INSERT/UPDATE/DELETE) AND REPEAT CYCLE
echo Note: In polling-based CDC, updated_at must be updated manually by the application
docker exec polling-cdc-postgres psql -U postgres -d testdb -c "INSERT INTO users (ID, name, email, age) VALUES (1003, 'New User', 'newuser@example.com', 28);"
if errorlevel 1 goto :error
docker exec polling-cdc-postgres psql -U postgres -d testdb -c "UPDATE users SET email = 'john.updated@example.com', age = 31, updated_at = CURRENT_TIMESTAMP WHERE ID = 1001;"
if errorlevel 1 goto :error
docker exec polling-cdc-postgres psql -U postgres -d testdb -c "DELETE FROM users WHERE ID = 1002;"
if errorlevel 1 goto :error
timeout /t 5 /nobreak >nul
docker exec polling-cdc-postgres psql -U postgres -d testdb -c "SELECT * FROM users;"
if errorlevel 1 goto :error
echo.

echo STEP 12: RUN POLLING PRODUCER TO PROCESS NEW CHANGES
docker exec polling-cdc-producer python polling_producer.py >nul 2>&1
timeout /t 1 /nobreak >nul 2>&1
echo.

echo STEP 13: CHECK DATA IN KAFKA TOPIC (New changes should appear)
timeout /t 2 /nobreak >nul 2>&1
echo Reading all messages from topic (should show 4 messages total):
docker exec polling-cdc-kafka sh -c "kafka-console-consumer --bootstrap-server kafka:29092 --topic polling-cdc.users --from-beginning --max-messages 10 --timeout-ms 10000 < /dev/null 2>/dev/null || true"
echo.

echo STEP 14: RUN SPARK - READ NEW DATA FROM KAFKA
docker exec polling-cdc-spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3 --driver-java-options "-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties -Dorg.apache.ivy.core.log.LogOptions=QUIET" --conf "spark.ui.showConsoleProgress=false" /app/spark_read_kafka.py 2>nul
if errorlevel 1 goto :error
echo.

echo STEP 15: RUN SPARK - PROCESS NEW DATA AND UPDATE DELTA TABLES
docker exec polling-cdc-spark /opt/spark/bin/spark-submit --packages io.delta:delta-core_2.12:2.1.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --driver-java-options "-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties -Dorg.apache.ivy.core.log.LogOptions=QUIET" --conf "spark.ui.showConsoleProgress=false" /app/spark_process_raw.py 2>nul
if errorlevel 1 goto :error
echo.

echo DONE
goto :end

:error
echo.
echo ERROR: Command failed. Check the output above.
exit /b 1

:end
endlocal

