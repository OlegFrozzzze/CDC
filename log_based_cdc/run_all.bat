@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

echo STEP 0: CLEAN RAW_STORE AND DDS
if exist raw_store rmdir /s /q raw_store 2>nul
if exist DDS rmdir /s /q DDS 2>nul
if exist raw_store\_checkpoints rmdir /s /q raw_store\_checkpoints 2>nul
echo.

echo STEP 1: STOP AND REMOVE EXISTING CONTAINERS
docker stop cdc-postgres cdc-kafka cdc-zookeeper cdc-debezium cdc-schema-registry cdc-spark cdc-kafka-ui 2>nul
docker rm -f cdc-postgres cdc-kafka cdc-zookeeper cdc-debezium cdc-schema-registry cdc-spark cdc-kafka-ui 2>nul
docker stop trigger-cdc-postgres trigger-cdc-kafka trigger-cdc-zookeeper trigger-cdc-producer trigger-cdc-spark trigger-cdc-kafka-ui 2>nul
docker rm -f trigger-cdc-postgres trigger-cdc-kafka trigger-cdc-zookeeper trigger-cdc-producer trigger-cdc-spark trigger-cdc-kafka-ui 2>nul
docker stop polling-cdc-postgres polling-cdc-kafka polling-cdc-zookeeper polling-cdc-producer polling-cdc-spark polling-cdc-kafka-ui 2>nul
docker rm -f polling-cdc-postgres polling-cdc-kafka polling-cdc-zookeeper polling-cdc-producer polling-cdc-spark polling-cdc-kafka-ui 2>nul
echo Stopping and removing ALL CDC-related containers...
for /f "tokens=*" %%i in ('docker ps -a -q --filter "name=cdc-" 2^>nul') do (docker rm -f %%i >nul 2>&1)
for /f "tokens=*" %%i in ('docker ps -a -q --filter "name=trigger-cdc-" 2^>nul') do (docker rm -f %%i >nul 2>&1)
for /f "tokens=*" %%i in ('docker ps -a -q --filter "name=polling-cdc-" 2^>nul') do (docker rm -f %%i >nul 2>&1)

docker-compose down -v
if errorlevel 1 goto :error

docker images cdc-spark:latest 2>nul | findstr "cdc-spark" >nul
if errorlevel 1 goto :build_image
echo Spark image already exists, skipping build
goto :skip_build

:build_image
echo Building Spark image (first time only, downloading base image ~500MB)...
echo Step 1: Pulling base image apache/spark-py:v3.2.3...
docker pull apache/spark-py:v3.2.3
if errorlevel 1 (
    echo.
    echo ERROR: Failed to pull base image from Docker Hub.
    echo This might be due to:
    echo   - Network connectivity issues
    echo   - Firewall blocking Docker Hub access
    echo   - Docker daemon not running properly
    echo.
    echo Please check your internet connection and Docker settings.
    echo You can try manually: docker pull apache/spark-py:v3.2.3
    goto :error
)
echo Step 2: Building custom Spark image...
docker-compose build spark
if errorlevel 1 goto :error

:skip_build
docker-compose up -d
if errorlevel 1 goto :error

echo Waiting for services to be ready...
set /a retries=0
:wait_postgres
docker exec cdc-postgres sh -c "pg_isready -U postgres < /dev/null" >nul 2>&1
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
docker exec cdc-kafka sh -c "kafka-broker-api-versions --bootstrap-server localhost:9092 < /dev/null" >nul 2>&1
if errorlevel 1 (
    set /a retries+=1
    if !retries! geq 30 (
        echo Kafka is not ready after 60 seconds
        goto :error
    )
    timeout /t 2 /nobreak >nul 2>&1
    goto :wait_kafka
)
set /a retries=0
:wait_debezium
curl.exe -s http://localhost:8083/connectors >nul 2>&1
if errorlevel 1 (
    set /a retries+=1
    if !retries! geq 30 (
        echo Debezium is not ready after 60 seconds
        goto :error
    )
    timeout /t 2 /nobreak >nul 2>&1
    goto :wait_debezium
)
timeout /t 10 /nobreak >nul
echo.

echo STEP 2: BUILD AND START CONTAINERS
echo STEP 3: CHECK SERVICES ARE UP
docker ps --filter "name=cdc-"
docker exec cdc-postgres sh -c "pg_isready -U postgres < /dev/null" >nul 2>&1
if errorlevel 1 goto :error
docker exec cdc-kafka sh -c "kafka-broker-api-versions --bootstrap-server localhost:9092 < /dev/null" >nul 2>&1
if errorlevel 1 goto :error
curl.exe -s http://localhost:8083/connectors >nul 2>&1
if errorlevel 1 goto :error
echo.

echo STEP 4: CHECK NO DATA IN DATABASE
docker exec cdc-postgres psql -U postgres -d testdb -c "\dt"
echo.

echo STEP 5: ADD DATA TO DATABASE
powershell -Command "Get-Content sql\init-db.sql | docker exec -i cdc-postgres psql -U postgres -d testdb"
if errorlevel 1 goto :error
echo.

echo STEP 6: CHECK DATA ADDED
docker exec cdc-postgres psql -U postgres -d testdb -c "\dt"
if errorlevel 1 goto :error
docker exec cdc-postgres psql -U postgres -d testdb -c "SELECT * FROM users;"
if errorlevel 1 goto :error
echo.

echo STEP 7: CREATE PUBLICATION AND REPLICATION SLOT
docker exec cdc-postgres psql -U postgres -d testdb -c "DROP PUBLICATION IF EXISTS debezium_publication;"
docker exec cdc-postgres psql -U postgres -d testdb -c "CREATE PUBLICATION debezium_publication FOR TABLE users;"
docker exec cdc-postgres psql -U postgres -d testdb -c "SELECT * FROM pg_publication WHERE pubname = 'debezium_publication';"
echo.

echo STEP 8: CHECK KAFKA TOPICS (SHOULD BE EMPTY)
docker exec cdc-kafka sh -c "kafka-topics --bootstrap-server localhost:9092 --list < /dev/null" 2>nul
echo.

echo STEP 9: START DEBEZIUM
curl.exe -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @debezium-connector-config.json
timeout /t 10 /nobreak >nul
echo.

echo STEP 10: CHECK CONFIG APPLIED
curl.exe http://localhost:8083/connectors
curl.exe http://localhost:8083/connectors/postgres-connector/status
curl.exe http://localhost:8083/connectors/postgres-connector/config
echo.

echo STEP 11: CHECK KAFKA TOPICS (TOPIC SHOULD APPEAR)
docker exec cdc-kafka sh -c "kafka-topics --bootstrap-server localhost:9092 --list < /dev/null" 2>nul
echo.

echo STEP 12: CHECK DATA IN KAFKA TOPIC
ping 127.0.0.1 -n 3 >nul 2>&1
docker exec cdc-kafka sh -c "kafka-console-consumer --bootstrap-server localhost:9092 --topic postgres-server.public.users --from-beginning --max-messages 10 --timeout-ms 5000 < /dev/null" 2>nul
echo Checking Schema Registry subjects (empty array is normal for JSON format):
curl.exe -s http://localhost:8081/subjects
echo.
echo Note: Empty array means JSON format is used (not Avro), which is expected.
echo.

echo STEP 13: RUN SPARK - READ FROM KAFKA AND SAVE TO RAW_STORE
docker exec cdc-spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3 --driver-java-options "-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties -Dorg.apache.ivy.core.log.LogOptions=QUIET" --conf "spark.ui.showConsoleProgress=false" /app/spark_read_kafka.py 2>nul
if errorlevel 1 goto :error
echo.

echo STEP 14: RUN SPARK - PROCESS RAW_STORE AND CREATE DELTA TABLES
docker exec cdc-spark /opt/spark/bin/spark-submit --packages io.delta:delta-core_2.12:2.1.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --driver-java-options "-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties -Dorg.apache.ivy.core.log.LogOptions=QUIET" --conf "spark.ui.showConsoleProgress=false" /app/spark_process_raw.py 2>nul
if errorlevel 1 goto :error
echo.

echo STEP 15: ADD NEW DATA AND REPEAT CYCLE
docker exec cdc-postgres psql -U postgres -d testdb -c "INSERT INTO users (ID, name, email, age) VALUES (1003, 'New User', 'newuser@example.com', 28);"
if errorlevel 1 goto :error
docker exec cdc-postgres psql -U postgres -d testdb -c "UPDATE users SET email = 'john.updated@example.com', age = 31 WHERE ID = 1001;"
if errorlevel 1 goto :error
docker exec cdc-postgres psql -U postgres -d testdb -c "DELETE FROM users WHERE ID = 1002;"
if errorlevel 1 goto :error
timeout /t 5 /nobreak >nul
docker exec cdc-postgres psql -U postgres -d testdb -c "SELECT * FROM users;"
if errorlevel 1 goto :error
echo.

docker exec cdc-spark /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3 --driver-java-options "-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties -Dorg.apache.ivy.core.log.LogOptions=QUIET" --conf "spark.ui.showConsoleProgress=false" /app/spark_read_kafka.py 2>nul
if errorlevel 1 goto :error
echo.
docker exec cdc-spark /opt/spark/bin/spark-submit --packages io.delta:delta-core_2.12:2.1.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --driver-java-options "-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties -Dorg.apache.ivy.core.log.LogOptions=QUIET" --conf "spark.ui.showConsoleProgress=false" /app/spark_process_raw.py 2>nul
if errorlevel 1 goto :error
echo.

echo DONE
goto :end

:error
echo ERROR: Command failed. Check the output above.
exit /b 1

:end
endlocal
