#!/bin/bash
echo STEP 0: CLEAN RAW_STORE AND DDS
rm -rf raw_store 2>/dev/null
rm -rf DDS 2>/dev/null
rm -rf raw_store/_checkpoints 2>/dev/null

echo STEP 1: CHECK AND CLEAN/CREATE CONTAINERS
docker ps -a --filter "name=cdc-"
docker-compose down -v

# Собираем образ только если его нет
if ! docker images --format "{{.Repository}}:{{.Tag}}" | grep -q "cdc-spark:latest"; then
    echo "Building Spark image (first time only, downloading base image ~500MB)..."
    docker-compose build spark
else
    echo "Spark image already exists, skipping build..."
fi

docker-compose up -d
sleep 30

echo STEP 2: CHECK SERVICES ARE UP
docker ps
docker exec cdc-postgres pg_isready -U postgres
docker exec cdc-kafka kafka-broker-api-versions --bootstrap-server localhost:9092
curl http://localhost:8083/connectors

echo STEP 3: CHECK NO DATA IN DATABASE
docker exec cdc-postgres psql -U postgres -d testdb -c "\dt"

echo STEP 4: ADD DATA TO DATABASE
docker exec -i cdc-postgres psql -U postgres -d testdb < sql/init-db.sql

echo STEP 5: CHECK DATA ADDED
docker exec cdc-postgres psql -U postgres -d testdb -c "\dt"
docker exec cdc-postgres psql -U postgres -d testdb -c "SELECT * FROM users;"

echo STEP 6: CREATE PUBLICATION AND REPLICATION SLOT
docker exec cdc-postgres psql -U postgres -d testdb -c "CREATE PUBLICATION debezium_publication FOR TABLE users;"
docker exec cdc-postgres psql -U postgres -d testdb -c "SELECT * FROM pg_publication WHERE pubname = 'debezium_publication';"

echo STEP 7: CHECK KAFKA TOPICS (SHOULD BE EMPTY)
docker exec cdc-kafka kafka-topics --bootstrap-server localhost:9092 --list

echo STEP 8: START DEBEZIUM
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @debezium-connector-config.json
sleep 10

echo STEP 9: CHECK CONFIG APPLIED
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/postgres-connector/status
curl http://localhost:8083/connectors/postgres-connector/config

echo STEP 10: CHECK KAFKA TOPICS (TOPIC SHOULD APPEAR)
docker exec cdc-kafka kafka-topics --bootstrap-server localhost:9092 --list

echo STEP 11: CHECK DATA IN KAFKA TOPIC
docker exec cdc-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic postgres-server.public.users --from-beginning --max-messages 10
curl http://localhost:8081/subjects

echo STEP 12: RUN SPARK - READ FROM KAFKA AND SAVE TO RAW_STORE
docker exec cdc-spark python /app/spark_read_kafka.py

echo STEP 13: RUN SPARK - PROCESS RAW_STORE AND CREATE DELTA TABLES
docker exec cdc-spark python /app/spark_process_raw.py

echo STEP 14: ADD NEW DATA AND REPEAT CYCLE
docker exec cdc-postgres psql -U postgres -d testdb -c "INSERT INTO users (ID, name, email, age) VALUES (1003, 'New User', 'newuser@example.com', 28);"
docker exec cdc-postgres psql -U postgres -d testdb -c "UPDATE users SET email = 'john.updated@example.com', age = 31 WHERE ID = 1001;"
docker exec cdc-postgres psql -U postgres -d testdb -c "DELETE FROM users WHERE ID = 1002;"
sleep 5
docker exec cdc-postgres psql -U postgres -d testdb -c "SELECT * FROM users;"
docker exec cdc-spark python /app/spark_read_kafka.py
docker exec cdc-spark python /app/spark_process_raw.py

echo DONE

