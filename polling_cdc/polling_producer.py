"""
CDC Producer для Polling-Based подхода
Периодически опрашивает таблицу users по полю updated_at и отправляет изменения в Kafka
"""
import os
import time
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging
from datetime import datetime, timezone, timedelta

logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Только наш logger на INFO, остальные на WARNING
# Понижаем уровень логирования для kafka-python, чтобы убрать verbose логи
logging.getLogger('kafka').setLevel(logging.WARNING)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "testdb")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "polling-cdc.users")

LAST_POLLED_FILE = "/tmp/last_polled_timestamp.txt"

def get_db_connection():
    """Создает подключение к PostgreSQL"""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB
    )

def get_kafka_producer():
    """Создает Kafka producer"""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: json.dumps(k).encode('utf-8') if k else None
    )

def get_last_polled_timestamp():
    """Читает последнее время опроса из файла, если файла нет - возвращает время час назад"""
    try:
        if os.path.exists(LAST_POLLED_FILE):
            with open(LAST_POLLED_FILE, 'r') as f:
                timestamp_str = f.read().strip()
                if timestamp_str:
                    return datetime.fromisoformat(timestamp_str)
    except Exception as e:
        logger.warning(f"Could not read last polled timestamp: {e}")
    return datetime.now(timezone.utc).replace(microsecond=0) - timedelta(hours=1)

def save_last_polled_timestamp(timestamp):
    """Сохраняет последнее время опроса в файл"""
    try:
        with open(LAST_POLLED_FILE, 'w') as f:
            f.write(timestamp.isoformat())
    except Exception as e:
        logger.error(f"Could not save last polled timestamp: {e}")

def format_message(record, operation='u'):
    """Форматирует запись в JSON для отправки в Kafka"""
    updated_at = record['updated_at']
    if isinstance(updated_at, datetime):
        ts_ms = int(updated_at.timestamp() * 1000000)
    else:
        try:
            dt = datetime.fromisoformat(str(updated_at).replace('Z', '+00:00'))
            ts_ms = int(dt.timestamp() * 1000000)
        except:
            ts_ms = int(time.time() * 1000000)
    
    created_at = record.get('created_at')
    if isinstance(created_at, datetime):
        created_at_ts = int(created_at.timestamp() * 1000000)
    elif created_at:
        try:
            dt = datetime.fromisoformat(str(created_at).replace('Z', '+00:00'))
            created_at_ts = int(dt.timestamp() * 1000000)
        except:
            created_at_ts = ts_ms
    else:
        created_at_ts = ts_ms
    
    message = {
        "id": record['id'],
        "__op": operation,
        "__source_ts_ms": ts_ms,
        "__deleted": "false" if operation != 'd' else "true",
        "name": record.get('name'),
        "email": record.get('email'),
        "age": record.get('age'),
        "created_at": created_at_ts
    }
    
    key = {"id": record['id']}
    return key, message

def poll_changes(conn, last_polled):
    """Опрашивает таблицу users, ищет записи с updated_at > last_polled, возвращает список изменений"""
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    query = """
        SELECT 
            ID as id,
            name,
            email,
            age,
            created_at,
            updated_at
        FROM users
        WHERE updated_at > %s
        ORDER BY updated_at, ID
    """
    
    cursor.execute(query, (last_polled,))
    records = cursor.fetchall()
    cursor.close()
    
    return records

def main():
    """Одноразовая задача: проверяет БД один раз, отправляет изменения в Kafka, завершает работу"""
    logger.info("Starting CDC Producer for Polling-Based approach (one-time job)...")
    logger.info(f"Connecting to PostgreSQL at {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    logger.info(f"Sending messages to Kafka at {KAFKA_BOOTSTRAP_SERVERS} topic {KAFKA_TOPIC}")

    conn = None
    producer = None
    try:
        conn = get_db_connection()
        producer = get_kafka_producer()
        logger.info("Connected to DB and Kafka Producer initialized.")

        last_polled = get_last_polled_timestamp()
        logger.info(f"Checking for changes since: {last_polled}")

        changes = poll_changes(conn, last_polled)
        
        if changes:
            logger.info(f"Found {len(changes)} changed records since {last_polled}")
            current_max_timestamp = last_polled
            processed_count = 0
            
            for record in changes:
                try:
                    if record['created_at'] and record['updated_at']:
                        created_at = record['created_at']
                        updated_at = record['updated_at']
                        
                        if isinstance(created_at, datetime) and isinstance(updated_at, datetime):
                            if created_at.tzinfo is None:
                                created_at = created_at.replace(tzinfo=timezone.utc)
                            if updated_at.tzinfo is None:
                                updated_at = updated_at.replace(tzinfo=timezone.utc)
                            diff = abs((updated_at - created_at).total_seconds())
                            operation = 'c' if diff < 1.0 else 'u'
                        else:
                            operation = 'u'
                    else:
                        operation = 'u'
                    
                    key, message = format_message(record, operation)
                    future = producer.send(KAFKA_TOPIC, key=key, value=message)
                    record_metadata = future.get(timeout=10)
                    logger.info(f"Sent message for ID {record['id']} to topic {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")
                    processed_count += 1
                    
                    if record['updated_at']:
                        if isinstance(record['updated_at'], datetime):
                            updated_at = record['updated_at']
                            if updated_at.tzinfo is None:
                                updated_at = updated_at.replace(tzinfo=timezone.utc)
                            if current_max_timestamp < updated_at:
                                current_max_timestamp = updated_at
                        else:
                            try:
                                dt = datetime.fromisoformat(str(record['updated_at']).replace('Z', '+00:00'))
                                if dt.tzinfo is None:
                                    dt = dt.replace(tzinfo=timezone.utc)
                                if current_max_timestamp < dt:
                                    current_max_timestamp = dt
                            except:
                                pass
                    
                except KafkaError as ke:
                    logger.error(f"Failed to send message for ID {record['id']}: {ke}")
                except Exception as e:
                    logger.error(f"Error processing record ID {record['id']}: {e}")
            
            if processed_count > 0:
                if current_max_timestamp > last_polled:
                    last_polled = current_max_timestamp
                else:
                    last_polled = current_max_timestamp + timedelta(microseconds=1)
                save_last_polled_timestamp(last_polled)
                logger.info(f"Successfully processed {processed_count} records, updated last polled timestamp to: {last_polled}")
            else:
                logger.warning(f"Found {len(changes)} records but none were processed successfully")
        else:
            logger.info(f"No changes found since {last_polled}")
        
        logger.info("Job completed, exiting")

    except psycopg2.Error as e:
        logger.critical(f"PostgreSQL connection error: {e}")
    except KafkaError as e:
        logger.critical(f"Kafka Producer error: {e}")
    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}")
    finally:
        if producer:
            producer.close()
        if conn:
            conn.close()
        logger.info("CDC Producer stopped.")

if __name__ == "__main__":
    main()
