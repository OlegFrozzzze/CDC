"""
CDC Producer для Trigger-Based подхода
Читает изменения из таблицы cdc_changes_log и отправляет их в Kafka
"""
import os
import time
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
# Понижаем уровень логирования для kafka-python, чтобы убрать verbose логи
logging.getLogger('kafka').setLevel(logging.WARNING)

# Настройки подключения
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "testdb")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "trigger-cdc.users")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "2"))

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

def convert_operation(op):
    """Конвертирует операцию из триггера в формат Debezium"""
    mapping = {
        'I': 'c',  # INSERT -> create
        'U': 'u',  # UPDATE -> update
        'D': 'd'   # DELETE -> delete
    }
    return mapping.get(op, op)

def format_message(change_record):
    """Форматирует запись изменения в формат, совместимый с Debezium"""
    operation = convert_operation(change_record['operation'])
    
    # Определяем данные в зависимости от операции
    if operation == 'd':
        # DELETE - используем old_data
        data = change_record['old_data'] or {}
    else:
        # INSERT/UPDATE - используем new_data
        data = change_record['new_data'] or {}
    
    # Формируем сообщение в формате, похожем на Debezium
    # Обрабатываем created_at - может быть строкой или datetime
    created_at = data.get('created_at')
    if created_at is None:
        created_at = change_record['changed_at']
    if hasattr(created_at, 'isoformat'):
        # Если это datetime объект, конвертируем в строку
        created_at = created_at.isoformat()
    
    # Формируем timestamp в миллисекундах
    source_ts_ms = None
    if change_record['changed_at']:
        if hasattr(change_record['changed_at'], 'timestamp'):
            source_ts_ms = int(change_record['changed_at'].timestamp() * 1000)
        else:
            # Если это строка, пытаемся распарсить
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(str(change_record['changed_at']))
                source_ts_ms = int(dt.timestamp() * 1000)
            except:
                pass
    
    message = {
        'id': change_record['record_id'],
        'name': data.get('name', ''),
        'email': data.get('email', ''),
        'age': data.get('age'),
        'created_at': str(created_at) if created_at else None,
        '__op': operation,
        '__source_ts_ms': source_ts_ms
    }
    
    # Для DELETE добавляем флаг
    if operation == 'd':
        message['__deleted'] = 'true'
    else:
        message['__deleted'] = 'false'
    
    return message

def process_changes(conn, producer):
    """Обрабатывает необработанные изменения из таблицы cdc_changes_log"""
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Получаем необработанные изменения
            cur.execute("""
                SELECT change_id, table_name, operation, record_id, 
                       old_data, new_data, changed_at
                FROM cdc_changes_log
                WHERE processed = FALSE
                ORDER BY changed_at ASC
                LIMIT 100
            """)
            
            changes = cur.fetchall()
            
            if not changes:
                return 0
            
            logger.info(f"Found {len(changes)} unprocessed changes")
            
            # Отправляем каждое изменение в Kafka
            for change in changes:
                try:
                    message = format_message(change)
                    key = {'id': change['record_id']}
                    
                    # Отправляем в Kafka
                    future = producer.send(KAFKA_TOPIC, key=key, value=message)
                    result = future.get(timeout=10)
                    
                    logger.info(f"Sent change {change['change_id']} (op={change['operation']}, id={change['record_id']}) to Kafka")
                    
                    # Помечаем как обработанное
                    cur.execute("""
                        UPDATE cdc_changes_log
                        SET processed = TRUE
                        WHERE change_id = %s
                    """, (change['change_id'],))
                    
                    conn.commit()
                    
                except KafkaError as e:
                    logger.error(f"Failed to send change {change['change_id']} to Kafka: {e}")
                    conn.rollback()
                    continue
                except Exception as e:
                    logger.error(f"Error processing change {change['change_id']}: {e}")
                    conn.rollback()
                    continue
            
            return len(changes)
            
    except Exception as e:
        logger.error(f"Error processing changes: {e}")
        return 0

def main():
    """Основной цикл обработки изменений"""
    logger.info("Starting CDC Producer...")
    logger.info(f"PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Topic: {KAFKA_TOPIC}")
    logger.info(f"Poll interval: {POLL_INTERVAL} seconds")
    
    # Ждем готовности сервисов
    logger.info("Waiting for services to be ready...")
    time.sleep(10)
    
    conn = None
    producer = None
    
    try:
        # Подключаемся к БД
        logger.info("Connecting to PostgreSQL...")
        conn = get_db_connection()
        logger.info("Connected to PostgreSQL")
        
        # Создаем Kafka producer
        logger.info("Connecting to Kafka...")
        producer = get_kafka_producer()
        logger.info("Connected to Kafka")
        
        # Основной цикл
        logger.info("Starting to process changes...")
        while True:
            try:
                count = process_changes(conn, producer)
                if count == 0:
                    time.sleep(POLL_INTERVAL)
                else:
                    # Если были изменения, проверяем еще раз сразу
                    time.sleep(0.5)
            except KeyboardInterrupt:
                logger.info("Received interrupt signal, shutting down...")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                time.sleep(POLL_INTERVAL)
                
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        if conn:
            conn.close()
        if producer:
            producer.close()
        logger.info("CDC Producer stopped")

if __name__ == "__main__":
    main()

