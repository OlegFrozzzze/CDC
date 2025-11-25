"""
Spark скрипт для чтения данных из Kafka и сохранения в raw_store
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max
from pyspark.sql.types import StringType
import json
import os
import sys

# Настройки
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "postgres-server.public.users"
RAW_STORE_BASE_PATH = os.getenv("RAW_STORE_BASE_PATH", "raw_store")
SCHEMA_VERSION = 1

def main():
    # Устанавливаем путь к Python для Spark
    python_exe = sys.executable
    os.environ['PYSPARK_PYTHON'] = python_exe
    os.environ['PYSPARK_DRIVER_PYTHON'] = python_exe
    
    # Настраиваем логирование ДО создания Spark сессии
    import logging
    logging.basicConfig(level=logging.ERROR)
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)
    logging.getLogger("org.apache.spark").setLevel(logging.ERROR)
    logging.getLogger("org.apache.hadoop").setLevel(logging.ERROR)
    logging.getLogger("org.apache.spark.sql").setLevel(logging.ERROR)
    
    # Создаём Spark сессию
    spark = SparkSession.builder \
        .appName("KafkaToRawStore") \
        .master(os.getenv("SPARK_MASTER", "local[*]")) \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.3") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "localhost") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties") \
        .getOrCreate()
    
    # Убираем все лишние логи Spark
    spark.sparkContext.setLogLevel("ERROR")
    
    # Определяем начальный offset из raw_store
    starting_offsets = "earliest"
    raw_path = f"{RAW_STORE_BASE_PATH}/version_{SCHEMA_VERSION}"
    
    try:
        if os.path.exists(raw_path):
            df_existing = spark.read.format("parquet").load(raw_path)
            if df_existing.count() > 0:
                max_offsets = df_existing.groupBy("partition").agg(spark_max("offset").alias("max_offset")).collect()
                if max_offsets:
                    offsets_dict = {}
                    for row in max_offsets:
                        offsets_dict[str(row["partition"])] = row["max_offset"] + 1
                    starting_offsets = json.dumps({KAFKA_TOPIC: offsets_dict})
                    print(f"Reading from checkpoint offsets: {starting_offsets}")
    except Exception as e:
        print(f"Could not read checkpoint, using 'earliest': {e}")
        starting_offsets = "earliest"
    
    # Читаем данные из Kafka
    df_batch = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", starting_offsets) \
        .load()
    
    # Проверяем, есть ли новые данные
    if df_batch.count() == 0:
        print("No new data in Kafka topic")
        spark.stop()
        return
    
    # Декодируем JSON сообщения
    df_decoded = df_batch.select(
        col("partition"),
        col("offset"),
        col("key").cast(StringType()).alias("key"),
        col("value").cast(StringType()).alias("message"),
        col("timestamp").alias("kafka_timestamp")
    )
    
    df_decoded.show(20, truncate=False)
    
    # Сохраняем данные в raw_store
    output_path = f"{RAW_STORE_BASE_PATH}/version_{SCHEMA_VERSION}"
    df_decoded \
        .write \
        .format("parquet") \
        .mode("append") \
        .save(output_path)
    
    # Показываем данные после записи
    print("\nData after save:")
    df_saved = spark.read.format("parquet").load(output_path)
    df_saved.show(20, truncate=False)
    
    spark.stop()

if __name__ == "__main__":
    main()
