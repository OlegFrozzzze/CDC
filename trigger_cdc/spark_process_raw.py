"""
Spark скрипт для чтения данных из raw_store и обработки операций (c/u/d) на Delta таблицах
(Trigger-Based CDC)
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, coalesce, from_unixtime, row_number, desc
from pyspark.sql.types import StringType, IntegerType, TimestampType, LongType
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import os
import sys

# Настройки
RAW_STORE_BASE_PATH = os.getenv("RAW_STORE_BASE_PATH", "raw_store")
DELTA_TABLE_PATH = os.getenv("DELTA_TABLE_PATH", "DDS/users")
SCHEMA_VERSION = 1

# Константы для операций
CREATE_OP = "c"
UPDATE_OP = "u"
DELETE_OP = "d"

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
    
    # Создаём Spark сессию с поддержкой Delta
    spark = SparkSession.builder \
        .appName("ProcessRawStore-Trigger") \
        .master(os.getenv("SPARK_MASTER", "local[*]")) \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "localhost") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties") \
        .getOrCreate()
    
    # Убираем все лишние логи Spark
    spark.sparkContext.setLogLevel("ERROR")
    
    # Читаем данные из raw_store
    raw_path = f"{RAW_STORE_BASE_PATH}/version_{SCHEMA_VERSION}"
    
    try:
        df_raw = spark.read.format("parquet").load(raw_path)
        if df_raw.count() == 0:
            print("No data in raw_store to process")
            spark.stop()
            return
    except Exception as e:
        print(f"Error reading data: {e}")
        spark.stop()
        return
    
    print("\nData from raw_store:")
    df_raw.show(20, truncate=False)
    
    # Извлекаем ID из key или message
    id_from_key = get_json_object(col("key"), "$.id").cast(LongType())
    id_from_message = get_json_object(col("message"), "$.id").cast(LongType())
    id_final = coalesce(id_from_key, id_from_message)
    
    # Извлекаем поля из message
    df_parsed = df_raw.select(
        col("partition"),
        col("offset"),
        col("kafka_timestamp"),
        id_final.alias("ID"),
        get_json_object(col("message"), "$.name").alias("name"),
        get_json_object(col("message"), "$.email").alias("email"),
        get_json_object(col("message"), "$.age").cast(IntegerType()).alias("age"),
        get_json_object(col("message"), "$.created_at").alias("created_at_str"),
        get_json_object(col("message"), "$.__op").alias("__op")
    ).filter(col("ID").isNotNull())
    
    print("\nParsed data:")
    df_parsed.show(20, truncate=False)
    
    # Преобразуем created_at из ISO строки в timestamp
    # В trigger-based подходе created_at приходит как ISO строка (например, "2025-11-24T14:32:35.015026")
    from pyspark.sql.functions import when, to_timestamp, lit
    
    # Парсим ISO формат с микросекундами: "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"
    df_with_timestamp = df_parsed.withColumn(
        "created_at",
        when(col("created_at_str").isNotNull() & (col("created_at_str") != ""),
             to_timestamp(col("created_at_str"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"))
        .otherwise(lit(None))
        .cast(TimestampType())
    )
    
    # Дедуплицируем данные: оставляем только последнюю операцию для каждого ID
    window_spec = Window.partitionBy("ID").orderBy(desc("kafka_timestamp"))
    df_deduplicated = df_with_timestamp.withColumn("rn", row_number().over(window_spec)) \
        .filter(col("rn") == 1) \
        .drop("rn")
    
    # Оставляем только поля для DDS
    df_processed = df_deduplicated.select(
        col("ID"),
        col("name"),
        col("email"),
        col("age"),
        col("created_at"),
        col("__op")
    )
    
    # Проверяем, что есть данные для обработки
    if df_processed.count() == 0:
        print("No data to process after deduplication")
        spark.stop()
        return
    
    print("\nProcessed data (after deduplication):")
    df_processed.show(20, truncate=False)
    
    # Проверяем существование Delta таблицы
    table_exists = False
    try:
        state_df = spark.read.format("delta").load(DELTA_TABLE_PATH)
        print("\nCurrent Delta table state:")
        state_df.show(20)
        table_exists = True
    except:
        pass
    
    # Используем coalesce только если данных немного, иначе оставляем распределение
    row_count = df_processed.count()
    if row_count < 1000:
        inc_df = df_processed.coalesce(1)
    else:
        inc_df = df_processed
    
    print("\nIncremental data for merge:")
    inc_df.show(20)
    
    # Если таблица не существует, создаём её
    if not table_exists:
        initial_data = inc_df.filter(~col("__op").isin(DELETE_OP)) \
            .select("ID", "name", "email", "age", "created_at")
        
        if initial_data.count() == 0:
            print("No data to create initial table (only deletes found)")
            spark.stop()
            return
        
        initial_data.coalesce(1) \
            .write.format("delta").mode("overwrite").option("mergeSchema", "true").save(DELTA_TABLE_PATH)
        state_df = spark.read.format("delta").load(DELTA_TABLE_PATH)
        print("\nDelta table created:")
        state_df.show(20)
        spark.stop()
        return
    
    # Выполняем merge
    try:
        delta_table = DeltaTable.forPath(spark, DELTA_TABLE_PATH)
        
        (delta_table.alias("state")
            .merge(inc_df.alias("inc"), "state.ID = inc.ID")
            .whenMatchedUpdate(
                condition=col("inc.__op").isin(UPDATE_OP, CREATE_OP),
                set={
                    "name": col("inc.name"),
                    "email": col("inc.email"),
                    "age": col("inc.age"),
                    "created_at": col("inc.created_at")
                }
            )
            .whenMatchedDelete(condition=col("inc.__op") == DELETE_OP)
            .whenNotMatchedInsert(
                condition=~col("inc.__op").isin(DELETE_OP),
                values={
                    "ID": col("inc.ID"),
                    "name": col("inc.name"),
                    "email": col("inc.email"),
                    "age": col("inc.age"),
                    "created_at": col("inc.created_at")
                }
            )
            .execute())
    except Exception as e:
        print(f"Error during merge operation: {e}")
        spark.stop()
        raise
    
    # Показываем результат
    print("\nDelta table state after merge:")
    final_df = spark.read.format("delta").load(DELTA_TABLE_PATH)
    final_df.show(20)
    
    spark.stop()

if __name__ == "__main__":
    main()

