import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, sum as spark_sum,
    count, avg, round as spark_round
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

# Настройка Hadoop для Windows
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

# ============================================================
# 1. СОЗДАНИЕ SPARK SESSION
# ============================================================
spark = SparkSession.builder \
    .appName("PizzaStreaming") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.sql.shuffle.partitions", "3") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✅ Spark Session создана")
print(f"📌 Spark version: {spark.version}")

# ============================================================
# 2. СХЕМА
# ============================================================
schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("restaurant", StringType(), True),
    StructField("restaurant_id", IntegerType(), True),
    StructField("total_sum", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("created_at", StringType(), True),
])

# ============================================================
# 3. ЧТЕНИЕ ИЗ KAFKA
# ============================================================
print("\n📖 Подключаемся к Kafka...")

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "orders_topic") \
    .option("startingOffsets", "earliest") \
    .load()

print("✅ Поток подключён")

# ============================================================
# 4. ОТЛАДКА: СЫРЫЕ СООБЩЕНИЯ
# ============================================================
print("\n🔍 Смотрим сырые сообщения...")

query_raw = df_raw.selectExpr("CAST(value AS STRING) as raw_value") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "file:///D:/piton/step_5_spark/checkpoint_raw") \
    .trigger(processingTime="5 seconds") \
    .start()

# ============================================================
# 5. ПАРСИНГ JSON + АГРЕГАЦИЯ
# ============================================================
df_parsed = df_raw \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

df_clean = df_parsed.filter(
    (col("total_sum").isNotNull()) & 
    (col("total_sum") > 0)
)

df_stats = df_clean.groupBy("restaurant").agg(
    count("*").alias("orders_count"),
    spark_round(spark_sum("total_sum"), 2).alias("total_revenue"),
    spark_round(avg("total_sum"), 2).alias("avg_check")
)

query_stats = df_stats.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "file:///D:/piton/step_5_spark/checkpoint_stats") \
    .trigger(processingTime="10 seconds") \
    .start()

print("\n🚀 Стриминг запущен! Ждём данных...")
print("=" * 60)

spark.streams.awaitAnyTermination()