from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, round as spark_round
import os

# ---- СОЗДАНИЕ SPARK SESSION ----
spark = SparkSession.builder \
    .appName("PizzaDWH_ETL") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✅ Spark Session создана!")
print(f"📌 Spark version: {spark.version}")
print(f"📌 Master: {spark.sparkContext.master}")

# ---- ЧТЕНИЕ ИЗ POSTGRESQL ----
print("\n📖 Читаем данные из PostgreSQL...")

df_orders = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/pizza_dwh") \
    .option("dbtable", "orders") \
    .option("user", "admin") \
    .option("password", "admin123") \
    .option("driver", "org.postgresql.Driver") \
    .load()

print(f"✅ Загружено строк: {df_orders.count()}")
df_orders.show(5)

# ---- ТРАНСФОРМАЦИИ (пример агрегации) ----
print("\n🔄 Считаем статистику по ресторанам...")

df_stats = df_orders.groupBy("restaurant").agg(
    count("*").alias("orders_count"),
    spark_round(sum("total_sum"), 2).alias("revenue"),
    spark_round(avg("total_sum"), 2).alias("avg_check")
).orderBy(col("revenue").desc())

df_stats.show()

# ---- ЗАПИСЬ РЕЗУЛЬТАТА ОБРАТНО В POSTGRESQL ----
print("\n💾 Пишем результат в PostgreSQL...")

df_stats.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/pizza_dwh") \
    .option("dbtable", "dm_spark_restaurant_stats") \
    .option("user", "admin") \
    .option("password", "admin123") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("✅ Результат записан в dm_spark_restaurant_stats!")
print("\n🎉 Spark ETL завершен!")

spark.stop()
import shutil
import os
import glob

# Чистим временные папки Spark вручную
temp_dir = os.path.join(os.environ.get('TEMP', ''), 'spark-*')
for folder in glob.glob(temp_dir):
    try:
        shutil.rmtree(folder, ignore_errors=True)
    except:
        pass