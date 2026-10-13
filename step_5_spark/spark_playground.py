from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, count, round as spark_round,
    max as spark_max, min as spark_min,
    when, lit, row_number, rank, lag
)
from pyspark.sql.window import Window
import os

# ---- НАСТРОЙКА HADOOP (для Windows) ----
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

# ---- СОЗДАНИЕ SPARK SESSION ----
spark = SparkSession.builder \
    .appName("PizzaDWH_Playground") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("\n" + "="*70)
print("🎮 SPARK PLAYGROUND")
print("="*70)
print(f"📌 Spark version: {spark.version}")
print(f"📌 Master: {spark.sparkContext.master}")
print(f"📌 Default parallelism: {spark.sparkContext.defaultParallelism}")

# ============================================================
# 1. ЧТЕНИЕ ДАННЫХ
# ============================================================
print("\n" + "="*70)
print("1️⃣ ЧТЕНИЕ ИЗ POSTGRESQL")
print("="*70)

df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/pizza_dwh") \
    .option("dbtable", "orders") \
    .option("user", "admin") \
    .option("password", "admin123") \
    .option("driver", "org.postgresql.Driver") \
    .load()

print(f"✅ Загружено строк: {df.count()}")
print(f"✅ Колонок: {len(df.columns)}")
print(f"✅ Колонки: {df.columns}")
print(f"\n📋 Схема DataFrame:")
df.printSchema()

# ============================================================
# 2. LAZY vs ACTION (важно!)
# ============================================================
print("\n" + "="*70)
print("2️⃣ LAZY vs ACTION")
print("="*70)

print("\n🔸 Создаём фильтр (это LAZY — ничего не выполняется):")
df_filtered = df.filter(col("total_sum") > 3000)
print("   df_filtered = df.filter(...) — прошло мгновенно!")

print("\n🔸 Теперь вызываем ACTION (тут Spark запускается):")
count_expensive = df_filtered.count()
print(f"   Заказов > 3000 руб: {count_expensive}")

# ============================================================
# 3. ПРОСМОТР ПЛАНА ВЫПОЛНЕНИЯ
# ============================================================
print("\n" + "="*70)
print("3️⃣ ПЛАН ВЫПОЛНЕНИЯ (как Spark оптимизирует)")
print("="*70)

df_complex = df.filter(col("status") == "delivered") \
    .groupBy("restaurant") \
    .agg(
        count("*").alias("cnt"),
        spark_round(sum("total_sum"), 2).alias("revenue")
    ) \
    .filter(col("revenue") > 10000) \
    .orderBy(col("revenue").desc())

print("\n🔍 Логический план:")
df_complex.explain(True)

print("\n📊 Результат:")
df_complex.show()

# ============================================================
# 4. ОКОННЫЕ ФУНКЦИИ В SPARK
# ============================================================
print("\n" + "="*70)
print("4️⃣ ОКОННЫЕ ФУНКЦИИ")
print("="*70)

window_spec = Window.partitionBy("restaurant").orderBy(col("total_sum").desc())

df_ranked = df.withColumn(
    "rank_in_restaurant",
    rank().over(window_spec)
).filter(col("rank_in_restaurant") <= 2) \
 .select("restaurant", "order_id", "total_sum", "rank_in_restaurant") \
 .orderBy("restaurant", "rank_in_restaurant")

print("🔸 Топ-2 заказа по каждому ресторану:")
df_ranked.show(30, truncate=False)

# ============================================================
# 5. СОЗДАНИЕ НОВЫХ КОЛОНОК (when/lit)
# ============================================================
print("\n" + "="*70)
print("5️⃣ СОЗДАНИЕ КОЛОНОК (CASE WHEN)")
print("="*70)

df_with_category = df.withColumn(
    "check_category",
    when(col("total_sum") < 1000, "Маленький")
    .when(col("total_sum") < 3000, "Средний")
    .otherwise("Большой")
)

print("\n🔸 Распределение по категориям чека:")
df_with_category.groupBy("check_category") \
    .agg(count("*").alias("orders_count")) \
    .orderBy("orders_count") \
    .show()

# ============================================================
# 6. SQL-ЗАПРОСЫ ПРЯМО В SPARK
# ============================================================
print("\n" + "="*70)
print("6️⃣ SQL В SPARK (Spark SQL)")
print("="*70)

df.createOrReplaceTempView("orders_view")

result = spark.sql("""
    SELECT 
        restaurant,
        COUNT(*) as orders_count,
        ROUND(SUM(total_sum), 2) as revenue,
        ROUND(AVG(total_sum), 2) as avg_check,
        MAX(total_sum) as max_order,
        MIN(total_sum) as min_order
    FROM orders_view
    GROUP BY restaurant
    ORDER BY revenue DESC
""")

print("\n🔸 Статистика через Spark SQL:")
result.show()

# ============================================================
# 7. КЕШИРОВАНИЕ (Cache vs Persist)
# ============================================================
print("\n" + "="*70)
print("7️⃣ КЕШИРОВАНИЕ")
print("="*70)

print("🔸 Кешируем DataFrame в память...")
df.cache()
df.count()  # триггерим кеш
print("✅ df закеширован в RAM")

print("🔸 Повторный доступ (теперь БЫСТРО, из RAM):")
df.groupBy("status").count().show()

# ============================================================
# 8. РАЗНИЦА: PANDAS vs SPARK
# ============================================================
print("\n" + "="*70)
print("8️⃣ КОНВЕРТАЦИЯ В PANDAS")
print("="*70)

pdf = df.limit(10).toPandas()
print(f"✅ Конвертировано в pandas: {len(pdf)} строк")
print(f"📌 Тип: {type(pdf)}")

# ============================================================
# ФИНАЛ
# ============================================================
print("\n" + "="*70)
print("🎉 PLAYGROUND ЗАВЕРШЕН!")
print("="*70)

spark.stop()