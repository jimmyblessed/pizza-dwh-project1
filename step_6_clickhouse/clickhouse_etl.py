import clickhouse_connect
import psycopg2
from datetime import datetime

# ============================================================
# 1. ПОДКЛЮЧЕНИЕ К CLICKHOUSE
# ============================================================
print("=" * 60)
print("🔌 Подключение к ClickHouse...")
print("=" * 60)

client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='admin',
    password='admin123',
    database='pizza_analytics'
)

print(f"✅ ClickHouse версия: {client.server_version}")

# ============================================================
# 2. СОЗДАНИЕ ТАБЛИЦЫ В CLICKHOUSE
# ============================================================
print("\n" + "=" * 60)
print("📋 Создание таблицы dm_orders...")
print("=" * 60)

client.command("""
    CREATE TABLE IF NOT EXISTS dm_orders (
        order_id UInt32,
        user_id UInt32,
        restaurant String,
        restaurant_id UInt8,
        total_sum Float64,
        status String,
        created_at DateTime,
        loaded_at DateTime DEFAULT now()
    )
    ENGINE = MergeTree()
    ORDER BY (created_at, order_id)
""")

print("✅ Таблица dm_orders создана")

# ============================================================
# 3. ЧТЕНИЕ ИЗ POSTGRESQL
# ============================================================
print("\n" + "=" * 60)
print("📖 Читаем данные из PostgreSQL...")
print("=" * 60)

pg_conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="pizza_dwh",
    user="admin",
    password="admin123"
)
pg_cur = pg_conn.cursor()
pg_cur.execute("SELECT order_id, user_id, restaurant, restaurant_id, total_sum, status, created_at FROM orders")
rows = pg_cur.fetchall()

print(f"✅ Загружено строк из PostgreSQL: {len(rows)}")

# ============================================================
# 4. ЗАПИСЬ В CLICKHOUSE
# ============================================================
print("\n" + "=" * 60)
print("💾 Пишем в ClickHouse...")
print("=" * 60)

# Удаляем старые данные (чтобы не было дублей)
client.command("TRUNCATE TABLE dm_orders")

# Вставляем новые
client.insert('dm_orders', rows, column_names=[
    'order_id', 'user_id', 'restaurant', 'restaurant_id',
    'total_sum', 'status', 'created_at'
])

print(f"✅ Записано строк в ClickHouse: {len(rows)}")

# ============================================================
# 5. АНАЛИТИКА В CLICKHOUSE (молниеносно!)
# ============================================================
print("\n" + "=" * 60)
print("📊 Аналитика в ClickHouse")
print("=" * 60)

# Запрос 1: Статистика по ресторанам
result = client.query("""
    SELECT 
        restaurant,
        count() as orders_count,
        round(sum(total_sum), 2) as revenue,
        round(avg(total_sum), 2) as avg_check
    FROM dm_orders
    GROUP BY restaurant
    ORDER BY revenue DESC
""")

print("\n🔸 Топ ресторанов по выручке:")
print(f"{'Ресторан':<20} {'Заказов':<10} {'Выручка':<12} {'Ср.чек':<10}")
print("-" * 60)
for row in result.result_rows:
    print(f"{row[0]:<20} {row[1]:<10} {row[2]:<12} {row[3]:<10}")

# Запрос 2: Заказы по дням
result = client.query("""
    SELECT 
        toDate(created_at) as day,
        count() as orders,
        round(sum(total_sum), 2) as revenue
    FROM dm_orders
    GROUP BY day
    ORDER BY day DESC
    LIMIT 10
""")

print("\n🔸 Заказы по дням:")
print(f"{'Дата':<15} {'Заказов':<10} {'Выручка':<12}")
print("-" * 40)
for row in result.result_rows:
    print(f"{str(row[0]):<15} {row[1]:<10} {row[2]:<12}")

# ============================================================
# 6. МОЩЬ CLICKHOUSE — БЫСТРЫЙ COUNT
# ============================================================
print("\n" + "=" * 60)
print("⚡ Скорость ClickHouse")
print("=" * 60)

import time

start = time.time()
result = client.query("SELECT count() FROM dm_orders")
count = result.result_rows[0][0]
elapsed = time.time() - start
print(f"🔸 COUNT(*) на {count} строк: {elapsed*1000:.2f} ms")

# Закрываем соединения
pg_cur.close()
pg_conn.close()

print("\n" + "=" * 60)
print("🎉 ETL в ClickHouse завершен!")
print("=" * 60)