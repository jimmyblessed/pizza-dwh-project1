import clickhouse_connect
import psycopg2
import time

# ============================================================
# ПОДКЛЮЧЕНИЕ
# ============================================================
print("=" * 70)
print("🔌 Подключение к ClickHouse...")
print("=" * 70)

ch = clickhouse_connect.get_client(
    host='localhost', port=8123,
    username='admin', password='admin123',
    database='pizza_analytics'
)

print(f"✅ ClickHouse версия: {ch.server_version}")

# ============================================================
# 1. MATERIALIZED VIEW — АВТООБНОВЛЕНИЕ АГРЕГАТОВ
# ============================================================
print("\n" + "=" * 70)
print("1️⃣  MATERIALIZED VIEW — автообновление агрегатов")
print("=" * 70)

# Создаём основную таблицу
ch.command("DROP TABLE IF EXISTS orders_raw")
ch.command("""
    CREATE TABLE orders_raw (
        order_id UInt32,
        user_id UInt32,
        restaurant String,
        total_sum Float64,
        status String,
        created_at DateTime
    )
    ENGINE = MergeTree()
    ORDER BY (created_at, order_id)
""")
print("✅ orders_raw создана")

# Создаём таблицу для агрегатов
ch.command("DROP TABLE IF EXISTS restaurant_stats_mv")
ch.command("""
    CREATE TABLE restaurant_stats_mv (
        restaurant String,
        orders_count UInt32,
        total_revenue Float64
    )
    ENGINE = SummingMergeTree()
    ORDER BY restaurant
""")
print("✅ restaurant_stats_mv создана")

# Создаём MATERIALIZED VIEW — автоматически обновляет агрегаты при INSERT
ch.command("DROP VIEW IF EXISTS mv_restaurant_stats")
ch.command("""
    CREATE MATERIALIZED VIEW mv_restaurant_stats
    TO restaurant_stats_mv
    AS SELECT
        restaurant,
        count() AS orders_count,
        sum(total_sum) AS total_revenue
    FROM orders_raw
    GROUP BY restaurant
""")
print("✅ MATERIALIZED VIEW mv_restaurant_stats создан")

# Проверяем: вставляем данные
print("\n📥 Вставляем данные в orders_raw...")

ch.command("""
    INSERT INTO orders_raw 
    SELECT 
        number AS order_id,
        (number % 1000) AS user_id,
        ['Пицца-Хаус', 'Суши-Мастер', 'Бургер-Кинг'][number % 3 + 1] AS restaurant,
        round(rand() / 100000000 * 5000, 2) AS total_sum,
        'delivered' AS status,
        now() AS created_at
    FROM numbers(1000)
""")
print("✅ Вставили 1000 заказов")

# Смотрим, что MV автоматически обновился
result = ch.query("SELECT * FROM restaurant_stats_mv ORDER BY restaurant")
print("\n🔸 АГРЕГАТЫ ОБНОВИЛИСЬ АВТОМАТИЧЕСКИ:")
print(f"{'Ресторан':<20} {'Заказов':<10} {'Выручка':<15}")
print("-" * 50)
for row in result.result_rows:
    print(f"{row[0]:<20} {row[1]:<10} {row[2]:<15.2f}")

# ============================================================
# 2. TTL — АВТОУДАЛЕНИЕ СТАРЫХ ДАННЫХ
# ============================================================
print("\n" + "=" * 70)
print("2️⃣  TTL — автоудаление старых данных")
print("=" * 70)

ch.command("DROP TABLE IF EXISTS orders_with_ttl")
ch.command("""
    CREATE TABLE orders_with_ttl (
        order_id UInt32,
        restaurant String,
        total_sum Float64,
        created_at DateTime
    )
    ENGINE = MergeTree()
    ORDER BY created_at
    TTL created_at + INTERVAL 7 DAY
""")
print("✅ orders_with_ttl создана")
print("   → данные автоматически удалятся через 7 дней")

# ============================================================
# 3. REPLACINGMERGETREE — ДЕДУПЛИКАЦИЯ
# ============================================================
print("\n" + "=" * 70)
print("3️⃣  ReplacingMergeTree — дедупликация")
print("=" * 70)

ch.command("DROP TABLE IF EXISTS users_replacing")
ch.command("""
    CREATE TABLE users_replacing (
        user_id UInt32,
        name String,
        city String,
        updated_at DateTime
    )
    ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY user_id
""")
print("✅ users_replacing создана")

# Вставляем одну и ту же запись дважды
print("\n📥 Вставляем user_id=1 дважды (дубликат)...")
ch.command("""
    INSERT INTO users_replacing VALUES 
    (1, 'Иван', 'Москва', '2026-10-01 10:00:00'),
    (1, 'Иван', 'Питер', '2026-10-02 10:00:00')
""")

# Смотрим
result = ch.query("SELECT * FROM users_replacing FINAL")
print("\n🔸 После FINAL (дедупликация):")
print(f"{'user_id':<10} {'name':<10} {'city':<10} {'updated_at'}")
print("-" * 60)
for row in result.result_rows:
    print(f"{row[0]:<10} {row[1]:<10} {row[2]:<10} {row[3]}")

print("\n💡 ClickHouse оставил только последнюю запись!")

# ============================================================
# 4. SUMMINGMERGETREE — АВТОСУММИРОВАНИЕ
# ============================================================
print("\n" + "=" * 70)
print("4️⃣  SummingMergeTree — автосуммирование")
print("=" * 70)

ch.command("DROP TABLE IF EXISTS sales_summing")
ch.command("""
    CREATE TABLE sales_summing (
        day Date,
        product String,
        amount Float64,
        quantity UInt32
    )
    ENGINE = SummingMergeTree((amount, quantity))
    ORDER BY (day, product)
""")
print("✅ sales_summing создана")

print("\n📥 Вставляем 3 продажи одного товара...")
ch.command("""
    INSERT INTO sales_summing VALUES
    ('2026-10-12', 'Пицца', 500, 1),
    ('2026-10-12', 'Пицца', 500, 2),
    ('2026-10-12', 'Пицца', 500, 3)
""")

# Триггерим merge
ch.command("OPTIMIZE TABLE sales_summing FINAL")

result = ch.query("SELECT * FROM sales_summing")
print("\n🔸 После автосуммирования:")
print(f"{'Дата':<15} {'Товар':<10} {'Сумма':<10} {'Кол-во'}")
print("-" * 50)
for row in result.result_rows:
    print(f"{str(row[0]):<15} {row[1]:<10} {row[2]:<10} {row[3]}")

print("\n💡 ClickHouse сам сложил 3 записи в 1!")

# ============================================================
# 5. СРАВНЕНИЕ С POSTGRESQL
# ============================================================
print("\n" + "=" * 70)
print("5️⃣  СРАВНЕНИЕ: PostgreSQL vs ClickHouse на 100 000 строк")
print("=" * 70)

# Читаем из PostgreSQL
pg_conn = psycopg2.connect(
    host="localhost", port=5432,
    database="pizza_dwh", user="admin", password="admin123"
)
pg_cur = pg_conn.cursor()

# Замеряем PostgreSQL
start = time.time()
pg_cur.execute("SELECT COUNT(*), SUM(total_sum) FROM orders")
pg_result = pg_cur.fetchone()
pg_time = time.time() - start
print(f"\n🔸 PostgreSQL: COUNT + SUM = {pg_time*1000:.2f} ms")
print(f"   Результат: {pg_result[0]} заказов, {pg_result[1]} руб.")

pg_cur.close()
pg_conn.close()

# Замеряем ClickHouse
start = time.time()
result = ch.query("SELECT count(), sum(total_sum) FROM orders_raw")
ch_result = result.result_rows[0]
ch_time = time.time() - start
print(f"\n🔸 ClickHouse: COUNT + SUM = {ch_time*1000:.2f} ms")
print(f"   Результат: {ch_result[0]} заказов, {ch_result[1]} руб.")

print(f"\n⚡ РАЗНИЦА: ClickHouse в {pg_time/ch_time:.1f}x быстрее!")

# ============================================================
# 6. СПИСОК ВСЕХ ТАБЛИЦ
# ============================================================
print("\n" + "=" * 70)
print("6️⃣  СПИСОК ТАБЛИЦ В CLICKHOUSE")
print("=" * 70)

result = ch.query("""
    SELECT name, engine 
    FROM system.tables 
    WHERE database = 'pizza_analytics'
""")
print(f"\n{'Таблица':<30} {'Движок':<25}")
print("-" * 60)
for row in result.result_rows:
    print(f"{row[0]:<30} {row[1]:<25}")

print("\n" + "=" * 70)
print("🎉 CLICKHOUSE УГЛУБЛЕНИЕ ЗАВЕРШЕНО!")
print("=" * 70)