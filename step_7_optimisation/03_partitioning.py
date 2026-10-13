import psycopg2

def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="pizza_dwh",
        user="admin",
        password="admin123"
    )

conn = get_connection()
cur = conn.cursor()

# ============================================================
# 1. СОЗДАЕМ ПАРТИЦИОНИРОВАННУЮ ТАБЛИЦУ
# ============================================================
print("="*70)
print("1️⃣  СОЗДАЕМ ПАРТИЦИОНИРОВАННУЮ ТАБЛИЦУ orders_partitioned")
print("="*70)

cur.execute("DROP TABLE IF EXISTS orders_partitioned CASCADE;")

cur.execute("""
    CREATE TABLE orders_partitioned (
        order_id INTEGER,
        user_id INTEGER,
        restaurant VARCHAR(100),
        restaurant_id INTEGER,
        total_sum DECIMAL(10, 2),
        status VARCHAR(50),
        created_at TIMESTAMP NOT NULL,
        PRIMARY KEY (order_id, created_at)
    ) PARTITION BY RANGE (created_at);
""")

print("✅ Таблица orders_partitioned создана (PARTITION BY RANGE created_at)")

# ============================================================
# 2. СОЗДАЕМ ПАРТИЦИИ ПО МЕСЯЦАМ
# ============================================================
print("\n" + "="*70)
print("2️⃣  СОЗДАЕМ ПАРТИЦИИ")
print("="*70)

cur.execute("""
    CREATE TABLE orders_2026_06 PARTITION OF orders_partitioned
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
""")
print("✅ Партиция orders_2026_06 (июнь)")

cur.execute("""
    CREATE TABLE orders_2026_07 PARTITION OF orders_partitioned
    FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');
""")
print("✅ Партиция orders_2026_07 (июль)")

cur.execute("""
    CREATE TABLE orders_2026_08 PARTITION OF orders_partitioned
    FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');
""")
print("✅ Партиция orders_2026_08 (август)")

# Default партиция для остальных дат
cur.execute("""
    CREATE TABLE orders_default PARTITION OF orders_partitioned
    DEFAULT;
""")
print("✅ Партиция orders_default (для остальных дат)")

conn.commit()

# ============================================================
# 3. КОПИРУЕМ ДАННЫЕ
# ============================================================
print("\n" + "="*70)
print("3️⃣  КОПИРУЕМ ДАННЫЕ ИЗ orders")
print("="*70)

cur.execute("""
    INSERT INTO orders_partitioned 
    SELECT order_id, user_id, restaurant, restaurant_id, total_sum, status, created_at
    FROM orders
    ON CONFLICT DO NOTHING;
""")
print(f"✅ Скопировано строк: {cur.rowcount}")

conn.commit()

# ============================================================
# 4. СМОТРИМ ПАРТИЦИИ
# ============================================================
print("\n" + "="*70)
print("4️⃣  СПИСОК ПАРТИЦИЙ")
print("="*70)

cur.execute("""
    SELECT 
        inhrelid::regclass AS partition_name,
        pg_size_pretty(pg_relation_size(inhrelid)) AS size
    FROM pg_inherits
    WHERE inhparent = 'orders_partitioned'::regclass;
""")

for row in cur.fetchall():
    print(f"📌 {row[0]:<30} {row[1]}")

# ============================================================
# 5. EXPLAIN ПО ПАРТИЦИЯМ
# ============================================================
print("\n" + "="*70)
print("5️⃣  EXPLAIN ANALYZE ПО ПАРТИЦИЯМ")
print("="*70)
print("🔍 Запрос: SELECT * FROM orders_partitioned WHERE created_at >= '2026-07-01';")
print()

cur.execute("""
    EXPLAIN ANALYZE 
    SELECT * FROM orders_partitioned 
    WHERE created_at >= '2026-07-01' AND created_at < '2026-08-01';
""")

for row in cur.fetchall():
    print(row[0])

print("\n" + "="*70)
print("🎯 ВЫВОД: PostgreSQL читает ТОЛЬКО партицию orders_2026_07!")
print("="*70)

cur.close()
conn.close()