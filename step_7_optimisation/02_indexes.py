import psycopg2
import time

def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="pizza_dwh",
        user="admin",
        password="admin123"
    )

def time_query(query, iterations=100):
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute(query)
    cur.fetchall()
    
    start = time.time()
    for _ in range(iterations):
        cur.execute(query)
        cur.fetchall()
    elapsed = time.time() - start
    
    cur.close()
    conn.close()
    return elapsed / iterations * 1000

# ============================================================
# 1. ЗАМЕР ДО ИНДЕКСА
# ============================================================
print("\n" + "="*70)
print("1️⃣  ЗАМЕР БЕЗ ИНДЕКСА")
print("="*70)

query = "SELECT * FROM orders WHERE user_id = 423;"
time_before = time_query(query)
print(f"🔸 Время ДО индекса: {time_before:.3f} ms")

# ============================================================
# 2. СОЗДАЕМ ИНДЕКС
# ============================================================
print("\n" + "="*70)
print("2️⃣  СОЗДАЕМ ИНДЕКС")
print("="*70)

conn = get_connection()
cur = conn.cursor()

# Обычный B-tree индекс
cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_orders_user_id 
    ON orders(user_id);
""")
print("✅ Создан индекс: idx_orders_user_id")

# Составной индекс
cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_orders_restaurant_status 
    ON orders(restaurant_id, status);
""")
print("✅ Создан составной индекс: idx_orders_restaurant_status")

# Индекс для диапазонов
cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_orders_created_at 
    ON orders(created_at);
""")
print("✅ Создан индекс: idx_orders_created_at")

conn.commit()
cur.close()
conn.close()

# ============================================================
# 3. ЗАМЕР ПОСЛЕ ИНДЕКСА
# ============================================================
print("\n" + "="*70)
print("3️⃣  ЗАМЕР ПОСЛЕ ИНДЕКСА")
print("="*70)

time_after = time_query(query)
print(f"🔸 Время ПОСЛЕ индекса: {time_after:.3f} ms")
print(f"🔸 УСКОРЕНИЕ: {time_before / time_after:.1f}x быстрее!")

# ============================================================
# 4. СМОТРИМ ПЛАН ПОСЛЕ ИНДЕКСА
# ============================================================
print("\n" + "="*70)
print("4️⃣  ПЛАН ПОСЛЕ ИНДЕКСА")
print("="*70)

conn = get_connection()
cur = conn.cursor()
cur.execute(f"EXPLAIN ANALYZE {query}")
for row in cur.fetchall():
    print(row[0])
cur.close()
conn.close()

# ============================================================
# 5. СМОТРИМ ВСЕ ИНДЕКСЫ
# ============================================================
print("\n" + "="*70)
print("5️⃣  ВСЕ ИНДЕКСЫ НА ТАБЛИЦЕ orders")
print("="*70)

conn = get_connection()
cur = conn.cursor()
cur.execute("""
    SELECT indexname, indexdef 
    FROM pg_indexes 
    WHERE tablename = 'orders';
""")
for row in cur.fetchall():
    print(f"📌 {row[0]}")
    print(f"   {row[1]}")
cur.close()
conn.close()