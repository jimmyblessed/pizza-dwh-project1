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

def run_explain(query, description, analyze=False):
    """Выполняет EXPLAIN и показывает план"""
    conn = get_connection()
    cur = conn.cursor()
    
    prefix = "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)" if analyze else "EXPLAIN"
    
    print(f"\n{'='*70}")
    print(f"📊 {description}")
    print('='*70)
    print(f"🔍 Запрос: {query.strip()[:100]}...")
    print()
    
    cur.execute(f"{prefix} {query}")
    plan = cur.fetchall()
    
    for row in plan:
        print(row[0])
    
    cur.close()
    conn.close()

# ============================================================
# 1. ПРОСТОЙ ЗАПРОС БЕЗ ИНДЕКСА
# ============================================================
print("\n" + "="*70)
print("1️⃣  EXPLAIN ANALYZE — СМОТРИМ ПЛАН ЗАПРОСА")
print("="*70)

query = "SELECT * FROM orders WHERE user_id = 423;"

run_explain(query, "Поиск по user_id (без индекса)", analyze=True)

# ============================================================
# 2. ЗАМЕР ВРЕМЕНИ
# ============================================================
print("\n" + "="*70)
print("2️⃣  ЗАМЕР ВРЕМЕНИ ВЫПОЛНЕНИЯ")
print("="*70)

def time_query(query, iterations=100):
    conn = get_connection()
    cur = conn.cursor()
    
    # Прогрев
    cur.execute(query)
    cur.fetchall()
    
    start = time.time()
    for _ in range(iterations):
        cur.execute(query)
        cur.fetchall()
    elapsed = time.time() - start
    
    cur.close()
    conn.close()
    
    return elapsed / iterations * 1000  # в миллисекундах

query_simple = "SELECT * FROM orders WHERE user_id = 423;"
time_no_index = time_query(query_simple)

print(f"🔸 Запрос по user_id: {time_no_index:.3f} ms (среднее за 100 запусков)")

# ============================================================
# 3. АГРЕГАЦИЯ
# ============================================================
query_agg = "SELECT restaurant, SUM(total_sum) FROM orders GROUP BY restaurant;"

run_explain(query_agg, "Агрегация по ресторанам", analyze=True)

# ============================================================
# 4. JOIN
# ============================================================
query_join = """
    SELECT f.order_id, r.restaurant_name, d.full_date, f.total_sum
    FROM fact_orders f
    JOIN dim_restaurants r ON f.restaurant_id = r.restaurant_id
    JOIN dim_date d ON f.date_id = d.date_id
    LIMIT 10;
"""

run_explain(query_join, "JOIN фактов с измерениями", analyze=True)

print("\n" + "="*70)
print("🎯 ВЫВОД: Смотрим на 'Seq Scan' (плохо) vs 'Index Scan' (хорошо)")
print("="*70)