import psycopg2
import json

def get_connection():
    return psycopg2.connect(
        host="localhost", port=5432,
        database="pizza_dwh", user="admin", password="admin123"
    )

conn = get_connection()
cur = conn.cursor()

# ============================================================
# 1. РЕКУРСИВНЫЕ CTE: ДЕРЕВО КАТЕГОРИЙ РЕСТОРАНОВ
# ============================================================
print("="*70)
print("1️⃣  РЕКУРСИВНЫЕ CTE: Дерево категорий ресторанов")
print("="*70)

# Создаём таблицу категорий
cur.execute("DROP TABLE IF EXISTS restaurant_categories CASCADE;")
cur.execute("""
    CREATE TABLE restaurant_categories (
        category_id SERIAL PRIMARY KEY,
        category_name VARCHAR(100),
        parent_id INTEGER REFERENCES restaurant_categories(category_id)
    );
""")

# Наполняем иерархией
cur.execute("""
    INSERT INTO restaurant_categories (category_id, category_name, parent_id) VALUES
    (1, 'Еда', NULL),
    (2, 'Фастфуд', 1),
    (3, 'Азиатская кухня', 1),
    (4, 'Бургеры', 2),
    (5, 'Пицца', 2),
    (6, 'Суши', 3),
    (7, 'Вок', 3),
    (8, 'Чизбургеры', 4),
    (9, 'Вегетарианские бургеры', 4);
""")
conn.commit()
print("✅ Таблица restaurant_categories создана с иерархией")

# Рекурсивный CTE — обходим дерево
print("\n🔸 Рекурсивный обход дерева:")
cur.execute("""
    WITH RECURSIVE category_tree AS (
        -- Базовый случай: корневые категории (parent_id IS NULL)
        SELECT 
            category_id,
            category_name,
            parent_id,
            1 AS level,
            category_name::text AS path
        FROM restaurant_categories
        WHERE parent_id IS NULL
        
        UNION ALL
        
        -- Рекурсивный случай: дочерние категории
        SELECT 
            c.category_id,
            c.category_name,
            c.parent_id,
            ct.level + 1,
            ct.path || ' → ' || c.category_name
        FROM restaurant_categories c
        JOIN category_tree ct ON c.parent_id = ct.category_id
    )
    SELECT 
        level,
        REPEAT('  ', level - 1) || category_name AS tree_view,
        path
    FROM category_tree
    ORDER BY path;
""")

print(f"\n{'Уровень':<10} {'Дерево':<40} {'Путь'}")
print("-" * 100)
for row in cur.fetchall():
    print(f"{row[0]:<10} {row[1]:<40} {row[2]}")

# ============================================================
# 2. МАТЕРИАЛИЗОВАННОЕ ПРЕДСТАВЛЕНИЕ
# ============================================================
print("\n" + "="*70)
print("2️⃣  MATERIALIZED VIEW: Кеш тяжелой агрегации")
print("="*70)

cur.execute("DROP MATERIALIZED VIEW IF EXISTS mv_restaurant_daily_stats;")

cur.execute("""
    CREATE MATERIALIZED VIEW mv_restaurant_daily_stats AS
    SELECT 
        restaurant,
        DATE(created_at) AS day,
        COUNT(*) AS orders_count,
        ROUND(SUM(total_sum), 2) AS revenue,
        ROUND(AVG(total_sum), 2) AS avg_check,
        COUNT(CASE WHEN status = 'cancelled' THEN 1 END) AS cancelled_count
    FROM orders
    GROUP BY restaurant, DATE(created_at);
""")
conn.commit()
print("✅ Materialized View mv_restaurant_daily_stats создана")

# Смотрим содержимое
cur.execute("""
    SELECT * FROM mv_restaurant_daily_stats
    ORDER BY revenue DESC
    LIMIT 5;
""")
print("\n🔸 Данные из Materialized View:")
print(f"{'Ресторан':<20} {'Дата':<12} {'Заказов':<10} {'Выручка':<12} {'Ср.чек':<10}")
print("-" * 70)
for row in cur.fetchall():
    print(f"{row[0]:<20} {str(row[1]):<12} {row[2]:<10} {row[3]:<12} {row[4]:<10}")

# Обновляем MV
print("\n🔸 Обновляем Materialized View...")
cur.execute("REFRESH MATERIALIZED VIEW mv_restaurant_daily_stats;")
conn.commit()
print("✅ MV обновлен")

# Создаём индекс на MV (для быстрого поиска)
cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_mv_restaurant 
    ON mv_restaurant_daily_stats(restaurant);
""")
print("✅ Индекс на MV создан")

# ============================================================
# 3. LATERAL JOIN: ТОП-3 ЗАКАЗА ПО КАЖДОМУ РЕСТОРАНУ
# ============================================================
print("\n" + "="*70)
print("3️⃣  LATERAL JOIN: Топ-3 заказа по каждому ресторану")
print("="*70)

cur.execute("""
    SELECT 
        r.restaurant,
        top_orders.order_id,
        top_orders.total_sum,
        top_orders.status
    FROM (SELECT DISTINCT restaurant FROM orders) r
    CROSS JOIN LATERAL (
        SELECT order_id, total_sum, status
        FROM orders o
        WHERE o.restaurant = r.restaurant
        ORDER BY total_sum DESC
        LIMIT 3
    ) AS top_orders
    ORDER BY r.restaurant, top_orders.total_sum DESC;
""")

print(f"\n{'Ресторан':<20} {'Order ID':<10} {'Сумма':<12} {'Статус':<15}")
print("-" * 60)
for row in cur.fetchall():
    print(f"{row[0]:<20} {row[1]:<10} {row[2]:<12} {row[3]:<15}")

# ============================================================
# 4. FULL-TEXT SEARCH
# ============================================================
print("\n" + "="*70)
print("4️⃣  FULL-TEXT SEARCH: Поиск по тексту")
print("="*70)

# Добавляем tsvector колонку
cur.execute("""
    ALTER TABLE orders 
    ADD COLUMN IF NOT EXISTS search_vector tsvector;
""")

# Обновляем tsvector (ресторан + статус)
cur.execute("""
    UPDATE orders
    SET search_vector = 
        to_tsvector('russian', COALESCE(restaurant, '') || ' ' || COALESCE(status, ''));
""")
conn.commit()
print("✅ tsvector колонка добавлена и заполнена")

# Создаём GIN индекс для быстрого поиска
cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_orders_search 
    ON orders USING GIN(search_vector);
""")
print("✅ GIN индекс создан")

# Ищем
cur.execute("""
    SELECT restaurant, status, total_sum
    FROM orders
    WHERE search_vector @@ to_tsquery('russian', 'пицц')
    LIMIT 10;
""")
print("\n🔸 Поиск по запросу 'пицц' (найдёт 'Пицца-Хаус'):")
print(f"{'Ресторан':<20} {'Статус':<15} {'Сумма':<10}")
print("-" * 50)
for row in cur.fetchall():
    print(f"{row[0]:<20} {row[1]:<15} {row[2]:<10}")

# ============================================================
# 5. JSONB: ГИБКИЕ ДАННЫЕ
# ============================================================
print("\n" + "="*70)
print("5️⃣  JSONB: Хранение гибких данных")
print("="*70)

cur.execute("DROP TABLE IF EXISTS restaurant_metadata CASCADE;")
cur.execute("""
    CREATE TABLE restaurant_metadata (
        restaurant_id INTEGER PRIMARY KEY,
        restaurant_name VARCHAR(100),
        metadata JSONB
    );
""")

# Вставляем данные с JSONB
cur.execute("""
    INSERT INTO restaurant_metadata (restaurant_id, restaurant_name, metadata) VALUES
    (1, 'Суши-Мастер', '{"rating": 4.8, "delivery_time": 30, "tags": ["суши", "роллы"], "address": {"city": "Москва", "street": "Ленина"}}'),
    (2, 'Пицца-Хаус', '{"rating": 4.5, "delivery_time": 45, "tags": ["пицца", "итальянская"], "address": {"city": "Москва", "street": "Пушкина"}}'),
    (3, 'Бургер-Кинг', '{"rating": 4.2, "delivery_time": 25, "tags": ["бургеры", "фастфуд"], "address": {"city": "Питер", "street": "Невский"}}');
""")
conn.commit()
print("✅ Таблица restaurant_metadata с JSONB создана")

# Запросы по JSONB
print("\n🔸 Рестораны с рейтингом > 4.5:")
cur.execute("""
    SELECT restaurant_name, metadata->>'rating' AS rating
    FROM restaurant_metadata
    WHERE (metadata->>'rating')::numeric > 4.5;
""")
for row in cur.fetchall():
    print(f"   {row[0]}: рейтинг {row[1]}")

print("\n🔸 Рестораны в Москве:")
cur.execute("""
    SELECT restaurant_name, metadata->'address'->>'city' AS city
    FROM restaurant_metadata
    WHERE metadata->'address'->>'city' = 'Москва';
""")
for row in cur.fetchall():
    print(f"   {row[0]}: {row[1]}")

print("\n🔸 Рестораны с тегом 'пицца':")
cur.execute("""
    SELECT restaurant_name
    FROM restaurant_metadata
    WHERE metadata->'tags' ? 'пицца';
""")
for row in cur.fetchall():
    print(f"   {row[0]}")

# ============================================================
# 6. UPSERT: INSERT ... ON CONFLICT DO UPDATE
# ============================================================
print("\n" + "="*70)
print("6️⃣  UPSERT: INSERT или UPDATE в одном запросе")
print("="*70)

# Создаём таблицу с уникальным ключом
cur.execute("DROP TABLE IF EXISTS user_stats CASCADE;")
cur.execute("""
    CREATE TABLE user_stats (
        user_id INTEGER PRIMARY KEY,
        orders_count INTEGER DEFAULT 0,
        total_spent NUMERIC DEFAULT 0,
        last_order_at TIMESTAMP
    );
""")

# UPSERT: если user_id существует — обновляем, иначе вставляем
cur.execute("""
    INSERT INTO user_stats (user_id, orders_count, total_spent, last_order_at)
    VALUES (423, 1, 1200.00, NOW())
    ON CONFLICT (user_id) DO UPDATE SET
        orders_count = user_stats.orders_count + 1,
        total_spent = user_stats.total_spent + EXCLUDED.total_spent,
        last_order_at = EXCLUDED.last_order_at;
""")
print("✅ UPSERT #1: user_id=423 (вставлен)")

# Повторный UPSERT — теперь обновление
cur.execute("""
    INSERT INTO user_stats (user_id, orders_count, total_spent, last_order_at)
    VALUES (423, 1, 800.00, NOW())
    ON CONFLICT (user_id) DO UPDATE SET
        orders_count = user_stats.orders_count + 1,
        total_spent = user_stats.total_spent + EXCLUDED.total_spent,
        last_order_at = EXCLUDED.last_order_at;
""")
print("✅ UPSERT #2: user_id=423 (обновлен)")

conn.commit()

# Смотрим результат
cur.execute("SELECT * FROM user_stats;")
print("\n🔸 Результат UPSERT:")
print(f"{'user_id':<10} {'orders_count':<15} {'total_spent':<15} {'last_order_at'}")
print("-" * 70)
for row in cur.fetchall():
    print(f"{row[0]:<10} {row[1]:<15} {row[2]:<15} {row[3]}")

cur.close()
conn.close()

print("\n" + "="*70)
print("🎉 ВСЕ ПРОДВИНУТЫЕ SQL-ТЕХНИКИ РАБОТАЮТ!")
print("="*70)