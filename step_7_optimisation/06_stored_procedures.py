import psycopg2

def get_connection():
    return psycopg2.connect(
        host="localhost", port=5432,
        database="pizza_dwh", user="admin", password="admin123"
    )

conn = get_connection()
cur = conn.cursor()

# ============================================================
# 1. ФУНКЦИЯ-СКАЛЯР: возвращает ОДНО значение
# ============================================================
print("="*70)
print("1️⃣  ФУНКЦИЯ: get_total_revenue() — возвращает число")
print("="*70)

cur.execute("""
    CREATE OR REPLACE FUNCTION get_total_revenue()
    RETURNS NUMERIC AS $$
    DECLARE
        total NUMERIC;
    BEGIN
        SELECT COALESCE(SUM(total_sum), 0) INTO total FROM orders;
        RETURN total;
    END;
    $$ LANGUAGE plpgsql;
""")
print("✅ Функция get_total_revenue() создана")

# Тестируем
cur.execute("SELECT get_total_revenue();")
revenue = cur.fetchone()[0]
print(f"🔸 Общая выручка: {revenue} руб.")

# ============================================================
# 2. ФУНКЦИЯ-ТАБЛИЦА: возвращает ТАБЛИЦУ
# ============================================================
print("\n" + "="*70)
print("2️⃣  ФУНКЦИЯ: get_restaurant_stats() — возвращает таблицу")
print("="*70)

cur.execute("""
    CREATE OR REPLACE FUNCTION get_restaurant_stats()
    RETURNS TABLE (
        restaurant_name VARCHAR,
        orders_count BIGINT,
        revenue NUMERIC,
        avg_check NUMERIC
    ) AS $$
    BEGIN
        RETURN QUERY
        SELECT 
            restaurant,
            COUNT(*),
            ROUND(SUM(total_sum), 2),
            ROUND(AVG(total_sum), 2)
        FROM orders
        GROUP BY restaurant
        ORDER BY SUM(total_sum) DESC;
    END;
    $$ LANGUAGE plpgsql;
""")
print("✅ Функция get_restaurant_stats() создана")

# Тестируем
cur.execute("SELECT * FROM get_restaurant_stats();")
print("\n🔸 Статистика по ресторанам:")
print(f"{'Ресторан':<20} {'Заказов':<10} {'Выручка':<15} {'Ср.чек':<10}")
print("-" * 60)
for row in cur.fetchall():
    print(f"{row[0]:<20} {row[1]:<10} {row[2]:<15} {row[3]:<10}")

# ============================================================
# 3. ФУНКЦИЯ С ПАРАМЕТРАМИ: за период
# ============================================================
print("\n" + "="*70)
print("3️⃣  ФУНКЦИЯ: get_stats_by_period(from_date, to_date)")
print("="*70)

cur.execute("""
    CREATE OR REPLACE FUNCTION get_stats_by_period(
        from_date DATE,
        to_date DATE
    )
    RETURNS TABLE (
        restaurant_name VARCHAR,
        orders_count BIGINT,
        revenue NUMERIC
    ) AS $$
    BEGIN
        RETURN QUERY
        SELECT 
            restaurant,
            COUNT(*),
            ROUND(SUM(total_sum), 2)
        FROM orders
        WHERE created_at::date BETWEEN from_date AND to_date
        GROUP BY restaurant
        ORDER BY SUM(total_sum) DESC;
    END;
    $$ LANGUAGE plpgsql;
""")
print("✅ Функция get_stats_by_period() создана")

# Тестируем — за последние 30 дней
cur.execute("""
    SELECT * FROM get_stats_by_period(
        (CURRENT_DATE - INTERVAL '30 days')::date,
        CURRENT_DATE
    );
""")
print("\n🔸 Статистика за последние 30 дней:")
print(f"{'Ресторан':<20} {'Заказов':<10} {'Выручка':<15}")
print("-" * 50)
for row in cur.fetchall():
    print(f"{row[0]:<20} {row[1]:<10} {row[2]:<15}")

# ============================================================
# 4. ПРОЦЕДУРА: делает несколько операций (с транзакцией)
# ============================================================
print("\n" + "="*70)
print("4️⃣  ПРОЦЕДУРА: create_order() — вставляет заказ + валидация")
print("="*70)

cur.execute("""
    CREATE OR REPLACE PROCEDURE create_order(
        p_order_id INTEGER,
        p_user_id INTEGER,
        p_restaurant VARCHAR,
        p_restaurant_id INTEGER,
        p_total_sum NUMERIC
    )
    LANGUAGE plpgsql
    AS $$
    BEGIN
        -- Валидация
        IF p_total_sum <= 0 THEN
            RAISE EXCEPTION 'Сумма должна быть больше 0';
        END IF;
        
        IF p_order_id IS NULL THEN
            RAISE EXCEPTION 'order_id не может быть NULL';
        END IF;
        
        -- Вставка (триггер сам запишет в audit)
        INSERT INTO orders (
            order_id, user_id, restaurant, restaurant_id, 
            total_sum, status, created_at
        )
        VALUES (
            p_order_id, p_user_id, p_restaurant, p_restaurant_id,
            p_total_sum, 'created', NOW()
        );
        
        RAISE NOTICE 'Заказ % создан успешно', p_order_id;
    END;
    $$;
""")
print("✅ Процедура create_order() создана")

# Тестируем — создаем заказ через процедуру
print("\n🔸 Создаем заказ через процедуру...")

# Берем новый order_id
cur.execute("SELECT COALESCE(MAX(order_id), 0) + 1 FROM orders;")
new_id = cur.fetchone()[0]

cur.execute("CALL create_order(%s, %s, %s, %s, %s);",
            (new_id, 999, 'Тестовый', 1, 1500.00))
conn.commit()
print(f"✅ Заказ {new_id} создан через процедуру!")

# ============================================================
# 5. ПРОВЕРКА ВАЛИДАЦИИ (должна упасть)
# ============================================================
print("\n" + "="*70)
print("5️⃣  ПРОВЕРКА ВАЛИДАЦИИ: отрицательная сумма")
print("="*70)

try:
    cur.execute("CALL create_order(%s, %s, %s, %s, %s);",
                (new_id + 1, 999, 'Тест', 1, -100.00))
    conn.commit()
    print("❌ Ошибка: должна была упасть, но прошла!")
except psycopg2.errors.RaiseException as e:
    conn.rollback()
    print(f"✅ Валидация сработала! Ошибка: {e}")

# ============================================================
# 6. СМОТРИМ ВСЕ ФУНКЦИИ В БД
# ============================================================
print("\n" + "="*70)
print("6️⃣  СПИСОК ВСЕХ ФУНКЦИЙ В БД")
print("="*70)

cur.execute("""
    SELECT 
        routine_name,
        routine_type,
        data_type
    FROM information_schema.routines
    WHERE routine_schema = 'public'
    ORDER BY routine_name;
""")

print(f"{'Имя':<30} {'Тип':<15} {'Возвращает':<15}")
print("-" * 70)
for row in cur.fetchall():
    print(f"{row[0]:<30} {row[1]:<15} {row[2] or 'N/A':<15}")

cur.close()
conn.close()

print("\n" + "="*70)
print("🎉 ХРАНИМЫЕ ПРОЦЕДУРЫ РАБОТАЮТ!")
print("="*70)