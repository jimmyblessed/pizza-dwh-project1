import psycopg2

def get_connection():
    return psycopg2.connect(
        host="localhost", port=5432,
        database="pizza_dwh", user="admin", password="admin123"
    )

conn = get_connection()
cur = conn.cursor()

# ============================================================
# 1. СОЗДАЕМ ТАБЛИЦУ AUDIT
# ============================================================
print("="*70)
print("1️⃣  СОЗДАЕМ ТАБЛИЦУ orders_audit")
print("="*70)

cur.execute("DROP TABLE IF EXISTS orders_audit CASCADE;")

cur.execute("""
    CREATE TABLE orders_audit (
        audit_id SERIAL PRIMARY KEY,
        operation VARCHAR(10) NOT NULL,
        order_id INTEGER,
        old_data JSONB,
        new_data JSONB,
        changed_by VARCHAR(100) DEFAULT CURRENT_USER,
        changed_at TIMESTAMP DEFAULT NOW()
    );
""")
print("✅ Таблица orders_audit создана")

# ============================================================
# 2. СОЗДАЕМ ФУНКЦИЮ-ТРИГГЕР
# ============================================================
print("\n" + "="*70)
print("2️⃣  СОЗДАЕМ ФУНКЦИЮ track_orders_changes()")
print("="*70)

cur.execute("""
    CREATE OR REPLACE FUNCTION track_orders_changes()
    RETURNS TRIGGER AS $$
    BEGIN
        IF TG_OP = 'INSERT' THEN
            INSERT INTO orders_audit (operation, order_id, new_data)
            VALUES ('INSERT', NEW.order_id, to_jsonb(NEW));
            RETURN NEW;
        
        ELSIF TG_OP = 'UPDATE' THEN
            INSERT INTO orders_audit (operation, order_id, old_data, new_data)
            VALUES ('UPDATE', NEW.order_id, to_jsonb(OLD), to_jsonb(NEW));
            RETURN NEW;
        
        ELSIF TG_OP = 'DELETE' THEN
            INSERT INTO orders_audit (operation, order_id, old_data)
            VALUES ('DELETE', OLD.order_id, to_jsonb(OLD));
            RETURN OLD;
        END IF;
    END;
    $$ LANGUAGE plpgsql;
""")
print("✅ Функция track_orders_changes() создана")

# ============================================================
# 3. ВЕШАЕМ ТРИГГЕР НА ТАБЛИЦУ orders
# ============================================================
print("\n" + "="*70)
print("3️⃣  ВЕШАЕМ ТРИГГЕР НА orders")
print("="*70)

cur.execute("DROP TRIGGER IF EXISTS trg_orders_audit ON orders;")

cur.execute("""
    CREATE TRIGGER trg_orders_audit
    AFTER INSERT OR UPDATE OR DELETE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION track_orders_changes();
""")
print("✅ Триггер trg_orders_audit создан")
print("   События: INSERT, UPDATE, DELETE")
print("   Режим: FOR EACH ROW")

conn.commit()

# ============================================================
# 4. БЕРЕМ МАКСИМАЛЬНЫЙ order_id И ДОБАВЛЯЕМ 1
# ============================================================
cur.execute("SELECT COALESCE(MAX(order_id), 0) + 1 FROM orders;")
new_order_id = cur.fetchone()[0]
print(f"\n📌 Будем использовать order_id = {new_order_id}")

# ============================================================
# 5. ТЕСТИРУЕМ: INSERT
# ============================================================
print("\n" + "="*70)
print("5️⃣  ТЕСТ: INSERT нового заказа")
print("="*70)

cur.execute("""
    INSERT INTO orders (order_id, user_id, restaurant, restaurant_id, total_sum, status, created_at)
    VALUES (%s, 555, 'Тестовый ресторан', 5, 1000.00, 'created', NOW());
""", (new_order_id,))
conn.commit()
print(f"✅ Вставлен заказ order_id={new_order_id}")

# ============================================================
# 6. ТЕСТИРУЕМ: UPDATE
# ============================================================
print("\n" + "="*70)
print("6️⃣  ТЕСТ: UPDATE статуса")
print("="*70)

cur.execute("""
    UPDATE orders SET status = 'delivered' WHERE order_id = %s;
""", (new_order_id,))
conn.commit()
print(f"✅ Обновлен статус заказа order_id={new_order_id} → 'delivered'")

# ============================================================
# 7. ТЕСТИРУЕМ: DELETE
# ============================================================
print("\n" + "="*70)
print("7️⃣  ТЕСТ: DELETE заказа")
print("="*70)

cur.execute("DELETE FROM orders WHERE order_id = %s;", (new_order_id,))
conn.commit()
print(f"✅ Удален заказ order_id={new_order_id}")

# ============================================================
# 8. СМОТРИМ AUDIT LOG
# ============================================================
print("\n" + "="*70)
print("8️⃣  AUDIT LOG (что записал триггер)")
print("="*70)

cur.execute("""
    SELECT audit_id, operation, order_id, changed_by, changed_at
    FROM orders_audit
    ORDER BY audit_id;
""")

print(f"\n{'ID':<5} {'OP':<10} {'ORDER_ID':<12} {'CHANGED_BY':<15} {'CHANGED_AT':<25}")
print("-" * 70)
for row in cur.fetchall():
    print(f"{row[0]:<5} {row[1]:<10} {row[2]:<12} {row[3]:<15} {str(row[4]):<25}")

# ============================================================
# 9. ДЕТАЛИ UPDATE
# ============================================================
print("\n" + "="*70)
print("9️⃣  ДЕТАЛИ UPDATE (что изменилось)")
print("="*70)

cur.execute("""
    SELECT 
        operation,
        old_data->>'status' as old_status,
        new_data->>'status' as new_status
    FROM orders_audit
    WHERE operation = 'UPDATE';
""")

for row in cur.fetchall():
    print(f"🔸 Операция: {row[0]}")
    print(f"   Старый статус: {row[1]}")
    print(f"   Новый статус:  {row[2]}")

cur.close()
conn.close()

print("\n" + "="*70)
print("🎉 ТРИГГЕРЫ РАБОТАЮТ!")
print("="*70)
print("\n💡 Открой DBeaver → orders_audit → увидишь все изменения")