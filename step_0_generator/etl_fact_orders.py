import os
import psycopg2

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=5432,
        database="pizza_dwh",
        user="admin",
        password="admin123"
    )

def load_fact_orders():
    conn = get_connection()
    cur = conn.cursor()
    
    # ETL: Extract → Transform → Load
    cur.execute("""
        INSERT INTO fact_orders (
            order_id, user_id, restaurant_id, date_id, 
            total_sum, status, created_at
        )
        SELECT 
            o.order_id,
            o.user_id,
            o.restaurant_id,
            CAST(TO_CHAR(o.created_at, 'YYYYMMDD') AS INTEGER) AS date_id,
            o.total_sum,
            o.status,
            o.created_at
        FROM orders o
        ON CONFLICT (order_id) DO NOTHING;
    """)
    
    rows_inserted = cur.rowcount
    conn.commit()
    
    # Проверяем
    cur.execute("SELECT COUNT(*) FROM fact_orders;")
    count = cur.fetchone()[0]
    print(f"✅ Загружено фактов (заказов): {count}")
    print(f"   Вставлено новых строк: {rows_inserted}")
    
    # Проверяем целостность
    cur.execute("""
        SELECT 
            COUNT(*) as total,
            COUNT(DISTINCT restaurant_id) as restaurants,
            COUNT(DISTINCT date_id) as dates
        FROM fact_orders;
    """)
    result = cur.fetchone()
    print(f"   Уникальных ресторанов: {result[1]}")
    print(f"   Уникальных дат: {result[2]}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    load_fact_orders()
