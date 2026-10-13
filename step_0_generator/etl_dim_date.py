import os
import psycopg2
from datetime import timedelta

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=5432,
        database="pizza_dwh",
        user="admin",
        password="admin123"
    )

def load_dim_date():
    conn = get_connection()
    cur = conn.cursor()
    
    # 1. Чистим старый календарь
    cur.execute("TRUNCATE TABLE dim_date CASCADE;")
    
    # 2. Берем реальные даты из orders
    cur.execute("SELECT MIN(created_at)::date, MAX(created_at)::date FROM orders;")
    min_date, max_date = cur.fetchone()
    print(f"📅 Диапазон дат из orders: {min_date} — {max_date}")
    
    # 3. Генерируем календарь под эти даты
    current = min_date
    while current <= max_date:
        date_id = int(current.strftime("%Y%m%d"))
        
        cur.execute("""
            INSERT INTO dim_date (
                date_id, full_date, day, month, year, 
                quarter, day_of_week, is_weekend
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_id) DO NOTHING;
        """, (
            date_id,
            current,
            current.day,
            current.month,
            current.year,
            (current.month - 1) // 3 + 1,
            current.weekday() + 1,
            current.weekday() >= 5
        ))
        
        current += timedelta(days=1)
    
    conn.commit()
    
    cur.execute("SELECT COUNT(*) FROM dim_date;")
    count = cur.fetchone()[0]
    print(f"✅ Загружено дней в календаре: {count}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    load_dim_date()
