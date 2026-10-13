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

def create_star_schema():
    conn = get_connection()
    cur = conn.cursor()
    
    # ---- 1. ИЗМЕРЕНИЕ: Рестораны ----
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_restaurants (
            restaurant_id INTEGER PRIMARY KEY,
            restaurant_name VARCHAR(100) NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)
    print("✅ dim_restaurants создана")
    
    # ---- 2. ИЗМЕРЕНИЕ: Дата (календарь) ----
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_date (
            date_id INTEGER PRIMARY KEY,          -- Формат: 20260705
            full_date DATE NOT NULL,
            day INTEGER NOT NULL,
            month INTEGER NOT NULL,
            year INTEGER NOT NULL,
            quarter INTEGER NOT NULL,
            day_of_week INTEGER NOT NULL,         -- 1=Пн, 7=Вс
            is_weekend BOOLEAN NOT NULL
        );
    """)
    print("✅ dim_date создана")
    
    # ---- 3. ФАКТ: Заказы ----
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_orders (
            order_id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            restaurant_id INTEGER NOT NULL REFERENCES dim_restaurants(restaurant_id),
            date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
            total_sum DECIMAL(10, 2) NOT NULL,
            status VARCHAR(50) NOT NULL,
            created_at TIMESTAMP NOT NULL
        );
    """)
    print("✅ fact_orders создана")
    
    conn.commit()
    cur.close()
    conn.close()
    print("\n🎉 Модель 'Звезда' создана!")

if __name__ == "__main__":
    create_star_schema()
