import os
import psycopg2

def create_tables():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=5432,
        database="pizza_dwh",
        user="admin",
        password="admin123"
    )
    
    cur = conn.cursor()
    
    # Создаем таблицу заказов
    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            restaurant VARCHAR(100),
            restaurant_id INTEGER,
            total_sum DECIMAL(10, 2),
            status VARCHAR(50),
            created_at TIMESTAMP
        );
    """)
    
    conn.commit()
    print("✅ Таблица orders создана!")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    create_tables()
