import psycopg2
import os

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=5432,
        database="pizza_dwh",
        user="admin",
        password="admin123"
    )

def load_dim_restaurants():
    conn = get_connection()
    cur = conn.cursor()
    
    # Берем уникальные рестораны из сырой таблицы orders
    cur.execute("""
        INSERT INTO dim_restaurants (restaurant_id, restaurant_name)
        SELECT DISTINCT 
            restaurant_id,
            restaurant
        FROM orders
        ON CONFLICT (restaurant_id) DO NOTHING;
    """)
    
    conn.commit()
    
    # Проверяем, сколько загрузилось
    cur.execute("SELECT COUNT(*) FROM dim_restaurants;")
    count = cur.fetchone()[0]
    print(f"✅ Загружено ресторанов: {count}")
    
    # Показываем список
    cur.execute("SELECT * FROM dim_restaurants ORDER BY restaurant_id;")
    for row in cur.fetchall():
        print(f"   {row[0]}: {row[1]}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    load_dim_restaurants()