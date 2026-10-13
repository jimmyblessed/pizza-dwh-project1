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

def create_mart():
    conn = get_connection()
    cur = conn.cursor()
    
    # Создаем витрину (MART) — готовую таблицу для BI
    cur.execute("""
        DROP TABLE IF EXISTS dm_restaurant_stats;
        
        CREATE TABLE dm_restaurant_stats AS
        SELECT 
            r.restaurant_name,
            d.year,
            d.month,
            d.day,
            COUNT(*) as orders_count,
            SUM(f.total_sum) as revenue,
            ROUND(AVG(f.total_sum)::numeric, 2) as avg_check,
            COUNT(CASE WHEN f.status = 'delivered' THEN 1 END) as delivered_count,
            COUNT(CASE WHEN f.status = 'cancelled' THEN 1 END) as cancelled_count
        FROM fact_orders f
        JOIN dim_restaurants r ON f.restaurant_id = r.restaurant_id
        JOIN dim_date d ON f.date_id = d.date_id
        GROUP BY r.restaurant_name, d.year, d.month, d.day;
    """)
    
    conn.commit()
    
    cur.execute("SELECT COUNT(*) FROM dm_restaurant_stats;")
    count = cur.fetchone()[0]
    print(f"✅ Витрина dm_restaurant_stats создана")
    print(f"   Строк в витрине: {count}")
    
    # Показываем пример
    print("\n📊 Пример данных из витрины:")
    cur.execute("""
        SELECT * FROM dm_restaurant_stats 
        ORDER BY revenue DESC 
        LIMIT 5;
    """)
    
    print(f"{'Ресторан':<20} {'Год':<6} {'Мес':<4} {'День':<5} {'Заказов':<8} {'Выручка':<12} {'Ср.чек':<10}")
    print("-" * 80)
    for row in cur.fetchall():
        print(f"{row[0]:<20} {row[1]:<6} {row[2]:<4} {row[3]:<5} {row[4]:<8} {row[5]:<12.2f} {row[6]:<10.2f}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    create_mart()
