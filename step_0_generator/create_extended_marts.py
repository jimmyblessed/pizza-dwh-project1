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

def create_extended_marts():
    conn = get_connection()
    cur = conn.cursor()
    
    # ============================================================
    # ВИТРИНА 1: Статистика по дням недели
    # ============================================================
    cur.execute("""
        DROP TABLE IF EXISTS dm_day_of_week_stats;
        
        CREATE TABLE dm_day_of_week_stats AS
        SELECT 
            d.day_of_week,
            CASE d.day_of_week
                WHEN 1 THEN 'Понедельник'
                WHEN 2 THEN 'Вторник'
                WHEN 3 THEN 'Среда'
                WHEN 4 THEN 'Четверг'
                WHEN 5 THEN 'Пятница'
                WHEN 6 THEN 'Суббота'
                WHEN 7 THEN 'Воскресенье'
            END AS day_name,
            COUNT(*) AS orders_count,
            SUM(f.total_sum) AS revenue,
            ROUND(AVG(f.total_sum)::numeric, 2) AS avg_check
        FROM fact_orders f
        JOIN dim_date d ON f.date_id = d.date_id
        GROUP BY d.day_of_week
        ORDER BY d.day_of_week;
    """)
    print("✅ dm_day_of_week_stats создана")
    
    # ============================================================
    # ВИТРИНА 2: Статистика по часам
    # ============================================================
    cur.execute("""
        DROP TABLE IF EXISTS dm_hourly_stats;
        
        CREATE TABLE dm_hourly_stats AS
        SELECT 
            EXTRACT(HOUR FROM f.created_at)::int AS hour,
            COUNT(*) AS orders_count,
            SUM(f.total_sum) AS revenue,
            ROUND(AVG(f.total_sum)::numeric, 2) AS avg_check
        FROM fact_orders f
        GROUP BY EXTRACT(HOUR FROM f.created_at)
        ORDER BY hour;
    """)
    print("✅ dm_hourly_stats создана")
    
    # ============================================================
    # ВИТРИНА 3: Топ пользователей
    # ============================================================
    cur.execute("""
        DROP TABLE IF EXISTS dm_top_users;
        
        CREATE TABLE dm_top_users AS
        SELECT 
            f.user_id,
            COUNT(*) AS orders_count,
            SUM(f.total_sum) AS total_spent,
            ROUND(AVG(f.total_sum)::numeric, 2) AS avg_check,
            MIN(f.created_at) AS first_order,
            MAX(f.created_at) AS last_order
        FROM fact_orders f
        GROUP BY f.user_id
        ORDER BY total_spent DESC;
    """)
    print("✅ dm_top_users создана")
    
    # ============================================================
    # ВИТРИНА 4: Динамика по дням с ростом
    # ============================================================
    cur.execute("""
        DROP TABLE IF EXISTS dm_daily_dynamics;
        
        CREATE TABLE dm_daily_dynamics AS
        WITH daily AS (
            SELECT 
                d.full_date,
                COUNT(*) AS orders_count,
                SUM(f.total_sum) AS revenue
            FROM fact_orders f
            JOIN dim_date d ON f.date_id = d.date_id
            GROUP BY d.full_date
        )
        SELECT 
            full_date,
            orders_count,
            revenue,
            LAG(revenue) OVER (ORDER BY full_date) AS prev_revenue,
            ROUND(
                (revenue - LAG(revenue) OVER (ORDER BY full_date)) 
                * 100.0 / NULLIF(LAG(revenue) OVER (ORDER BY full_date), 0), 
                2
            ) AS revenue_growth_percent
        FROM daily
        ORDER BY full_date;
    """)
    print("✅ dm_daily_dynamics создана")
    
    # ============================================================
    # ВИТРИНА 5: Отмены по ресторанам
    # ============================================================
    cur.execute("""
        DROP TABLE IF EXISTS dm_cancellations;
        
        CREATE TABLE dm_cancellations AS
        SELECT 
            r.restaurant_name,
            COUNT(*) AS total_orders,
            COUNT(CASE WHEN f.status = 'cancelled' THEN 1 END) AS cancelled_orders,
            ROUND(
                COUNT(CASE WHEN f.status = 'cancelled' THEN 1 END) * 100.0 / COUNT(*),
                2
            ) AS cancel_rate_percent,
            SUM(CASE WHEN f.status = 'cancelled' THEN f.total_sum ELSE 0 END) AS lost_revenue
        FROM fact_orders f
        JOIN dim_restaurants r ON f.restaurant_id = r.restaurant_id
        GROUP BY r.restaurant_name
        ORDER BY cancel_rate_percent DESC;
    """)
    print("✅ dm_cancellations создана")
    
    conn.commit()
    
    # Показываем примеры
    print("\n📊 Пример: Топ-5 пользователей")
    cur.execute("SELECT user_id, orders_count, total_spent FROM dm_top_users LIMIT 5;")
    for row in cur.fetchall():
        print(f"   Юзер {row[0]}: {row[1]} заказов, {row[2]} руб.")
    
    print("\n📊 Пример: Статистика по дням недели")
    cur.execute("SELECT day_name, orders_count, revenue FROM dm_day_of_week_stats;")
    for row in cur.fetchall():
        print(f"   {row[0]}: {row[1]} заказов, {row[2]} руб.")
    
    print("\n📊 Пример: Отмены по ресторанам")
    cur.execute("SELECT restaurant_name, cancel_rate_percent, lost_revenue FROM dm_cancellations;")
    for row in cur.fetchall():
        print(f"   {row[0]}: {row[1]}% отмен, потеряно {row[2]} руб.")
    
    cur.close()
    conn.close()
    print("\n🎉 Все расширенные витрины созданы!")

if __name__ == "__main__":
    create_extended_marts()
