import os
import psycopg2
from datetime import datetime

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=5432,
        database="pizza_dwh",
        user="admin",
        password="admin123"
    )

def run_query(query, description, column_names):
    """Выполняет запрос и выводит результат в виде таблицы"""
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute(query)
    results = cur.fetchall()
    
    print(f"\n{'='*70}")
    print(f"📊 {description}")
    print('='*70)
    
    if results:
        # Выводим названия колонок
        header = " | ".join([f"{name:>15}" for name in column_names])
        print(header)
        print("-" * len(header))
        
        # Выводим строки
        for row in results:
            formatted_row = []
            for i, value in enumerate(row):
                # Форматируем значения для красивого вывода
                if isinstance(value, Decimal):
                    formatted_row.append(f"{value:>15.2f}")
                elif isinstance(value, datetime):
                    formatted_row.append(f"{value.strftime('%Y-%m-%d %H:%M'):>15}")
                elif isinstance(value, str):
                    formatted_row.append(f"{value:>15}")
                else:
                    formatted_row.append(f"{value:>15}")
            print(" | ".join(formatted_row))
    else:
        print("(нет данных)")
    
    cur.close()
    conn.close()
    return results

# Добавляем импорт Decimal
from decimal import Decimal

# ---- 1. Общая статистика ----
query1 = """
    SELECT 
        COUNT(*) as total_orders,
        COUNT(DISTINCT user_id) as unique_users,
        ROUND(AVG(total_sum)::numeric, 2) as avg_check,
        SUM(total_sum) as total_revenue
    FROM orders;
"""
run_query(query1, "Общая статистика по заказам", 
          ["Всего заказов", "Уникальных юзеров", "Средний чек", "Общая выручка"])

# ---- 2. Топ-5 ресторанов по выручке ----
query2 = """
    SELECT 
        restaurant,
        COUNT(*) as orders_count,
        SUM(total_sum) as revenue,
        ROUND(AVG(total_sum)::numeric, 2) as avg_check
    FROM orders
    GROUP BY restaurant
    ORDER BY revenue DESC
    LIMIT 5;
"""
run_query(query2, "Топ-5 ресторанов по выручке", 
          ["Ресторан", "Заказов", "Выручка", "Средний чек"])

# ---- 3. Статусы заказов ----
query3 = """
    SELECT 
        status,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
    FROM orders
    GROUP BY status
    ORDER BY count DESC;
"""
run_query(query3, "Распределение заказов по статусам", 
          ["Статус", "Количество", "Процент"])

# ---- 4. Динамика по дням (последние 7 дней) ----
query4 = """
    SELECT 
        DATE(created_at) as day,
        COUNT(*) as orders,
        SUM(total_sum) as revenue,
        ROUND(AVG(total_sum)::numeric, 2) as avg_check
    FROM orders
    WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY DATE(created_at)
    ORDER BY day DESC;
"""
run_query(query4, "Динамика заказов за последние 7 дней", 
          ["Дата", "Заказов", "Выручка", "Средний чек"])

# ---- 5. Самый дорогой заказ ----
query5 = """
    SELECT 
        order_id,
        restaurant,
        total_sum,
        status,
        created_at
    FROM orders
    ORDER BY total_sum DESC
    LIMIT 1;
"""
run_query(query5, "Самый дорогой заказ", 
          ["ID заказа", "Ресторан", "Сумма", "Статус", "Время"])

print("\n✅ Все запросы выполнены!")
