import os
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List
import psycopg2
from psycopg2 import sql

# ---- КОНФИГУРАЦИЯ ----
RESTAURANTS: List[str] = [
    "Суши-Мастер", 
    "Пицца-Хаус", 
    "Бургер-Кинг", 
    "Вок-Экспресс", 
    "Грузинский дворик"
]

STATUSES: List[str] = [
    "created", "confirmed", "cooking", 
    "delivering", "delivered", "cancelled"
]

# ---- ПОДКЛЮЧЕНИЕ К БД ----
def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=5432,
        database="pizza_dwh",
        user="admin",
        password="admin123"
    )

# ---- ГЕНЕРАЦИЯ ОДНОГО ЗАКАЗА ----
def generate_order(order_id: int) -> Dict:
    restaurant = random.choice(RESTAURANTS)
    total_sum = round(random.uniform(200, 5000), 2)
    status = random.choices(
        STATUSES, 
        weights=[5, 10, 15, 20, 40, 10],
        k=1
    )[0]
    days_ago = random.randint(0, 7)
    hours_ago = random.randint(0, 23)
    minutes_ago = random.randint(0, 59)
    created_at = datetime.now() - timedelta(days=days_ago, hours=hours_ago, minutes=minutes_ago)
    
    return {
        "order_id": order_id,
        "user_id": random.randint(1, 1000),
        "restaurant": restaurant,
        "restaurant_id": RESTAURANTS.index(restaurant) + 1,
        "total_sum": total_sum,
        "status": status,
        "created_at": created_at
    }

# ---- СОХРАНЕНИЕ В БД ----
def save_order_to_db(order: Dict) -> None:
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO orders (order_id, user_id, restaurant, restaurant_id, 
                           total_sum, status, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_id) DO NOTHING;
    """, (
        order["order_id"],
        order["user_id"],
        order["restaurant"],
        order["restaurant_id"],
        order["total_sum"],
        order["status"],
        order["created_at"]
    ))
    
    conn.commit()
    cur.close()
    conn.close()

# ---- ГЛАВНАЯ ФУНКЦИЯ ----
def main():
    print("🚀 Генератор заказов запущен! (Запись в PostgreSQL)")
    print("=" * 50)
    
    order_counter = 1
    
    try:
        while True:
            order = generate_order(order_counter)
            save_order_to_db(order)
            print(f"✅ Заказ #{order['order_id']} сохранен в БД | {order['restaurant']} | {order['total_sum']} руб.")
            
            order_counter += 1
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n" + "=" * 50)
        print(f"🛑 Генератор остановлен. Всего создано заказов: {order_counter - 1}")

if __name__ == "__main__":
    main()
