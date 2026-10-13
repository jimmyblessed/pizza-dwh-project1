import os
import json
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List
import csv
from pathlib import Path

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
    
    order = {
        "order_id": order_id,
        "user_id": random.randint(1, 1000),
        "restaurant": restaurant,
        "restaurant_id": RESTAURANTS.index(restaurant) + 1,
        "total_sum": total_sum,
        "status": status,
        "created_at": created_at.isoformat()
    }
    return order

def save_orders_to_csv(orders: List[Dict], batch_number: int) -> None:
    Path("data").mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"data/orders_batch_{timestamp}.csv"
    
    fieldnames = ["order_id", "user_id", "restaurant", "restaurant_id", 
                  "total_sum", "status", "created_at"]
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(orders)
    
    print(f"💾 Сохранено {len(orders)} заказов в {filename}")

def main():
    print("🚀 Генератор заказов запущен! Нажми Ctrl+C для остановки.")
    print("=" * 50)
    
    order_counter = 1
    batch = []
    batch_size = 10
    batch_number = 1
    
    try:
        while True:
            order = generate_order(order_counter)
            batch.append(order)
            order_counter += 1
            
            if len(batch) >= batch_size:
                save_orders_to_csv(batch, batch_number)
                batch = []
                batch_number += 1
            
            time.sleep(1)
            
    except KeyboardInterrupt:
        if batch:
            save_orders_to_csv(batch, batch_number)
            print(f"💾 Сохранены остатки: {len(batch)} заказов")
        
        print("\n" + "=" * 50)
        print(f"🛑 Генератор остановлен. Всего создано заказов: {order_counter - 1}")

if __name__ == "__main__":
    main()
