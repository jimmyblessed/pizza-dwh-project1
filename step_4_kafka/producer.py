import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

# ---- КОНФИГУРАЦИЯ ----
RESTAURANTS = [
    "Суши-Мастер", 
    "Пицца-Хаус", 
    "Бургер-Кинг", 
    "Вок-Экспресс", 
    "Грузинский дворик"
]

STATUSES = ["created", "confirmed", "cooking", "delivering", "delivered", "cancelled"]

# ---- ПОДКЛЮЧЕНИЕ К KAFKA ----
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

# ---- ГЕНЕРАЦИЯ ЗАКАЗА ----
def generate_order(order_id: int) -> dict:
    restaurant = random.choice(RESTAURANTS)
    total_sum = round(random.uniform(200, 5000), 2)
    status = random.choices(STATUSES, weights=[5, 10, 15, 20, 40, 10], k=1)[0]
    
    days_ago = random.randint(0, 7)
    hours_ago = random.randint(0, 23)
    created_at = datetime.now() - timedelta(days=days_ago, hours=hours_ago)
    
    return {
        "order_id": order_id,
        "user_id": random.randint(1, 1000),
        "restaurant": restaurant,
        "restaurant_id": RESTAURANTS.index(restaurant) + 1,
        "total_sum": total_sum,
        "status": status,
        "created_at": created_at.isoformat()
    }

# ---- ГЛАВНЫЙ ЦИКЛ ----
def main():
    print("🚀 Producer запущен! Отправляем заказы в Kafka...")
    print("=" * 60)
    
    order_counter = 1
    
    try:
        while True:
            order = generate_order(order_counter)
            
            # Отправляем в топик orders_topic
            future = producer.send('orders_topic', value=order)
            result = future.get(timeout=10)
            
            print(f"📤 Отправлено: order_id={order['order_id']} | "
                  f"{order['restaurant']} | {order['total_sum']} руб. | "
                  f"partition={result.partition}")
            
            order_counter += 1
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n" + "=" * 60)
        print(f"🛑 Producer остановлен. Отправлено: {order_counter - 1} заказов")
        producer.close()

if __name__ == "__main__":
    main()