import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

RESTAURANTS = ["Суши-Мастер", "Пицца-Хаус", "Бургер-Кинг", "Вок-Экспресс", "Грузинский дворик"]

def generate_order(order_id):
    # Каждое 5-е сообщение — "плохое" (для теста DLQ)
    is_bad = (order_id % 5 == 0)
    
    if is_bad:
        # Плохое: total_sum отрицательная или NULL
        total_sum = -100 if order_id % 10 == 0 else None
        print(f"⚠️  Генерим ПЛОХОЕ сообщение #{order_id} (total_sum={total_sum})")
    else:
        total_sum = round(random.uniform(200, 5000), 2)
        print(f"✅ Генерим хорошее сообщение #{order_id}")
    
    return {
        "order_id": order_id,
        "user_id": random.randint(1, 1000),
        "restaurant": random.choice(RESTAURANTS),
        "restaurant_id": random.randint(1, 5),
        "total_sum": total_sum,
        "status": "created",
        "created_at": datetime.now().isoformat()
    }

def main():
    print("🚀 Producer (с ошибками) запущен!")
    print("=" * 60)
    
    try:
        for order_id in range(1, 21):
            order = generate_order(order_id)
            producer.send('orders_topic', value=order)
            producer.flush()
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        producer.close()
        print("\n🛑 Producer остановлен")

if __name__ == "__main__":
    main()