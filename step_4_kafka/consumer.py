import json
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime

# ---- ПОДКЛЮЧЕНИЕ К POSTGRESQL ----
def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="pizza_dwh",
        user="admin",
        password="admin123"
    )

# ---- ПОДКЛЮЧЕНИЕ К KAFKA ----
consumer = KafkaConsumer(
    'orders_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',        # Читать с начала, если нет offset'а
    enable_auto_commit=True,             # Автоматически сохранять offset
    group_id='pizza_consumer_group',     # Имя группы потребителей
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# ---- СОХРАНЕНИЕ В БД ----
def save_to_db(order: dict):
    conn = get_db_connection()
    cur = conn.cursor()
    
    try:
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
        return True
    except Exception as e:
        conn.rollback()
        print(f"❌ Ошибка сохранения: {e}")
        return False
    finally:
        cur.close()
        conn.close()

# ---- ГЛАВНЫЙ ЦИКЛ ----
def main():
    print("🚀 Consumer запущен! Читаем из Kafka → пишем в PostgreSQL...")
    print(f"📌 Топик: orders_topic")
    print(f"📌 Consumer Group: pizza_consumer_group")
    print("=" * 70)
    
    counter = 0
    
    try:
        for message in consumer:
            order = message.value
            partition = message.partition
            offset = message.offset
            
            if save_to_db(order):
                counter += 1
                print(f"✅ [{counter}] order_id={order['order_id']} | "
                      f"partition={partition} | offset={offset} | "
                      f"{order['restaurant']} | {order['total_sum']} руб.")
            
    except KeyboardInterrupt:
        print("\n" + "=" * 70)
        print(f"🛑 Consumer остановлен. Обработано сообщений: {counter}")
        consumer.close()

if __name__ == "__main__":
    main()