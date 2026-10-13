import json
import psycopg2
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime

# ---- ПРОДЮСЕР ДЛЯ DLQ ----
dlq_producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

def get_db_connection():
    return psycopg2.connect(
        host="localhost", port=5432,
        database="pizza_dwh", user="admin", password="admin123"
    )

def validate_order(order):
    """
    Валидация: возвращает (is_valid, error_message).
    """
    if order.get("total_sum") is None:
        return False, "total_sum is NULL"
    
    if order.get("total_sum") <= 0:
        return False, f"total_sum must be > 0, got {order['total_sum']}"
    
    if not order.get("restaurant"):
        return False, "restaurant is empty"
    
    if not order.get("order_id"):
        return False, "order_id is missing"
    
    return True, None

def save_to_db(order):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO orders (order_id, user_id, restaurant, restaurant_id,
                               total_sum, status, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING;
        """, (
            order["order_id"], order["user_id"], order["restaurant"],
            order["restaurant_id"], order["total_sum"], order["status"],
            order["created_at"]
        ))
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"❌ DB ошибка: {e}")
        return False
    finally:
        cur.close()
        conn.close()

def send_to_dlq(order, error_reason):
    """
    Отправляет "плохое" сообщение в DLQ с причиной.
    """
    dlq_message = {
        "original_order": order,
        "error_reason": error_reason,
        "failed_at": datetime.now().isoformat()
    }
    dlq_producer.send('orders_topic_dlq', value=dlq_message)
    dlq_producer.flush()
    print(f"💀 Отправлено в DLQ: order_id={order.get('order_id')}, причина: {error_reason}")

# ---- CONSUMER ----
consumer = KafkaConsumer(
    'orders_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=False,    # Ручной commit!
    group_id='pizza_consumer_group_v2',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def main():
    print("🚀 Consumer (с DLQ) запущен!")
    print("=" * 60)
    
    stats = {"success": 0, "failed": 0}
    
    try:
        for message in consumer:
            order = message.value
            order_id = order.get('order_id', 'unknown')
            
            # Валидация
            is_valid, error = validate_order(order)
            
            if not is_valid:
                # Плохое сообщение → DLQ
                send_to_dlq(order, error)
                stats["failed"] += 1
            else:
                # Хорошее → в БД
                if save_to_db(order):
                    stats["success"] += 1
                    print(f"✅ [{stats['success']}] order_id={order_id} сохранён")
                else:
                    send_to_dlq(order, "DB insert failed")
                    stats["failed"] += 1
            
            # Ручной commit (после обработки)
            consumer.commit()
    
    except KeyboardInterrupt:
        print("\n" + "=" * 60)
        print(f"🛑 Consumer остановлен")
        print(f"✅ Успешно: {stats['success']}")
        print(f"💀 В DLQ:   {stats['failed']}")
    finally:
        consumer.close()
        dlq_producer.close()

if __name__ == "__main__":
    main()