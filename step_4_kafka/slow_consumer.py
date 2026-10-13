import json
import time
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'orders_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='slow_consumer_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def main():
    print("🐢 Slow Consumer запущен (обработка 3 сек на сообщение)")
    print("=" * 60)
    
    counter = 0
    try:
        for message in consumer:
            order = message.value
            counter += 1
            print(f"🐢 [{counter}] order_id={order['order_id']} | partition={message.partition} | offset={message.offset}")
            time.sleep(3)   # ← задержка 3 секунды
    except KeyboardInterrupt:
        print(f"\n🛑 Остановлен. Обработано: {counter}")

if __name__ == "__main__":
    main()