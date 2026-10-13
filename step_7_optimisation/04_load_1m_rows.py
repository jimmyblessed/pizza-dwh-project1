import psycopg2
import random
from datetime import datetime, timedelta

def get_connection():
    return psycopg2.connect(
        host="localhost", port=5432,
        database="pizza_dwh", user="admin", password="admin123"
    )

conn = get_connection()
cur = conn.cursor()

RESTAURANTS = ["Суши-Мастер", "Пицца-Хаус", "Бургер-Кинг", "Вок-Экспресс", "Грузинский дворик"]
STATUSES = ["created", "confirmed", "cooking", "delivering", "delivered", "cancelled"]

print("🚀 Загружаем 1 000 000 строк в orders...")

batch_size = 10000
total = 1_000_000

for batch_start in range(0, total, batch_size):
    rows = []
    for i in range(batch_size):
        order_id = 1000 + batch_start + i
        rows.append((
            order_id,
            random.randint(1, 100000),
            random.choice(RESTAURANTS),
            random.randint(1, 5),
            round(random.uniform(200, 5000), 2),
            random.choice(STATUSES),
            datetime.now() - timedelta(days=random.randint(0, 365))
        ))
    
    args = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s)", row).decode() for row in rows)
    cur.execute(f"INSERT INTO orders (order_id, user_id, restaurant, restaurant_id, total_sum, status, created_at) VALUES {args} ON CONFLICT DO NOTHING")
    conn.commit()
    
    if (batch_start + batch_size) % 100000 == 0:
        print(f"   Загружено: {batch_start + batch_size:,}")

print("✅ Готово!")

# Проверяем
cur.execute("SELECT COUNT(*) FROM orders")
print(f"📊 Всего строк в orders: {cur.fetchone()[0]:,}")

cur.close()
conn.close()