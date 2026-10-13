import os
import psycopg2

conn = psycopg2.connect(
    host=os.getenv("DB_HOST", "localhost"),
    port=5432,
    database="pizza_dwh",
    user="admin",
    password="admin123"
)

cur = conn.cursor()
cur.execute("SELECT COUNT(*) FROM orders;")
count = cur.fetchone()[0]
print(f"📊 Всего заказов в БД: {count}")

cur.execute("SELECT * FROM orders ORDER BY order_id DESC LIMIT 50;")
orders = cur.fetchall()
for order in orders:
    print(order)

cur.close()
conn.close()
