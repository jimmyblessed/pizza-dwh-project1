import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

conn = psycopg2.connect(
    host=os.getenv("DB_HOST", "localhost"),
    port=5432,
    database="pizza_dwh",
    user="admin",
    password="admin123"
)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

cur = conn.cursor()
cur.execute("SELECT 1 FROM pg_database WHERE datname = 'airflow';")
exists = cur.fetchone()

if not exists:
    cur.execute("CREATE DATABASE airflow;")
    print("✅ База airflow создана")
else:
    print("ℹ️ База airflow уже существует")

cur.close()
conn.close()
