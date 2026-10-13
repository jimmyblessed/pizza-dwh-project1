from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import psycopg2


def get_connection():
    return psycopg2.connect(
        host="pizza_postgres", port=5432,
        database="pizza_dwh", user="admin", password="admin123"
    )


def make_stats_function(restaurant_name):
    """
    Фабрика функций: создаёт функцию для конкретного ресторана.
    Это называется "closure" (замыкание).
    """
    def get_stats(**context):
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                COUNT(*) as cnt,
                ROUND(SUM(total_sum), 2) as revenue,
                ROUND(AVG(total_sum), 2) as avg_check
            FROM orders
            WHERE restaurant = %s
        """, (restaurant_name,))
        result = cur.fetchone()
        cur.close()
        conn.close()
        
        print(f"📊 Статистика для {restaurant_name}:")
        print(f"   Заказов: {result[0]}")
        print(f"   Выручка: {result[1]} руб.")
        print(f"   Средний чек: {result[2]} руб.")
        
        context['ti'].xcom_push(key='stats', value={
            "restaurant": restaurant_name,
            "orders": result[0],
            "revenue": float(result[1]) if result[1] else 0,
        })
    
    return get_stats


# ============================================================
# КОНФИГУРАЦИЯ: список ресторанов
# ============================================================
RESTAURANTS = [
    "Суши-Мастер",
    "Пицца-Хаус",
    "Бургер-Кинг",
    "Вок-Экспресс",
    "Грузинский дворик",
]

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


# ============================================================
# ГЕНЕРАЦИЯ DAG'ов В ЦИКЛЕ
# ============================================================
for restaurant in RESTAURANTS:
    # Формируем безопасное имя (для dag_id)
    dag_name = restaurant.lower().replace(" ", "_").replace("-", "_")
    
    with DAG(
        dag_id=f'pizza_stats_{dag_name}',
        default_args=default_args,
        description=f'Статистика для ресторана {restaurant}',
        schedule_interval='@daily',
        start_date=datetime(2026, 10, 1),
        catchup=False,
        tags=['pizza', 'dynamic', restaurant],
    ) as dag:
        
        # ---- START ----
        start = BashOperator(
            task_id='start',
            bash_command=f'echo "Старт обработки для {restaurant}"',
        )
        
        # ---- СТАТИСТИКА ----
        stats = PythonOperator(
            task_id='get_stats',
            python_callable=make_stats_function(restaurant),
        )
        
        # ---- ОТЧЁТ ----
        report = BashOperator(
            task_id='send_report',
            bash_command=f'echo "Отчёт для {restaurant} отправлен"',
        )
        
        # ---- ЗАВИСИМОСТИ ----
        start >> stats >> report