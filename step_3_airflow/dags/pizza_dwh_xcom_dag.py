from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import psycopg2

# ============================================================
# ФУНКЦИИ ДЛЯ ЗАДАЧ
# ============================================================

def get_connection():
    return psycopg2.connect(
        host="pizza_postgres",  # ВАЖНО: имя контейнера для Airflow
        port=5432,
        database="pizza_dwh",
        user="admin",
        password="admin123"
    )

def extract_order_stats(**context):
    """
    Задача 1: Получаем статистику из БД и передаём через XCom.
    """
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            COUNT(*) as total_orders,
            ROUND(SUM(total_sum), 2) as total_revenue,
            ROUND(AVG(total_sum), 2) as avg_check
        FROM orders
        WHERE created_at::date = CURRENT_DATE
    """)
    
    result = cur.fetchone()
    stats = {
        "total_orders": result[0],
        "total_revenue": float(result[1]) if result[1] else 0,
        "avg_check": float(result[2]) if result[2] else 0,
    }
    
    cur.close()
    conn.close()
    
    print(f"📊 Статистика за сегодня: {stats}")
    
    # Передаём через XCom
    context['ti'].xcom_push(key='order_stats', value=stats)
    
    return stats

def check_revenue(**context):
    """
    Задача 2: Проверяем выручку. Возвращаем имя следующей задачи.
    BranchPythonOperator использует это для ветвления.
    """
    # Забираем из XCom
    stats = context['ti'].xcom_pull(key='order_stats', task_ids='extract_order_stats')
    
    revenue = stats['total_revenue']
    print(f"💰 Выручка: {revenue} руб.")
    
    if revenue > 10000:
        print("✅ Выручка ВЫСОКАЯ → идём в high_revenue")
        return 'high_revenue'
    else:
        print("⚠️ Выручка НИЗКАЯ → идём в low_revenue")
        return 'low_revenue'

def generate_daily_report(**context):
    """
    Задача 4: Финальный отчёт с использованием XCom.
    """
    stats = context['ti'].xcom_pull(key='order_stats', task_ids='extract_order_stats')
    
    print("=" * 60)
    print("📋 ЕЖЕДНЕВНЫЙ ОТЧЁТ")
    print("=" * 60)
    print(f"Заказов сегодня: {stats['total_orders']}")
    print(f"Выручка:         {stats['total_revenue']} руб.")
    print(f"Средний чек:     {stats['avg_check']} руб.")
    print("=" * 60)

# ============================================================
# ОПРЕДЕЛЕНИЕ DAG
# ============================================================

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='pizza_dwh_xcom_dag',
    default_args=default_args,
    description='DAG с XCom, Branching и Task Groups',
    schedule_interval='@daily',
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=['pizza', 'xcom', 'branching'],
) as dag:
    
    # ---- ЗАДАЧА 1: Извлечь статистику ----
    task_extract = PythonOperator(
        task_id='extract_order_stats',
        python_callable=extract_order_stats,
    )
    
    # ---- ЗАДАЧА 2: Проверить выручку (BRANCHING) ----
    task_check = BranchPythonOperator(
        task_id='check_revenue',
        python_callable=check_revenue,
    )
    
    # ---- ЗАДАЧА 3а: Высокая выручка ----
    task_high = BashOperator(
        task_id='high_revenue',
        bash_command='echo "🎉 Высокая выручка! Отправляем праздничный отчёт."',
    )
    
    # ---- ЗАДАЧА 3б: Низкая выручка ----
    task_low = BashOperator(
        task_id='low_revenue',
        bash_command='echo "⚠️ Низкая выручка. Нужно проверить маркетинг."',
    )
    
    # ---- ЗАДАЧА 4: Финальный отчёт ----
    task_report = PythonOperator(
        task_id='generate_daily_report',
        python_callable=generate_daily_report,
        trigger_rule='none_failed',  # Запустится, даже если одна из веток пропущена
    )
    
    # ---- ЗАВИСИМОСТИ ----
    task_extract >> task_check
    task_check >> [task_high, task_low]
    [task_high, task_low] >> task_report