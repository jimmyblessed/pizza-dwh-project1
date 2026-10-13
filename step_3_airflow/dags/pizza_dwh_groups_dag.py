from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.sql import SqlSensor
from airflow.utils.task_group import TaskGroup
import psycopg2


def get_connection():
    return psycopg2.connect(
        host="pizza_postgres", port=5432,
        database="pizza_dwh", user="admin", password="admin123"
    )


# ============================================================
# ФУНКЦИИ ДЛЯ ЗАДАЧ
# ============================================================

def check_data_quality(**context):
    """Проверяем, что в orders есть данные за сегодня."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM orders WHERE created_at::date = CURRENT_DATE")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    
    print(f"📊 Заказов за сегодня: {count}")
    if count == 0:
        print("⚠️ Нет данных за сегодня!")


def calculate_stats(**context):
    """Считаем статистику."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT restaurant, COUNT(*), ROUND(SUM(total_sum), 2)
        FROM orders GROUP BY restaurant
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    print(f"📊 Статистика по {len(rows)} ресторанам:")
    for row in rows:
        print(f"   {row[0]}: {row[1]} заказов, {row[2]} руб.")


def send_report(**context):
    print("📧 Отчёт отправлен!")


# ============================================================
# ОПРЕДЕЛЕНИЕ DAG
# ============================================================

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='pizza_dwh_groups_dag',
    default_args=default_args,
    description='DAG с Task Groups и Sensors',
    schedule_interval='@daily',
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=['pizza', 'groups', 'sensors'],
) as dag:
    
    # ---- START ----
    start = EmptyOperator(task_id='start')
    
    # ============================================================
    # ГРУППА 1: ЗАГРУЗКА ДАННЫХ
    # ============================================================
    with TaskGroup(group_id='load_data') as load_group:
        
        # Sensor: ждём, пока в БД появится запись за сегодня
        wait_for_data = SqlSensor(
            task_id='wait_for_data',
            conn_id='postgres_default',  # ВАЖНО: нужно настроить!
            sql="SELECT COUNT(*) FROM orders WHERE created_at::date = CURRENT_DATE",
            poke_interval=10,       # проверять каждые 10 секунд
            timeout=60,             # максимум 60 секунд ждать
            mode='poke',            # режим poke
        )
        
        check_quality = PythonOperator(
            task_id='check_data_quality',
            python_callable=check_data_quality,
        )
        
        wait_for_data >> check_quality
    
    # ============================================================
    # ГРУППА 2: ОБРАБОТКА
    # ============================================================
    with TaskGroup(group_id='process_data') as process_group:
        
        calc_restaurants = PythonOperator(
            task_id='calc_restaurant_stats',
            python_callable=calculate_stats,
        )
        
        calc_daily = BashOperator(
            task_id='calc_daily_stats',
            bash_command='echo "Считаем статистику по дням"',
        )
        
        calc_hourly = BashOperator(
            task_id='calc_hourly_stats',
            bash_command='echo "Считаем статистику по часам"',
        )
        
        # Эти три задачи параллельны
        [calc_restaurants, calc_daily, calc_hourly]
    
    # ============================================================
    # ГРУППА 3: ОТПРАВКА
    # ============================================================
    with TaskGroup(group_id='send_reports') as report_group:
        
        send_email = PythonOperator(
            task_id='send_email_report',
            python_callable=send_report,
        )
        
        send_slack = BashOperator(
            task_id='send_slack_notification',
            bash_command='echo "Отправлено в Slack"',
        )
    
    # ---- END ----
    end = EmptyOperator(task_id='end')
    
    # ---- ЗАВИСИМОСТИ ----
    start >> load_group >> process_group >> report_group >> end