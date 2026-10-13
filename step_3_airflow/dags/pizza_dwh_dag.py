from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Параметры по умолчанию для всех задач
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Определяем DAG
with DAG(
    dag_id='pizza_dwh_pipeline',
    default_args=default_args,
    description='ETL пайплайн для Pizza DWH',
    schedule_interval='@daily',     # Запускать каждый день
    start_date=datetime(2026, 7, 1),# С какой даты начинать
    catchup=False,                  # Не догонять пропущенные дни
    tags=['pizza', 'dwh', 'etl'],
) as dag:

    # Задача 1: Загрузить рестораны
    task_dim_restaurants = BashOperator(
        task_id='load_dim_restaurants',
        bash_command='cd /opt/airflow/scripts && python etl_dim_restaurants.py',
    )

    # Задача 2: Загрузить календарь
    task_dim_date = BashOperator(
        task_id='load_dim_date',
        bash_command='cd /opt/airflow/scripts && python etl_dim_date.py',
    )

    # Задача 3: Загрузить факты
    task_fact_orders = BashOperator(
        task_id='load_fact_orders',
        bash_command='cd /opt/airflow/scripts && python etl_fact_orders.py',
    )

    # Задача 4: Создать витрину
    task_create_mart = BashOperator(
        task_id='create_mart',
        bash_command='cd /opt/airflow/scripts && python create_mart.py',
    )

    # Задача 5: Создать расширенные витрины
    task_create_extended_marts = BashOperator(
        task_id='create_extended_marts',
        bash_command='cd /opt/airflow/scripts && python create_extended_marts.py',
    )

    # ---- ЗАВИСИМОСТИ (порядок выполнения) ----
    # dim_restaurants и dim_date можно параллельно
    # потом fact_orders, потом витрины
    
    task_dim_restaurants >> task_dim_date >> task_fact_orders >> task_create_mart >> task_create_extended_marts