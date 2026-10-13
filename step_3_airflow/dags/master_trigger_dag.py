from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator

RESTAURANTS = [
    "суши_мастер",
    "пицца_хаус",
    "бургер_кинг",
    "вок_экспресс",
    "грузинский_дворик",
]

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='master_trigger_all_stats',
    default_args=default_args,
    description='Мастер-DAG: запускает все DAGи по ресторанам',
    schedule_interval='@daily',
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=['master', 'trigger'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    trigger_tasks = []
    
    for restaurant in RESTAURANTS:
        trigger = TriggerDagRunOperator(
            task_id=f'trigger_{restaurant}',
            trigger_dag_id=f'pizza_stats_{restaurant}',
            wait_for_completion=False,  # не ждать завершения
        )
        trigger_tasks.append(trigger)
    
    start >> trigger_tasks >> end