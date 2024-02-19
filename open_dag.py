from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from Open_weather import get_weather_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'Ope_waether_dag',
    default_args=default_args,
    description='Open_weather dag',
    schedule_interval=timedelta(days=1),
)

run_dag = PythonOperator(
    task_id='complete_twitter_etl',
    python_callable=get_weather_data,
    dag=dag, 
)

run_dag