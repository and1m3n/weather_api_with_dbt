import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
from api_request import main


default_args = {
    'description': 'A DAG to orchetrate data',
    'start_date':datetime(2025,4,30),
    'cathup':False,
}

dag = DAG(
    dag_id='weather-api-orchestrrator',
    default_args = default_args,
    shedule = timedelta(minutes=7)
)

with dag:
    task1= PythonOperator(
        task_id='example_task',
        python_callable=main
    )