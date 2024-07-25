from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def punya_rais_task1():
    return 'Hello world from first Airflow DAG Rais!'

dag = DAG(
        'alterra_rais_airflow_task1',
        description='Hello, Ini DAG Rais',
        schedule_interval='0 */5 * * *',
        start_date=datetime(2024, 7, 25),
        catchup=False
    )

operator_hello_world = PythonOperator(
    task_id='task1-airflow-rais',
    python_callable=punya_rais_task1,
    dag=dag
)

operator_hello_world