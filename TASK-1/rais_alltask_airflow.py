from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator


dag = DAG(
        'rais-alltask-airflow',
        description='Hello, Ini DAG Rais',
        schedule_interval='0 */5 * * *',
        start_date=datetime(2024, 7, 25),
        catchup=False
    )

start = DummyOperator(
    task_id='start',
    dag=dag,
)

def variable_to_xcom(**kwargs):
    value = 'my_value'
    kwargs['ti'].xcom_push(key='my_key', value=value)

variable_to_xcom_task = PythonOperator(
    task_id='variable_to_xcom_task',
    provide_context=True,
    python_callable=variable_to_xcom,
    dag=dag,
)

def pull_multiple_values_at_once(**kwargs):
    ti = kwargs['ti']
    value1 = ti.xcom_pull(task_ids='variable_to_xcom_task', key='my_key')
    value2 = ti.xcom_pull(task_ids='another_task', key='another_key')
    print(f'Pulled values: {value1}, {value2}')

pull_multiple_values_at_once_task = PythonOperator(
    task_id='pull_multiple_values_at_once_task',
    provide_context=True,
    python_callable=pull_multiple_values_at_once,
    dag=dag,
)

start >> variable_to_xcom_task >> pull_multiple_values_at_once_task