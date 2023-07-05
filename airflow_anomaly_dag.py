from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=150)
}

dag = DAG(
    'parallel_tasks',
    default_args=default_args,
    start_date=datetime(2023, 7, 3),
    schedule_interval=timedelta(days=1)  # Daily schedule
)

task3 = BashOperator(
    task_id='task_3',
    bash_command='/home/naya/miniconda3/envs/airflow/bin/python /home/naya/anomaly/producer_get_raw_data.py --history=no',
    dag=dag,
    execution_date_fn=lambda dt: dt.replace(hour=0, minute=5)  # Task3 will run every day at 00:05 AM
)

task4 = BashOperator(
    task_id='task_4',
    bash_command='/home/naya/miniconda3/envs/airflow/bin/python /home/naya/anomaly/prediction.py --MIN_PRED_RECORD=20 --NEXT_TRAIN_QTY=200',
    dag=dag,
    execution_date_fn=lambda dt: dt.replace(hour=0, minute=5, weekday=4)  # Task4 will run weekly on Friday at 00:05 AM
)
