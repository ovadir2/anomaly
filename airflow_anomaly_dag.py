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
    schedule=timedelta(days=1)  # Daily schedule
)

task3 = BashOperator(
    task_id='task_1',
    bash_command='/home/naya/miniconda3/envs/airflow/bin/python /home/naya/anomaly/producer_get_raw_data.py --history=no',
    dag=dag
)

task4 = BashOperator(
    task_id='task_2',
    bash_command='/home/naya/miniconda3/envs/airflow/bin/python /home/naya/anomaly/prediction.py --MIN_PRED_RECORD=20 --NEXT_TRAIN_QTY=200',
    dag=dag,
    schedule=timedelta(days=7)  # Weekly schedule
)
