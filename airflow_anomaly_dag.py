from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 3),
    'retries': 0,
    'retry_delay': timedelta(seconds=150)
}

dag = DAG(
    'parallel_tasks',
    default_args=default_args,
    schedule=timedelta(seconds=100)
)

# task1 = BashOperator(
#     task_id='task_1',
#     bash_command='/home/naya/miniconda3/envs/airflow/bin/python /home/naya/anomaly/consumer_anomaly_analysis.py',
#     dag=dag,
#     retries=0
# )

# task2 = BashOperator(
#     task_id='task_2',
#     bash_command='/home/naya/miniconda3/envs/airflow/bin/python /home/naya/anomaly/consumer_predict_hdfs.py',
#     dag=dag,
#     retries=0
# )

task3 = BashOperator(
    task_id='task_3',
    bash_command='/home/naya/miniconda3/envs/airflow/bin/python /home/naya/anomaly/producer_get_raw_data.py --history=no',
    dag=dag,
    execution_timeout=timedelta(seconds=70),
    retry_delay=timedelta(seconds=50)
)

task4 = BashOperator(
    task_id='task_4',
    bash_command='/home/naya/miniconda3/envs/airflow/bin/python /home/naya/anomaly/prediction.py --MIN_PRED_RECORD=20 --NEXT_TRAIN_QTY=200',
    dag=dag
)