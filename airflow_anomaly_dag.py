from airflow import DAG
from airflow.operators.bash  import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'naya',
    'start_date': datetime(2023, 7, 3),
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

dag = DAG('anomaly_tasks', default_args=default_args, schedule_interval='*/30 * * * *')

task1 = BashOperator(
    task_id='consumer_anomaly_analysis',
    bash_command='/home/naya/miniconda3/envs/airflow/bin/python /home/naya/anomaly/consumer_anomaly_analysis.py',
    dag=dag
)

task2 = BashOperator(
    task_id='consumer_predict_hdfs',
    bash_command='/home/naya/miniconda3/envs/airflow/bin/python /home/naya/anomaly/consumer_predict_hdfs.py',
    dag=dag
)

task3 = BashOperator(
    task_id='producer_get_raw_data',
    bash_command='/home/naya/miniconda3/envs/airflow/bin/python /home/naya/anomaly/producer_get_raw_data.py --history=no',
    dag=dag
)

task4 = BashOperator(
    task_id='prediction',
    bash_command='/home/naya/miniconda3/envs/airflow/bin/python /home/naya/anomaly/prediction.py --MIN_PRED_RECORD=20 --NEXT_TRAIN_QTY=200',
    dag=dag
)

task1 >> task2 >> task3 >> task4
