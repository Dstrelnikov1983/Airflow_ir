from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime

with DAG(
    'test_dataproc_ssh',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    
    test_connection = SSHOperator(
        task_id='test_ssh_connection',
        ssh_conn_id='hadoop_ssh',
        command='hostname && whoami && hdfs version',
    )