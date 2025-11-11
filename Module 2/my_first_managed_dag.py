from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def print_cloud_info():
    """Функция для вывода информации о среде выполнения"""
    print("Running on Yandex Cloud!")
    print("Managed Airflow Service")
    return "Success"


# Параметры по умолчанию для всех задач DAG
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

# Определение DAG
dag = DAG(
    'my_first_managed_dag',
    default_args=default_args,
    description='First DAG in Managed Airflow',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['managed', 'tutorial'],
)

# Задача 1: Вывод текущей даты
task1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

# Задача 2: Вывод информации о среде
task2 = PythonOperator(
    task_id='print_cloud_info',
    python_callable=print_cloud_info,
    dag=dag,
)

# Задача 3: Список файлов в /tmp
task3 = BashOperator(
    task_id='list_files',
    bash_command='ls -la /tmp',
    dag=dag,
)

# Определение зависимостей между задачами
task1 >> task2 >> task3