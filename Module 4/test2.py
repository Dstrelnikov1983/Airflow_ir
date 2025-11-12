from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.yandex.hooks.yandex import YandexCloudBaseHook
from datetime import datetime
import logging

# Конфигурация
CLUSTER_ID = 'c9q3cm4602cmmuhcb35v'  # ЗАМЕНИТЕ на ваш ID кластера
YANDEX_CONN_ID = 'yandexcloud_default'

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 0  # Для теста не нужны retries
}

def test_connection(**context):
    """Тестирование подключения к Yandex Cloud"""
    
    logging.info("=" * 60)
    logging.info("ТЕСТ 1: Проверка подключения к Yandex Cloud")
    logging.info("=" * 60)
    
    try:
        # Создание hook для работы с Yandex Cloud API
        hook = YandexCloudBaseHook(yandexcloud_conn_id=YANDEX_CONN_ID)
        
        logging.info("✓ Hook создан успешно")
        
        # Получение информации о кластере
        logging.info(f"Получение информации о кластере: {CLUSTER_ID}")
        
        cluster_info = hook.sdk.client(
            hook.sdk.services.dataproc.ClusterServiceStub
        ).Get(hook.sdk.services.dataproc.GetClusterRequest(cluster_id=CLUSTER_ID))
        
        logging.info("=" * 60)
        logging.info("✅ ПОДКЛЮЧЕНИЕ УСПЕШНО!")
        logging.info("=" * 60)
        logging.info(f"Имя кластера: {cluster_info.name}")
        logging.info(f"Статус: {cluster_info.status}")
        logging.info(f"Версия: {cluster_info.config.version_id}")
        logging.info(f"Зона: {cluster_info.zone_id}")
        logging.info("=" * 60)
        
        # Сохранение информации в XCom
        context['ti'].xcom_push(key='cluster_name', value=cluster_info.name)
        context['ti'].xcom_push(key='cluster_status', value=str(cluster_info.status))
        
        return {
            'status': 'SUCCESS',
            'cluster_name': cluster_info.name,
            'cluster_id': CLUSTER_ID
        }
        
    except Exception as e:
        logging.error("=" * 60)
        logging.error("❌ ОШИБКА ПОДКЛЮЧЕНИЯ!")
        logging.error("=" * 60)
        logging.error(f"Тип ошибки: {type(e).__name__}")
        logging.error(f"Сообщение: {str(e)}")
        logging.error("=" * 60)
        logging.error("Возможные причины:")
        logging.error("1. Неверный CLUSTER_ID")
        logging.error("2. Service Account не имеет роли dataproc.viewer")
        logging.error("3. Connection настроен неправильно")
        logging.error("4. Authorized key невалидный")
        logging.error("=" * 60)
        raise

def print_summary(**context):
    """Вывод итогов теста"""
    
    cluster_name = context['ti'].xcom_pull(task_ids='test_connection', key='cluster_name')
    cluster_status = context['ti'].xcom_pull(task_ids='test_connection', key='cluster_status')
    
    logging.info("")
    logging.info("=" * 60)
    logging.info("📊 ИТОГИ ТЕСТА 1")
    logging.info("=" * 60)
    logging.info(f"✅ Подключение к Yandex Cloud: OK")
    logging.info(f"✅ Доступ к кластеру: OK")
    logging.info(f"✅ Service Account работает: OK")
    logging.info("")
    logging.info(f"Кластер: {cluster_name}")
    logging.info(f"Статус: {cluster_status}")
    logging.info("=" * 60)
    logging.info("🎉 ТЕСТ 1 ПРОЙДЕН УСПЕШНО!")
    logging.info("=" * 60)

with DAG(
    'test_01_service_account_connection',
    default_args=default_args,
    description='Тест 1: Проверка подключения Service Account к Data Proc',
    schedule_interval=None,  # Запуск вручную
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test', 'service-account', 'connection']
) as dag:
    
    # Задача 1: Тест подключения
    test_conn = PythonOperator(
        task_id='test_connection',
        python_callable=test_connection,
        provide_context=True
    )
    
    # Задача 2: Вывод итогов
    summary = PythonOperator(
        task_id='print_summary',
        python_callable=print_summary,
        provide_context=True
    )
    
    test_conn >> summary
