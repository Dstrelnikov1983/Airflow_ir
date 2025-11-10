"""
ETL Pipeline DAG для обработки транзакций в Yandex Cloud
Лабораторная работа №2

Этот DAG реализует полный цикл ETL:
- Extract: Извлечение данных из CSV в Yandex Object Storage
- Validate: Валидация данных на корректность
- Transform: Трансформация и расчет метрик
- Load: Сохранение результатов в JSON
- Notify: Отправка уведомления о результатах

Автор: Data Engineering Team
Версия: 2.0
Дата: 2025-01-15
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import pandas as pd
import json
import io
import logging

# Настройка логирования
logger = logging.getLogger(__name__)

# ==================== КОНФИГУРАЦИЯ ====================

# Параметры S3 (Yandex Object Storage)
S3_BUCKET = 'dagstore'  # ⚠️ ЗАМЕНИТЕ на ваш бакет!
S3_INPUT_KEY = 'input/transactions.csv'
S3_OUTPUT_PREFIX = 'output/'
S3_CONN_ID = 'yandex_s3'

# Default arguments для DAG
default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'email': ['mail@dstrelnikov.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

# ==================== ФУНКЦИИ ETL ====================

def extract_data(**context):
    """
    Извлечение данных из CSV файла в Yandex Object Storage
    
    Использует S3Hook для подключения к Yandex Object Storage
    и загрузки CSV файла в pandas DataFrame
    """
    logger.info("📥 Начало извлечения данных из Yandex Object Storage")
    
    try:
        # Подключение к S3
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        
        # Чтение файла из S3
        logger.info(f"Чтение файла: s3://{S3_BUCKET}/{S3_INPUT_KEY}")
        file_content = s3_hook.read_key(
            key=S3_INPUT_KEY,
            bucket_name=S3_BUCKET
        )
        
        # Преобразование в DataFrame
        df = pd.read_csv(io.StringIO(file_content))
        
        logger.info(f"✅ Загружено {len(df)} строк")
        logger.info(f"📊 Колонки: {list(df.columns)}")
        
        # Преобразование DataFrame в словарь для XCom
        data_dict = df.to_dict('records')
        
        # Передача данных через XCom
        ti = context['task_instance']
        ti.xcom_push(key='raw_data', value=data_dict)
        ti.xcom_push(key='row_count', value=len(df))
        
        logger.info("✅ Данные успешно извлечены и переданы через XCom")
        
        return {
            'status': 'success',
            'row_count': len(df),
            'columns': list(df.columns)
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка при извлечении данных: {str(e)}")
        raise


def validate_data(**context):
    """
    Валидация данных на корректность
    
    Выполняет следующие проверки:
    - Пустые значения
    - Дубликаты
    - Отрицательные цены и количества
    - Некорректные суммы
    """
    logger.info("🔍 Начало валидации данных")
    
    try:
        # Получение данных из XCom
        ti = context['task_instance']
        data_dict = ti.xcom_pull(task_ids='extract', key='raw_data')
        
        if not data_dict:
            raise ValueError("Нет данных для валидации")
        
        # Преобразование обратно в DataFrame
        df = pd.DataFrame(data_dict)
        
        # Список проблем
        issues = []
        warnings = []
        
        # ========== ПРОВЕРКА 1: Пустые значения ==========
        null_counts = df.isnull().sum()
        null_columns = null_counts[null_counts > 0]
        
        if len(null_columns) > 0:
            issue = f"Найдены пустые значения: {null_columns.to_dict()}"
            issues.append(issue)
            logger.warning(f"⚠️  {issue}")
        
        # ========== ПРОВЕРКА 2: Дубликаты ==========
        duplicates = df[df.duplicated(subset=['transaction_id'], keep=False)]
        if len(duplicates) > 0:
            issue = f"Найдено {len(duplicates)} дубликатов transaction_id"
            issues.append(issue)
            logger.warning(f"⚠️  {issue}")
        
        # ========== ПРОВЕРКА 3: Отрицательные цены ==========
        if 'price' in df.columns:
            negative_prices = df[df['price'] < 0]
            if len(negative_prices) > 0:
                issue = f"Найдено {len(negative_prices)} записей с отрицательной ценой"
                issues.append(issue)
                logger.warning(f"⚠️  {issue}")
        
        # ========== ПРОВЕРКА 4: Отрицательное количество ==========
        if 'quantity' in df.columns:
            negative_qty = df[df['quantity'] < 0]
            if len(negative_qty) > 0:
                issue = f"Найдено {len(negative_qty)} записей с отрицательным количеством"
                issues.append(issue)
                logger.warning(f"⚠️  {issue}")
        
        # ========== ПРОВЕРКА 5: Некорректные суммы ==========
        if all(col in df.columns for col in ['amount', 'price', 'quantity']):
            df['calculated_amount'] = df['price'] * df['quantity']
            incorrect_amounts = df[
                abs(df['amount'] - df['calculated_amount']) > 0.01
            ]
            if len(incorrect_amounts) > 0:
                warning = f"Найдено {len(incorrect_amounts)} записей с некорректной суммой"
                warnings.append(warning)
                logger.warning(f"⚠️  {warning}")
        
        # ========== ПРОВЕРКА 6: Отрицательные суммы ==========
        if 'amount' in df.columns:
            negative_amounts = df[df['amount'] < 0]
            if len(negative_amounts) > 0:
                issue = f"Найдено {len(negative_amounts)} записей с отрицательной суммой"
                issues.append(issue)
                logger.warning(f"⚠️  {issue}")
        
        # Формирование результата валидации
        validation_result = {
            'is_valid': len(issues) == 0,
            'total_records': len(df),
            'issues_count': len(issues),
            'warnings_count': len(warnings),
            'issues': issues,
            'warnings': warnings,
            'timestamp': datetime.now().isoformat()
        }
        
        # Логирование результатов
        if validation_result['is_valid']:
            logger.info("✅ Валидация успешна: данные корректны")
        else:
            logger.warning("⚠️  Обнаружены проблемы в данных:")
            for issue in issues:
                logger.warning(f"  - {issue}")
        
        if warnings:
            logger.info("ℹ️  Предупреждения:")
            for warning in warnings:
                logger.info(f"  - {warning}")
        
        # Передача результата через XCom
        ti.xcom_push(key='validation_result', value=validation_result)
        
        return validation_result
        
    except Exception as e:
        logger.error(f"❌ Ошибка при валидации: {str(e)}")
        raise


def transform_data(**context):
    """
    Трансформация данных и расчет бизнес-метрик
    
    Вычисляет:
    - Общую выручку
    - Средний чек
    - Количество транзакций и уникальных клиентов
    - Топ-10 товаров
    - Разбивку по категориям
    - Статистику по периодам
    """
    logger.info("🔄 Начало трансформации данных")
    
    try:
        # Получение данных из XCom
        ti = context['task_instance']
        data_dict = ti.xcom_pull(task_ids='extract', key='raw_data')
        
        if not data_dict:
            raise ValueError("Нет данных для трансформации")
        
        df = pd.DataFrame(data_dict)
        
        # ========== БАЗОВЫЕ МЕТРИКИ ==========
        
        # Общая сумма продаж
        total_sales = round(float(df['amount'].sum()), 2)
        
        # Средний чек
        average_transaction = round(float(df['amount'].mean()), 2)
        
        # Количество транзакций
        transaction_count = int(len(df))
        
        # Уникальные клиенты
        unique_customers = int(df['customer_id'].nunique())
        
        # Общее количество проданных товаров
        total_items_sold = int(df['quantity'].sum())
        
        # Среднее количество товаров на транзакцию
        average_items_per_transaction = round(float(df['quantity'].mean()), 2)
        
        logger.info(f"💰 Общая выручка: ${total_sales:,.2f}")
        logger.info(f"📊 Средний чек: ${average_transaction:,.2f}")
        logger.info(f"🛒 Транзакций: {transaction_count}")
        logger.info(f"👥 Уникальных покупателей: {unique_customers}")
        
        # ========== ТОП ТОВАРОВ ==========
        
        top_products = (
            df.groupby('product')['amount']
            .sum()
            .sort_values(ascending=False)
            .head(10)
            .to_dict()
        )
        
        top_products_formatted = {
            k: round(v, 2) for k, v in top_products.items()
        }
        
        logger.info("🏆 Топ-3 товара по выручке:")
        for i, (product, amount) in enumerate(list(top_products_formatted.items())[:3], 1):
            logger.info(f"  {i}. {product}: ${amount:,.2f}")
        
        # ========== РАЗБИВКА ПО КАТЕГОРИЯМ ==========
        
        sales_by_category = (
            df.groupby('category')['amount']
            .sum()
            .sort_values(ascending=False)
            .to_dict()
        )
        
        sales_by_category_formatted = {
            k: round(v, 2) for k, v in sales_by_category.items()
        }
        
        # Процент от общей выручки по категориям
        category_percentages = {
            k: round((v / total_sales) * 100, 2)
            for k, v in sales_by_category_formatted.items()
        }
        
        # ========== СТАТИСТИКА ПО ПЕРИОДАМ ==========
        
        # Преобразование даты
        df['date'] = pd.to_datetime(df['date'])
        
        # Группировка по дням
        daily_sales = (
            df.groupby(df['date'].dt.date)['amount']
            .sum()
            .to_dict()
        )
        
        daily_sales_formatted = {
            str(k): round(v, 2) for k, v in daily_sales.items()
        }
        
        # ========== СТАТИСТИКА ПО КЛИЕНТАМ ==========
        
        # Топ клиенты
        top_customers = (
            df.groupby('customer_id')['amount']
            .sum()
            .sort_values(ascending=False)
            .head(5)
            .to_dict()
        )
        
        top_customers_formatted = {
            f"Customer_{k}": round(v, 2) 
            for k, v in top_customers.items()
        }
        
        # ========== ФОРМИРОВАНИЕ РЕЗУЛЬТАТА ==========
        
        metrics = {
            'summary': {
                'total_sales': total_sales,
                'average_transaction': average_transaction,
                'transaction_count': transaction_count,
                'unique_customers': unique_customers,
                'total_items_sold': total_items_sold,
                'average_items_per_transaction': average_items_per_transaction
            },
            'top_products': top_products_formatted,
            'sales_by_category': sales_by_category_formatted,
            'category_percentages': category_percentages,
            'daily_sales': daily_sales_formatted,
            'top_customers': top_customers_formatted,
            'generated_at': datetime.now().isoformat()
        }
        
        # Передача результатов через XCom
        ti.xcom_push(key='metrics', value=metrics)
        
        logger.info("✅ Трансформация данных завершена успешно")
        
        return metrics
        
    except Exception as e:
        logger.error(f"❌ Ошибка при трансформации: {str(e)}")
        raise


def load_data(**context):
    """
    Загрузка результатов в JSON файл в Yandex Object Storage
    
    Сохраняет результаты валидации и метрики в JSON формате
    """
    logger.info("💾 Начало загрузки результатов")
    
    try:
        # Получение данных из XCom
        ti = context['task_instance']
        metrics = ti.xcom_pull(task_ids='transform', key='metrics')
        validation_result = ti.xcom_pull(task_ids='validate', key='validation_result')
        
        # Формирование финального результата
        result = {
            'execution_date': context['ds'],
            'execution_timestamp': datetime.now().isoformat(),
            'dag_id': context['dag'].dag_id,
            'run_id': context['run_id'],
            'validation': validation_result,
            'metrics': metrics
        }
        
        # Преобразование в JSON
        json_content = json.dumps(result, indent=2, ensure_ascii=False)
        
        # Формирование имени файла
        output_filename = f"etl_results_{context['ds']}.json"
        output_key = f"{S3_OUTPUT_PREFIX}{output_filename}"
        
        # Подключение к S3
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        
        # Загрузка в S3
        logger.info(f"Загрузка файла: s3://{S3_BUCKET}/{output_key}")
        s3_hook.load_string(
            string_data=json_content,
            key=output_key,
            bucket_name=S3_BUCKET,
            replace=True
        )
        
        logger.info(f"✅ Результаты сохранены в: s3://{S3_BUCKET}/{output_key}")
        logger.info(f"📊 Размер файла: {len(json_content)} bytes")
        
        # Передача информации о файле
        ti.xcom_push(key='output_file', value=output_key)
        
        return {
            'status': 'success',
            'output_file': output_key,
            'file_size': len(json_content)
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка при загрузке: {str(e)}")
        raise


def send_notification(**context):
    """
    Формирование отчета о результатах выполнения
    
    В production окружении здесь можно добавить:
    - Отправку email через EmailOperator
    - Отправку в Slack/Telegram
    - Запись в систему мониторинга
    """
    logger.info("📧 Формирование уведомления")
    
    try:
        # Получение данных из XCom
        ti = context['task_instance']
        metrics = ti.xcom_pull(task_ids='transform', key='metrics')
        validation_result = ti.xcom_pull(task_ids='validate', key='validation_result')
        output_info = ti.xcom_pull(task_ids='load')
        
        # Формирование отчета
        report_lines = [
            "\n" + "="*60,
            "📊 ОТЧЕТ О ВЫПОЛНЕНИИ ETL ПРОЦЕССА",
            "="*60,
            f"📅 Дата выполнения: {context['ds']}",
            f"⏰ Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"🆔 DAG ID: {context['dag'].dag_id}",
            f"🔄 Run ID: {context['run_id']}",
            "",
            "🔍 ВАЛИДАЦИЯ:",
            f"  Статус: {'✅ Успешно' if validation_result['is_valid'] else '⚠️  С проблемами'}",
            f"  Обработано записей: {validation_result['total_records']}",
            f"  Найдено проблем: {validation_result['issues_count']}",
            f"  Предупреждений: {validation_result['warnings_count']}",
        ]
        
        if validation_result['issues']:
            report_lines.append("\n  ⚠️  Обнаруженные проблемы:")
            for issue in validation_result['issues']:
                report_lines.append(f"    - {issue}")
        
        summary = metrics['summary']
        report_lines.extend([
            "",
            "💰 МЕТРИКИ:",
            f"  Общая выручка: ${summary['total_sales']:,.2f}",
            f"  Средний чек: ${summary['average_transaction']:,.2f}",
            f"  Количество транзакций: {summary['transaction_count']}",
            f"  Уникальных покупателей: {summary['unique_customers']}",
            f"  Продано товаров: {summary['total_items_sold']}",
            f"  Среднее кол-во товаров на транзакцию: {summary['average_items_per_transaction']}",
        ])
        
        report_lines.extend([
            "",
            "🏆 ТОП-3 ТОВАРА:"
        ])
        
        for i, (product, amount) in enumerate(list(metrics['top_products'].items())[:3], 1):
            report_lines.append(f"  {i}. {product}: ${amount:,.2f}")
        
        report_lines.extend([
            "",
            "📁 РЕЗУЛЬТАТЫ:",
            f"  Файл сохранен: {output_info['output_file']}",
            f"  Размер файла: {output_info['file_size']} bytes",
            f"  Бакет: {S3_BUCKET}",
            "",
            "="*60,
            "✅ ETL процесс завершен успешно!",
            "="*60 + "\n"
        ])
        
        # Вывод отчета в лог
        report = "\n".join(report_lines)
        logger.info(report)
        
        return {
            'status': 'notification_sent',
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"❌ Ошибка при отправке уведомления: {str(e)}")
        raise


# ==================== ОПРЕДЕЛЕНИЕ DAG ====================

# Создание DAG
dag = DAG(
    dag_id='etl_pipeline_yandex',
    default_args=default_args,
    description='ETL Pipeline для обработки транзакций в Yandex Cloud',
    schedule_interval='@daily',  # Ежедневный запуск
    start_date=datetime(2025, 1, 1),
    catchup=False,  # Не запускать для пропущенных дат
    tags=['etl', 'yandex-cloud', 'lab2', 'production'],
    doc_md=__doc__,
)

# ==================== СОЗДАНИЕ ЗАДАЧ ====================

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag,
    doc_md="""
    ### Extract Task
    Извлекает данные из CSV файла в Yandex Object Storage
    
    **Входные данные:** CSV файл из S3
    **Выходные данные:** Словарь транзакций в XCom
    """,
)

validate_task = PythonOperator(
    task_id='validate',
    python_callable=validate_data,
    dag=dag,
    doc_md="""
    ### Validate Task
    Проверяет корректность данных
    
    **Проверки:**
    - Пустые значения
    - Дубликаты
    - Отрицательные значения
    - Некорректные суммы
    """,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag,
    doc_md="""
    ### Transform Task
    Выполняет трансформацию данных и расчет метрик
    
    **Метрики:**
    - Общая выручка
    - Средний чек
    - Топ товары и категории
    - Статистика по периодам
    """,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    dag=dag,
    doc_md="""
    ### Load Task
    Сохраняет результаты в JSON файл в Object Storage
    
    **Формат:** JSON
    **Расположение:** output/etl_results_<date>.json
    """,
)

notify_task = PythonOperator(
    task_id='send_notification',
    python_callable=send_notification,
    dag=dag,
    doc_md="""
    ### Notification Task
    Формирует и выводит отчет о выполнении
    
    **Содержит:**
    - Результаты валидации
    - Бизнес-метрики
    - Информацию о сохраненных файлах
    """,
)

# ==================== ОПРЕДЕЛЕНИЕ ЗАВИСИМОСТЕЙ ====================

# Последовательная цепочка задач
extract_task >> validate_task >> transform_task >> load_task >> notify_task
