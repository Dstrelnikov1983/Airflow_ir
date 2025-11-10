# ETL Pipeline для аналитики продаж

## Содержание

* [1. Общая архитектура](#1-%D0%BE%D0%B1%D1%89%D0%B0%D1%8F-%D0%B0%D1%80%D1%85%D0%B8%D1%82%D0%B5%D0%BA%D1%82%D1%83%D1%80%D0%B0)
* [2. Extract - Извлечение данных](#2-extract---%D0%B8%D0%B7%D0%B2%D0%BB%D0%B5%D1%87%D0%B5%D0%BD%D0%B8%D0%B5-%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85)
* [3. Validate - Валидация данных](#3-validate---%D0%B2%D0%B0%D0%BB%D0%B8%D0%B4%D0%B0%D1%86%D0%B8%D1%8F-%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85)
* [4. Transform - Трансформация данных](#4-transform---%D1%82%D1%80%D0%B0%D0%BD%D1%81%D1%84%D0%BE%D1%80%D0%BC%D0%B0%D1%86%D0%B8%D1%8F-%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85)
* [5. Load - Загрузка в ClickHouse](#5-load---%D0%B7%D0%B0%D0%B3%D1%80%D1%83%D0%B7%D0%BA%D0%B0-%D0%B2-clickhouse)
* [6. Generate Report - Генерация отчета](#6-generate-report---%D0%B3%D0%B5%D0%BD%D0%B5%D1%80%D0%B0%D1%86%D0%B8%D1%8F-%D0%BE%D1%82%D1%87%D0%B5%D1%82%D0%B0)
* [7. Send Email - Отправка отчета](#7-send-email---%D0%BE%D1%82%D0%BF%D1%80%D0%B0%D0%B2%D0%BA%D0%B0-%D0%BE%D1%82%D1%87%D0%B5%D1%82%D0%B0)
* [8. Telegram Alerts - Уведомления](#8-telegram-alerts---%D1%83%D0%B2%D0%B5%D0%B4%D0%BE%D0%BC%D0%BB%D0%B5%D0%BD%D0%B8%D1%8F)
* [9. Граф зависимостей](#9-%D0%B3%D1%80%D0%B0%D1%84-%D0%B7%D0%B0%D0%B2%D0%B8%D1%81%D0%B8%D0%BC%D0%BE%D1%81%D1%82%D0%B5%D0%B9)
* [10. Дополнительные компоненты](#10-%D0%B4%D0%BE%D0%BF%D0%BE%D0%BB%D0%BD%D0%B8%D1%82%D0%B5%D0%BB%D1%8C%D0%BD%D1%8B%D0%B5-%D0%BA%D0%BE%D0%BC%D0%BF%D0%BE%D0%BD%D0%B5%D0%BD%D1%82%D1%8B)

---

## 1. Общая архитектура

### 1.1 Визуальная схема Pipeline

```
START → Extract → Validate → Transform → [Load + Report] → Email → Success → END
                                              ↓              ↓
                                         ClickHouse    HTML Report
                                            
[При ошибке на любом этапе] → Telegram Alert
```

### 1.2 Основные параметры DAG

python

```python
from airflow import DAG
from datetime import datetime, timedelta

default_args = {
    'owner': 'analytics_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

dag = DAG(
    'sales_etl_daily',
    default_args=default_args,
    description='Daily ETL pipeline for sales analytics',
    schedule_interval='0 2 * * *',  # Каждый день в 02:00
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'sales', 'analytics']
)
```

**Ключевые параметры:**

* **Schedule** :`0 2 * * *` (ежедневно в 02:00)
* **Retries** : 3 попытки с экспоненциальной задержкой
* **Catchup** : False (не запускать пропущенные DAG runs)
* **Max Active Runs** : 1 (только один запуск одновременно)

---

## 2. Extract - Извлечение данных

### 2.1 Описание

Извлечение данных о продажах за предыдущий день из PostgreSQL с использованием инкрементальной загрузки по дате.

### 2.2 Входные данные

* **Источник** : PostgreSQL (таблица`sales`)
* **Период** : Предыдущий день от`execution_date`

### 2.3 Выходные данные (XCom)

* `sales_data` - JSON с данными о продажах
* `record_count` - Количество извлеченных записей

### 2.4 Код задачи

python

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

def extract_sales_data(**context):
    """
    Извлечение данных о продажах за предыдущий день
    """
    execution_date = context['execution_date']
    target_date = execution_date.date()
  
    # Подключение к PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='sales_postgres')
  
    # SQL запрос с фильтрацией по дате
    query = """
        SELECT 
            sale_id,
            product_id,
            customer_id,
            sale_date,
            quantity,
            amount,
            store_id,
            created_at
        FROM sales
        WHERE DATE(sale_date) = %s
        ORDER BY sale_id
    """
  
    # Извлечение данных
    df = pg_hook.get_pandas_df(query, parameters=(target_date,))
  
    # Логирование
    context['task_instance'].log.info(f"Extracted {len(df)} records for {target_date}")
  
    # Сохранение в XCom для передачи следующим задачам
    context['task_instance'].xcom_push(key='sales_data', value=df.to_json())
    context['task_instance'].xcom_push(key='record_count', value=len(df))
  
    return len(df)

extract_task = PythonOperator(
    task_id='extract_sales',
    python_callable=extract_sales_data,
    provide_context=True,
    dag=dag
)
```

### 2.5 Особенности реализации

* ✅ Параметризованный SQL запрос (защита от SQL injection)
* ✅ Использование Airflow Hooks для подключения к БД
* ✅ Логирование количества извлеченных записей
* ✅ Передача данных через XCom

---

## 3. Validate - Валидация данных

### 3.1 Описание

Проверка качества данных на наличие:

* Дубликатов по`sale_id`
* Пропущенных значений в критических полях
* Аномалий (отрицательные суммы, выбросы)

### 3.2 Входные данные (XCom)

* `sales_data` из задачи`extract_sales`

### 3.3 Выходные данные (XCom)

* `validated_data` - Очищенные данные без дубликатов
* `validation_results` - Отчет о результатах валидации

### 3.4 Код задачи

python

```python
import pandas as pd
from airflow.exceptions import AirflowException

def validate_sales_data(**context):
    """
    Валидация данных: дубликаты, пропуски, аномалии
    """
    # Получение данных из XCom
    ti = context['task_instance']
    sales_json = ti.xcom_pull(task_ids='extract_sales', key='sales_data')
    df = pd.read_json(sales_json)
  
    validation_results = {
        'total_records': len(df),
        'issues': []
    }
  
    # 1. Проверка на дубликаты
    duplicates = df[df.duplicated(subset=['sale_id'], keep=False)]
    if len(duplicates) > 0:
        validation_results['issues'].append({
            'type': 'duplicates',
            'count': len(duplicates),
            'ids': duplicates['sale_id'].tolist()
        })
        ti.log.warning(f"Found {len(duplicates)} duplicate records")
  
    # 2. Проверка на пропущенные значения
    missing = df.isnull().sum()
    critical_columns = ['sale_id', 'product_id', 'amount']
    for col in critical_columns:
        if missing[col] > 0:
            validation_results['issues'].append({
                'type': 'missing_values',
                'column': col,
                'count': int(missing[col])
            })
            raise AirflowException(f"Critical column {col} has {missing[col]} missing values")
  
    # 3. Проверка на аномалии
    # Отрицательные значения
    negative_amounts = df[df['amount'] < 0]
    if len(negative_amounts) > 0:
        validation_results['issues'].append({
            'type': 'negative_amounts',
            'count': len(negative_amounts)
        })
        raise AirflowException(f"Found {len(negative_amounts)} records with negative amounts")
  
    # Выбросы (например, amount > 3 стандартных отклонения)
    mean_amount = df['amount'].mean()
    std_amount = df['amount'].std()
    outliers = df[df['amount'] > mean_amount + 3 * std_amount]
    if len(outliers) > 0:
        validation_results['issues'].append({
            'type': 'outliers',
            'count': len(outliers),
            'threshold': float(mean_amount + 3 * std_amount)
        })
        ti.log.warning(f"Found {len(outliers)} outlier records")
  
    # Удаление дубликатов
    df_clean = df.drop_duplicates(subset=['sale_id'], keep='first')
  
    # Сохранение очищенных данных
    ti.xcom_push(key='validated_data', value=df_clean.to_json())
    ti.xcom_push(key='validation_results', value=validation_results)
  
    ti.log.info(f"Validation completed: {len(df_clean)} clean records")
    return validation_results

validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_sales_data,
    provide_context=True,
    dag=dag
)
```

### 3.5 Правила валидации

<pre class="font-ui border-border-100/50 overflow-x-scroll w-full rounded border-[0.5px] shadow-[0_2px_12px_hsl(var(--always-black)/5%)]"><table class="bg-bg-100 min-w-full border-separate border-spacing-0 text-sm leading-[1.88888] whitespace-normal"><thead class="border-b-border-100/50 border-b-[0.5px] text-left"><tr class="[tbody>&]:odd:bg-bg-500/10"><th class="text-text-000 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] px-2 [&:not(:first-child)]:border-l-[0.5px]">Проверка</th><th class="text-text-000 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] px-2 [&:not(:first-child)]:border-l-[0.5px]">Критичность</th><th class="text-text-000 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] px-2 [&:not(:first-child)]:border-l-[0.5px]">Действие</th></tr></thead><tbody><tr class="[tbody>&]:odd:bg-bg-500/10"><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Дубликаты</td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Средняя</td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Warning + удаление дубликатов</td></tr><tr class="[tbody>&]:odd:bg-bg-500/10"><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Пропуски в критических полях</td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Высокая</td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">AirflowException (останов DAG)</td></tr><tr class="[tbody>&]:odd:bg-bg-500/10"><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Отрицательные суммы</td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Высокая</td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">AirflowException (останов DAG)</td></tr><tr class="[tbody>&]:odd:bg-bg-500/10"><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Выбросы (>3σ)</td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Низкая</td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Warning (продолжение работы)</td></tr></tbody></table></pre>

---

## 4. Transform - Трансформация данных

### 4.1 Описание

Расчет бизнес-метрик и агрегация данных по различным измерениям:

* Добавление расчетных полей
* Агрегация по продуктам
* Агрегация по магазинам
* Дневная сводка

### 4.2 Входные данные (XCom)

* `validated_data` из задачи`validate_data`

### 4.3 Выходные данные (XCom)

* `transformed_data` - Обогащенные данные с расчетными полями
* `product_metrics` - Метрики по продуктам
* `store_metrics` - Метрики по магазинам
* `daily_summary` - Дневная сводка

### 4.4 Код задачи

python

```python
def transform_sales_data(**context):
    """
    Трансформация: расчет метрик и агрегация
    """
    ti = context['task_instance']
    validated_json = ti.xcom_pull(task_ids='validate_data', key='validated_data')
    df = pd.read_json(validated_json)
  
    # 1. Добавление расчетных полей
    df['avg_price'] = df['amount'] / df['quantity']
    df['sale_month'] = pd.to_datetime(df['sale_date']).dt.to_period('M')
    df['sale_week'] = pd.to_datetime(df['sale_date']).dt.to_period('W')
  
    # 2. Агрегация по продуктам
    product_metrics = df.groupby('product_id').agg({
        'amount': ['sum', 'mean', 'count'],
        'quantity': 'sum',
        'customer_id': 'nunique'
    }).reset_index()
    product_metrics.columns = ['product_id', 'total_revenue', 'avg_sale', 
                                 'transaction_count', 'total_quantity', 'unique_customers']
  
    # 3. Агрегация по магазинам
    store_metrics = df.groupby('store_id').agg({
        'amount': ['sum', 'mean'],
        'sale_id': 'count',
        'customer_id': 'nunique'
    }).reset_index()
    store_metrics.columns = ['store_id', 'total_revenue', 'avg_transaction', 
                              'transaction_count', 'unique_customers']
  
    # 4. Дневная сводка
    daily_summary = {
        'date': df['sale_date'].iloc[0] if len(df) > 0 else None,
        'total_revenue': float(df['amount'].sum()),
        'total_transactions': len(df),
        'avg_transaction_value': float(df['amount'].mean()),
        'unique_customers': int(df['customer_id'].nunique()),
        'unique_products': int(df['product_id'].nunique())
    }
  
    # Сохранение результатов
    ti.xcom_push(key='transformed_data', value=df.to_json())
    ti.xcom_push(key='product_metrics', value=product_metrics.to_json())
    ti.xcom_push(key='store_metrics', value=store_metrics.to_json())
    ti.xcom_push(key='daily_summary', value=daily_summary)
  
    ti.log.info(f"Transformation completed: {len(df)} records processed")
    return daily_summary

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_sales_data,
    provide_context=True,
    dag=dag
)
```

### 4.5 Расчетные метрики

#### Уровень транзакций

* `avg_price` - Средняя цена за единицу товара
* `sale_month` - Месяц продажи
* `sale_week` - Неделя продажи

#### Уровень продуктов

* `total_revenue` - Общая выручка
* `avg_sale` - Средний чек
* `transaction_count` - Количество транзакций
* `total_quantity` - Общее количество проданных единиц
* `unique_customers` - Уникальные покупатели

#### Уровень магазинов

* `total_revenue` - Общая выручка магазина
* `avg_transaction` - Средняя транзакция
* `transaction_count` - Количество транзакций
* `unique_customers` - Уникальные покупатели

---

## 5. Load - Загрузка в ClickHouse

### 5.1 Описание

Загрузка трансформированных данных и метрик в аналитическое хранилище ClickHouse для последующего анализа.

### 5.2 Входные данные (XCom)

* `transformed_data` - Детальные продажи
* `product_metrics` - Метрики по продуктам
* `store_metrics` - Метрики по магазинам

### 5.3 Целевые таблицы ClickHouse

* `sales_fact` - Факт-таблица продаж
* `product_metrics_daily` - Дневные метрики по продуктам
* `store_metrics_daily` - Дневные метрики по магазинам

### 5.4 Код задачи

python

```python
from clickhouse_driver import Client

def load_to_clickhouse(**context):
    """
    Загрузка данных в ClickHouse
    """
    ti = context['task_instance']
  
    # Получение данных
    transformed_json = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
    product_metrics_json = ti.xcom_pull(task_ids='transform_data', key='product_metrics')
    store_metrics_json = ti.xcom_pull(task_ids='transform_data', key='store_metrics')
  
    df_sales = pd.read_json(transformed_json)
    df_products = pd.read_json(product_metrics_json)
    df_stores = pd.read_json(store_metrics_json)
  
    # Подключение к ClickHouse
    client = Client(
        host='clickhouse_host',
        port=9000,
        user='analytics_user',
        password='password',
        database='analytics'
    )
  
    # 1. Загрузка детальных продаж
    client.execute(
        'INSERT INTO sales_fact VALUES',
        df_sales.to_dict('records')
    )
    ti.log.info(f"Loaded {len(df_sales)} records to sales_fact")
  
    # 2. Загрузка метрик по продуктам
    client.execute(
        'INSERT INTO product_metrics_daily VALUES',
        df_products.to_dict('records')
    )
    ti.log.info(f"Loaded {len(df_products)} product metrics")
  
    # 3. Загрузка метрик по магазинам
    client.execute(
        'INSERT INTO store_metrics_daily VALUES',
        df_stores.to_dict('records')
    )
    ti.log.info(f"Loaded {len(df_stores)} store metrics")
  
    return {
        'sales_loaded': len(df_sales),
        'products_loaded': len(df_products),
        'stores_loaded': len(df_stores)
    }

load_task = PythonOperator(
    task_id='load_to_clickhouse',
    python_callable=load_to_clickhouse,
    provide_context=True,
    dag=dag
)
```

### 5.5 Схемы таблиц ClickHouse

#### Таблица `sales_fact`

sql

```sql
CREATE TABLE sales_fact (
    sale_id UInt64,
    product_id UInt32,
    customer_id UInt32,
    sale_date Date,
    quantity UInt16,
    amount Decimal(10, 2),
    store_id UInt16,
    avg_price Decimal(10, 2),
    created_at DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(sale_date)
ORDER BY (sale_date, sale_id);
```

#### Таблица `product_metrics_daily`

sql

```sql
CREATE TABLE product_metrics_daily (
    date Date,
    product_id UInt32,
    total_revenue Decimal(12, 2),
    avg_sale Decimal(10, 2),
    transaction_count UInt32,
    total_quantity UInt32,
    unique_customers UInt32
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (date, product_id);
```

---

## 6. Generate Report - Генерация отчета

### 6.1 Описание

Создание HTML-отчета с ключевыми метриками, таблицами и визуализациями для отправки стейкхолдерам.

### 6.2 Входные данные (XCom)

* `daily_summary` - Дневная сводка
* `validation_results` - Результаты валидации
* `product_metrics` - Метрики по продуктам

### 6.3 Выходные данные (XCom)

* `report_path` - Путь к сгенерированному HTML файлу

### 6.4 Код задачи

python

```python
from jinja2 import Template

def generate_report(**context):
    """
    Генерация HTML отчета с метриками
    """
    ti = context['task_instance']
    execution_date = context['execution_date']
  
    # Получение данных
    daily_summary = ti.xcom_pull(task_ids='transform_data', key='daily_summary')
    validation_results = ti.xcom_pull(task_ids='validate_data', key='validation_results')
  
    # Подготовка данных для визуализации
    product_metrics_json = ti.xcom_pull(task_ids='transform_data', key='product_metrics')
    df_products = pd.read_json(product_metrics_json)
  
    # Создание графика топ-10 продуктов
    top_products = df_products.nlargest(10, 'total_revenue')
  
    # HTML шаблон отчета
    html_template = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Sales Report - {{ date }}</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            .metric { display: inline-block; margin: 10px; padding: 15px; 
                      background: #f0f0f0; border-radius: 5px; }
            .metric-value { font-size: 24px; font-weight: bold; color: #2c3e50; }
            .metric-label { font-size: 12px; color: #7f8c8d; }
            table { border-collapse: collapse; width: 100%; margin-top: 20px; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
            th { background-color: #3498db; color: white; }
        </style>
    </head>
    <body>
        <h1>Daily Sales Report - {{ date }}</h1>
      
        <h2>Summary Metrics</h2>
        <div class="metric">
            <div class="metric-value">{{ "%.2f"|format(total_revenue) }} ₽</div>
            <div class="metric-label">Total Revenue</div>
        </div>
        <div class="metric">
            <div class="metric-value">{{ total_transactions }}</div>
            <div class="metric-label">Transactions</div>
        </div>
        <div class="metric">
            <div class="metric-value">{{ "%.2f"|format(avg_transaction) }} ₽</div>
            <div class="metric-label">Avg Transaction</div>
        </div>
      
        <h2>Data Quality</h2>
        <p>Total records processed: {{ validation_results.total_records }}</p>
        <p>Issues found: {{ validation_results.issues|length }}</p>
      
        <h2>Top 10 Products by Revenue</h2>
        <table>
            <tr>
                <th>Product ID</th>
                <th>Revenue</th>
                <th>Transactions</th>
                <th>Unique Customers</th>
            </tr>
            {% for product in top_products %}
            <tr>
                <td>{{ product.product_id }}</td>
                <td>{{ "%.2f"|format(product.total_revenue) }}</td>
                <td>{{ product.transaction_count }}</td>
                <td>{{ product.unique_customers }}</td>
            </tr>
            {% endfor %}
        </table>
    </body>
    </html>
    """
  
    # Рендеринг отчета
    template = Template(html_template)
    report_html = template.render(
        date=daily_summary['date'],
        total_revenue=daily_summary['total_revenue'],
        total_transactions=daily_summary['total_transactions'],
        avg_transaction=daily_summary['avg_transaction_value'],
        validation_results=validation_results,
        top_products=top_products.to_dict('records')
    )
  
    # Сохранение отчета
    report_path = f"/tmp/sales_report_{execution_date.strftime('%Y%m%d')}.html"
    with open(report_path, 'w') as f:
        f.write(report_html)
  
    ti.xcom_push(key='report_path', value=report_path)
    ti.log.info(f"Report generated: {report_path}")
  
    return report_path

generate_report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    provide_context=True,
    dag=dag
)
```

### 6.5 Структура отчета

**Отчет включает следующие секции:**

1. **Summary Metrics** - Ключевые метрики дня
   * Общая выручка
   * Количество транзакций
   * Средний чек
2. **Data Quality** - Информация о качестве данных
   * Количество обработанных записей
   * Найденные проблемы
3. **Top 10 Products** - Топ-10 продуктов по выручке
   * ID продукта
   * Выручка
   * Количество транзакций
   * Уникальные покупатели

---

## 7. Send Email - Отправка отчета

### 7.1 Описание

Отправка сгенерированного HTML-отчета на email список получателей с вложением.

### 7.2 Входные данные (XCom)

* `daily_summary` - Дневная сводка для письма
* `report_path` - Путь к HTML файлу отчета

### 7.3 Получатели

* `analytics@company.com`
* `sales@company.com`

### 7.4 Код задачи

python

```python
from airflow.operators.email import EmailOperator

def prepare_email_content(**context):
    """
    Подготовка содержимого email
    """
    ti = context['task_instance']
    daily_summary = ti.xcom_pull(task_ids='transform_data', key='daily_summary')
    report_path = ti.xcom_pull(task_ids='generate_report', key='report_path')
  
    email_body = f"""
    <h2>Daily Sales Report</h2>
    <p>Date: {daily_summary['date']}</p>
    <p>Total Revenue: {daily_summary['total_revenue']:.2f} ₽</p>
    <p>Transactions: {daily_summary['total_transactions']}</p>
    <p>Average Transaction: {daily_summary['avg_transaction_value']:.2f} ₽</p>
    <p>Unique Customers: {daily_summary['unique_customers']}</p>
  
    <p>Detailed report is attached.</p>
    """
  
    ti.xcom_push(key='email_body', value=email_body)
    return email_body

prepare_email_task = PythonOperator(
    task_id='prepare_email',
    python_callable=prepare_email_content,
    provide_context=True,
    dag=dag
)

send_email_task = EmailOperator(
    task_id='send_email',
    to=['analytics@company.com', 'sales@company.com'],
    subject='Daily Sales Report - {{ ds }}',
    html_content="{{ task_instance.xcom_pull(task_ids='prepare_email', key='email_body') }}",
    files=["{{ task_instance.xcom_pull(task_ids='generate_report', key='report_path') }}"],
    dag=dag
)
```

### 7.5 Настройка SMTP

В `airflow.cfg` необходимо настроить параметры SMTP:

ini

```ini
[smtp]
smtp_host = smtp.gmail.com
smtp_starttls = True
smtp_ssl = False
smtp_user = airflow@company.com
smtp_password = your_password
smtp_port = 587
smtp_mail_from = airflow@company.com
```

---

## 8. Telegram Alerts - Уведомления

### 8.1 Описание

Отправка уведомлений в Telegram при возникновении ошибок на любом этапе ETL процесса, а также при успешном завершении.

### 8.2 Типы уведомлений

* **Error Alert** - При ошибке любой задачи (через callback)
* **Success Alert** - При успешном завершении всего DAG

### 8.3 Код задачи

python

```python
from airflow.providers.telegram.operators.telegram import TelegramOperator

def send_telegram_alert(**context):
    """
    Формирование сообщения для Telegram при ошибке
    """
    ti = context['task_instance']
    exception = context.get('exception')
    task_id = context.get('task_instance').task_id
    dag_id = context.get('task_instance').dag_id
    execution_date = context['execution_date']
  
    message = f"""
🚨 *ETL Pipeline Error*

*DAG:* {dag_id}
*Task:* {task_id}
*Execution Date:* {execution_date.strftime('%Y-%m-%d %H:%M:%S')}
*Error:* {str(exception)[:200]}

Please check Airflow UI for details.
    """
  
    return message

# Callback функция для отправки уведомлений
def telegram_failure_callback(context):
    """
    Callback при ошибке задачи
    """
    message = send_telegram_alert(**context)
  
    telegram_alert = TelegramOperator(
        task_id='telegram_alert_on_failure',
        telegram_conn_id='telegram_conn',
        chat_id='-1001234567890',  # ID чата/канала
        text=message,
        parse_mode='Markdown'
    )
  
    telegram_alert.execute(context=context)

# Применение callback ко всем задачам DAG
default_args['on_failure_callback'] = telegram_failure_callback

# Отдельная задача для успешного завершения
def send_success_notification(**context):
    """
    Уведомление об успешном завершении
    """
    ti = context['task_instance']
    daily_summary = ti.xcom_pull(task_ids='transform_data', key='daily_summary')
  
    message = f"""
✅ *Sales ETL Completed Successfully*

*Date:* {daily_summary['date']}
*Revenue:* {daily_summary['total_revenue']:.2f} ₽
*Transactions:* {daily_summary['total_transactions']}
*Customers:* {daily_summary['unique_customers']}
    """
  
    return message

success_notification_prep = PythonOperator(
    task_id='prepare_success_notification',
    python_callable=send_success_notification,
    provide_context=True,
    dag=dag
)

success_notification = TelegramOperator(
    task_id='success_notification',
    telegram_conn_id='telegram_conn',
    chat_id='-1001234567890',
    text="{{ task_instance.xcom_pull(task_ids='prepare_success_notification') }}",
    parse_mode='Markdown',
    dag=dag
)
```

### 8.4 Настройка Telegram Connection

В Airflow UI создать connection с параметрами:

- **Conn Id**: `telegram_conn`
- **Conn Type**: `HTTP`
- **Host**: `https://api.telegram.org`
- **Password**: `your_bot_token`

### 8.5 Формат сообщений

#### При ошибке

```
🚨 ETL Pipeline Error

DAG: sales_etl_daily
Task: validate_data
Execution Date: 2024-11-10 02:00:00
Error: Found 5 records with negative amounts

Please check Airflow UI for details.
```

#### При успехе

```
✅ Sales ETL Completed Successfully

Date: 2024-11-09
Revenue: 1,234,567.89 ₽
Transactions: 5,432
Customers: 3,210
```

---

## 9. Граф зависимостей

### 9.1 Определение зависимостей

python

```python
from airflow.operators.dummy import DummyOperator

# Стартовая и финальная задачи
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

# Основная цепочка зависимостей
start >> extract_task >> validate_task >> transform_task

# Параллельное выполнение загрузки и генерации отчета
transform_task >> [load_task, generate_report_task]

# Отправка email зависит от генерации отчета
generate_report_task >> prepare_email_task >> send_email_task

# Подготовка уведомления об успехе
send_email_task >> success_notification_prep

# Уведомление об успехе после всех задач
[load_task, success_notification_prep] >> success_notification >> end

# Telegram alert вызывается через on_failure_callback автоматически
```

### 9.2 Визуализация графа

```
                                ┌─────────────┐
                                │    START    │
                                └──────┬──────┘
                                       │
                                ┌──────▼──────┐
                                │   extract   │
                                │   _sales    │
                                └──────┬──────┘
                                       │
                                ┌──────▼──────┐
                                │  validate   │
                                │    _data    │
                                └──────┬──────┘
                                       │
                                ┌──────▼──────┐
                                │ transform   │
                                │    _data    │
                                └──────┬──────┘
                                       │
                    ┌──────────────────┴──────────────────┐
                    │                                      │
             ┌──────▼──────┐                      ┌───────▼────────┐
             │    load     │                      │   generate     │
             │     _to     │                      │    _report     │
             │ clickhouse  │                      └───────┬────────┘
             └──────┬──────┘                              │
                    │                              ┌──────▼──────┐
                    │                              │   prepare   │
                    │                              │   _email    │
                    │                              └──────┬──────┘
                    │                                     │
                    │                              ┌──────▼──────┐
                    │                              │    send     │
                    │                              │   _email    │
                    │                              └──────┬──────┘
                    │                                     │
                    │                              ┌──────▼──────┐
                    │                              │   prepare   │
                    │                              │  _success   │
                    │                              │_notification│
                    │                              └──────┬──────┘
                    │                                     │
                    └──────────────────┬──────────────────┘
                                       │
                                ┌──────▼──────┐
                                │   success   │
                                │_notification│
                                └──────┬──────┘
                                       │
                                ┌──────▼──────┐
                                │     END     │
                                └─────────────┘

        ┌────────────────────────────────────────────┐
        │  При ошибке на ЛЮБОМ этапе:                │
        │  on_failure_callback → telegram_alert      │
        └────────────────────────────────────────────┘
```

### 9.3 Характеристики графа

<pre class="font-ui border-border-100/50 overflow-x-scroll w-full rounded border-[0.5px] shadow-[0_2px_12px_hsl(var(--always-black)/5%)]"><table class="bg-bg-100 min-w-full border-separate border-spacing-0 text-sm leading-[1.88888] whitespace-normal"><thead class="border-b-border-100/50 border-b-[0.5px] text-left"><tr class="[tbody>&]:odd:bg-bg-500/10"><th class="text-text-000 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] px-2 [&:not(:first-child)]:border-l-[0.5px]">Характеристика</th><th class="text-text-000 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] px-2 [&:not(:first-child)]:border-l-[0.5px]">Значение</th></tr></thead><tbody><tr class="[tbody>&]:odd:bg-bg-500/10"><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Общее количество задач</td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">11</td></tr><tr class="[tbody>&]:odd:bg-bg-500/10"><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Параллельные ветки</td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">2 (load + report)</td></tr><tr class="[tbody>&]:odd:bg-bg-500/10"><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Максимальная глубина</td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">8 уровней</td></tr><tr class="[tbody>&]:odd:bg-bg-500/10"><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Точки синхронизации</td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">2 (после transform, перед success)</td></tr></tbody></table></pre>

---

## 10. Дополнительные компоненты

### 10.1 Логирование

python

```python
import logging
from airflow.utils.log.logging_mixin import LoggingMixin

class CustomLogger(LoggingMixin):
    """
    Кастомный логгер для детального логирования
    """
    def log_pipeline_start(self, context):
        self.log.info("="*50)
        self.log.info(f"Pipeline started: {context['dag'].dag_id}")
        self.log.info(f"Execution date: {context['execution_date']}")
        self.log.info("="*50)
  
    def log_metrics(self, task_name, metrics):
        self.log.info(f"[{task_name}] Metrics: {metrics}")
  
    def log_data_quality(self, task_name, quality_report):
        self.log.info(f"[{task_name}] Data Quality Report:")
        for key, value in quality_report.items():
            self.log.info(f"  - {key}: {value}")

# Использование в задачах
logger = CustomLogger()

def enhanced_extract(**context):
    logger.log_pipeline_start(context)
    # ... остальной код
    logger.log_metrics('extract', {'records': len(df)})
```

### 10.2 Мониторинг и метрики

python

```python
from airflow.providers.statsd.hooks.statsd import StatsD

def send_metrics(**context):
    """
    Отправка метрик в StatsD/Prometheus
    """
    ti = context['task_instance']
    daily_summary = ti.xcom_pull(task_ids='transform_data', key='daily_summary')
  
    statsd = StatsD()
  
    # Бизнес-метрики
    statsd.gauge('sales.daily.revenue', daily_summary['total_revenue'])
    statsd.gauge('sales.daily.transactions', daily_summary['total_transactions'])
    statsd.gauge('sales.daily.avg_value', daily_summary['avg_transaction_value'])
    statsd.gauge('sales.daily.customers', daily_summary['unique_customers'])
  
    # Технические метрики
    statsd.timing('sales.etl.duration', context['dag_run'].duration.total_seconds())
    statsd.incr('sales.etl.success')

metrics_task = PythonOperator(
    task_id='send_metrics',
    python_callable=send_metrics,
    provide_context=True,
    dag=dag
)
```

### 10.3 Обработка SLA

python

```python
from airflow.models import Variable

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    Callback при нарушении SLA
    """
    message = f"""
⚠️ *SLA Violation*

*DAG:* {dag.dag_id}
*Tasks:* {[task.task_id for task in task_list]}
*Expected:* {slas[0].timestamp}

Action required!
    """
  
    # Отправка в Telegram
    telegram_alert = TelegramOperator(
        task_id='sla_alert',
        telegram_conn_id='telegram_conn',
        chat_id=Variable.get('telegram_alert_chat_id'),
        text=message,
        parse_mode='Markdown'
    )
  
    # Отправка email
    send_email(
        to=['ops@company.com'],
        subject=f'SLA Violation - {dag.dag_id}',
        html_content=message
    )

# Добавление в default_args
default_args['sla'] = timedelta(hours=2)
default_args['sla_miss_callback'] = sla_miss_callback
```

### 10.4 Идемпотентность и повторная обработка

python

```python
def idempotent_load(**context):
    """
    Идемпотентная загрузка с проверкой существующих данных
    """
    ti = context['task_instance']
    execution_date = context['execution_date']
    target_date = execution_date.date()
  
    # Подключение к ClickHouse
    client = Client(...)
  
    # Проверка существующих данных
    existing_count = client.execute(
        f"SELECT COUNT(*) FROM sales_fact WHERE DATE(sale_date) = '{target_date}'"
    )[0][0]
  
    if existing_count > 0:
        ti.log.warning(f"Found {existing_count} existing records for {target_date}")
      
        # Удаление старых данных
        client.execute(f"ALTER TABLE sales_fact DELETE WHERE DATE(sale_date) = '{target_date}'")
        ti.log.info(f"Deleted {existing_count} old records")
  
    # Загрузка новых данных
    # ... остальной код загрузки
```

### 10.5 Data Quality Tests

python

```python
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator

# Интеграция с Great Expectations
ge_validation = GreatExpectationsOperator(
    task_id='validate_with_ge',
    expectation_suite_name='sales_expectations',
    batch_kwargs={
        'datasource': 'postgres_datasource',
        'table': 'sales',
        'data_asset_name': 'sales_data'
    },
    data_context_root_dir='/path/to/great_expectations',
    dag=dag
)

# Альтернатива - кастомные проверки
def advanced_validation(**context):
    """
    Расширенная валидация данных
    """
    ti = context['task_instance']
    df = pd.read_json(ti.xcom_pull(task_ids='extract_sales', key='sales_data'))
  
    tests = {
        'no_future_dates': df['sale_date'] <= pd.Timestamp.now(),
        'positive_amounts': df['amount'] > 0,
        'valid_quantities': (df['quantity'] > 0) & (df['quantity'] < 10000),
        'no_nulls_critical': df[['sale_id', 'amount']].notnull().all(),
        'amount_quantity_match': (df['amount'] / df['quantity']) > 0
    }
  
    failed_tests = {name: test.sum() for name, test in tests.items() if not test.all()}
  
    if failed_tests:
        raise AirflowException(f"Validation failed: {failed_tests}")
  
    ti.log.info("All validation tests passed")
```

---

## 11. Структура проекта

```
airflow_project/
│
├── dags/
│   ├── sales_etl_dag.py              # Основной DAG
│   ├── __init__.py
│   │
│   ├── config/
│   │   ├── __init__.py
│   │   ├── sql_queries.py            # SQL запросы
│   │   ├── email_templates.py        # Шаблоны email
│   │   └── constants.py              # Константы
│   │
│   ├── operators/
│   │   ├── __init__.py
│   │   └── custom_operators.py       # Кастомные операторы
│   │
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── validators.py             # Функции валидации
│   │   ├── transformers.py           # Функции трансформации
│   │   ├── loaders.py                # Функции загрузки
│   │   └── logger.py                 # Логирование
│   │
│   └── tests/
│       ├── __init__.py
│       ├── test_sales_etl.py         # Unit тесты
│       └── test_integration.py       # Интеграционные тесты
│
├── plugins/
│   └── custom_hooks/
│       └── clickhouse_hook.py        # Кастомный Hook для ClickHouse
│
├── requirements.txt                   # Python зависимости
├── docker-compose.yml                 # Docker конфигурация
└── README.md                          # Документация
```

---

## 12. Ключевые особенности реализации

### ✅ Параллельное выполнение

- **Задачи**: `load_to_clickhouse` и `generate_report` выполняются параллельно
- **Выгода**: Сокращение общего времени выполнения DAG на ~30%
- **Требование**: Задачи должны быть независимыми

### ✅ Retry стратегия

- **Количество попыток**: 3
- **Задержка**: Экспоненциальная (5 → 10 → 20 минут)
- **Максимальная задержка**: 30 минут
- **Применение**: Автоматически ко всем задачам через `default_args`

### ✅ Обработка ошибок

- **Механизм**: `on_failure_callback` для всех задач
- **Действие**: Автоматическая отправка уведомления в Telegram
- **Критические ошибки**: Выброс `AirflowException` для останова DAG

### ✅ Логирование

- **Уровни**: INFO, WARNING, ERROR
- **Использование**: `task_instance.log` для логирования в каждой задаче
- **Метрики**: Передача через XCom для построения отчетов

### ✅ Идемпотентность

- **Подход**: Фильтрация по `execution_date`
- **Перезапуск**: Безопасный повторный запуск без дублирования
- **Очистка**: Удаление старых данных перед загрузкой (опционально)

### ✅ Мониторинг

- **Метрики**: Отправка в StatsD/Prometheus
- **Алерты**: Telegram + Email
- **SLA**: Контроль времени выполнения (2 часа)

---

## 13. Конфигурация Connections

### PostgreSQL Connection

```
Conn Id: sales_postgres
Conn Type: Postgres
Host: postgres.company.com
Schema: sales_db
Login: airflow_user
Password: ********
Port: 5432
```

### ClickHouse Connection

```
Conn Id: clickhouse_analytics
Conn Type: Generic
Host: clickhouse.company.com
Login: analytics_user
Password: ********
Port: 9000
Extra: {"database": "analytics"}
```

### Telegram Connection

```
Conn Id: telegram_conn
Conn Type: HTTP
Host: https://api.telegram.org
Password: bot_token_here
```

### SMTP Configuration (airflow.cfg)

ini

```ini
[smtp]
smtp_host = smtp.gmail.com
smtp_starttls = True
smtp_ssl = False
smtp_user = airflow@company.com
smtp_password = app_password_here
smtp_port = 587
smtp_mail_from = airflow@company.com
```

---

## 14. Мониторинг и метрики

### 14.1 Ключевые метрики для отслеживания

<pre class="font-ui border-border-100/50 overflow-x-scroll w-full rounded border-[0.5px] shadow-[0_2px_12px_hsl(var(--always-black)/5%)]"><table class="bg-bg-100 min-w-full border-separate border-spacing-0 text-sm leading-[1.88888] whitespace-normal"><thead class="border-b-border-100/50 border-b-[0.5px] text-left"><tr class="[tbody>&]:odd:bg-bg-500/10"><th class="text-text-000 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] px-2 [&:not(:first-child)]:border-l-[0.5px]">Метрика</th><th class="text-text-000 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] px-2 [&:not(:first-child)]:border-l-[0.5px]">Тип</th><th class="text-text-000 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] px-2 [&:not(:first-child)]:border-l-[0.5px]">Описание</th></tr></thead><tbody><tr class="[tbody>&]:odd:bg-bg-500/10"><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]"><code class="bg-text-200/5 border border-0.5 border-border-300 text-danger-000 whitespace-pre-wrap rounded-[0.4rem] px-1 py-px text-[0.9rem]">sales.etl.duration</code></td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Timing</td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Время выполнения всего DAG</td></tr><tr class="[tbody>&]:odd:bg-bg-500/10"><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]"><code class="bg-text-200/5 border border-0.5 border-border-300 text-danger-000 whitespace-pre-wrap rounded-[0.4rem] px-1 py-px text-[0.9rem]">sales.etl.success</code></td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Counter</td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Количество успешных запусков</td></tr><tr class="[tbody>&]:odd:bg-bg-500/10"><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]"><code class="bg-text-200/5 border border-0.5 border-border-300 text-danger-000 whitespace-pre-wrap rounded-[0.4rem] px-1 py-px text-[0.9rem]">sales.etl.failure</code></td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Counter</td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Количество неудачных запусков</td></tr><tr class="[tbody>&]:odd:bg-bg-500/10"><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]"><code class="bg-text-200/5 border border-0.5 border-border-300 text-danger-000 whitespace-pre-wrap rounded-[0.4rem] px-1 py-px text-[0.9rem]">sales.daily.revenue</code></td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Gauge</td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Дневная выручка</td></tr><tr class="[tbody>&]:odd:bg-bg-500/10"><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]"><code class="bg-text-200/5 border border-0.5 border-border-300 text-danger-000 whitespace-pre-wrap rounded-[0.4rem] px-1 py-px text-[0.9rem]">sales.daily.transactions</code></td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Gauge</td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Количество транзакций</td></tr><tr class="[tbody>&]:odd:bg-bg-500/10"><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]"><code class="bg-text-200/5 border border-0.5 border-border-300 text-danger-000 whitespace-pre-wrap rounded-[0.4rem] px-1 py-px text-[0.9rem]">sales.daily.avg_value</code></td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Gauge</td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Средний чек</td></tr><tr class="[tbody>&]:odd:bg-bg-500/10"><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]"><code class="bg-text-200/5 border border-0.5 border-border-300 text-danger-000 whitespace-pre-wrap rounded-[0.4rem] px-1 py-px text-[0.9rem]">sales.data_quality.issues</code></td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Gauge</td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Количество проблем с данными</td></tr></tbody></table></pre>

### 14.2 Дашборд для Grafana

sql

```sql
-- Метрики производительности ETL
SELECT 
    execution_date,
    dag_id,
    duration,
    state
FROM dag_run
WHERE dag_id = 'sales_etl_daily'
    AND execution_date >= now() - interval '30 days'
ORDER BY execution_date DESC;

-- Бизнес-метрики
SELECT 
    sale_date,
    SUM(amount) as total_revenue,
    COUNT(*) as transaction_count,
    AVG(amount) as avg_transaction
FROM sales_fact
WHERE sale_date >= today() - interval '30 days'
GROUP BY sale_date
ORDER BY sale_date DESC;
```

---

## 15. Тестирование

### 15.1 Unit тесты

python

```python
import unittest
from unittest.mock import Mock, patch
from dags.sales_etl_dag import validate_sales_data

class TestSalesETL(unittest.TestCase):
  
    def test_validation_passes_clean_data(self):
        """Тест валидации с чистыми данными"""
        context = {
            'task_instance': Mock()
        }
      
        # Mock данных
        clean_data = pd.DataFrame({
            'sale_id': [1, 2, 3],
            'amount': [100, 200, 300],
            'product_id': [1, 2, 3]
        })
      
        context['task_instance'].xcom_pull.return_value = clean_data.to_json()
      
        # Выполнение
        result = validate_sales_data(**context)
      
        # Проверка
        self.assertEqual(len(result['issues']), 0)
  
    def test_validation_fails_negative_amounts(self):
        """Тест валидации с отрицательными суммами"""
        context = {
            'task_instance': Mock()
        }
      
        # Mock данных с отрицательной суммой
        bad_data = pd.DataFrame({
            'sale_id': [1, 2, 3],
            'amount': [100, -200, 300],
            'product_id': [1, 2, 3]
        })
      
        context['task_instance'].xcom_pull.return_value = bad_data.to_json()
      
        # Проверка выброса исключения
        with self.assertRaises(AirflowException):
            validate_sales_data(**context)

if __name__ == '__main__':
    unittest.main()
```

### 15.2 Интеграционные тесты

python

```python
from airflow.models import DagBag

class TestDAGIntegrity(unittest.TestCase):
  
    def setUp(self):
        self.dagbag = DagBag()
  
    def test_dag_loaded(self):
        """Проверка загрузки DAG"""
        dag = self.dagbag.get_dag(dag_id='sales_etl_daily')
        self.assertIsNotNone(dag)
        self.assertEqual(len(dag.tasks), 11)
  
    def test_dag_structure(self):
        """Проверка структуры зависимостей"""
        dag = self.dagbag.get_dag(dag_id='sales_etl_daily')
      
        # Проверка зависимостей
        extract_task = dag.get_task('extract_sales')
        validate_task = dag.get_task('validate_data')
      
        self.assertIn(validate_task, extract_task.downstream_list)
```

---

## 16. Оптимизация и Best Practices

### 16.1 Оптимизация производительности

python

```python
# 1. Используйте Bulk Insert вместо построчной вставки
def optimized_load(**context):
    # Плохо: построчная вставка
    for row in df.iterrows():
        client.execute('INSERT INTO table VALUES', [row])
  
    # Хорошо: bulk insert
    client.execute('INSERT INTO table VALUES', df.to_dict('records'))

# 2. Используйте партиционирование данных
def partitioned_transform(**context):
    # Обработка данных по частям
    chunk_size = 10000
    for chunk in pd.read_json(data, chunksize=chunk_size):
        process_chunk(chunk)

# 3. Переиспользуйте connections
@cached
def get_clickhouse_client():
    return Client(...)
```

### 16.2 Best Practices

**DO's:**

* ✅ Используйте XCom для передачи небольших данных (<1MB)
* ✅ Храните большие данные в S3/GCS и передавайте только пути
* ✅ Логируйте все критические операции
* ✅ Используйте параметризованные SQL запросы
* ✅ Добавляйте тайм-ауты для всех операций
* ✅ Используйте идемпотентные операции

**DON'Ts:**

* ❌ Не передавайте большие данные через XCom
* ❌ Не используйте глобальные переменные
* ❌ Не игнорируйте ошибки валидации
* ❌ Не используйте время запуска DAG для бизнес-логики
* ❌ Не делайте задачи слишком большими

---

## 17. Заключение

### 17.1 Краткая сводка

Спроектированный ETL Pipeline обеспечивает:

* ✅**Надежность** : Retry механизм, обработка ошибок, валидация данных
* ✅**Масштабируемость** : Параллельное выполнение, партиционирование
* ✅**Мониторинг** : Логирование, метрики, алерты
* ✅**Прозрачность** : Детальные отчеты, уведомления
* ✅**Идемпотентность** : Безопасные перезапуски

### 17.2 Метрики успеха

<pre class="font-ui border-border-100/50 overflow-x-scroll w-full rounded border-[0.5px] shadow-[0_2px_12px_hsl(var(--always-black)/5%)]"><table class="bg-bg-100 min-w-full border-separate border-spacing-0 text-sm leading-[1.88888] whitespace-normal"><thead class="border-b-border-100/50 border-b-[0.5px] text-left"><tr class="[tbody>&]:odd:bg-bg-500/10"><th class="text-text-000 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] px-2 [&:not(:first-child)]:border-l-[0.5px]">Метрика</th><th class="text-text-000 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] px-2 [&:not(:first-child)]:border-l-[0.5px]">Целевое значение</th></tr></thead><tbody><tr class="[tbody>&]:odd:bg-bg-500/10"><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Время выполнения DAG</td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">< 30 минут</td></tr><tr class="[tbody>&]:odd:bg-bg-500/10"><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Success Rate</td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">> 99%</td></tr><tr class="[tbody>&]:odd:bg-bg-500/10"><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">SLA соблюдение</td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">> 95%</td></tr><tr class="[tbody>&]:odd:bg-bg-500/10"><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Время обнаружения ошибок</td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">< 5 минут</td></tr><tr class="[tbody>&]:odd:bg-bg-500/10"><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">Data Quality Score</td><td class="border-t-border-100/50 [&:not(:first-child)]:-x-[hsla(var(--border-100) / 0.5)] border-t-[0.5px] px-2 [&:not(:first-child)]:border-l-[0.5px]">> 98%</td></tr></tbody></table></pre>
