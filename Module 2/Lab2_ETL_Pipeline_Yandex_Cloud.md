# Лабораторная работа №2

## ETL Pipeline с PythonOperator и XCom в Yandex Managed Service for Apache Airflow

---

## 📋 Цель работы

Освоить создание ETL-пайплайна с использованием **PythonOperator**, научиться работать с механизмом **XCom** для передачи данных между задачами, познакомиться с обработкой CSV файлов, валидацией данных и работой с Yandex Object Storage.

---

## 🎯 Задачи лабораторной работы

1. Создать DAG с использованием PythonOperator
2. Реализовать полный ETL цикл (Extract, Transform, Load)
3. Освоить передачу данных через XCom
4. Научиться валидации и трансформации данных
5. Работать с Yandex Object Storage через S3Hook
6. Настроить обработку ошибок и логирование

---

## 📚 Предварительные требования

**Доступ:**
- Yandex Managed Service for Apache Airflow (уже развернут)
- Yandex Object Storage с бакетом для данных
- Настроенное подключение `yandex_s3` в Airflow

**Знания:**
- Базовые знания Python
- Понимание концепции DAG
- Библиотеки: pandas, boto3

---

## 💼 Бизнес-сценарий

Компания **"Retail Analytics"** нуждается в ежедневном ETL-процессе:

- 📥 Каждый день поступает CSV файл с транзакциями в Object Storage
- 🔍 Необходимо проверить данные на корректность
- 📊 Рассчитать бизнес-метрики (выручка, средний чек, топ товары)
- 💾 Сохранить результат в JSON формате
- 📧 Отправить уведомление о результатах

---

## 📊 Часть 1. Подготовка данных

### Шаг 1. Генерация тестовых данных

Используйте предоставленный скрипт `generate_data.py`:

```bash
# Генерация 1000 транзакций
python generate_data.py

# С параметрами
python generate_data.py -n 5000 -b 0.15 -s
```

**Структура данных:**

| Колонка | Тип | Описание |
|---------|-----|----------|
| transaction_id | integer | ID транзакции |
| date | date | Дата (YYYY-MM-DD) |
| customer_id | integer | ID клиента |
| category | string | Категория товара |
| product | string | Название товара |
| quantity | integer | Количество |
| price | float | Цена за единицу |
| amount | float | Сумма (quantity × price) |

### Шаг 2. Загрузка в Object Storage

Загрузите файл `transactions.csv` в ваш бакет:
- Путь: `s3://your-bucket-name/input/transactions.csv`

---

## 🔧 Часть 2. Создание DAG

### Шаг 3. Архитектура решения

Наш ETL pipeline состоит из 5 задач:

```
Extract → Validate → Transform → Load → Notify
```

**Описание задач:**

1. **Extract** - Извлечение из S3 → pandas DataFrame → XCom
2. **Validate** - Проверка данных (nulls, duplicates, negatives)
3. **Transform** - Расчет метрик и агрегация
4. **Load** - Сохранение JSON в S3
5. **Notify** - Формирование отчета

### Шаг 4. Код DAG

**📄 Полный код:** `etl_pipeline_yandex.py` (отдельный файл)

**Ключевые компоненты:**

#### Конфигурация
```python
S3_BUCKET = 'airflow-etl-data-<ваш-id>'  # Замените!
S3_INPUT_KEY = 'input/transactions.csv'
S3_OUTPUT_PREFIX = 'output/'
S3_CONN_ID = 'yandex_s3'
```

#### Функция Extract
```python
def extract_data(**context):
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    file_content = s3_hook.read_key(
        key=S3_INPUT_KEY,
        bucket_name=S3_BUCKET
    )
    df = pd.read_csv(io.StringIO(file_content))
    
    # Передача через XCom
    ti = context['task_instance']
    ti.xcom_push(key='raw_data', value=df.to_dict('records'))
```

#### Функция Validate
Проверяет:
- Пустые значения
- Дубликаты по transaction_id
- Отрицательные цены/количества
- Некорректные суммы

#### Функция Transform
Рассчитывает:
- Общую выручку
- Средний чек
- Топ-10 товаров
- Разбивку по категориям
- Статистику по дням

#### Функция Load
```python
def load_data(**context):
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    json_content = json.dumps(result, indent=2)
    
    s3_hook.load_string(
        string_data=json_content,
        key=output_key,
        bucket_name=S3_BUCKET
    )
```

### Шаг 5. Загрузка DAG в Airflow

**Вариант 1: Через веб-интерфейс**
1. Скопируйте файл `etl_pipeline_yandex.py`
2. Загрузите в папку DAGs вашего кластера

**Вариант 2: Через Git**
1. Добавьте файл в репозиторий
2. Airflow синхронизирует автоматически

⚠️ **Важно:** Замените `<ваш-id>` в `S3_BUCKET` на реальное имя бакета!

---

## 🧪 Часть 3. Тестирование

### Шаг 6. Проверка DAG

1. Откройте веб-интерфейс Airflow
2. Найдите DAG `etl_pipeline_yandex`
3. Убедитесь, что DAG без ошибок

### Шаг 7. Запуск DAG

1. Включите DAG (переключатель ON)
2. Нажмите ▶️ **Trigger DAG**
3. Откройте **Graph View** для мониторинга

**Статусы задач:**
- ⚪ Не запущена
- 🟡 Выполняется
- 🟢 Успешно
- 🔴 Ошибка

### Шаг 8. Просмотр логов

Для каждой задачи:
1. Кликните на задачу в Graph View
2. Выберите **Log**
3. Изучите вывод

**Пример лога Extract:**
```
[2025-01-15, 10:30:00] INFO - 📥 Начало извлечения данных
[2025-01-15, 10:30:01] INFO - Чтение файла: s3://bucket/input/transactions.csv
[2025-01-15, 10:30:02] INFO - ✅ Загружено 1000 строк
```

### Шаг 9. Проверка результатов

**XCom данные:**
1. Admin → XCom
2. Найдите записи для `etl_pipeline_yandex`

**Выходной файл:**
1. Откройте Object Storage
2. Проверьте `output/etl_results_<дата>.json`

**Структура JSON:**
```json
{
  "execution_date": "2025-01-15",
  "validation": {
    "is_valid": false,
    "issues": ["Найдено 50 записей с отрицательной суммой"]
  },
  "metrics": {
    "summary": {
      "total_sales": 2857433.50,
      "average_transaction": 2857.43
    },
    "top_products": {
      "Ноутбук": 125000.00
    }
  }
}
```

---

## 🔬 Часть 4. Практические задания

### 💡 Задание 1: Тестирование валидации

**Цель:** Проверить обработку некорректных данных

**Шаги:**
1. Сгенерируйте файл с 15% плохих данных:
```bash
python generate_data.py -n 1000 -b 0.15
```

2. Загрузите в S3
3. Запустите DAG
4. Проанализируйте логи валидации

**Вопросы:**
- Сколько проблем обнаружено?
- Какие типы ошибок встречаются?
- Как это влияет на метрики?

### 💡 Задание 2: Добавление новых метрик

**Цель:** Расширить аналитические возможности

**Добавьте в функцию `transform_data`:**

1. **Средняя цена по категориям**
```python
avg_price_by_category = (
    df.groupby('category')['price'].mean().to_dict()
)
```

2. **Распределение по дням недели**
```python
df['weekday'] = df['date'].dt.day_name()
transactions_by_weekday = (
    df.groupby('weekday')['transaction_id'].count().to_dict()
)
```

3. **Топ клиентов по количеству покупок**
```python
top_customers_count = (
    df.groupby('customer_id')['transaction_id']
    .count()
    .sort_values(ascending=False)
    .head(5)
    .to_dict()
)
```

### 💡 Задание 3: Обработка ошибок

**Эксперимент 1: Отсутствующий файл**
1. Удалите файл из S3
2. Запустите DAG
3. Изучите обработку ошибки

**Эксперимент 2: Некорректный формат**
1. Создайте CSV с другой структурой
2. Загрузите в S3
3. Определите, где произойдет ошибка

**Вопросы:**
- Как система обрабатывает ошибки?
- Сколько попыток повтора?
- Как это влияет на последующие задачи?

### 💡 Задание 4: Параллельная валидация

**Цель:** Оптимизировать производительность

**Задача:** Разделите валидацию на независимые задачи:

```python
# Вместо одной задачи validate
validate_nulls_task = PythonOperator(...)
validate_duplicates_task = PythonOperator(...)
validate_negatives_task = PythonOperator(...)

# Параллельное выполнение
extract_task >> [validate_nulls_task, validate_duplicates_task, validate_negatives_task]
[validate_nulls_task, validate_duplicates_task, validate_negatives_task] >> transform_task
```

---

## ❓ Контрольные вопросы

### Базовый уровень

1. **Что такое XCom?**
   - Механизм передачи данных между задачами в Airflow

2. **Какие ограничения у XCom?**
   - Данные хранятся в метабазе (ограничение по размеру)

3. **В чем разница между `xcom_push` и `return`?**
   - Оба сохраняют данные в XCom, но `return` делает это автоматически

4. **Зачем нужна валидация данных?**
   - Обеспечить качество и корректность данных

5. **Что происходит при ошибке в задаче?**
   - Airflow пытается повторить задачу (по параметру `retries`)

### Продвинутый уровень

6. **Как передать большие объемы данных?**
   - Использовать промежуточное хранилище (S3), передавать только пути

7. **Как работает S3Hook с Yandex Object Storage?**
   - S3-совместимый API, требует настройки endpoint

8. **Как настроить инкрементальную загрузку?**
   - Использовать водяные знаки (watermarks), сохранять последнюю дату обработки

9. **Стратегии масштабирования pipeline?**
   - Партиционирование данных, параллелизм задач, использование Celery/Kubernetes Executor

10. **Как организовать мониторинг?**
    - Использовать метрики Airflow, настроить алерты, интегрироваться с системами мониторинга

---

## 🎓 Best Practices

### 1. Идемпотентность
```python
# Плохо: добавление к существующим данным
df = existing_data().append(new_data)

# Хорошо: перезапись
save(new_data, mode='overwrite')
```

### 2. Логирование
```python
import logging
logger = logging.getLogger(__name__)

logger.info("Начало обработки")
logger.warning("Найдены проблемы")
logger.error("Критическая ошибка")
```

### 3. Обработка ошибок
```python
try:
    result = risky_operation()
except SpecificException as e:
    logger.error(f"Ошибка: {e}")
    # Восстановление
except Exception as e:
    raise AirflowException(f"Критическая ошибка: {e}")
```

### 4. Управление секретами
```python
from airflow.models import Variable

# Через Variables
aws_key = Variable.get("aws_access_key_id")

# Через Connections
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection('yandex_s3')
```

### 5. Оптимизация XCom
```python
# Плохо: передача больших данных
ti.xcom_push('data', huge_dataframe.to_dict())

# Хорошо: передача метаданных
s3_path = save_to_s3(huge_dataframe)
ti.xcom_push('data_location', s3_path)
```

---

## 📚 Результаты работы

После завершения вы должны:

### ✅ Знать
- Архитектуру Airflow
- Механизм XCom
- Работу с S3Hook
- Методы валидации данных
- Best practices ETL

### ✅ Уметь
- Создавать ETL pipeline
- Работать с Object Storage
- Валидировать данные
- Отлаживать DAG
- Обрабатывать ошибки

### ✅ Понимать
- Жизненный цикл ETL
- Важность качества данных
- Принципы масштабирования
- Стратегии мониторинга

---

## 🔗 Полезные ссылки

**Документация:**
- [Airflow PythonOperator](https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html)
- [XCom](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html)
- [Yandex Managed Airflow](https://cloud.yandex.ru/docs/managed-airflow/)
- [Pandas](https://pandas.pydata.org/docs/)

---

**Удачи в освоении Data Engineering! 🚀**
