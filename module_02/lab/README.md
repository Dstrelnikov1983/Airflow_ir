# Лабораторная работа №02: Пайплайн sensor_readings.csv → валидация → PostgreSQL

**Модуль:** 02 — Устройство DAG
**Организация:** ТЧЭ-15, Западно-Сибирская дирекция тяги, депо Новосибирск-Главный
**Уровень:** Основной + задания повышенной сложности
**Продолжительность:** 60–90 минут
**Платформа:** Yandex Managed Service for Apache Airflow™

---

## Цель

Создать production-подобный DAG `rzd_buxa_pipeline` с полной цепочкой обработки сенсорных данных ТЧЭ-15:

```
S3KeySensor → read_from_s3 → validate_buxa_temp → load_to_postgres → write_report_to_s3
```

Все операции с файлами — только через `S3Hook` (Yandex Object Storage). Деплой DAG — через Object Storage.

**Парк ТЧЭ-15:** ВЛ80С, ЭП2К, ЭП1М, 2ТЭ116, 2ТЭ25КМ "Витязь", ЧМЭ3, ЭС2Г "Ласточка", ЭД4М.

**Критический параметр:** температура буксы > **80°C** — аварийный сигнал.

---

## Предварительные условия

- Managed Airflow запущен, доступен Airflow UI
- Бакеты и данные настроены (из практической работы №02):
  - `rzd-airflow-dags/` — бакет связан с Managed Airflow
  - `rzd-airflow-data/` — содержит `sensor_readings.csv` и другие CSV
  - `rzd-airflow-results/` — для записи результатов
- Connection `yandex_s3` настроен в Airflow UI:
  - Conn Type: `Amazon S3`
  - Extra: `{"endpoint_url": "https://storage.yandexcloud.net", "region_name": "ru-central1"}`
- Connection `rzd_postgres` настроен (Managed PostgreSQL, схема `rzd_analytics`)
- Variables `s3_bucket_data` и `s3_bucket_results` заданы в Airflow UI
- Таблица `rzd_analytics.sensor_readings` создана (DDL из практики №02)

---

## Задание

### 1. Написать DAG `rzd_buxa_pipeline` с пятью задачами

Граф зависимостей:

```
pipeline_start
    └── check_s3_file  (S3KeySensor)
          └── read_from_s3  (PythonOperator)
                └── validate_buxa_temp  (PythonOperator)
                      └── load_to_postgres  (PythonOperator)
                            └── write_report_to_s3  (PythonOperator)
                                  └── pipeline_end
```

### 2. Реализовать функцию `read_csv_from_s3(bucket, key)` через S3Hook

Функция должна читать CSV из Object Storage без обращения к локальной файловой системе.

### 3. Использовать шаблоны Jinja в ключах S3

Ключ файла с данными:
```
sensor_readings/{{ ds_nodash }}/data.csv
```

Fallback на базовый файл, если файл за конкретную дату не найден.

### 4. Передавать статистику между задачами через XCom

- `read_from_s3` → сохраняет ключ S3 (`xcom_push(key='s3_key', ...)`)
- `validate_buxa_temp` → возвращает словарь со статистикой
- `load_to_postgres` → возвращает количество загруженных строк
- `write_report_to_s3` → читает XCom из всех предыдущих задач

### 5. Реализовать идемпотентную загрузку в PostgreSQL

Использовать `ON CONFLICT (reading_id) DO NOTHING` — повторный запуск не создаёт дублей.

### 6. Записывать итоговый отчёт в Object Storage

Ключ результата: `sensor_readings/{ds_nodash}/report.csv`

Отчёт содержит агрегированную статистику по перегревам букс.

### 7. Задеплоить DAG в бакет `rzd-airflow-dags`

```bash
yc storage cp rzd_buxa_pipeline.py s3://rzd-airflow-dags/dags/rzd_buxa_pipeline.py
```

### 8. Проверить выполнение в Airflow UI

- Включить DAG, запустить вручную (**Trigger DAG**)
- Проверить логи каждой задачи: статистика перегревов, количество загруженных строк
- Убедиться, что отчёт появился в `rzd-airflow-results/sensor_readings/`

### 9. Проверить идемпотентность

Запустить DAG повторно за ту же дату. Выполнить SQL-запрос к `rzd_analytics.sensor_readings` — количество строк не должно измениться.

---

## Полный код DAG

```python
"""
DAG: rzd_buxa_pipeline
Организация: ТЧЭ-15, Западно-Сибирская дирекция тяги
Описание: Production-like пайплайн мониторинга температуры букс.
          Полная цепочка: Object Storage → валидация → PostgreSQL → отчёт в S3.

Платформа: Yandex Managed Service for Apache Airflow™
Все операции с файлами — через S3Hook (Yandex Object Storage).

Задачи:
    check_s3_file      — S3KeySensor: ждём CSV в Object Storage
    read_from_s3       — читаем sensor_readings.csv через S3Hook
    validate_buxa_temp — валидация температур букс (порог 80°C)
    load_to_postgres   — загрузка чистых данных через PostgresHook
    write_report_to_s3 — запись CSV-отчёта в rzd-airflow-results

Расписание: каждый час
Теги: rzd, tche15, safety, buxa, lab
"""

# ─────────────────────────────────────────────────────────────────────────────
# Блок 1: Импорты
# ─────────────────────────────────────────────────────────────────────────────
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
from io import StringIO
import pandas as pd
import logging

# ─────────────────────────────────────────────────────────────────────────────
# Блок 2: Константы
# ─────────────────────────────────────────────────────────────────────────────
S3_CONN_ID         = 'yandex_s3'
PG_CONN_ID         = 'rzd_postgres'
BUXA_CRITICAL_TEMP = 80.0      # °C — критический порог температуры буксы
BAD_DATA_THRESHOLD = 15.0      # % плохих данных для предупреждения

# ─────────────────────────────────────────────────────────────────────────────
# Блок 3: Вспомогательные функции для работы с Object Storage
# Все операции с файлами — ТОЛЬКО через S3Hook.
# pd.read_csv('/local/path') недопустимо в Managed Airflow.
# ─────────────────────────────────────────────────────────────────────────────

def read_csv_from_s3(bucket: str, key: str, conn_id: str = S3_CONN_ID) -> pd.DataFrame:
    """
    Читает CSV-файл из Yandex Object Storage и возвращает DataFrame.
    Использует S3Hook — единственный допустимый способ чтения файлов
    в Yandex Managed Service for Apache Airflow™.
    """
    hook    = S3Hook(aws_conn_id=conn_id)
    obj     = hook.get_key(key=key, bucket_name=bucket)
    content = obj.get()['Body'].read().decode('utf-8')
    df      = pd.read_csv(StringIO(content))
    logging.info(f"[ТЧЭ-15] Прочитан файл s3://{bucket}/{key}: {len(df)} строк")
    return df


def write_csv_to_s3(
    df: pd.DataFrame,
    bucket: str,
    key: str,
    conn_id: str = S3_CONN_ID,
) -> None:
    """
    Записывает DataFrame в Yandex Object Storage в формате CSV.
    Использует hook.load_string() — без локального файла на диске.
    """
    hook       = S3Hook(aws_conn_id=conn_id)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=key,
        bucket_name=bucket,
        replace=True,
    )
    logging.info(f"[ТЧЭ-15] Записан файл s3://{bucket}/{key}: {len(df)} строк")


def write_text_to_s3(
    text: str,
    bucket: str,
    key: str,
    conn_id: str = S3_CONN_ID,
) -> None:
    """Записывает текстовый контент в Object Storage через hook.load_string()."""
    hook = S3Hook(aws_conn_id=conn_id)
    hook.load_string(
        string_data=text,
        key=key,
        bucket_name=bucket,
        replace=True,
    )
    logging.info(f"[ТЧЭ-15] Записан текстовый файл: s3://{bucket}/{key}")


# ─────────────────────────────────────────────────────────────────────────────
# Блок 4: Аргументы по умолчанию
# ─────────────────────────────────────────────────────────────────────────────
default_args = {
    'owner':             'rzd-tche15-lab',
    'depends_on_past':   False,
    'email':             ['duty_engineer@rzd.ru', 'tche15-analytics@rzd.ru'],
    'email_on_failure':  True,
    'email_on_retry':    False,
    'retries':           2,
    'retry_delay':       timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# ─────────────────────────────────────────────────────────────────────────────
# Блок 5: Функции задач (TaskFlow API — декоратор @task)
# ─────────────────────────────────────────────────────────────────────────────

@task
def read_from_s3(ds_nodash: str, ds: str) -> dict:
    """
    Задача 1 (после сенсора): читает sensor_readings.csv из Object Storage.
    Пробует файл по шаблону sensor_readings/{ds_nodash}/data.csv.
    Если не найден — использует базовый sensor_readings.csv.
    Возвращает словарь {'s3_key': ..., 'row_count': ...} — передаётся через XCom.
    """
    bucket    = Variable.get('s3_bucket_data')
    dated_key = f"sensor_readings/{ds_nodash}/data.csv"
    base_key  = 'sensor_readings.csv'

    hook = S3Hook(aws_conn_id=S3_CONN_ID)
    if hook.check_for_key(key=dated_key, bucket_name=bucket):
        key = dated_key
        logging.info(f"[ТЧЭ-15] Используем файл за {ds}: s3://{bucket}/{key}")
    else:
        key = base_key
        logging.warning(
            f"[ТЧЭ-15] Файл за {ds} не найден, используем базовый: "
            f"s3://{bucket}/{key}"
        )

    df = read_csv_from_s3(bucket=bucket, key=key)

    logging.info(f"[ТЧЭ-15] Строк в файле:    {len(df)}")
    logging.info(f"[ТЧЭ-15] Локомотивов:       {df['loco_id'].nunique()}")
    logging.info(f"[ТЧЭ-15] Серии:             {df['series'].unique().tolist()}")
    logging.info(
        f"[ТЧЭ-15] Период:           "
        f"{pd.to_datetime(df['timestamp']).min()} — "
        f"{pd.to_datetime(df['timestamp']).max()}"
    )

    for sensor_type, group in df.groupby('sensor_type'):
        logging.info(
            f"  {sensor_type}: {len(group)} записей, "
            f"среднее={group['value'].mean():.2f}, "
            f"мин={group['value'].min():.2f}, "
            f"макс={group['value'].max():.2f}"
        )

    return {
        's3_key':   key,
        'row_count': len(df),
        'loco_count': int(df['loco_id'].nunique()),
        'series':   df['series'].unique().tolist(),
    }


@task
def validate_buxa_temp(read_result: dict) -> dict:
    """
    Задача 2: валидация температур букс локомотивов.
    Получает ключ S3 из XCom задачи read_from_s3.
    Проверяет:
      - структуру обязательных колонок
      - долю плохих записей (quality_flag=0)
      - критические перегревы буксы (>80°C)
    Возвращает словарь со статистикой валидации.
    """
    bucket = Variable.get('s3_bucket_data')
    key    = read_result['s3_key']

    df = read_csv_from_s3(bucket=bucket, key=key)

    # Проверка 1: обязательные колонки
    required_columns = {
        'reading_id', 'loco_id', 'series', 'timestamp',
        'sensor_type', 'value', 'unit', 'quality_flag',
    }
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(
            f"[ТЧЭ-15] Критическая ошибка: отсутствуют колонки {missing}. "
            "Формат файла телеметрии изменился?"
        )
    logging.info("[ТЧЭ-15] Проверка 1/3: структура колонок — OK")

    # Проверка 2: quality_flag
    total   = len(df)
    bad     = int((df['quality_flag'] == 0).sum())
    good    = total - bad
    bad_pct = bad / total * 100 if total > 0 else 0.0

    logging.info(
        f"[ТЧЭ-15] Проверка 2/3: quality_flag — "
        f"хорошие: {good}, плохие: {bad} ({bad_pct:.1f}%)"
    )
    if bad_pct > BAD_DATA_THRESHOLD:
        logging.warning(
            f"[ТЧЭ-15] ПРЕДУПРЕЖДЕНИЕ: {bad_pct:.1f}% плохих записей. "
            "Проверьте датчики локомотивов!"
        )

    # Проверка 3: критические перегревы букс
    buxa_df  = df[df['sensor_type'] == 'buxa_temp_c'].copy()
    critical = buxa_df[buxa_df['value'] > BUXA_CRITICAL_TEMP]

    critical_by_series: dict = {}
    if len(critical) > 0:
        logging.warning(
            f"[ТЧЭ-15] КРИТИЧНО: {len(critical)} случаев перегрева буксы "
            f"(>{BUXA_CRITICAL_TEMP}°C)!"
        )
        for _, row in critical.iterrows():
            logging.warning(
                f"  ПЕРЕГРЕВ: {row['loco_id']} ({row['series']}) — "
                f"{row['value']:.1f}°C в {row['timestamp']}"
            )
        critical_by_series = (
            critical.groupby('series')['reading_id'].count().to_dict()
        )
    else:
        logging.info("[ТЧЭ-15] Проверка 3/3: критических перегревов букс нет — OK")

    val_result = {
        's3_key':            key,
        'total':             total,
        'good':              good,
        'bad':               bad,
        'bad_pct':           round(bad_pct, 2),
        'buxa_critical_cnt': len(critical),
        'critical_by_series': critical_by_series,
        'needs_alert':       len(critical) > 0,
        'locos_checked':     int(df['loco_id'].nunique()),
    }

    logging.info(f"[ТЧЭ-15] Итоги валидации: {val_result}")
    return val_result


@task(execution_timeout=timedelta(minutes=20))
def load_to_postgres(val_result: dict) -> int:
    """
    Задача 3: загрузка данных с quality_flag=1 в Managed PostgreSQL.
    Читает CSV из Object Storage через S3Hook (ключ из XCom).
    Использует PostgresHook (Connection rzd_postgres).
    Идемпотентность: ON CONFLICT (reading_id) DO NOTHING.
    Возвращает количество загруженных строк.
    """
    bucket = Variable.get('s3_bucket_data')
    key    = val_result['s3_key']

    df       = read_csv_from_s3(bucket=bucket, key=key)
    df_clean = df[df['quality_flag'] == 1].copy()

    logging.info(
        f"[ТЧЭ-15] Подготовлено к загрузке: {len(df_clean)} строк "
        f"(пропущено {len(df) - len(df_clean)} записей с quality_flag=0)"
    )

    pg_hook  = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn     = pg_hook.get_conn()
    cur      = conn.cursor()
    inserted = 0

    try:
        for _, row in df_clean.iterrows():
            cur.execute(
                """INSERT INTO sensor_readings
                   (reading_id, loco_id, series, timestamp,
                    sensor_type, value, unit, quality_flag)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (reading_id) DO NOTHING""",
                (
                    row['reading_id'],
                    row['loco_id'],
                    row['series'],
                    row['timestamp'],
                    row['sensor_type'],
                    float(row['value']),
                    row['unit'],
                    int(row['quality_flag']),
                ),
            )
            inserted += cur.rowcount

        conn.commit()
        logging.info(
            f"[ТЧЭ-15] Успешно загружено строк в rzd_analytics.sensor_readings: "
            f"{inserted}"
        )
        return inserted

    except Exception as e:
        conn.rollback()
        logging.error(f"[ТЧЭ-15] Ошибка загрузки в PostgreSQL: {e}")
        raise
    finally:
        cur.close()
        conn.close()


@task(retries=1)
def write_report_to_s3(val_result: dict, inserted_cnt: int, ds_nodash: str, ds: str) -> None:
    """
    Задача 4: формирует CSV-отчёт и записывает его в Object Storage.
    Использует hook.load_string() — без локального файла на диске.
    Ключ отчёта: sensor_readings/{ds_nodash}/report.csv
    """
    bucket_results = Variable.get('s3_bucket_results')

    # Формируем DataFrame отчёта
    report_data = {
        'run_date':           [ds],
        'depot_code':         [Variable.get('depot_code', default_var='TCH-15')],
        'locos_checked':      [val_result.get('locos_checked', 0)],
        'total_readings':     [val_result.get('total', 0)],
        'good_readings':      [val_result.get('good', 0)],
        'bad_readings':       [val_result.get('bad', 0)],
        'bad_pct':            [val_result.get('bad_pct', 0.0)],
        'buxa_critical_cnt':  [val_result.get('buxa_critical_cnt', 0)],
        'critical_by_series': [str(val_result.get('critical_by_series', {}))],
        'inserted_to_pg':     [inserted_cnt],
        'needs_alert':        [val_result.get('needs_alert', False)],
    }
    report_df  = pd.DataFrame(report_data)
    report_key = f"sensor_readings/{ds_nodash}/report.csv"

    write_csv_to_s3(df=report_df, bucket=bucket_results, key=report_key)

    # Дублируем краткий итог в лог
    logging.info(
        f"[ТЧЭ-15] Отчёт записан: s3://{bucket_results}/{report_key}\n"
        f"  Дата:                    {ds}\n"
        f"  Локомотивов проверено:   {val_result.get('locos_checked')}\n"
        f"  Загружено в PostgreSQL:  {inserted_cnt}\n"
        f"  Критич. перегревов букс: {val_result.get('buxa_critical_cnt')}\n"
        f"  По сериям:               {val_result.get('critical_by_series')}"
    )

    if val_result.get('needs_alert'):
        logging.warning(
            "[ТЧЭ-15] АЛЕРТ БЕЗОПАСНОСТИ: обнаружены критические перегревы букс! "
            "Требуется проверка локомотивов дежурным инженером ТЧЭ-15."
        )


# ─────────────────────────────────────────────────────────────────────────────
# Блок 6: Определение DAG
# ─────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id='rzd_buxa_pipeline',
    description='ТЧЭ-15: пайплайн мониторинга температуры букс через Object Storage',
    schedule='0 * * * *',
    start_date=datetime(2024, 3, 1),
    catchup=False,
    default_args=default_args,
    tags=['rzd', 'tche15', 'safety', 'buxa', 'lab'],
    max_active_runs=1,
    doc_md="""
## rzd_buxa_pipeline DAG

Production-like пайплайн ТЧЭ-15 (депо Новосибирск-Главный).

**Платформа:** Yandex Managed Service for Apache Airflow™
**Хранилище:** Yandex Object Storage (все файловые операции через S3Hook)
**БД:** Yandex Managed PostgreSQL, схема rzd_analytics

### Функциональность
- S3KeySensor ждёт появления файла в бакете
- Чтение CSV через S3Hook (без локальной файловой системы)
- Валидация температур букс: порог 80°C
- Идемпотентная загрузка: ON CONFLICT DO NOTHING
- Отчёт CSV записывается в rzd-airflow-results через S3Hook

### Парк ТЧЭ-15
ВЛ80С, ЭП2К, ЭП1М, 2ТЭ116, 2ТЭ25КМ "Витязь", ЧМЭ3, ЭС2Г "Ласточка", ЭД4М
    """,
) as dag:

    pipeline_start = EmptyOperator(task_id='pipeline_start')

    # Задача 0 (сенсор): ожидание файла в Object Storage
    check_s3_file = S3KeySensor(
        task_id='check_s3_file',
        bucket_name='{{ var.value.s3_bucket_data }}',
        bucket_key='sensor_readings.csv',
        aws_conn_id=S3_CONN_ID,
        poke_interval=300,
        timeout=7200,
        mode='reschedule',
    )

    pipeline_end = EmptyOperator(task_id='pipeline_end')

    # ─────────────────────────────────────────────────────────────
    # Блок 7: Граф зависимостей (TaskFlow API)
    # pipeline_start → check_s3_file → read_from_s3
    #   → validate_buxa_temp → load_to_postgres
    #   → write_report_to_s3 → pipeline_end
    # ─────────────────────────────────────────────────────────────
    read_result  = read_from_s3(
        ds_nodash="{{ ds_nodash }}",
        ds="{{ ds }}",
    )
    val_result   = validate_buxa_temp(read_result)
    inserted_cnt = load_to_postgres(val_result)
    write_report_to_s3(
        val_result=val_result,
        inserted_cnt=inserted_cnt,
        ds_nodash="{{ ds_nodash }}",
        ds="{{ ds }}",
    )

    (
        pipeline_start
        >> check_s3_file
        >> read_result
        >> pipeline_end
    )
```

---

## Деплой и тестирование

### Загрузка DAG в Object Storage

Сохраните код выше в файл `rzd_buxa_pipeline.py` на своём компьютере, затем загрузите в бакет.

Через Yandex Cloud CLI:

```bash
yc storage cp rzd_buxa_pipeline.py s3://rzd-airflow-dags/dags/rzd_buxa_pipeline.py
```

Через Yandex Cloud Console:

1. Откройте бакет `rzd-airflow-dags`
2. Нажмите **Загрузить объекты**
3. Выберите файл `rzd_buxa_pipeline.py`

Проверьте успешную загрузку:

```bash
yc storage ls s3://rzd-airflow-dags/dags/
```

### Проверка в Airflow UI

1. Откройте Airflow UI → список DAG
2. Подождите 1–3 минуты — планировщик сканирует бакет автоматически
3. Найдите `rzd_buxa_pipeline` в списке
4. Если DAG не появился — проверьте **Admin → Import Errors** на предмет синтаксических ошибок

### Запуск и наблюдение

1. Включите тумблер DAG (Enabled)
2. Нажмите **▷ Trigger DAG**
3. Откройте **Graph View** — наблюдайте за статусами задач:

| Цвет | Статус |
|---|---|
| Серый | scheduled / no status |
| Оранжевый | queued |
| Зелёный мигающий | running |
| Зелёный | success |
| Красный | failed |
| Светло-зелёный | up_for_retry |

4. Кликните на задачу `validate_buxa_temp` → **Log** — убедитесь, что видны строки:
   - `[ТЧЭ-15] Проверка 1/3: структура колонок — OK`
   - `[ТЧЭ-15] Проверка 2/3: quality_flag — ...`
   - `[ТЧЭ-15] Итоги валидации: {...}`

5. Кликните на `write_report_to_s3` → **Log** — убедитесь, что видна строка:
   - `[ТЧЭ-15] Отчёт записан: s3://rzd-airflow-results/sensor_readings/...`

### Ожидаемый результат

После успешного выполнения:

- DAG `rzd_buxa_pipeline` в Airflow UI со статусом **success** (зелёный)
- В логе `validate_buxa_temp` — статистика валидации: количество перегревов, доля плохих данных
- В логе `load_to_postgres` — строка `Успешно загружено строк в rzd_analytics.sensor_readings: N`
- В бакете `rzd-airflow-results/sensor_readings/{ds_nodash}/report.csv` — файл с итоговой статистикой
- В XCom (`Admin → XCom`) — значения от `read_from_s3`, `validate_buxa_temp`, `load_to_postgres`

Проверка результата в Object Storage:

```bash
yc storage ls s3://rzd-airflow-results/sensor_readings/
```

Проверка результата в PostgreSQL (через Query Tool в Yandex Cloud Console или psql):

```sql
-- Количество загруженных записей
SELECT COUNT(*) AS total FROM sensor_readings;

-- Статистика по сериям
SELECT series, COUNT(*) AS cnt FROM sensor_readings GROUP BY series ORDER BY cnt DESC;

-- Критические перегревы букс
SELECT loco_id, series, MAX(value) AS max_temp
FROM sensor_readings
WHERE sensor_type = 'buxa_temp_c'
GROUP BY loco_id, series
HAVING MAX(value) > 80
ORDER BY max_temp DESC;
```

---

## Задания повышенной сложности

### Задание 1: Ветвление на основе результата валидации

**Цель:** добавить `BranchPythonOperator` — если обнаружены критические перегревы, направить граф в ветку `send_alert`, иначе пропустить уведомление.

```python
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty  import EmptyOperator

@task.branch
def branch_on_critical(val_result: dict) -> str:
    """Возвращает task_id следующей задачи в зависимости от результата валидации."""
    if val_result.get('needs_alert'):
        return 'log_critical_alert'
    return 'skip_alert'

@task
def log_critical_alert(val_result: dict) -> None:
    """Логирует критический алерт о перегреве букс."""
    logging.warning(
        f"[ТЧЭ-15] АЛЕРТ БЕЗОПАСНОСТИ!\n"
        f"  Критических перегревов: {val_result['buxa_critical_cnt']}\n"
        f"  По сериям: {val_result['critical_by_series']}\n"
        "  Дежурному инженеру: проверьте указанные локомотивы!"
    )
    # В production: добавьте EmailOperator или HTTP-запрос к Telegram Bot API

skip_alert = EmptyOperator(task_id='skip_alert', trigger_rule='none_failed_min_one_success')
```

Обновите граф:

```python
branch     = branch_on_critical(val_result)
alert_task = log_critical_alert(val_result)
branch >> [alert_task, skip_alert] >> inserted_cnt
```

### Задание 2: Батчевая загрузка вместо построчного INSERT

**Цель:** заменить цикл `for _, row in df.iterrows()` на батчевую вставку через `execute_values` — значительно ускоряет загрузку для больших файлов.

```python
from psycopg2.extras import execute_values

@task(execution_timeout=timedelta(minutes=20))
def load_to_postgres_batch(val_result: dict) -> int:
    """
    Батчевая загрузка данных в PostgreSQL через execute_values.
    В 10-50 раз быстрее построчного INSERT для файлов > 10 000 строк.
    """
    bucket   = Variable.get('s3_bucket_data')
    key      = val_result['s3_key']
    df       = read_csv_from_s3(bucket=bucket, key=key)
    df_clean = df[df['quality_flag'] == 1].copy()

    records = [
        (
            row['reading_id'], row['loco_id'],    row['series'],
            row['timestamp'],  row['sensor_type'], float(row['value']),
            row['unit'],       int(row['quality_flag']),
        )
        for _, row in df_clean.iterrows()
    ]

    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn    = pg_hook.get_conn()
    cur     = conn.cursor()
    try:
        execute_values(
            cur,
            """INSERT INTO sensor_readings
               (reading_id, loco_id, series, timestamp,
                sensor_type, value, unit, quality_flag)
               VALUES %s
               ON CONFLICT (reading_id) DO NOTHING""",
            records,
            page_size=500,
        )
        inserted = cur.rowcount
        conn.commit()
        logging.info(f"[ТЧЭ-15] Батчевая загрузка: {inserted} строк")
        return inserted
    except Exception as e:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
```

### Задание 3: Агрегированный отчёт по перегревам буксы в S3

**Цель:** вместо записи одной строки статистики формировать детальный CSV-отчёт по каждому локомотиву с перегревами.

```python
@task(retries=1)
def write_detailed_report_to_s3(val_result: dict, ds_nodash: str) -> None:
    """
    Читает данные повторно, агрегирует перегревы по локомотивам
    и записывает детальный отчёт в Object Storage.
    Ключ: sensor_readings/{ds_nodash}/overheats_detail.csv
    """
    bucket        = Variable.get('s3_bucket_data')
    bucket_result = Variable.get('s3_bucket_results')
    key           = val_result['s3_key']

    df       = read_csv_from_s3(bucket=bucket, key=key)
    buxa_df  = df[df['sensor_type'] == 'buxa_temp_c'].copy()
    critical = buxa_df[buxa_df['value'] > BUXA_CRITICAL_TEMP]

    if critical.empty:
        logging.info("[ТЧЭ-15] Перегревов нет — детальный отчёт не создаётся.")
        return

    # Агрегируем по локомотиву
    detail_df = (
        critical
        .groupby(['loco_id', 'series'])
        .agg(
            overheat_count=('value', 'count'),
            max_temp_c=('value', 'max'),
            avg_temp_c=('value', 'mean'),
            first_event=('timestamp', 'min'),
            last_event=('timestamp', 'max'),
        )
        .reset_index()
        .sort_values('max_temp_c', ascending=False)
    )

    report_key = f"sensor_readings/{ds_nodash}/overheats_detail.csv"
    write_csv_to_s3(df=detail_df, bucket=bucket_result, key=report_key)
    logging.info(
        f"[ТЧЭ-15] Детальный отчёт записан: "
        f"s3://{bucket_result}/{report_key} "
        f"({len(detail_df)} локомотивов с перегревами)"
    )
```

Добавьте вызов в граф DAG после `write_report_to_s3`.
