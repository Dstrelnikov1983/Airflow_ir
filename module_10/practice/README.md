# Практическая работа №10: Production-ready DAG с идемпотентностью и Object Storage

**Модуль:** 10 — Практика использования Airflow  
**Продолжительность:** 45–60 минут  
**Уровень:** Средний  
**Платформа:** Yandex Managed Service for Apache Airflow™

---

## Цель и задачи

Научиться применять production best practices при разработке DAG в среде Yandex Managed Service for Apache Airflow, где **доступ к локальной файловой системе отсутствует** — все файловые операции выполняются через Yandex Object Storage (S3-совместимый).

После выполнения работы вы будете уметь:

- читать и записывать CSV-файлы через `S3Hook` (aws_conn_id=`yandex_s3`);
- обеспечивать идемпотентность через DELETE/INSERT по дате;
- реализовывать атомарную запись: сначала во временный ключ `tmp/`, затем переименование в `results/`;
- регистрировать ошибки в Object Storage через `on_failure_callback`;
- документировать DAG через `doc_md` с указанием бакетов и ключей;
- деплоить DAG-файлы через бакет `rzd-airflow-dags/`.

---

## Предварительные условия

### 1. Managed Airflow — настройка Connection для Object Storage

В Airflow UI откройте **Admin → Connections → Add Connection**:

| Параметр | Значение |
|----------|----------|
| Conn Id | `yandex_s3` |
| Conn Type | `Amazon Web Services` |
| Login | `<Access Key ID сервисного аккаунта>` |
| Password | `<Secret Access Key>` |
| Extra | `{"endpoint_url": "https://storage.yandexcloud.net", "region_name": "ru-central1"}` |

### 2. Managed PostgreSQL — настройка Connection

| Параметр | Значение |
|----------|----------|
| Conn Id | `rzd_postgres` |
| Conn Type | `Postgres` |
| Host | `<FQDN кластера>.mdb.yandexcloud.net` |
| Schema | `rzd_analytics` |
| Login | `airflow` |
| Password | `<пароль из среды>` |
| Port | `5432` |

### 3. Структура бакетов Object Storage

```
rzd-airflow-dags/          — DAG-файлы (связан с Managed Airflow)
rzd-airflow-data/          — входные данные
  ├── sensor_readings.csv
  ├── locomotives.csv
  ├── trips.csv
  └── schedule_adherence.csv
rzd-airflow-results/       — результаты обработки
  ├── tmp/                 — временные файлы (атомарная запись)
  ├── results/             — финальные результаты
  └── errors/              — логи ошибок (on_failure_callback)
```

### 4. Переменные Airflow (Admin → Variables)

| Ключ | Значение |
|------|----------|
| `s3_bucket_data` | `rzd-airflow-data` |
| `s3_bucket_results` | `rzd-airflow-results` |
| `depot_code` | `TCH-15` |
| `delay_threshold_min` | `15` |

### 5. Таблицы PostgreSQL rzd_analytics

Перед началом работы убедитесь, что в БД существуют таблицы `sensor_readings`, `trips`, `locomotives`, `kpi_daily`. Если таблицы не созданы — выполните SQL-скрипт из материалов курса.

---

## Шаги выполнения

### Шаг 1. Загрузка тестовых данных в Object Storage

Данные для практики загружаются **только через Yandex Object Storage**. Локальная файловая система в Managed Airflow недоступна.

Загрузите CSV-файлы из материалов курса в бакет `rzd-airflow-data` через Yandex Cloud Console или CLI:

```bash
# Загрузка через Yandex Cloud CLI
yc storage cp sensor_readings.csv s3://rzd-airflow-data/sensor_readings.csv
yc storage cp locomotives.csv     s3://rzd-airflow-data/locomotives.csv
yc storage cp trips.csv           s3://rzd-airflow-data/trips.csv
```

Или через AWS CLI, настроенный на Yandex Object Storage:

```bash
aws s3 cp sensor_readings.csv s3://rzd-airflow-data/sensor_readings.csv \
    --endpoint-url https://storage.yandexcloud.net
```

Убедитесь в наличии файлов через Yandex Cloud Console → Object Storage → `rzd-airflow-data`.

### Шаг 2. Настройка соединений в Airflow UI

1. Войдите в Airflow UI (ссылка из Yandex Cloud Console → Managed Airflow).
2. Откройте **Admin → Connections** и создайте соединения `yandex_s3` и `rzd_postgres` по таблицам из раздела «Предварительные условия».
3. Проверьте соединения кнопкой **Test** (при наличии).

### Шаг 3. Изучение паттернов работы с S3Hook

Прежде чем писать DAG, изучите базовые паттерны чтения и записи через `S3Hook`:

**Чтение CSV из Object Storage:**

```python
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from io import StringIO
import pandas as pd

def read_csv_from_s3(
    bucket: str,
    key: str,
    conn_id: str = 'yandex_s3',
) -> pd.DataFrame:
    hook = S3Hook(aws_conn_id=conn_id)
    obj = hook.get_key(key=key, bucket_name=bucket)
    content = obj.get()['Body'].read().decode('utf-8')
    return pd.read_csv(StringIO(content))
```

**Запись CSV в Object Storage:**

```python
def write_csv_to_s3(
    df: pd.DataFrame,
    bucket: str,
    key: str,
    conn_id: str = 'yandex_s3',
) -> None:
    hook = S3Hook(aws_conn_id=conn_id)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=key,
        bucket_name=bucket,
        replace=True,
    )
```

**Атомарная запись (tmp → final):**

```python
def atomic_write_s3(
    df: pd.DataFrame,
    bucket: str,
    tmp_key: str,
    final_key: str,
    conn_id: str = 'yandex_s3',
) -> None:
    hook = S3Hook(aws_conn_id=conn_id)
    # 1. Записать во временный ключ
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=tmp_key,
        bucket_name=bucket,
        replace=True,
    )
    # 2. Скопировать во финальный ключ
    hook.copy_object(
        source_bucket_key=tmp_key,
        dest_bucket_key=final_key,
        source_bucket_name=bucket,
        dest_bucket_name=bucket,
    )
    # 3. Удалить временный ключ
    hook.delete_objects(bucket=bucket, keys=[tmp_key])
```

### Шаг 4. Создание DAG с идемпотентностью и атомарной записью

Создайте файл `rzd_kpi_s3_pipeline.py` со следующим содержимым (полный код приведён в разделе «Полный код»).

Ключевые требования к реализации:

| Требование | Реализация |
|------------|------------|
| Идемпотентность | DELETE FROM kpi_daily WHERE kpi_date = `{{ ds }}` перед INSERT |
| Атомарность | Запись в `results/tmp/{{ ds }}/`, затем `copy_object` в `results/final/` |
| Ошибки | `on_failure_callback` пишет error-лог в `errors/{{ ds }}/` |
| Документация | `doc_md` с перечнем бакетов и ключей |
| Деплой | Через бакет `rzd-airflow-dags/`, не через локальную папку `dags/` |

### Шаг 5. Деплой DAG через Object Storage

В Managed Airflow **нельзя** копировать файлы через `ssh` / `scp` / локальную папку. Деплой выполняется исключительно через Object Storage:

```bash
# Загрузить DAG-файл в бакет, связанный с Managed Airflow
yc storage cp rzd_kpi_s3_pipeline.py \
    s3://rzd-airflow-dags/dags/rzd_kpi_s3_pipeline.py
```

Или через AWS CLI:

```bash
aws s3 cp rzd_kpi_s3_pipeline.py \
    s3://rzd-airflow-dags/dags/rzd_kpi_s3_pipeline.py \
    --endpoint-url https://storage.yandexcloud.net
```

После загрузки подождите 30–60 секунд и обновите страницу Airflow UI — DAG появится в списке автоматически.

### Шаг 6. Проверка работы DAG

**6.1. Ручной запуск DAG:**

В Airflow UI найдите DAG `rzd_kpi_s3_pipeline` → нажмите **Trigger DAG** → укажите дату.

**6.2. Проверка идемпотентности:**

Запустите DAG дважды за одну дату и убедитесь, что строки в PostgreSQL не дублируются:

```sql
-- Выполнить в psql или через Admin → Connections → Test
SELECT
    kpi_date,
    COUNT(*)                              AS total_rows,
    COUNT(DISTINCT loco_id)              AS unique_locos,
    COUNT(*) = COUNT(DISTINCT loco_id)   AS is_idempotent
FROM kpi_daily
WHERE kpi_date = '2024-03-15'
GROUP BY kpi_date;
-- Столбец is_idempotent должен быть TRUE
```

**6.3. Проверка атомарной записи в S3:**

Через Yandex Cloud Console → Object Storage → `rzd-airflow-results`:
- Убедитесь, что файл присутствует в `results/final/2024-03-15/kpi_daily.csv`.
- Убедитесь, что временный файл `results/tmp/` удалён после успешного выполнения.

**6.4. Проверка on_failure_callback:**

Отключите соединение `yandex_s3` (установите неверный ключ) и запустите DAG снова. Убедитесь, что:
- В `rzd-airflow-results/errors/2024-03-15/` появился файл error-лога.
- В логах задачи отображается сообщение об ошибке.

---

## Полный код

```python
"""
rzd_kpi_s3_pipeline.py

Production-ready DAG расчёта KPI локомотивов депо ТЧЭ-15.
Западно-Сибирская дирекция тяги, депо Новосибирск-Главный.

Среда выполнения: Yandex Managed Service for Apache Airflow.
Все файловые операции — через Yandex Object Storage (S3Hook).
Локальная файловая система не используется.

Бакеты:
  - rzd-airflow-data/          — входные CSV
  - rzd-airflow-results/tmp/   — временные файлы (атомарная запись)
  - rzd-airflow-results/results/final/ — финальные результаты
  - rzd-airflow-results/errors/ — логи ошибок
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

# ── Константы ─────────────────────────────────────────────────────────────────

S3_CONN_ID      = 'yandex_s3'
PG_CONN_ID      = 'rzd_postgres'
BUCKET_DATA     = 'rzd-airflow-data'
BUCKET_RESULTS  = 'rzd-airflow-results'

KEY_SENSOR      = 'sensor_readings.csv'
KEY_TRIPS       = 'trips.csv'


# ── Вспомогательные функции S3 ────────────────────────────────────────────────

def read_csv_from_s3(
    bucket: str,
    key: str,
    conn_id: str = S3_CONN_ID,
) -> pd.DataFrame:
    """Читает CSV из Object Storage и возвращает DataFrame."""
    hook = S3Hook(aws_conn_id=conn_id)
    obj = hook.get_key(key=key, bucket_name=bucket)
    content = obj.get()['Body'].read().decode('utf-8')
    return pd.read_csv(StringIO(content))


def write_csv_to_s3(
    df: pd.DataFrame,
    bucket: str,
    key: str,
    conn_id: str = S3_CONN_ID,
) -> None:
    """Записывает DataFrame как CSV в Object Storage."""
    hook = S3Hook(aws_conn_id=conn_id)
    buf = StringIO()
    df.to_csv(buf, index=False)
    hook.load_string(
        string_data=buf.getvalue(),
        key=key,
        bucket_name=bucket,
        replace=True,
    )


def atomic_write_s3(
    df: pd.DataFrame,
    bucket: str,
    tmp_key: str,
    final_key: str,
    conn_id: str = S3_CONN_ID,
) -> None:
    """
    Атомарная запись в Object Storage:
    1. Записать DataFrame во временный ключ (tmp/).
    2. Скопировать во финальный ключ (results/final/).
    3. Удалить временный ключ.
    """
    hook = S3Hook(aws_conn_id=conn_id)

    # Шаг 1 — запись во временный ключ
    buf = StringIO()
    df.to_csv(buf, index=False)
    hook.load_string(
        string_data=buf.getvalue(),
        key=tmp_key,
        bucket_name=bucket,
        replace=True,
    )
    log.info("Временный файл записан: s3://%s/%s", bucket, tmp_key)

    # Шаг 2 — копирование во финальный ключ
    hook.copy_object(
        source_bucket_key=tmp_key,
        dest_bucket_key=final_key,
        source_bucket_name=bucket,
        dest_bucket_name=bucket,
    )
    log.info("Финальный файл записан: s3://%s/%s", bucket, final_key)

    # Шаг 3 — удаление временного ключа
    hook.delete_objects(bucket=bucket, keys=[tmp_key])
    log.info("Временный файл удалён: s3://%s/%s", bucket, tmp_key)


# ── on_failure_callback ───────────────────────────────────────────────────────

def on_failure_write_s3(context: dict) -> None:
    """
    При ошибке задачи:
    - Логирует ошибку через log.error.
    - Записывает error-лог в Object Storage:
      rzd-airflow-results/errors/{{ ds }}/{{ task_id }}.json
    """
    ti  = context['task_instance']
    dag = context['dag']
    ds  = context['ds']
    exc = context.get('exception', 'неизвестная ошибка')

    log.error(
        "[RZD ALERT] Сбой задачи: dag=%s task=%s date=%s err=%s",
        dag.dag_id, ti.task_id, ds, exc,
    )

    error_payload = json.dumps(
        {
            "dag_id":    dag.dag_id,
            "task_id":   ti.task_id,
            "exec_date": ds,
            "error":     str(exc),
        },
        ensure_ascii=False,
        indent=2,
    )

    error_key = f"errors/{ds}/{ti.task_id}.json"

    try:
        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        hook.load_string(
            string_data=error_payload,
            key=error_key,
            bucket_name=BUCKET_RESULTS,
            replace=True,
        )
        log.info(
            "Error-лог записан: s3://%s/%s",
            BUCKET_RESULTS, error_key,
        )
    except Exception as cb_exc:
        log.error(
            "Не удалось записать error-лог в S3: %s", cb_exc,
        )


# ── Callable-функции ──────────────────────────────────────────────────────────

def extract_sensor_data(**ctx) -> None:
    """
    Задача 1: читает sensor_readings.csv из rzd-airflow-data/,
    фильтрует по дате выполнения (ctx['ds']),
    передаёт количество строк через XCom.
    """
    ds = ctx['ds']
    log.info("Чтение телеметрии за %s из s3://%s/%s", ds, BUCKET_DATA, KEY_SENSOR)

    df = read_csv_from_s3(bucket=BUCKET_DATA, key=KEY_SENSOR)
    df['reading_date'] = pd.to_datetime(df['reading_date']).dt.date.astype(str)
    df_day = df[df['reading_date'] == ds].copy()

    log.info("Строк телеметрии за %s: %d", ds, len(df_day))

    if df_day.empty:
        raise ValueError(
            f"Нет данных телеметрии за {ds} в s3://{BUCKET_DATA}/{KEY_SENSOR}. "
            "Проверить поступление данных с датчиков локомотивов."
        )

    ctx['ti'].xcom_push(key='sensor_row_count', value=len(df_day))


def calculate_kpi(**ctx) -> None:
    """
    Задача 2: читает trips.csv и sensor_readings.csv из rzd-airflow-data/,
    вычисляет KPI за ctx['ds'],
    выполняет UPSERT в PostgreSQL (DELETE + INSERT — идемпотентность),
    записывает результат атомарно в rzd-airflow-results/.
    """
    ds   = ctx['ds']
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

    # Чтение данных из Object Storage
    log.info("Чтение данных из s3://%s/", BUCKET_DATA)
    df_sensor = read_csv_from_s3(bucket=BUCKET_DATA, key=KEY_SENSOR)
    df_trips  = read_csv_from_s3(bucket=BUCKET_DATA, key=KEY_TRIPS)

    df_sensor['reading_date'] = (
        pd.to_datetime(df_sensor['reading_date']).dt.date.astype(str)
    )
    df_trips['trip_date'] = (
        pd.to_datetime(df_trips['trip_date']).dt.date.astype(str)
    )

    df_sensor_day = df_sensor[df_sensor['reading_date'] == ds]
    df_trips_day  = df_trips[df_trips['trip_date'] == ds]

    if df_sensor_day.empty:
        raise ValueError(
            f"Нет данных телеметрии за {ds}. Пропуск расчёта KPI."
        )

    # Расчёт KPI
    kpi = (
        df_sensor_day
        .groupby('loco_id')
        .agg(
            buxa_overheat=('buxa_temp_max', lambda x: (x > 80).sum()),
            fuel_per_km=(
                'fuel_rate',
                lambda x: (
                    x.mean()
                    / df_sensor_day.loc[x.index, 'speed_kmh'].replace(0, pd.NA).mean()
                    if df_sensor_day.loc[x.index, 'speed_kmh'].mean() > 0
                    else None
                ),
            ),
        )
        .reset_index()
    )
    kpi['kpi_date'] = ds

    if not df_trips_day.empty:
        otd = (
            df_trips_day.groupby('loco_id')['on_time']
            .mean()
            .mul(100)
            .round(2)
            .reset_index()
            .rename(columns={'on_time': 'otd_pct'})
        )
        kpi = kpi.merge(otd, on='loco_id', how='left')
    else:
        kpi['otd_pct'] = None

    log.info("KPI рассчитан: %d локомотивов за %s", len(kpi), ds)

    # Идемпотентность: DELETE перед INSERT
    log.info("DELETE FROM kpi_daily WHERE kpi_date = '%s'", ds)
    hook.run(
        "DELETE FROM kpi_daily WHERE kpi_date = %(ds)s",
        parameters={'ds': ds},
    )

    # INSERT в PostgreSQL
    for _, row in kpi.iterrows():
        hook.run(
            """
            INSERT INTO kpi_daily
                (kpi_date, loco_id, otd_pct, fuel_per_km, buxa_overheat)
            VALUES (%(kpi_date)s, %(loco_id)s,
                    %(otd_pct)s, %(fuel_per_km)s, %(buxa_overheat)s)
            """,
            parameters={
                'kpi_date':     row['kpi_date'],
                'loco_id':      row['loco_id'],
                'otd_pct':      row.get('otd_pct'),
                'fuel_per_km':  row.get('fuel_per_km'),
                'buxa_overheat': int(row['buxa_overheat']),
            },
        )

    log.info("KPI записан в PostgreSQL: %d строк.", len(kpi))

    # Атомарная запись результата в Object Storage
    tmp_key   = f"results/tmp/{ds}/kpi_daily.csv"
    final_key = f"results/final/{ds}/kpi_daily.csv"

    atomic_write_s3(
        df=kpi,
        bucket=BUCKET_RESULTS,
        tmp_key=tmp_key,
        final_key=final_key,
    )
    log.info(
        "Результат записан атомарно: s3://%s/%s",
        BUCKET_RESULTS, final_key,
    )


def verify_results(**ctx) -> None:
    """
    Задача 3: читает финальный файл из Object Storage и проверяет корректность.
    """
    ds        = ctx['ds']
    final_key = f"results/final/{ds}/kpi_daily.csv"

    log.info("Проверка результата: s3://%s/%s", BUCKET_RESULTS, final_key)

    df = read_csv_from_s3(bucket=BUCKET_RESULTS, key=final_key)

    assert not df.empty, (
        f"Файл результата пуст: s3://{BUCKET_RESULTS}/{final_key}"
    )
    assert 'loco_id' in df.columns, "Отсутствует столбец loco_id в результате"
    assert 'kpi_date' in df.columns, "Отсутствует столбец kpi_date в результате"

    log.info(
        "Проверка пройдена: %d строк KPI за %s в s3://%s/%s",
        len(df), ds, BUCKET_RESULTS, final_key,
    )


# ── DAG ───────────────────────────────────────────────────────────────────────

DAG_DOC = """
## rzd_kpi_s3_pipeline

**Назначение:** расчёт суточных KPI локомотивного парка ТЧЭ-15
с атомарной записью результатов в Yandex Object Storage.

**Среда:** Yandex Managed Service for Apache Airflow.
Локальная файловая система **не используется**.

**Расписание:** ежедневно в 06:00 MSK.

**Бакеты Object Storage:**

| Бакет | Назначение | Ключи |
|-------|------------|-------|
| `rzd-airflow-data` | Входные данные | `sensor_readings.csv`, `trips.csv` |
| `rzd-airflow-results` | Результаты | `results/final/{{ ds }}/kpi_daily.csv` |
| `rzd-airflow-results` | Временные файлы | `results/tmp/{{ ds }}/kpi_daily.csv` |
| `rzd-airflow-results` | Логи ошибок | `errors/{{ ds }}/{{ task_id }}.json` |
| `rzd-airflow-dags` | DAG-файлы | `dags/rzd_kpi_s3_pipeline.py` |

**Идемпотентность:** DELETE WHERE kpi_date = `{{ ds }}` перед INSERT.

**Атомарность:** запись через tmp/ + copy_object + delete tmp.

**Ответственный:** Отдел АСУ ТЧЭ-15 · duty@tceh15.rzd.ru
"""

default_args = {
    "owner":                     "asu-tceh15",
    "retries":                   3,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":           timedelta(hours=1),
    "on_failure_callback":       on_failure_write_s3,
    "email_on_failure":          False,
    "email_on_retry":            False,
    "execution_timeout":         timedelta(hours=2),
}

with DAG(
    dag_id="rzd_kpi_s3_pipeline",
    description=(
        "KPI локомотивов ТЧЭ-15 — S3 + PostgreSQL, "
        "идемпотентность, атомарная запись"
    ),
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 3, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    doc_md=DAG_DOC,
    tags=["rzd", "tceh-15", "production", "s3", "kpi"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_sensor_data",
        python_callable=extract_sensor_data,
        sla=timedelta(minutes=10),
        doc_md=(
            "Читает `sensor_readings.csv` из `rzd-airflow-data/` через S3Hook. "
            "Фильтрует по дате выполнения. "
            "Бросает ValueError при отсутствии данных."
        ),
    )

    calc_task = PythonOperator(
        task_id="calculate_kpi",
        python_callable=calculate_kpi,
        sla=timedelta(minutes=20),
        doc_md=(
            "Читает CSV из `rzd-airflow-data/` через S3Hook. "
            "DELETE + INSERT в kpi_daily (идемпотентность). "
            "Атомарная запись результата в "
            "`rzd-airflow-results/results/final/{{ ds }}/kpi_daily.csv`."
        ),
    )

    verify_task = PythonOperator(
        task_id="verify_results",
        python_callable=verify_results,
        sla=timedelta(minutes=25),
        doc_md=(
            "Читает финальный файл из `rzd-airflow-results/results/final/` "
            "через S3Hook и проверяет корректность результата."
        ),
    )

    extract_task >> calc_task >> verify_task
```

### Деплой DAG в Managed Airflow

```bash
# Загрузить DAG в бакет, связанный с Managed Airflow
yc storage cp rzd_kpi_s3_pipeline.py \
    s3://rzd-airflow-dags/dags/rzd_kpi_s3_pipeline.py

# Или через AWS CLI
aws s3 cp rzd_kpi_s3_pipeline.py \
    s3://rzd-airflow-dags/dags/rzd_kpi_s3_pipeline.py \
    --endpoint-url https://storage.yandexcloud.net
```

> **Важно:** НЕ использовать `airflow dags`, `ssh`, `scp` или локальную папку `dags/`.  
> В Managed Airflow DAG-файлы загружаются **только через Object Storage**.

---

## Контрольные вопросы

1. **Почему в Yandex Managed Service for Apache Airflow нельзя использовать `open()` или `pd.read_csv('path/to/file.csv')` для чтения данных?**  
   Опишите, чем `S3Hook.get_key()` отличается от чтения локального файла, и почему это важно для горизонтально масштабируемой среды.

2. **Объясните разницу между следующими подходами к обеспечению идемпотентности:**
   - `DELETE WHERE kpi_date = '{{ ds }}'` + INSERT
   - `INSERT ... ON CONFLICT (kpi_date, loco_id) DO UPDATE SET ...`
   
   В каком случае каждый из них предпочтительнее? Какие риски существуют у DELETE + INSERT при параллельном выполнении?

3. **Зачем при атомарной записи в Object Storage используется промежуточный ключ `tmp/`?**  
   Что произойдёт, если задача упадёт в момент записи — между `load_string` и `copy_object`? Как это влияет на следующий retry?

4. **`on_failure_callback` записывает error-лог в `rzd-airflow-results/errors/{{ ds }}/`.  
   Почему callback сам обёрнут в `try/except`? Что может пойти не так внутри callback, и как это влияет на статус задачи в Airflow?**

5. **DAG настроен с `catchup=False`. Представьте, что DAG не запускался 5 дней из-за технических работ.  
   Какой подход следует использовать для заполнения пропущенных дат? Напишите команду для запуска backfill через Airflow CLI или UI и объясните, почему важно ограничить параллелизм (`max_active_runs=1`) при выполнении backfill в данном пайплайне.**
