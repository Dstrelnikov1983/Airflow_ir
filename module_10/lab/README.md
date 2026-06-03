# Лабораторная работа №10: Production DAG: UPSERT + атомарная запись в S3 + callbacks

**Модуль:** 10 — Практика использования Airflow  
**Продолжительность:** 60–90 минут  
**Уровень:** Продвинутый  
**Платформа:** Yandex Managed Service for Apache Airflow™

---

## Цель

Разработать production-ready DAG `mes_production.py` для Yandex Managed Service for Apache Airflow, реализующий полный пайплайн обработки производственных данных депо ТЧЭ-15 с применением следующих практик:

- все файловые операции — через `S3Hook` (aws_conn_id=`yandex_s3`), локальная файловая система не используется;
- чтение CSV из `rzd-airflow-data/` через `S3Hook`;
- UPSERT в PostgreSQL через `ON CONFLICT DO UPDATE`;
- атомарная запись результатов: сначала в `rzd-airflow-results/tmp/`, после успеха — `copy_object` в `results/final/`;
- `on_failure_callback` пишет error-лог в `rzd-airflow-results/errors/{{ ds }}/`;
- деплой DAG-файла через бакет `rzd-airflow-dags/`.

---

## Предварительные условия

### 1. Yandex Object Storage — настройка Connection

В Airflow UI (**Admin → Connections → Add Connection**):

| Параметр | Значение |
|----------|----------|
| Conn Id | `yandex_s3` |
| Conn Type | `Amazon Web Services` |
| Login | `<Access Key ID сервисного аккаунта Yandex Cloud>` |
| Password | `<Secret Access Key>` |
| Extra | `{"endpoint_url": "https://storage.yandexcloud.net", "region_name": "ru-central1"}` |

### 2. Yandex Managed PostgreSQL — настройка Connection

| Параметр | Значение |
|----------|----------|
| Conn Id | `rzd_postgres` |
| Conn Type | `Postgres` |
| Host | `<FQDN кластера>.mdb.yandexcloud.net` |
| Schema | `rzd_analytics` |
| Login | `airflow` |
| Password | `<пароль из Yandex Cloud Console>` |
| Port | `5432` |

### 3. Структура бакетов Object Storage

```
rzd-airflow-dags/               — DAG-файлы (связан с Managed Airflow)
  └── dags/
      └── mes_production.py

rzd-airflow-data/               — входные данные
  ├── sensor_readings.csv
  ├── locomotives.csv
  ├── trips.csv
  ├── schedule_adherence.csv
  └── maintenance.csv

rzd-airflow-results/            — результаты и логи
  ├── tmp/                      — временные файлы (атомарная запись)
  │   └── {{ ds }}/
  ├── final/                    — финальные результаты
  │   └── {{ ds }}/
  └── errors/                   — логи ошибок (on_failure_callback)
      └── {{ ds }}/
```

### 4. Переменные Airflow (Admin → Variables)

| Ключ | Значение |
|------|----------|
| `s3_bucket_data` | `rzd-airflow-data` |
| `s3_bucket_results` | `rzd-airflow-results` |
| `depot_code` | `TCH-15` |
| `delay_threshold_min` | `15` |

### 5. Схема БД rzd_analytics

Убедитесь, что в Yandex Managed PostgreSQL существуют таблицы.  
Если таблицы не созданы — выполните SQL через psql или Yandex Cloud Console:

```sql
-- Парк локомотивов
CREATE TABLE IF NOT EXISTS locomotives (
    loco_id        VARCHAR(20)  PRIMARY KEY,
    series         VARCHAR(20)  NOT NULL,
    traction_type  VARCHAR(10)  NOT NULL,
    depot_code     VARCHAR(10)  NOT NULL DEFAULT 'TCEH15',
    assigned_date  DATE         NOT NULL,
    moto_hours_cur NUMERIC(10,2) DEFAULT 0,
    next_to_type   VARCHAR(10),
    next_to_hours  NUMERIC(10,2),
    status         VARCHAR(20)  DEFAULT 'active'
);

-- Телеметрия датчиков
CREATE TABLE IF NOT EXISTS sensor_readings (
    id               BIGSERIAL    PRIMARY KEY,
    loco_id          VARCHAR(20)  NOT NULL REFERENCES locomotives(loco_id),
    reading_ts       TIMESTAMPTZ  NOT NULL,
    reading_date     DATE         NOT NULL,
    buxa_temp_max    NUMERIC(6,2),
    traction_current NUMERIC(8,2),
    catenary_voltage NUMERIC(8,2),
    fuel_rate        NUMERIC(8,2),
    moto_hours       NUMERIC(10,2),
    speed_kmh        NUMERIC(6,2),
    lat              NUMERIC(10,6),
    lon              NUMERIC(10,6)
);

-- Поездки
CREATE TABLE IF NOT EXISTS trips (
    trip_id         BIGSERIAL    PRIMARY KEY,
    loco_id         VARCHAR(20)  NOT NULL REFERENCES locomotives(loco_id),
    trip_date       DATE         NOT NULL,
    route_code      VARCHAR(30)  NOT NULL,
    departure_plan  TIMESTAMPTZ,
    departure_fact  TIMESTAMPTZ,
    arrival_plan    TIMESTAMPTZ,
    arrival_fact    TIMESTAMPTZ,
    train_weight_t  NUMERIC(10,2),
    distance_km     NUMERIC(10,2),
    on_time         BOOLEAN,
    delay_min       INTEGER      DEFAULT 0,
    UNIQUE (loco_id, trip_date, route_code)
);

-- Ежедневные KPI
CREATE TABLE IF NOT EXISTS kpi_daily (
    kpi_date      DATE          NOT NULL,
    loco_id       VARCHAR(20)   NOT NULL REFERENCES locomotives(loco_id),
    otd_pct       NUMERIC(5,2),
    fuel_per_km   NUMERIC(8,4),
    energy_per_km NUMERIC(8,4),
    buxa_overheat INTEGER       DEFAULT 0,
    trips_count   INTEGER       DEFAULT 0,
    avg_delay_min NUMERIC(7,2)  DEFAULT 0,
    updated_at    TIMESTAMPTZ   DEFAULT now(),
    PRIMARY KEY (kpi_date, loco_id)
);

-- Уведомления о ТО
CREATE TABLE IF NOT EXISTS maintenance_alerts (
    id              BIGSERIAL    PRIMARY KEY,
    loco_id         VARCHAR(20)  NOT NULL,
    alert_date      DATE         NOT NULL,
    alert_type      VARCHAR(30)  NOT NULL,
    moto_hours_cur  NUMERIC(10,2),
    moto_hours_next NUMERIC(10,2),
    hours_remaining NUMERIC(10,2),
    description     TEXT,
    is_critical     BOOLEAN      DEFAULT false,
    created_at      TIMESTAMPTZ  DEFAULT now(),
    UNIQUE (loco_id, alert_date, alert_type)
);

-- Индексы
CREATE INDEX IF NOT EXISTS idx_sensor_readings_date
    ON sensor_readings (reading_date, loco_id);
CREATE INDEX IF NOT EXISTS idx_trips_date
    ON trips (trip_date, loco_id);
CREATE INDEX IF NOT EXISTS idx_kpi_daily_date
    ON kpi_daily (kpi_date);
```

### 6. Загрузка тестовых данных в Object Storage

Загружайте CSV-файлы **только в Object Storage** — не на локальную файловую систему:

```bash
# Через Yandex Cloud CLI
yc storage cp sensor_readings.csv s3://rzd-airflow-data/sensor_readings.csv
yc storage cp locomotives.csv     s3://rzd-airflow-data/locomotives.csv
yc storage cp trips.csv           s3://rzd-airflow-data/trips.csv
yc storage cp maintenance.csv     s3://rzd-airflow-data/maintenance.csv

# Или через AWS CLI (с настроенным endpoint)
aws s3 cp sensor_readings.csv s3://rzd-airflow-data/sensor_readings.csv \
    --endpoint-url https://storage.yandexcloud.net
```

---

## Задание

Реализуйте DAG `mes_production.py` по следующим шагам.

### Шаг 1. Создать файл DAG и подготовить импорты

Создайте файл `mes_production.py`. В начале файла разместите:

```python
"""
mes_production.py

Production DAG обработки данных MES-системы депо ТЧЭ-15.
Западно-Сибирская дирекция тяги, депо Новосибирск-Главный.

Среда: Yandex Managed Service for Apache Airflow.
Все файловые операции — через Yandex Object Storage (S3Hook).
Локальная файловая система НЕ используется.
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
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
```

### Шаг 2. Определить константы и вспомогательные функции S3

Реализуйте три вспомогательные функции для работы с Object Storage.  
**Использовать только `S3Hook`, никаких локальных путей.**

```python
S3_CONN_ID     = 'yandex_s3'
PG_CONN_ID     = 'rzd_postgres'
BUCKET_DATA    = 'rzd-airflow-data'
BUCKET_RESULTS = 'rzd-airflow-results'

log = logging.getLogger(__name__)


def read_csv_from_s3(
    bucket: str,
    key: str,
    conn_id: str = S3_CONN_ID,
) -> pd.DataFrame:
    """Читает CSV из Object Storage через S3Hook."""
    hook = S3Hook(aws_conn_id=conn_id)
    obj  = hook.get_key(key=key, bucket_name=bucket)
    content = obj.get()['Body'].read().decode('utf-8')
    return pd.read_csv(StringIO(content))


def write_csv_to_s3(
    df: pd.DataFrame,
    bucket: str,
    key: str,
    conn_id: str = S3_CONN_ID,
) -> None:
    """Записывает DataFrame как CSV в Object Storage через S3Hook."""
    hook = S3Hook(aws_conn_id=conn_id)
    buf  = StringIO()
    df.to_csv(buf, index=False)
    hook.load_string(
        string_data=buf.getvalue(),
        key=key,
        bucket_name=bucket,
        replace=True,
    )


def atomic_copy_s3(
    bucket: str,
    tmp_key: str,
    final_key: str,
    conn_id: str = S3_CONN_ID,
) -> None:
    """
    Атомарное перемещение объекта в Object Storage:
    copy_object(tmp → final) + delete_objects(tmp).
    """
    hook = S3Hook(aws_conn_id=conn_id)
    hook.copy_object(
        source_bucket_key=tmp_key,
        dest_bucket_key=final_key,
        source_bucket_name=bucket,
        dest_bucket_name=bucket,
    )
    hook.delete_objects(bucket=bucket, keys=[tmp_key])
    log.info(
        "Атомарное перемещение: s3://%s/%s → s3://%s/%s",
        bucket, tmp_key, bucket, final_key,
    )
```

### Шаг 3. Реализовать on_failure_callback

`on_failure_callback` должен записывать JSON-лог ошибки в Object Storage.  
**Запрещено использовать `open()` или любые локальные пути.**

```python
def on_failure_write_error_s3(context: dict) -> None:
    """
    При сбое задачи записывает JSON error-лог в:
    s3://rzd-airflow-results/errors/{{ ds }}/{{ task_id }}.json
    """
    ti  = context['task_instance']
    dag = context['dag']
    ds  = context['ds']
    exc = context.get('exception', 'неизвестная ошибка')

    log.error(
        "[MES FAIL] dag=%s task=%s date=%s error=%s",
        dag.dag_id, ti.task_id, ds, exc,
    )

    payload = json.dumps(
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
            string_data=payload,
            key=error_key,
            bucket_name=BUCKET_RESULTS,
            replace=True,
        )
        log.info(
            "Error-лог записан: s3://%s/%s",
            BUCKET_RESULTS, error_key,
        )
    except Exception as cb_err:
        log.error(
            "Не удалось записать error-лог в S3: %s", cb_err,
        )
```

### Шаг 4. Реализовать callable-функции задач

#### 4.1. load_sensor_data — чтение и валидация данных из S3

Реализуйте функцию, которая:
- читает `sensor_readings.csv` из `rzd-airflow-data/` через `read_csv_from_s3`;
- фильтрует строки по `ctx['ds']`;
- бросает `ValueError` при отсутствии данных;
- сохраняет количество строк в XCom.

```python
def load_sensor_data(**ctx) -> None:
    """Читает sensor_readings.csv из rzd-airflow-data/ через S3Hook."""
    ds  = ctx['ds']
    key = 'sensor_readings.csv'

    log.info("Чтение: s3://%s/%s", BUCKET_DATA, key)
    df = read_csv_from_s3(bucket=BUCKET_DATA, key=key)

    df['reading_date'] = (
        pd.to_datetime(df['reading_date']).dt.date.astype(str)
    )
    df_day = df[df['reading_date'] == ds]

    if df_day.empty:
        raise ValueError(
            f"Нет данных телеметрии за {ds} "
            f"в s3://{BUCKET_DATA}/{key}"
        )

    log.info("Строк за %s: %d", ds, len(df_day))
    ctx['ti'].xcom_push(key='sensor_row_count', value=len(df_day))
```

#### 4.2. upsert_kpi_to_postgres — UPSERT KPI в PostgreSQL

Реализуйте функцию, которая:
- читает `sensor_readings.csv` и `trips.csv` из `rzd-airflow-data/` через `S3Hook`;
- вычисляет KPI (OTD%, расход, перегревы букс);
- выполняет UPSERT в `kpi_daily` через `ON CONFLICT DO UPDATE`.

```python
def upsert_kpi_to_postgres(**ctx) -> None:
    """
    Читает CSV из rzd-airflow-data/ через S3Hook.
    UPSERT KPI в PostgreSQL (ON CONFLICT DO UPDATE).
    """
    ds   = ctx['ds']
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

    log.info("Чтение данных из s3://%s/", BUCKET_DATA)
    df_sensor = read_csv_from_s3(
        bucket=BUCKET_DATA, key='sensor_readings.csv'
    )
    df_trips = read_csv_from_s3(
        bucket=BUCKET_DATA, key='trips.csv'
    )

    df_sensor['reading_date'] = (
        pd.to_datetime(df_sensor['reading_date']).dt.date.astype(str)
    )
    df_trips['trip_date'] = (
        pd.to_datetime(df_trips['trip_date']).dt.date.astype(str)
    )

    df_s = df_sensor[df_sensor['reading_date'] == ds]
    df_t = df_trips[df_trips['trip_date'] == ds]

    if df_s.empty:
        raise ValueError(
            f"Нет данных sensor_readings за {ds}"
        )

    # Расчёт KPI по датчикам
    kpi = (
        df_s.groupby('loco_id')
        .agg(
            buxa_overheat=('buxa_temp_max', lambda x: int((x > 80).sum())),
            fuel_per_km=(
                'fuel_rate',
                lambda x: round(
                    float(x.mean())
                    / float(df_s.loc[x.index, 'speed_kmh']
                             .replace(0, pd.NA).mean())
                    if df_s.loc[x.index, 'speed_kmh'].mean() > 0
                    else 0.0,
                    4,
                ),
            ),
        )
        .reset_index()
    )

    # OTD% из поездок
    if not df_t.empty:
        otd = (
            df_t.groupby('loco_id')
            .agg(
                otd_pct=('on_time', lambda x: round(x.mean() * 100, 2)),
                trips_count=('trip_id', 'count'),
                avg_delay_min=('delay_min', lambda x: round(x.mean(), 2)),
            )
            .reset_index()
        )
        kpi = kpi.merge(otd, on='loco_id', how='left')
    else:
        kpi['otd_pct']      = None
        kpi['trips_count']  = 0
        kpi['avg_delay_min'] = 0.0

    kpi['kpi_date'] = ds

    # UPSERT через ON CONFLICT DO UPDATE
    upsert_sql = """
        INSERT INTO kpi_daily
            (kpi_date, loco_id,
             otd_pct, fuel_per_km,
             buxa_overheat, trips_count, avg_delay_min,
             updated_at)
        VALUES
            (%(kpi_date)s, %(loco_id)s,
             %(otd_pct)s, %(fuel_per_km)s,
             %(buxa_overheat)s, %(trips_count)s, %(avg_delay_min)s,
             now())
        ON CONFLICT (kpi_date, loco_id)
        DO UPDATE SET
            otd_pct       = EXCLUDED.otd_pct,
            fuel_per_km   = EXCLUDED.fuel_per_km,
            buxa_overheat = EXCLUDED.buxa_overheat,
            trips_count   = EXCLUDED.trips_count,
            avg_delay_min = EXCLUDED.avg_delay_min,
            updated_at    = EXCLUDED.updated_at
    """

    for _, row in kpi.iterrows():
        hook.run(
            upsert_sql,
            parameters={
                'kpi_date':      row['kpi_date'],
                'loco_id':       row['loco_id'],
                'otd_pct':       row.get('otd_pct'),
                'fuel_per_km':   row.get('fuel_per_km'),
                'buxa_overheat': row['buxa_overheat'],
                'trips_count':   int(row.get('trips_count', 0)),
                'avg_delay_min': float(row.get('avg_delay_min', 0.0)),
            },
        )

    log.info(
        "UPSERT kpi_daily завершён: %d строк за %s.", len(kpi), ds,
    )
    ctx['ti'].xcom_push(key='kpi_row_count', value=len(kpi))


#### 4.3. write_results_atomic — атомарная запись в S3

def write_results_atomic(**ctx) -> None:
    """
    Читает KPI из PostgreSQL за ctx['ds'],
    записывает сначала в rzd-airflow-results/tmp/{{ ds }}/kpi_daily.csv,
    затем copy_object → results/final/{{ ds }}/kpi_daily.csv.
    """
    ds   = ctx['ds']
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

    rows = hook.get_records(
        """
        SELECT k.kpi_date, k.loco_id, l.series, l.traction_type,
               k.otd_pct, k.fuel_per_km, k.buxa_overheat,
               k.trips_count, k.avg_delay_min
        FROM kpi_daily k
        JOIN locomotives l ON l.loco_id = k.loco_id
        WHERE k.kpi_date = %s
        ORDER BY k.loco_id
        """,
        (ds,),
    )

    if not rows:
        raise ValueError(
            f"Нет данных KPI за {ds} в kpi_daily. "
            "Задача upsert_kpi_to_postgres не выполнена?"
        )

    columns = [
        'kpi_date', 'loco_id', 'series', 'traction_type',
        'otd_pct', 'fuel_per_km', 'buxa_overheat',
        'trips_count', 'avg_delay_min',
    ]
    df = pd.DataFrame(rows, columns=columns)

    tmp_key   = f"tmp/{ds}/kpi_daily.csv"
    final_key = f"final/{ds}/kpi_daily.csv"

    # Запись во временный ключ
    write_csv_to_s3(df=df, bucket=BUCKET_RESULTS, key=tmp_key)
    log.info("Записан tmp: s3://%s/%s", BUCKET_RESULTS, tmp_key)

    # Атомарное перемещение в финальный ключ
    atomic_copy_s3(
        bucket=BUCKET_RESULTS,
        tmp_key=tmp_key,
        final_key=final_key,
    )
    log.info(
        "Результат доступен: s3://%s/%s", BUCKET_RESULTS, final_key,
    )
```

#### 4.4. upsert_maintenance — UPSERT уведомлений о ТО

```python
def upsert_maintenance(**ctx) -> None:
    """
    Читает maintenance.csv из rzd-airflow-data/ через S3Hook.
    UPSERT в таблицу maintenance_alerts (ON CONFLICT DO UPDATE).
    """
    ds   = ctx['ds']
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

    log.info("Чтение: s3://%s/maintenance.csv", BUCKET_DATA)
    df = read_csv_from_s3(bucket=BUCKET_DATA, key='maintenance.csv')

    df['alert_date'] = (
        pd.to_datetime(df['alert_date']).dt.date.astype(str)
    )
    df_day = df[df['alert_date'] == ds]

    if df_day.empty:
        log.info("Нет записей о ТО за %s. Пропуск.", ds)
        return

    upsert_sql = """
        INSERT INTO maintenance_alerts
            (loco_id, alert_date, alert_type,
             moto_hours_cur, moto_hours_next,
             hours_remaining, description, is_critical)
        VALUES
            (%(loco_id)s, %(alert_date)s, %(alert_type)s,
             %(moto_hours_cur)s, %(moto_hours_next)s,
             %(hours_remaining)s, %(description)s, %(is_critical)s)
        ON CONFLICT (loco_id, alert_date, alert_type)
        DO UPDATE SET
            moto_hours_cur  = EXCLUDED.moto_hours_cur,
            hours_remaining = EXCLUDED.hours_remaining,
            description     = EXCLUDED.description,
            is_critical     = EXCLUDED.is_critical
    """

    for _, row in df_day.iterrows():
        hook.run(upsert_sql, parameters=row.to_dict())

    log.info("UPSERT maintenance_alerts: %d строк за %s.", len(df_day), ds)
```

### Шаг 5. Определить default_args, doc_md и DAG

```python
DAG_DOC = """
## mes_production

**Назначение:** пайплайн MES-системы депо ТЧЭ-15 —
обработка данных телеметрии, расчёт KPI, уведомления о ТО.

**Среда:** Yandex Managed Service for Apache Airflow.
Локальная файловая система **не используется**.

**Расписание:** ежедневно в 06:00 MSK.

**Бакеты Object Storage:**

| Бакет | Операция | Ключи |
|-------|----------|-------|
| `rzd-airflow-data` | Чтение (S3Hook) | `sensor_readings.csv`, `trips.csv`, `maintenance.csv` |
| `rzd-airflow-results` | Запись tmp (S3Hook) | `tmp/{{ ds }}/kpi_daily.csv` |
| `rzd-airflow-results` | copy_object → final | `final/{{ ds }}/kpi_daily.csv` |
| `rzd-airflow-results` | Error-лог (callback) | `errors/{{ ds }}/{{ task_id }}.json` |
| `rzd-airflow-dags` | Деплой DAG | `dags/mes_production.py` |

**UPSERT:** `ON CONFLICT (kpi_date, loco_id) DO UPDATE SET` — идемпотентность.

**Атомарность:** запись в `tmp/` + `copy_object` + `delete_objects(tmp)`.

**Ответственный:** Отдел АСУ ТЧЭ-15 · duty@tceh15.rzd.ru
"""

default_args = {
    "owner":                     "asu-tceh15",
    "retries":                   3,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":           timedelta(hours=1),
    "on_failure_callback":       on_failure_write_error_s3,
    "email_on_failure":          False,
    "email_on_retry":            False,
    "execution_timeout":         timedelta(hours=2),
}
```

### Шаг 6. Собрать задачи и зависимости

Реализуйте структуру DAG:

```
wait_for_data
     │
     ▼
load_sensor_data
     │
     ▼
upsert_kpi_to_postgres ──── upsert_maintenance
     │
     ▼
write_results_atomic
```

```python
with DAG(
    dag_id="mes_production",
    description="MES ТЧЭ-15: S3 → UPSERT PostgreSQL → S3 атомарная запись",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 3, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    doc_md=DAG_DOC,
    tags=["rzd", "tceh-15", "production", "mes", "s3"],
) as dag:

    wait_task = S3KeySensor(
        task_id="wait_for_data",
        bucket_name=BUCKET_DATA,
        bucket_key="sensor_readings.csv",
        aws_conn_id=S3_CONN_ID,
        poke_interval=300,
        timeout=7200,
        mode="reschedule",
        doc_md=(
            "Ожидает наличия файла sensor_readings.csv "
            "в rzd-airflow-data/. mode=reschedule не блокирует worker."
        ),
    )

    load_task = PythonOperator(
        task_id="load_sensor_data",
        python_callable=load_sensor_data,
        sla=timedelta(minutes=10),
        doc_md=(
            "Читает sensor_readings.csv из rzd-airflow-data/ через S3Hook. "
            "Фильтр по reading_date = {{ ds }}. "
            "ValueError при отсутствии данных."
        ),
    )

    upsert_kpi_task = PythonOperator(
        task_id="upsert_kpi_to_postgres",
        python_callable=upsert_kpi_to_postgres,
        sla=timedelta(minutes=20),
        doc_md=(
            "Читает CSV из rzd-airflow-data/ через S3Hook. "
            "UPSERT в kpi_daily (ON CONFLICT DO UPDATE). Идемпотентно."
        ),
    )

    upsert_maint_task = PythonOperator(
        task_id="upsert_maintenance",
        python_callable=upsert_maintenance,
        sla=timedelta(minutes=20),
        doc_md=(
            "Читает maintenance.csv из rzd-airflow-data/ через S3Hook. "
            "UPSERT в maintenance_alerts. Идемпотентно."
        ),
    )

    write_task = PythonOperator(
        task_id="write_results_atomic",
        python_callable=write_results_atomic,
        sla=timedelta(minutes=25),
        doc_md=(
            "Читает KPI из PostgreSQL. "
            "Записывает в rzd-airflow-results/tmp/{{ ds }}/kpi_daily.csv, "
            "затем copy_object → results/final/{{ ds }}/kpi_daily.csv."
        ),
    )

    wait_task >> load_task >> [upsert_kpi_task, upsert_maint_task]
    upsert_kpi_task >> write_task
```

### Шаг 7. Деплой DAG через Object Storage

В Managed Airflow деплой выполняется **только через Object Storage**. Команды `airflow dags`, `ssh`, `scp` и локальная папка `dags/` недоступны.

```bash
# Загрузить DAG в бакет, связанный с Managed Airflow
yc storage cp mes_production.py \
    s3://rzd-airflow-dags/dags/mes_production.py

# Или через AWS CLI
aws s3 cp mes_production.py \
    s3://rzd-airflow-dags/dags/mes_production.py \
    --endpoint-url https://storage.yandexcloud.net
```

После загрузки обновите страницу Airflow UI — DAG `mes_production` появится автоматически через 30–60 секунд.

**Проверка в UI:**
1. Убедитесь, что DAG виден в списке и не содержит ошибок импорта.
2. Откройте **Graph View** — должна отображаться структура из 5 задач.
3. Откройте **DAG Details → Docs** — убедитесь, что `doc_md` с таблицей бакетов отображается.

### Шаг 8. Проверка идемпотентности UPSERT

Запустите задачу `upsert_kpi_to_postgres` дважды за одну дату:

В Airflow UI: найдите задачу → **Clear** → повторный запуск.

Или через запуск DAG с той же датой.

Проверьте PostgreSQL:

```sql
-- Выполнить через psql или Yandex Cloud Console
SELECT
    kpi_date,
    COUNT(*)                              AS total_rows,
    COUNT(DISTINCT loco_id)              AS unique_locos,
    COUNT(*) = COUNT(DISTINCT loco_id)   AS is_idempotent
FROM kpi_daily
WHERE kpi_date = '2024-03-15'
GROUP BY kpi_date;
-- is_idempotent должен быть TRUE
```

### Шаг 9. Проверка атомарной записи и error-логов в Object Storage

**Проверка атомарной записи:**

Через Yandex Cloud Console → Object Storage → `rzd-airflow-results`:
- Убедитесь, что `final/2024-03-15/kpi_daily.csv` существует.
- Убедитесь, что `tmp/2024-03-15/kpi_daily.csv` отсутствует (удалён после копирования).

**Проверка on_failure_callback:**

1. Временно введите неверный Access Key в Connection `yandex_s3`.
2. Запустите DAG — задача `load_sensor_data` упадёт.
3. Убедитесь, что файл `errors/2024-03-15/load_sensor_data.json` появился в `rzd-airflow-results`.
4. Откройте файл в Cloud Console — проверьте, что JSON содержит поля `dag_id`, `task_id`, `error`.
5. Восстановите правильный Access Key.

---

## Полный код DAG

```python
"""
mes_production.py

Production DAG обработки данных MES-системы депо ТЧЭ-15.
Западно-Сибирская дирекция тяги, депо Новосибирск-Главный.

Среда: Yandex Managed Service for Apache Airflow.
Все файловые операции — через Yandex Object Storage (S3Hook).
Локальная файловая система НЕ используется.

Бакеты:
  rzd-airflow-data/              — входные CSV (чтение)
  rzd-airflow-results/tmp/       — временные файлы атомарной записи
  rzd-airflow-results/final/     — финальные результаты
  rzd-airflow-results/errors/    — error-логи (on_failure_callback)
  rzd-airflow-dags/dags/         — деплой DAG-файла
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

# ── Константы ─────────────────────────────────────────────────────────────────

S3_CONN_ID     = 'yandex_s3'
PG_CONN_ID     = 'rzd_postgres'
BUCKET_DATA    = 'rzd-airflow-data'
BUCKET_RESULTS = 'rzd-airflow-results'


# ── Вспомогательные функции S3 ────────────────────────────────────────────────

def read_csv_from_s3(
    bucket: str,
    key: str,
    conn_id: str = S3_CONN_ID,
) -> pd.DataFrame:
    """Читает CSV из Object Storage через S3Hook."""
    hook    = S3Hook(aws_conn_id=conn_id)
    obj     = hook.get_key(key=key, bucket_name=bucket)
    content = obj.get()['Body'].read().decode('utf-8')
    return pd.read_csv(StringIO(content))


def write_csv_to_s3(
    df: pd.DataFrame,
    bucket: str,
    key: str,
    conn_id: str = S3_CONN_ID,
) -> None:
    """Записывает DataFrame как CSV в Object Storage через S3Hook."""
    hook = S3Hook(aws_conn_id=conn_id)
    buf  = StringIO()
    df.to_csv(buf, index=False)
    hook.load_string(
        string_data=buf.getvalue(),
        key=key,
        bucket_name=bucket,
        replace=True,
    )


def atomic_copy_s3(
    bucket: str,
    tmp_key: str,
    final_key: str,
    conn_id: str = S3_CONN_ID,
) -> None:
    """
    Атомарное перемещение объекта:
    copy_object(tmp → final) + delete_objects(tmp).
    """
    hook = S3Hook(aws_conn_id=conn_id)
    hook.copy_object(
        source_bucket_key=tmp_key,
        dest_bucket_key=final_key,
        source_bucket_name=bucket,
        dest_bucket_name=bucket,
    )
    hook.delete_objects(bucket=bucket, keys=[tmp_key])
    log.info(
        "Атомарное перемещение: s3://%s/%s → s3://%s/%s",
        bucket, tmp_key, bucket, final_key,
    )


# ── on_failure_callback ───────────────────────────────────────────────────────

def on_failure_write_error_s3(context: dict) -> None:
    """
    При сбое задачи записывает JSON error-лог в:
    s3://rzd-airflow-results/errors/{{ ds }}/{{ task_id }}.json
    """
    ti  = context['task_instance']
    dag = context['dag']
    ds  = context['ds']
    exc = context.get('exception', 'неизвестная ошибка')

    log.error(
        "[MES FAIL] dag=%s task=%s date=%s error=%s",
        dag.dag_id, ti.task_id, ds, exc,
    )

    payload = json.dumps(
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
            string_data=payload,
            key=error_key,
            bucket_name=BUCKET_RESULTS,
            replace=True,
        )
        log.info(
            "Error-лог записан: s3://%s/%s", BUCKET_RESULTS, error_key,
        )
    except Exception as cb_err:
        log.error("Не удалось записать error-лог в S3: %s", cb_err)


# ── Callable-функции ──────────────────────────────────────────────────────────

def load_sensor_data(**ctx) -> None:
    """
    Читает sensor_readings.csv из rzd-airflow-data/ через S3Hook.
    Фильтрует по reading_date = ctx['ds'].
    Бросает ValueError при отсутствии данных.
    """
    ds  = ctx['ds']
    key = 'sensor_readings.csv'

    log.info("Чтение: s3://%s/%s", BUCKET_DATA, key)
    df = read_csv_from_s3(bucket=BUCKET_DATA, key=key)

    df['reading_date'] = (
        pd.to_datetime(df['reading_date']).dt.date.astype(str)
    )
    df_day = df[df['reading_date'] == ds]

    if df_day.empty:
        raise ValueError(
            f"Нет данных телеметрии за {ds} "
            f"в s3://{BUCKET_DATA}/{key}"
        )

    log.info("Строк за %s: %d", ds, len(df_day))
    ctx['ti'].xcom_push(key='sensor_row_count', value=len(df_day))


def upsert_kpi_to_postgres(**ctx) -> None:
    """
    Читает sensor_readings.csv и trips.csv из rzd-airflow-data/ через S3Hook.
    Вычисляет KPI за ctx['ds'].
    UPSERT в kpi_daily (ON CONFLICT DO UPDATE) — идемпотентно.
    """
    ds   = ctx['ds']
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

    log.info("Чтение данных из s3://%s/", BUCKET_DATA)
    df_sensor = read_csv_from_s3(bucket=BUCKET_DATA, key='sensor_readings.csv')
    df_trips  = read_csv_from_s3(bucket=BUCKET_DATA, key='trips.csv')

    df_sensor['reading_date'] = (
        pd.to_datetime(df_sensor['reading_date']).dt.date.astype(str)
    )
    df_trips['trip_date'] = (
        pd.to_datetime(df_trips['trip_date']).dt.date.astype(str)
    )

    df_s = df_sensor[df_sensor['reading_date'] == ds]
    df_t = df_trips[df_trips['trip_date'] == ds]

    if df_s.empty:
        raise ValueError(
            f"Нет данных sensor_readings за {ds}"
        )

    # Расчёт KPI по датчикам
    kpi = (
        df_s.groupby('loco_id')
        .agg(
            buxa_overheat=(
                'buxa_temp_max',
                lambda x: int((x > 80).sum()),
            ),
            fuel_per_km=(
                'fuel_rate',
                lambda x: round(
                    float(x.mean())
                    / float(
                        df_s.loc[x.index, 'speed_kmh']
                        .replace(0, pd.NA).mean()
                    )
                    if df_s.loc[x.index, 'speed_kmh'].mean() > 0
                    else 0.0,
                    4,
                ),
            ),
        )
        .reset_index()
    )

    # OTD% из поездок
    if not df_t.empty:
        otd = (
            df_t.groupby('loco_id')
            .agg(
                otd_pct=(
                    'on_time',
                    lambda x: round(float(x.mean()) * 100, 2),
                ),
                trips_count=('trip_date', 'count'),
                avg_delay_min=(
                    'delay_min',
                    lambda x: round(float(x.mean()), 2),
                ),
            )
            .reset_index()
        )
        kpi = kpi.merge(otd, on='loco_id', how='left')
    else:
        kpi['otd_pct']       = None
        kpi['trips_count']   = 0
        kpi['avg_delay_min'] = 0.0

    kpi['kpi_date'] = ds

    upsert_sql = """
        INSERT INTO kpi_daily
            (kpi_date, loco_id,
             otd_pct, fuel_per_km,
             buxa_overheat, trips_count, avg_delay_min,
             updated_at)
        VALUES
            (%(kpi_date)s, %(loco_id)s,
             %(otd_pct)s, %(fuel_per_km)s,
             %(buxa_overheat)s, %(trips_count)s, %(avg_delay_min)s,
             now())
        ON CONFLICT (kpi_date, loco_id)
        DO UPDATE SET
            otd_pct       = EXCLUDED.otd_pct,
            fuel_per_km   = EXCLUDED.fuel_per_km,
            buxa_overheat = EXCLUDED.buxa_overheat,
            trips_count   = EXCLUDED.trips_count,
            avg_delay_min = EXCLUDED.avg_delay_min,
            updated_at    = EXCLUDED.updated_at
    """

    for _, row in kpi.iterrows():
        hook.run(
            upsert_sql,
            parameters={
                'kpi_date':      row['kpi_date'],
                'loco_id':       row['loco_id'],
                'otd_pct':       row.get('otd_pct'),
                'fuel_per_km':   row.get('fuel_per_km'),
                'buxa_overheat': row['buxa_overheat'],
                'trips_count':   int(row.get('trips_count', 0)),
                'avg_delay_min': float(row.get('avg_delay_min', 0.0)),
            },
        )

    log.info("UPSERT kpi_daily: %d строк за %s.", len(kpi), ds)
    ctx['ti'].xcom_push(key='kpi_row_count', value=len(kpi))


def write_results_atomic(**ctx) -> None:
    """
    Читает KPI за ctx['ds'] из PostgreSQL.
    Атомарная запись в Object Storage:
      1. S3Hook.load_string → rzd-airflow-results/tmp/{{ ds }}/kpi_daily.csv
      2. S3Hook.copy_object → rzd-airflow-results/final/{{ ds }}/kpi_daily.csv
      3. S3Hook.delete_objects(tmp)
    """
    ds   = ctx['ds']
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

    rows = hook.get_records(
        """
        SELECT k.kpi_date, k.loco_id, l.series, l.traction_type,
               k.otd_pct, k.fuel_per_km, k.buxa_overheat,
               k.trips_count, k.avg_delay_min
        FROM kpi_daily k
        JOIN locomotives l ON l.loco_id = k.loco_id
        WHERE k.kpi_date = %s
        ORDER BY k.loco_id
        """,
        (ds,),
    )

    if not rows:
        raise ValueError(
            f"Нет данных KPI за {ds} в kpi_daily."
        )

    df = pd.DataFrame(
        rows,
        columns=[
            'kpi_date', 'loco_id', 'series', 'traction_type',
            'otd_pct', 'fuel_per_km', 'buxa_overheat',
            'trips_count', 'avg_delay_min',
        ],
    )

    tmp_key   = f"tmp/{ds}/kpi_daily.csv"
    final_key = f"final/{ds}/kpi_daily.csv"

    # Шаг 1 — запись во временный ключ
    write_csv_to_s3(df=df, bucket=BUCKET_RESULTS, key=tmp_key)
    log.info("Tmp записан: s3://%s/%s", BUCKET_RESULTS, tmp_key)

    # Шаги 2–3 — copy_object + delete tmp
    atomic_copy_s3(
        bucket=BUCKET_RESULTS,
        tmp_key=tmp_key,
        final_key=final_key,
    )
    log.info("Результат: s3://%s/%s", BUCKET_RESULTS, final_key)


def upsert_maintenance(**ctx) -> None:
    """
    Читает maintenance.csv из rzd-airflow-data/ через S3Hook.
    UPSERT в maintenance_alerts (ON CONFLICT DO UPDATE).
    """
    ds   = ctx['ds']
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    key  = 'maintenance.csv'

    log.info("Чтение: s3://%s/%s", BUCKET_DATA, key)
    df = read_csv_from_s3(bucket=BUCKET_DATA, key=key)

    df['alert_date'] = (
        pd.to_datetime(df['alert_date']).dt.date.astype(str)
    )
    df_day = df[df['alert_date'] == ds]

    if df_day.empty:
        log.info("Нет записей о ТО за %s. Пропуск.", ds)
        return

    upsert_sql = """
        INSERT INTO maintenance_alerts
            (loco_id, alert_date, alert_type,
             moto_hours_cur, moto_hours_next,
             hours_remaining, description, is_critical)
        VALUES
            (%(loco_id)s, %(alert_date)s, %(alert_type)s,
             %(moto_hours_cur)s, %(moto_hours_next)s,
             %(hours_remaining)s, %(description)s, %(is_critical)s)
        ON CONFLICT (loco_id, alert_date, alert_type)
        DO UPDATE SET
            moto_hours_cur  = EXCLUDED.moto_hours_cur,
            hours_remaining = EXCLUDED.hours_remaining,
            description     = EXCLUDED.description,
            is_critical     = EXCLUDED.is_critical
    """

    for _, row in df_day.iterrows():
        hook.run(upsert_sql, parameters=row.to_dict())

    log.info("UPSERT maintenance_alerts: %d строк за %s.", len(df_day), ds)


# ── DAG Definition ────────────────────────────────────────────────────────────

DAG_DOC = """
## mes_production

**Назначение:** пайплайн MES-системы депо ТЧЭ-15 —
обработка данных телеметрии, расчёт KPI, уведомления о ТО.

**Среда:** Yandex Managed Service for Apache Airflow.
Локальная файловая система **не используется**.

**Расписание:** ежедневно в 06:00 MSK.

**Бакеты Object Storage:**

| Бакет | Операция | Ключи |
|-------|----------|-------|
| `rzd-airflow-data` | Чтение (S3Hook) | `sensor_readings.csv`, `trips.csv`, `maintenance.csv` |
| `rzd-airflow-results` | Запись tmp | `tmp/{{ ds }}/kpi_daily.csv` |
| `rzd-airflow-results` | copy_object → final | `final/{{ ds }}/kpi_daily.csv` |
| `rzd-airflow-results` | Error-лог (callback) | `errors/{{ ds }}/{{ task_id }}.json` |
| `rzd-airflow-dags` | Деплой DAG | `dags/mes_production.py` |

**UPSERT:** `ON CONFLICT (kpi_date, loco_id) DO UPDATE SET`.

**Атомарность:** `load_string(tmp)` → `copy_object(tmp → final)` → `delete(tmp)`.

**Ответственный:** Отдел АСУ ТЧЭ-15 · duty@tceh15.rzd.ru
"""

default_args = {
    "owner":                     "asu-tceh15",
    "retries":                   3,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":           timedelta(hours=1),
    "on_failure_callback":       on_failure_write_error_s3,
    "email_on_failure":          False,
    "email_on_retry":            False,
    "execution_timeout":         timedelta(hours=2),
}

with DAG(
    dag_id="mes_production",
    description=(
        "MES ТЧЭ-15: S3 → UPSERT PostgreSQL → S3 атомарная запись"
    ),
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 3, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    doc_md=DAG_DOC,
    tags=["rzd", "tceh-15", "production", "mes", "s3"],
) as dag:

    wait_task = S3KeySensor(
        task_id="wait_for_data",
        bucket_name=BUCKET_DATA,
        bucket_key="sensor_readings.csv",
        aws_conn_id=S3_CONN_ID,
        poke_interval=300,
        timeout=7200,
        mode="reschedule",
        doc_md=(
            "Ожидает sensor_readings.csv в rzd-airflow-data/. "
            "mode=reschedule — не блокирует worker между проверками."
        ),
    )

    load_task = PythonOperator(
        task_id="load_sensor_data",
        python_callable=load_sensor_data,
        sla=timedelta(minutes=10),
        doc_md=(
            "S3Hook.get_key(sensor_readings.csv) из rzd-airflow-data/. "
            "Фильтр по reading_date = {{ ds }}. "
            "ValueError при отсутствии данных."
        ),
    )

    upsert_kpi_task = PythonOperator(
        task_id="upsert_kpi_to_postgres",
        python_callable=upsert_kpi_to_postgres,
        sla=timedelta(minutes=20),
        doc_md=(
            "S3Hook читает sensor_readings.csv + trips.csv. "
            "UPSERT kpi_daily ON CONFLICT DO UPDATE. Идемпотентно."
        ),
    )

    upsert_maint_task = PythonOperator(
        task_id="upsert_maintenance",
        python_callable=upsert_maintenance,
        sla=timedelta(minutes=20),
        doc_md=(
            "S3Hook читает maintenance.csv из rzd-airflow-data/. "
            "UPSERT maintenance_alerts ON CONFLICT DO UPDATE."
        ),
    )

    write_task = PythonOperator(
        task_id="write_results_atomic",
        python_callable=write_results_atomic,
        sla=timedelta(minutes=25),
        doc_md=(
            "Читает kpi_daily из PostgreSQL. "
            "load_string → rzd-airflow-results/tmp/{{ ds }}/kpi_daily.csv. "
            "copy_object → final/{{ ds }}/kpi_daily.csv. "
            "delete_objects(tmp)."
        ),
    )

    wait_task >> load_task >> [upsert_kpi_task, upsert_maint_task]
    upsert_kpi_task >> write_task
```

---

## Деплой: загрузка .py в rzd-airflow-dags/ и проверка в UI

### Загрузка DAG-файла

```bash
# Через Yandex Cloud CLI
yc storage cp mes_production.py \
    s3://rzd-airflow-dags/dags/mes_production.py

# Через AWS CLI
aws s3 cp mes_production.py \
    s3://rzd-airflow-dags/dags/mes_production.py \
    --endpoint-url https://storage.yandexcloud.net
```

> **Важно:** в Yandex Managed Service for Apache Airflow DAG-файлы загружаются  
> **исключительно через Object Storage**. Команды `ssh`, `scp`, `airflow dags`,  
> локальная папка `dags/` — недоступны.

### Проверка в Airflow UI

1. Откройте Airflow UI → список DAG-файлов.
2. Убедитесь, что `mes_production` отображается без ошибок импорта.
3. **Graph View** — должна отображаться цепочка из 5 задач.
4. **DAG Details → Docs** — проверьте `doc_md` с таблицей бакетов.
5. Запустите DAG вручную (**Trigger DAG**) и проследите выполнение.

---

## Ожидаемый результат

После успешного выполнения DAG за дату `2024-03-15`:

| Ресурс | Ожидаемое состояние |
|--------|---------------------|
| PostgreSQL `kpi_daily` | Строки за `2024-03-15` присутствуют, дублей нет |
| PostgreSQL `maintenance_alerts` | Записи о ТО за `2024-03-15` присутствуют |
| `s3://rzd-airflow-results/final/2024-03-15/kpi_daily.csv` | Файл существует |
| `s3://rzd-airflow-results/tmp/2024-03-15/` | Директория отсутствует (tmp удалён) |
| `s3://rzd-airflow-results/errors/2024-03-15/` | Пуста (при успешном выполнении) |
| Airflow UI → Graph View | Все 5 задач в статусе `success` |

Повторный запуск DAG за ту же дату должен дать идентичный результат без дублей в PostgreSQL.

---

## Задания повышенной сложности

### Задание 1. S3KeySensor вместо опроса файла

Текущий `S3KeySensor` проверяет только наличие `sensor_readings.csv`.  
Доработайте DAG: добавьте второй сенсор, который ожидает файл с динамическим ключом, зависящим от даты:

```python
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

wait_daily_file = S3KeySensor(
    task_id="wait_for_daily_sensor_file",
    bucket_name=BUCKET_DATA,
    bucket_key="daily/{{ ds_nodash }}_sensor_readings.csv",
    aws_conn_id=S3_CONN_ID,
    poke_interval=300,
    timeout=7200,
    mode="reschedule",
    doc_md=(
        "Ожидает ежедневный файл телеметрии с шаблоном {{ ds_nodash }}. "
        "mode=reschedule освобождает worker между проверками."
    ),
)
```

Объясните: почему `mode="reschedule"` критически важен при `timeout=7200` в Managed Airflow?  
Чем это отличается от `mode="poke"` с точки зрения потребления ресурсов кластера?

### Задание 2. Версионирование результатов в S3

Вместо перезаписи финального файла реализуйте хранение нескольких версий:

```python
def write_results_versioned(**ctx) -> None:
    """
    Атомарная запись с версионированием:
    final/{{ ds }}/kpi_daily_v{{ run_id_hash }}.csv
    """
    import hashlib

    ds     = ctx['ds']
    run_id = ctx['run_id']
    v_hash = hashlib.md5(run_id.encode()).hexdigest()[:8]

    tmp_key   = f"tmp/{ds}/kpi_daily.csv"
    final_key = f"final/{ds}/kpi_daily_v{v_hash}.csv"
    # ... (использовать write_csv_to_s3 + atomic_copy_s3)
```

После этого реализуйте задачу `cleanup_old_versions`, которая оставляет в `final/{{ ds }}/` только последние 3 версии файла, удаляя старые через `S3Hook.delete_objects()`.

### Задание 3. Unit-тесты с mock S3Hook

Напишите тесты, которые проверяют логику callable-функций **без реального обращения к Object Storage**:

```python
# tests/test_mes_production.py
import pytest
from unittest.mock import MagicMock, patch
from io import StringIO
import pandas as pd

from dags.mes_production import (
    load_sensor_data,
    upsert_kpi_to_postgres,
    write_results_atomic,
    on_failure_write_error_s3,
)


class TestLoadSensorData:

    def test_raises_on_empty_data(self):
        """ValueError при отсутствии данных за указанную дату."""
        mock_hook = MagicMock()
        empty_csv = "loco_id,reading_date,buxa_temp_max\n"
        mock_obj  = MagicMock()
        mock_obj.get.return_value = {
            'Body': MagicMock(
                read=MagicMock(return_value=empty_csv.encode('utf-8'))
            )
        }
        mock_hook.get_key.return_value = mock_obj

        with patch(
            'dags.mes_production.S3Hook',
            return_value=mock_hook,
        ):
            ctx = {'ds': '2024-03-15', 'ti': MagicMock()}
            with pytest.raises(ValueError, match="Нет данных телеметрии"):
                load_sensor_data(**ctx)

    def test_pushes_row_count_to_xcom(self):
        """Сохраняет количество строк в XCom."""
        mock_hook = MagicMock()
        data_csv  = (
            "loco_id,reading_date,buxa_temp_max\n"
            "VL80-001,2024-03-15,65\n"
            "VL80-002,2024-03-15,72\n"
        )
        mock_obj = MagicMock()
        mock_obj.get.return_value = {
            'Body': MagicMock(
                read=MagicMock(return_value=data_csv.encode('utf-8'))
            )
        }
        mock_hook.get_key.return_value = mock_obj
        mock_ti = MagicMock()

        with patch(
            'dags.mes_production.S3Hook',
            return_value=mock_hook,
        ):
            ctx = {'ds': '2024-03-15', 'ti': mock_ti}
            load_sensor_data(**ctx)

        mock_ti.xcom_push.assert_called_once_with(
            key='sensor_row_count', value=2
        )


class TestOnFailureCallback:

    def test_writes_json_to_s3(self):
        """Callback записывает JSON error-лог в Object Storage."""
        mock_hook = MagicMock()
        mock_ti   = MagicMock()
        mock_ti.task_id = 'load_sensor_data'
        mock_dag  = MagicMock()
        mock_dag.dag_id = 'mes_production'

        context = {
            'task_instance': mock_ti,
            'dag':           mock_dag,
            'ds':            '2024-03-15',
            'exception':     ValueError("test error"),
        }

        with patch(
            'dags.mes_production.S3Hook',
            return_value=mock_hook,
        ):
            on_failure_write_error_s3(context)

        mock_hook.load_string.assert_called_once()
        call_kwargs = mock_hook.load_string.call_args[1]
        assert call_kwargs['key'] == 'errors/2024-03-15/load_sensor_data.json'
        assert 'mes_production' in call_kwargs['string_data']


# Запуск:
# pytest tests/test_mes_production.py -v
```
