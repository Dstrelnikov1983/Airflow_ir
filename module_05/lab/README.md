# Лабораторная работа №05: Зависимый пайплайн — DAG расчёта OTD ждёт DAG ingestion

**Модуль:** 05 — Зависимости задач в Airflow
**Продолжительность:** 60–90 минут
**Платформа:** Yandex Managed Service for Apache Airflow™
**Уровень:** Средний / Продвинутый

---

## Цель

Разработать и протестировать систему из двух взаимозависимых DAG для депо ТЧЭ-15 Новосибирск:

- **DAG 1 (`trips_ingestion_dag`)** — ожидает появления `trips.csv` в Object Storage, читает данные через `S3Hook`, валидирует и загружает в PostgreSQL `rzd_analytics`
- **DAG 2 (`otd_analysis_dag`)** — ждёт завершения DAG 1 через `ExternalTaskSensor`, рассчитывает OTD (On-Time Delivery — процент отправлений по расписанию), записывает отчёт в Object Storage

По итогам работы студент получает функционирующий двухуровневый пайплайн, в котором все операции с файлами выполняются через `S3Hook` без обращений к локальной файловой системе.

---

## Архитектура решения

```
rzd-airflow-data/trips.csv
        ↓  S3KeySensor
trips_ingestion_dag (06:00 UTC):
  wait_trips_file
        ↓
  read_trips_from_s3          ← S3Hook.get_key() → pd.read_csv(StringIO)
        ↓
  validate_trips              ← проверка полноты, типов, отсутствия дубликатов
        ↓
  load_trips_to_postgres      ← PostgresHook → INSERT в rzd_analytics.trips
        ↓  (эту задачу ждёт ExternalTaskSensor)
        ↓
otd_analysis_dag (06:30 UTC):
  wait_ingestion_done         ← ExternalTaskSensor(trips_ingestion_dag, load_trips_to_postgres)
        ↓
  calc_otd                    ← читает trips из S3, считает OTD по depot_code
        ↓
  write_s3_report             ← S3Hook.load_string() → rzd-airflow-results/otd/
```

---

## Предварительные условия

- Managed Airflow кластер запущен и доступен через Yandex Cloud Console
- Бакеты `rzd-airflow-dags`, `rzd-airflow-data`, `rzd-airflow-results` созданы (из практики №05)
- CSV-файлы датасета загружены в `rzd-airflow-data` (из практики №05)
- Connection `yandex_s3` настроен в Airflow UI (тип Amazon Web Services, endpoint `https://storage.yandexcloud.net`)
- Connection `rzd_postgres` настроен в Airflow UI (Managed PostgreSQL, схема `rzd_analytics`)
- Variables `s3_bucket_data`, `s3_bucket_results`, `depot_code`, `delay_threshold_min` заданы

Если что-то не настроено — выполните соответствующие шаги из практической работы №05.

---

## Задание

### Шаг 1. Подготовить схему PostgreSQL

Подключитесь к кластеру Managed PostgreSQL и выполните скрипт `init_lab05.sql`
(файл находится в репозитории в папке `module_05/lab/`):

```sql
-- init_lab05.sql
-- Инициализация схемы для лабораторной работы №05

CREATE SCHEMA IF NOT EXISTS rzd_analytics;

-- Таблица рейсов (загружается DAG 1)
CREATE TABLE IF NOT EXISTS rzd_analytics.trips (
    id                SERIAL PRIMARY KEY,
    trip_id           VARCHAR(50) UNIQUE NOT NULL,
    loco_id           VARCHAR(20) NOT NULL,
    depot_code        VARCHAR(20),
    departure_time    TIMESTAMP,
    arrival_time      TIMESTAMP,
    scheduled_dep     TIMESTAMP,
    scheduled_arr     TIMESTAMP,
    distance_km       NUMERIC(8, 2),
    delay_dep_min     NUMERIC(6, 1),
    delay_arr_min     NUMERIC(6, 1),
    status            VARCHAR(20),
    loaded_at         TIMESTAMP DEFAULT NOW()
);

-- Таблица OTD-отчётов (заполняется DAG 2)
CREATE TABLE IF NOT EXISTS rzd_analytics.otd_reports (
    id                SERIAL PRIMARY KEY,
    report_date       DATE NOT NULL,
    depot_code        VARCHAR(20) NOT NULL,
    total_trips       INTEGER,
    on_time_trips     INTEGER,
    otd_pct           NUMERIC(5, 2),
    avg_delay_min     NUMERIC(6, 2),
    max_delay_min     NUMERIC(6, 2),
    delay_threshold_min INTEGER,
    s3_report_key     VARCHAR(500),
    calculated_at     TIMESTAMP DEFAULT NOW(),
    UNIQUE (report_date, depot_code)
);
```

Подключиться можно через Yandex Cloud Console: кластер PostgreSQL → **SQL** → выполнить скрипт.

### Шаг 2. Создать DAG-файлы

Скопируйте код из раздела "Полный код DAG" в два файла:
- `trips_ingestion_dag.py`
- `otd_analysis_dag.py`

### Шаг 3. Задеплоить оба DAG в Managed Airflow

```bash
# DAG 1: ingestion
yc storage cp trips_ingestion_dag.py \
    s3://rzd-airflow-dags/dags/trips_ingestion_dag.py

# DAG 2: анализ OTD
yc storage cp otd_analysis_dag.py \
    s3://rzd-airflow-dags/dags/otd_analysis_dag.py
```

Дождитесь появления обоих DAG в Airflow UI (1–3 мин).
Убедитесь, что в колонке **Last Parse Time** нет ошибок импорта.

### Шаг 4. Убедиться, что trips.csv доступен в S3

```bash
yc storage ls s3://rzd-airflow-data/ | grep trips
# Ожидается: trips.csv
```

Если файл отсутствует, загрузите его:
```bash
yc storage cp trips.csv s3://rzd-airflow-data/trips.csv
```

### Шаг 5. Запустить trips_ingestion_dag

В Airflow UI: DAG `trips_ingestion_dag` → кнопка **Trigger DAG**.
Наблюдайте в **Graph View**:
- `wait_trips_file` ожидает в режиме `reschedule` — периодически статус `up_for_reschedule`
- После обнаружения файла в S3 переходит в `success`
- `read_trips_from_s3`, `validate_trips`, `load_trips_to_postgres` выполняются последовательно

### Шаг 6. Запустить otd_analysis_dag и проследить ExternalTaskSensor

В Airflow UI: DAG `otd_analysis_dag` → **Trigger DAG**.
Задача `wait_ingestion_done` должна находиться в ожидании
пока `load_trips_to_postgres` в `trips_ingestion_dag` не завершится успешно.

### Шаг 7. Проверить результаты в Object Storage и PostgreSQL

```bash
# Проверить OTD-отчёт в S3
yc storage ls s3://rzd-airflow-results/otd/

# Просмотреть содержимое отчёта
yc storage cat s3://rzd-airflow-results/otd/$(date +%Y%m%d)/otd_report.csv
```

В PostgreSQL выполните проверочный запрос:
```sql
SELECT report_date, depot_code, total_trips, on_time_trips, otd_pct, avg_delay_min
FROM rzd_analytics.otd_reports
ORDER BY report_date DESC
LIMIT 5;
```

---

## Полный код DAG

### DAG 1: trips_ingestion_dag.py

Загрузите в `s3://rzd-airflow-dags/dags/trips_ingestion_dag.py`:

```python
"""
Лабораторная работа №05 — DAG 1: trips_ingestion_dag.
Депо ТЧЭ-15 Новосибирск.

S3KeySensor → read (S3Hook) → validate → load PostgreSQL

Среда: Yandex Managed Service for Apache Airflow™
Все файловые операции — через S3Hook (Yandex Object Storage).
НЕ использовать: open(), pd.read_csv("/path"), локальные пути.
"""

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

log = logging.getLogger(__name__)


# ── Вспомогательные функции ──────────────────────────────────────────

def read_csv_from_s3(bucket: str, key: str, conn_id: str = "yandex_s3") -> pd.DataFrame:
    """Читает CSV из Yandex Object Storage через S3Hook."""
    hook = S3Hook(aws_conn_id=conn_id)
    obj = hook.get_key(key=key, bucket_name=bucket)
    content = obj.get()["Body"].read().decode("utf-8")
    return pd.read_csv(StringIO(content))


def write_csv_to_s3(
    df: pd.DataFrame,
    bucket: str,
    key: str,
    conn_id: str = "yandex_s3",
) -> None:
    """Записывает DataFrame в CSV в Yandex Object Storage через S3Hook."""
    hook = S3Hook(aws_conn_id=conn_id)
    buf = StringIO()
    df.to_csv(buf, index=False)
    hook.load_string(
        string_data=buf.getvalue(),
        key=key,
        bucket_name=bucket,
        replace=True,
    )
    log.info("Записан файл s3://%s/%s (%d строк)", bucket, key, len(df))


# ── Задача 1: чтение trips.csv из S3 ────────────────────────────────

def read_trips_from_s3(**ctx) -> dict:
    """
    Читает trips.csv из бакета rzd-airflow-data через S3Hook.
    Возвращает статистику для последующей валидации.
    """
    bucket = Variable.get("s3_bucket_data")
    key = "trips.csv"

    df = read_csv_from_s3(bucket=bucket, key=key)
    log.info("[read] Прочитано %d строк из s3://%s/%s", len(df), bucket, key)

    # Базовая статистика
    stats = {
        "rows": len(df),
        "columns": list(df.columns),
        "locos": df["loco_id"].nunique() if "loco_id" in df.columns else 0,
        "has_nulls_trip_id": int(df["trip_id"].isna().sum())
        if "trip_id" in df.columns
        else -1,
    }
    log.info("[read] Статистика: %s", stats)
    return stats


# ── Задача 2: валидация ──────────────────────────────────────────────

def validate_trips(**ctx) -> dict:
    """
    Проверяет качество данных trips.csv:
    - наличие обязательных колонок
    - отсутствие дублей trip_id
    - допустимый уровень пропусков

    При критических ошибках бросает ValueError — DAG упадёт.
    """
    ti = ctx["ti"]
    stats = ti.xcom_pull(task_ids="read_trips_from_s3")

    required_cols = {"trip_id", "loco_id", "departure_time", "arrival_time"}
    actual_cols = set(stats.get("columns", []))
    missing = required_cols - actual_cols

    errors = []
    if missing:
        errors.append(f"Отсутствуют обязательные колонки: {missing}")
    if stats.get("rows", 0) == 0:
        errors.append("Файл trips.csv пуст")
    if stats.get("has_nulls_trip_id", 0) > 0:
        errors.append(
            f"Обнаружены NULL в trip_id: {stats['has_nulls_trip_id']} строк"
        )

    if errors:
        raise ValueError(f"Валидация не прошла: {'; '.join(errors)}")

    log.info(
        "[validate] OK: %d строк, %d локомотивов",
        stats["rows"],
        stats["locos"],
    )
    return {"valid": True, "rows": stats["rows"], "locos": stats["locos"]}


# ── Задача 3: загрузка в PostgreSQL ─────────────────────────────────

def load_trips_to_postgres(**ctx) -> dict:
    """
    Читает trips.csv из S3 и загружает в rzd_analytics.trips (PostgreSQL).
    Использует INSERT ... ON CONFLICT DO NOTHING для идемпотентности.
    """
    ti = ctx["ti"]
    validation = ti.xcom_pull(task_ids="validate_trips")

    if not validation or not validation.get("valid"):
        raise ValueError("Данные не прошли валидацию — загрузка отменена")

    bucket = Variable.get("s3_bucket_data")
    key = "trips.csv"
    df = read_csv_from_s3(bucket=bucket, key=key)

    # Добавляем depot_code из переменной, если колонки нет
    if "depot_code" not in df.columns:
        df["depot_code"] = Variable.get("depot_code", default_var="TCH-15")

    hook_pg = PostgresHook(postgres_conn_id="rzd_postgres")
    conn = hook_pg.get_conn()
    cursor = conn.cursor()

    inserted = 0
    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO rzd_analytics.trips
                (trip_id, loco_id, depot_code, departure_time, arrival_time,
                 scheduled_dep, scheduled_arr, distance_km,
                 delay_dep_min, delay_arr_min, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (trip_id) DO NOTHING
            """,
            (
                row.get("trip_id"),
                row.get("loco_id"),
                row.get("depot_code"),
                row.get("departure_time"),
                row.get("arrival_time"),
                row.get("scheduled_dep"),
                row.get("scheduled_arr"),
                row.get("distance_km"),
                row.get("delay_dep_min"),
                row.get("delay_arr_min"),
                row.get("status"),
            ),
        )
        inserted += 1

    conn.commit()
    cursor.close()
    conn.close()

    log.info("[load] Загружено %d строк в rzd_analytics.trips", inserted)
    return {"loaded": inserted, "table": "rzd_analytics.trips"}


# ── Определение DAG 1 ────────────────────────────────────────────────

default_args = {
    "owner": "rzd_data_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="trips_ingestion_dag",
    description="DAG 1: S3KeySensor → read trips → validate → load PostgreSQL",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=default_args,
    tags=["lab05", "rzd", "ingestion", "producer"],
) as dag:

    # S3KeySensor: ждём trips.csv в бакете rzd-airflow-data
    wait_trips_file = S3KeySensor(
        task_id="wait_trips_file",
        bucket_name="rzd-airflow-data",
        bucket_key="trips.csv",
        aws_conn_id="yandex_s3",
        poke_interval=300,    # каждые 5 минут
        timeout=7200,         # максимум 2 часа
        mode="reschedule",    # освобождает воркер между проверками
        soft_fail=False,
    )

    task_read = PythonOperator(
        task_id="read_trips_from_s3",
        python_callable=read_trips_from_s3,
    )

    task_validate = PythonOperator(
        task_id="validate_trips",
        python_callable=validate_trips,
    )

    task_load = PythonOperator(
        task_id="load_trips_to_postgres",
        python_callable=load_trips_to_postgres,
    )

    # Линейная цепочка: сенсор → чтение → валидация → загрузка
    wait_trips_file >> task_read >> task_validate >> task_load
```

### DAG 2: otd_analysis_dag.py

Загрузите в `s3://rzd-airflow-dags/dags/otd_analysis_dag.py`:

```python
"""
Лабораторная работа №05 — DAG 2: otd_analysis_dag.
Депо ТЧЭ-15 Новосибирск.

ExternalTaskSensor(trips_ingestion_dag) → calc_otd → write_s3_report

Среда: Yandex Managed Service for Apache Airflow™
Все файловые операции — через S3Hook (Yandex Object Storage).
"""

import logging
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor

log = logging.getLogger(__name__)


# ── Вспомогательные функции ──────────────────────────────────────────

def read_csv_from_s3(bucket: str, key: str, conn_id: str = "yandex_s3") -> pd.DataFrame:
    """Читает CSV из Yandex Object Storage через S3Hook."""
    hook = S3Hook(aws_conn_id=conn_id)
    obj = hook.get_key(key=key, bucket_name=bucket)
    content = obj.get()["Body"].read().decode("utf-8")
    return pd.read_csv(StringIO(content))


def write_csv_to_s3(
    df: pd.DataFrame,
    bucket: str,
    key: str,
    conn_id: str = "yandex_s3",
) -> None:
    """Записывает DataFrame в CSV в Yandex Object Storage через S3Hook."""
    hook = S3Hook(aws_conn_id=conn_id)
    buf = StringIO()
    df.to_csv(buf, index=False)
    hook.load_string(
        string_data=buf.getvalue(),
        key=key,
        bucket_name=bucket,
        replace=True,
    )
    log.info("Записан файл s3://%s/%s (%d строк)", bucket, key, len(df))


# ── Задача 1: расчёт OTD ────────────────────────────────────────────

def calc_otd(**ctx) -> dict:
    """
    Читает trips.csv из rzd-airflow-data через S3Hook.
    Рассчитывает OTD (On-Time Delivery) по depot_code.

    OTD = (рейсы с задержкой <= порога) / (всего рейсов) * 100%
    """
    bucket_data = Variable.get("s3_bucket_data")
    key = "trips.csv"
    depot_code = Variable.get("depot_code", default_var="TCH-15")
    delay_threshold = int(
        Variable.get("delay_threshold_min", default_var="15")
    )

    df = read_csv_from_s3(bucket=bucket_data, key=key)
    log.info("[otd] Прочитано %d строк из s3://%s/%s", len(df), bucket_data, key)

    # Фильтр по депо
    if "depot_code" in df.columns:
        df_depot = df[df["depot_code"] == depot_code].copy()
    else:
        df_depot = df.copy()
        df_depot["depot_code"] = depot_code

    if df_depot.empty:
        log.warning("[otd] Нет данных для депо %s", depot_code)
        return {
            "depot_code": depot_code,
            "total_trips": 0,
            "on_time_trips": 0,
            "otd_pct": 0.0,
            "avg_delay_min": 0.0,
            "max_delay_min": 0.0,
        }

    # Расчёт задержки прибытия (если нет готовой колонки)
    if "delay_arr_min" not in df_depot.columns:
        df_depot["scheduled_arr"] = pd.to_datetime(
            df_depot["scheduled_arr"], errors="coerce"
        )
        df_depot["arrival_time"] = pd.to_datetime(
            df_depot["arrival_time"], errors="coerce"
        )
        df_depot["delay_arr_min"] = (
            (df_depot["arrival_time"] - df_depot["scheduled_arr"])
            .dt.total_seconds()
            .div(60)
            .fillna(0)
        )

    total = len(df_depot)
    on_time = int((df_depot["delay_arr_min"] <= delay_threshold).sum())
    otd_pct = round(on_time / total * 100, 2) if total > 0 else 0.0
    avg_delay = round(float(df_depot["delay_arr_min"].mean()), 2)
    max_delay = round(float(df_depot["delay_arr_min"].max()), 2)

    result = {
        "run_date": ctx["ds"],
        "depot_code": depot_code,
        "total_trips": total,
        "on_time_trips": on_time,
        "otd_pct": otd_pct,
        "avg_delay_min": avg_delay,
        "max_delay_min": max_delay,
        "delay_threshold_min": delay_threshold,
    }

    log.info("[otd] Результат: %s", result)

    if otd_pct < 85.0:
        log.warning(
            "[otd] ВНИМАНИЕ: OTD ниже целевого значения! "
            "Текущий: %.1f%%, целевой: 85%%",
            otd_pct,
        )

    return result


# ── Задача 2: запись отчёта в S3 и PostgreSQL ────────────────────────

def write_s3_report(**ctx) -> None:
    """
    Записывает OTD-отчёт в rzd-airflow-results через S3Hook
    и сохраняет агрегат в PostgreSQL rzd_analytics.otd_reports.
    """
    ti = ctx["ti"]
    ds_nodash = ctx["ds_nodash"]
    otd_result = ti.xcom_pull(task_ids="calc_otd")

    # Запись CSV-отчёта в Object Storage
    df_report = pd.DataFrame([otd_result])
    bucket_results = Variable.get("s3_bucket_results")
    key_out = f"otd/{ds_nodash}/otd_report.csv"
    write_csv_to_s3(df=df_report, bucket=bucket_results, key=key_out)

    # Сохранение агрегата в PostgreSQL
    hook_pg = PostgresHook(postgres_conn_id="rzd_postgres")
    hook_pg.run(
        """
        INSERT INTO rzd_analytics.otd_reports
            (report_date, depot_code, total_trips, on_time_trips, otd_pct,
             avg_delay_min, max_delay_min, delay_threshold_min, s3_report_key)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (report_date, depot_code) DO UPDATE
            SET total_trips        = EXCLUDED.total_trips,
                on_time_trips      = EXCLUDED.on_time_trips,
                otd_pct            = EXCLUDED.otd_pct,
                avg_delay_min      = EXCLUDED.avg_delay_min,
                max_delay_min      = EXCLUDED.max_delay_min,
                delay_threshold_min= EXCLUDED.delay_threshold_min,
                s3_report_key      = EXCLUDED.s3_report_key,
                calculated_at      = NOW()
        """,
        parameters=(
            otd_result["run_date"],
            otd_result["depot_code"],
            otd_result["total_trips"],
            otd_result["on_time_trips"],
            otd_result["otd_pct"],
            otd_result["avg_delay_min"],
            otd_result["max_delay_min"],
            otd_result["delay_threshold_min"],
            f"s3://{bucket_results}/{key_out}",
        ),
    )

    log.info(
        "[write] Отчёт OTD сохранён: s3://%s/%s и в rzd_analytics.otd_reports",
        bucket_results,
        key_out,
    )


# ── Определение DAG 2 ────────────────────────────────────────────────

default_args = {
    "owner": "rzd_analytics_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="otd_analysis_dag",
    description="DAG 2: ExternalTaskSensor → calc OTD → write S3 report + PostgreSQL",
    schedule_interval="30 6 * * *",   # 06:30 UTC, на 30 мин позже ingestion DAG
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=default_args,
    tags=["lab05", "rzd", "otd", "consumer"],
) as dag:

    # ExternalTaskSensor: ждём load_trips_to_postgres в trips_ingestion_dag
    # trips_ingestion_dag стартует в 06:00, otd_analysis_dag в 06:30
    # execution_delta = 30 минут
    wait_ingestion_done = ExternalTaskSensor(
        task_id="wait_ingestion_done",
        external_dag_id="trips_ingestion_dag",
        external_task_id="load_trips_to_postgres",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        execution_delta=timedelta(minutes=30),
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    task_calc_otd = PythonOperator(
        task_id="calc_otd",
        python_callable=calc_otd,
    )

    task_write_report = PythonOperator(
        task_id="write_s3_report",
        python_callable=write_s3_report,
    )

    # Граф DAG 2: сенсор → расчёт → запись
    wait_ingestion_done >> task_calc_otd >> task_write_report
```

---

## Деплой и тестирование

### Загрузка DAG-файлов в Object Storage

```bash
# Деплой обоих DAG
yc storage cp trips_ingestion_dag.py \
    s3://rzd-airflow-dags/dags/trips_ingestion_dag.py

yc storage cp otd_analysis_dag.py \
    s3://rzd-airflow-dags/dags/otd_analysis_dag.py

# Проверить загруженные файлы
yc storage ls s3://rzd-airflow-dags/dags/
```

### Проверка в Airflow UI

1. Откройте Airflow UI через Yandex Cloud Console (кластер Managed Airflow → **Открыть Airflow UI**).
2. Дождитесь появления DAG `trips_ingestion_dag` и `otd_analysis_dag` в списке (1–3 мин).
3. Убедитесь, что у обоих DAG нет ошибок в колонке **Schedule** — должна стоять зелёная метка.
4. Откройте **trips_ingestion_dag → Graph View** — должен отображаться граф:
   `wait_trips_file → read_trips_from_s3 → validate_trips → load_trips_to_postgres`
5. Откройте **otd_analysis_dag → Graph View** — должен отображаться граф:
   `wait_ingestion_done → calc_otd → write_s3_report`

### Ожидаемый результат нормального запуска

| DAG | Задача | Ожидаемый статус |
|---|---|---|
| `trips_ingestion_dag` | `wait_trips_file` | `success` (файл обнаружен в S3) |
| `trips_ingestion_dag` | `read_trips_from_s3` | `success` |
| `trips_ingestion_dag` | `validate_trips` | `success` |
| `trips_ingestion_dag` | `load_trips_to_postgres` | `success` |
| `otd_analysis_dag` | `wait_ingestion_done` | `success` (после load_trips_to_postgres) |
| `otd_analysis_dag` | `calc_otd` | `success` |
| `otd_analysis_dag` | `write_s3_report` | `success` |

В бакете `rzd-airflow-results` появится файл `otd/YYYYMMDD/otd_report.csv`.
В таблице `rzd_analytics.otd_reports` появится новая строка.

---

## Задания повышенной сложности

### Задание 1. Уведомление в лог при таймауте ExternalTaskSensor

Добавьте в `otd_analysis_dag` callback-функцию, которая выводит детальное сообщение
при превышении таймаута `wait_ingestion_done`:

```python
def on_ingestion_timeout(context):
    """Вызывается при таймауте ExternalTaskSensor."""
    log.error(
        "АЛЕРТ: Таймаут ожидания trips_ingestion_dag! "
        "execution_date=%s. Проверьте статус DAG ingestion в Airflow UI.",
        context["execution_date"],
    )
    # При необходимости: вызов Telegram Bot API или email через SMTP

wait_ingestion_done = ExternalTaskSensor(
    task_id="wait_ingestion_done",
    # ... остальные параметры без изменений ...
    on_failure_callback=on_ingestion_timeout,
)
```

Протестируйте сценарий: запустите `otd_analysis_dag` без предварительного запуска `trips_ingestion_dag` и убедитесь, что в логах появляется сообщение алерта.

### Задание 2. Чтение данных из PostgreSQL вместо повторного чтения S3

Измените функцию `calc_otd` так, чтобы она читала данные из таблицы
`rzd_analytics.trips` (куда загружает DAG 1), а не повторно читала trips.csv из S3.
Это устраняет двойную зависимость от S3 и делает пайплайн более надёжным:

```python
def calc_otd_from_postgres(**ctx) -> dict:
    """Читает trips из PostgreSQL вместо повторного чтения S3."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    depot_code = Variable.get("depot_code", default_var="TCH-15")
    delay_threshold = int(Variable.get("delay_threshold_min", default_var="15"))

    hook_pg = PostgresHook(postgres_conn_id="rzd_postgres")
    df = hook_pg.get_pandas_df(
        sql="""
            SELECT loco_id, depot_code, delay_arr_min
            FROM rzd_analytics.trips
            WHERE depot_code = %(depot)s
              AND DATE(departure_time) = %(run_date)s
        """,
        parameters={"depot": depot_code, "run_date": ctx["ds"]},
    )
    # ... расчёт OTD аналогично исходной функции ...
```

### Задание 3. Параллельный анализ по нескольким депо (fan-out/fan-in)

Создайте расширенную версию `otd_analysis_dag`, которая рассчитывает OTD параллельно
для нескольких депо, а затем объединяет результаты в единый сводный отчёт:

```python
from airflow.models import Variable

DEPOT_CODES = ["TCH-15", "TCH-09", "TCH-22"]

# Динамическое создание задач fan-out
calc_tasks = []
for depot in DEPOT_CODES:
    t = PythonOperator(
        task_id=f"calc_otd_{depot.replace('-', '_')}",
        python_callable=calc_otd_for_depot,
        op_kwargs={"depot_code": depot},
    )
    calc_tasks.append(t)

# Fan-in: сводный отчёт после всех ветвей
merge_report = PythonOperator(
    task_id="merge_depot_reports",
    python_callable=merge_all_depots,
)

# Граф: сенсор → [calc_TCH15 | calc_TCH09 | calc_TCH22] → merge_report
wait_ingestion_done >> calc_tasks >> merge_report
```

Разместите сводный отчёт по пути `s3://rzd-airflow-results/otd/all_depots/YYYYMMDD/summary.csv`.
