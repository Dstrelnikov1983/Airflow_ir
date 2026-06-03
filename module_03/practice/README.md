# Практическая работа №03: Расписания DAG для периодического чтения из Object Storage

**Организация:** РЖД, Западно-Сибирская дирекция тяги, депо Новосибирск-Главный (ТЧЭ-15)
**Платформа:** Yandex Managed Service for Apache Airflow™ + Yandex Object Storage
**Продолжительность:** 40–50 минут
**Уровень:** Начальный / Средний

---

## Цель и задачи

**Цель:** научиться создавать DAG с разными расписаниями, которые читают входные данные из Yandex Object Storage (S3) и записывают результаты обратно в Object Storage — без использования локальной файловой системы.

**Задачи:**

1. Развернуть объектное хранилище и загрузить тестовые CSV-файлы ТЧЭ-15.
2. Настроить Airflow Connections и Variables для работы с Yandex Object Storage и Managed PostgreSQL.
3. Создать DAG телеметрии (каждые 15 мин): `S3KeySensor → читать данные → вставить в PostgreSQL`.
4. Создать DAG посменного отчёта (каждые 8 ч): `читать trips.csv → расчёт ОТД → записать отчёт в S3`.
5. Выполнить backfill за 2024-03-01 — 2024-03-07 из данных, хранящихся в Object Storage.
6. Изучить поведение параметров `catchup`, `backfill` и `max_active_runs`.

---

## Необходимые ресурсы

| Ресурс | Значение |
|---|---|
| Managed Airflow | предоставлен преподавателем |
| Yandex Cloud Console | console.yandex.cloud |
| Бакет входных данных | `rzd-airflow-data` |
| Бакет DAG-файлов | `rzd-airflow-dags` |
| Бакет результатов | `rzd-airflow-results` |
| Managed PostgreSQL | кластер `rzd-analytics`, база `rzd_analytics` |
| Сервисный аккаунт S3 | с ролями `storage.viewer` и `storage.uploader` |

---

## Подготовка Object Storage (ОБЯЗАТЕЛЬНЫЙ РАЗДЕЛ)

### Шаг П-1. Создание бакетов через Yandex Cloud Console

1. Откройте [console.yandex.cloud](https://console.yandex.cloud) → выберите свой каталог.
2. В левом меню: **Object Storage** → **Создать бакет**.
3. Создайте три бакета:

| Бакет | Назначение | Доступ |
|---|---|---|
| `rzd-airflow-dags` | DAG-файлы Python | Приватный |
| `rzd-airflow-data` | Входные CSV-файлы | Приватный |
| `rzd-airflow-results` | Результаты обработки | Приватный |

> Регион для всех бакетов: **ru-central1**.

### Шаг П-2. Загрузка CSV-файлов в бакет данных

Загрузите тестовые файлы в бакет `rzd-airflow-data`. Структура ключей:

```
rzd-airflow-data/
├── sensor_readings.csv
├── locomotives.csv
├── trips.csv
├── schedule_adherence.csv
└── maintenance.csv
```

**Через Yandex Cloud Console:**

1. Откройте бакет `rzd-airflow-data` → кнопка **Загрузить объекты**.
2. Выберите файлы из комплекта практической работы (папка `data/`).
3. Нажмите **Загрузить**.

**Через Yandex CLI (альтернатива):**

```bash
yc storage cp data/sensor_readings.csv     s3://rzd-airflow-data/sensor_readings.csv
yc storage cp data/locomotives.csv         s3://rzd-airflow-data/locomotives.csv
yc storage cp data/trips.csv               s3://rzd-airflow-data/trips.csv
yc storage cp data/schedule_adherence.csv  s3://rzd-airflow-data/schedule_adherence.csv
yc storage cp data/maintenance.csv         s3://rzd-airflow-data/maintenance.csv
```

### Шаг П-3. Создание сервисного аккаунта и ключей доступа

1. В Yandex Cloud Console: **IAM** → **Сервисные аккаунты** → **Создать**.
2. Имя: `airflow-s3-sa`. Нажмите **Создать**.
3. Откройте созданный аккаунт → вкладка **Роли** → **Назначить роль** в каталоге:
   - `storage.viewer` — для чтения из бакетов
   - `storage.uploader` — для записи результатов
4. Вкладка **Ключи доступа** → **Создать новый ключ** → тип **Статический ключ**.
5. Сохраните **Идентификатор ключа** (Access Key ID) и **Секретный ключ** (Secret Access Key) — они понадобятся для настройки Connection в Airflow.

> Секретный ключ отображается только один раз. Сохраните его немедленно.

---

## Настройка Airflow Connections и Variables (в UI)

### Connection: yandex_s3

Откройте Airflow UI → **Admin → Connections → + Add Connection**:

| Поле | Значение |
|---|---|
| Conn Id | `yandex_s3` |
| Conn Type | `Amazon Web Services` |
| Login | `<Access Key ID сервисного аккаунта>` |
| Password | `<Secret Access Key>` |
| Extra | `{"endpoint_url": "https://storage.yandexcloud.net", "region_name": "ru-central1"}` |

Нажмите **Save**.

### Connection: rzd_postgres

Откройте **Admin → Connections → + Add Connection**:

| Поле | Значение |
|---|---|
| Conn Id | `rzd_postgres` |
| Conn Type | `Postgres` |
| Host | `<FQDN кластера>.mdb.yandexcloud.net` |
| Schema | `rzd_analytics` |
| Login | `rzd_user` |
| Password | `<пароль из Yandex Lockbox или от преподавателя>` |
| Port | `5432` |

### Variables

Откройте **Admin → Variables → + Add Variable**:

| Key | Value |
|---|---|
| `s3_bucket_data` | `rzd-airflow-data` |
| `s3_bucket_results` | `rzd-airflow-results` |
| `depot_code` | `TCH-15` |
| `delay_threshold_min` | `15` |

---

## Деплой DAG-файла в Managed Airflow

В Yandex Managed Airflow DAG-файлы хранятся в Object Storage. Локальная папка `dags/` недоступна.

### Загрузка DAG через Yandex Cloud Console

1. Откройте бакет `rzd-airflow-dags` → **Загрузить объекты**.
2. Выберите файл `glonass_telemetry_dag.py` или `shift_adherence_dag.py`.
3. Убедитесь, что файл попал в корень бакета или в папку `dags/`.
4. В Managed Airflow Console: **DAG-файлы** → убедитесь, что указан бакет `rzd-airflow-dags`.

### Загрузка DAG через Yandex CLI

```bash
yc storage cp glonass_telemetry_dag.py  s3://rzd-airflow-dags/dags/glonass_telemetry_dag.py
yc storage cp shift_adherence_dag.py    s3://rzd-airflow-dags/dags/shift_adherence_dag.py
```

### Проверка в Airflow UI

1. Откройте Airflow UI → список DAG.
2. Подождите 1–3 минуты: Managed Airflow сканирует бакет периодически.
3. Убедитесь, что DAG появился и статус — **active** (не paused, нет ошибок импорта).

> Если DAG не появился через 5 минут — проверьте **Admin → Import Errors**.

---

## Шаги выполнения

### Шаг 1. Проверьте доступность Object Storage из Airflow

В Airflow UI откройте **Admin → Connections → yandex_s3** → кнопка **Test**. Ожидаемый результат: `Connection successfully tested`.

### Шаг 2. Разместите DAG телеметрии в Object Storage

Создайте файл `glonass_telemetry_dag.py` (код приведён в разделе «Полный код DAG» ниже) и загрузите его в бакет:

```bash
yc storage cp glonass_telemetry_dag.py s3://rzd-airflow-dags/dags/glonass_telemetry_dag.py
```

После появления DAG в UI убедитесь: расписание `*/15 * * * *`, `catchup=False`.

### Шаг 3. Разместите DAG посменного отчёта в Object Storage

Создайте файл `shift_adherence_dag.py` и загрузите:

```bash
yc storage cp shift_adherence_dag.py s3://rzd-airflow-dags/dags/shift_adherence_dag.py
```

Расписание `0 17,1,9 * * *` (00:00 / 08:00 / 16:00 НСК), `catchup=True`.

### Шаг 4. Проверьте работу S3KeySensor

1. Откройте DAG `glonass_telemetry` → граф задач.
2. Найдите задачу `wait_for_telemetry_file` (S3KeySensor).
3. Убедитесь, что задача ждёт появления файла в бакете `rzd-airflow-data`.
4. Загрузите тестовый файл телеметрии в бакет (если ещё не загружен) — задача должна перейти из `poking` в `success`.

### Шаг 5. Выполните backfill из Object Storage за 2024-03-01 — 2024-03-07

Данные за эту неделю уже находятся в бакете `rzd-airflow-data`. Запустите backfill через Airflow UI:

1. Airflow UI → DAG `shift_adherence_report` → **Trigger DAG w/ config**.
2. Либо через CLI (если доступен):

```bash
airflow dags backfill \
    --dag-id shift_adherence_report \
    --start-date 2024-03-01 \
    --end-date   2024-03-07 \
    --reset-dagruns
```

Ожидаемый результат: 21 DAG run (7 дней × 3 смены), каждый читает CSV из `rzd-airflow-data/` и пишет отчёт в `rzd-airflow-results/shift_reports/`.

### Шаг 6. Проверьте результаты в Object Storage

1. Откройте бакет `rzd-airflow-results` → папку `shift_reports/`.
2. Убедитесь, что появились CSV-файлы с именами вида `20240301_shift1.csv`.
3. Откройте один из файлов: проверьте столбцы `route`, `avg_delay_min`, `otd_pct`.
4. В Airflow UI → DAG `shift_adherence_report` → **Grid View**: все 21 run зелёные.

---

## Полный код DAG

### DAG 1: Телеметрия ГЛОНАСС (каждые 15 минут)

Файл: `glonass_telemetry_dag.py`

```python
"""
DAG: glonass_telemetry
Назначение: сбор телеметрии датчиков локомотивов ТЧЭ-15 из Object Storage
Расписание: каждые 15 минут
Платформа: Yandex Managed Airflow + Yandex Object Storage
"""

from __future__ import annotations

import logging
from datetime import timedelta
from io import StringIO

import pandas as pd
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

NSK_TZ         = pendulum.timezone("Asia/Novosibirsk")
S3_CONN_ID     = "yandex_s3"
POSTGRES_CONN  = "rzd_postgres"


# ------------------------------------------------------------------ #
#  Вспомогательные функции                                           #
# ------------------------------------------------------------------ #

def read_csv_from_s3(bucket: str, key: str, conn_id: str = S3_CONN_ID) -> pd.DataFrame:
    """Читает CSV-файл из Yandex Object Storage через S3Hook."""
    hook = S3Hook(aws_conn_id=conn_id)
    obj = hook.get_key(key=key, bucket_name=bucket)
    content = obj.get()["Body"].read().decode("utf-8")
    return pd.read_csv(StringIO(content))


def write_csv_to_s3(
    df: pd.DataFrame,
    bucket: str,
    key: str,
    conn_id: str = S3_CONN_ID,
) -> None:
    """Записывает DataFrame как CSV в Yandex Object Storage."""
    hook = S3Hook(aws_conn_id=conn_id)
    buf = StringIO()
    df.to_csv(buf, index=False)
    hook.load_string(
        string_data=buf.getvalue(),
        key=key,
        bucket_name=bucket,
        replace=True,
    )


# ------------------------------------------------------------------ #
#  Функции задач                                                      #
# ------------------------------------------------------------------ #

def load_telemetry(**context) -> dict:
    """
    Читает sensor_readings.csv из Object Storage,
    фильтрует записи ГЛОНАСС по текущему интервалу.
    """
    bucket = Variable.get("s3_bucket_data", default_var="rzd-airflow-data")
    ds_nodash = context["ds_nodash"]

    # Ключ S3 с шаблоном даты: данные могут лежать по дням
    key = f"sensor_readings/{ds_nodash}/data.csv"

    # Fallback: корневой файл если посуточного нет
    hook = S3Hook(aws_conn_id=S3_CONN_ID)
    if not hook.check_for_key(key=key, bucket_name=bucket):
        key = "sensor_readings.csv"

    log.info("Читаем телеметрию из s3://%s/%s", bucket, key)

    df = read_csv_from_s3(bucket=bucket, key=key)
    glonass_df = df[df["sensor_type"] == "GLONASS"].copy()

    log.info(
        "Интервал %s — %s: найдено %d записей ГЛОНАСС из %d",
        context["data_interval_start"],
        context["data_interval_end"],
        len(glonass_df),
        len(df),
    )

    return {
        "count":           len(glonass_df),
        "total_rows":      len(df),
        "s3_key":          key,
        "interval_start":  str(context["data_interval_start"]),
    }


def validate_telemetry(**context) -> None:
    """
    Проверяет корректность данных телеметрии.
    Логирует предупреждение если данных нет.
    """
    ti = context["ti"]
    result = ti.xcom_pull(task_ids="load_telemetry")
    count = result.get("count", 0)

    log.info("Валидация: %d записей ГЛОНАСС за интервал %s",
             count, result.get("interval_start"))

    if count == 0:
        log.warning(
            "Нет данных ГЛОНАСС за интервал %s — "
            "проверьте связь с локомотивами или загрузку файла в S3.",
            result.get("interval_start"),
        )


def insert_to_postgres(**context) -> None:
    """
    Вставляет записи телеметрии в PostgreSQL (rzd_analytics.sensor_readings).
    Читает данные повторно из S3 для полноты.
    """
    ti = context["ti"]
    result = ti.xcom_pull(task_ids="load_telemetry")
    bucket = Variable.get("s3_bucket_data", default_var="rzd-airflow-data")
    key = result["s3_key"]

    df = read_csv_from_s3(bucket=bucket, key=key)
    df_glonass = df[df["sensor_type"] == "GLONASS"].copy()

    if df_glonass.empty:
        log.warning("Нет данных для вставки в PostgreSQL.")
        return

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN)
    rows_inserted = 0

    for _, row in df_glonass.iterrows():
        hook.run(
            """
            INSERT INTO rzd_analytics.sensor_readings
                (locomotive_id, sensor_type, recorded_at, value_json, dag_run_id)
            VALUES (%(locomotive_id)s, %(sensor_type)s, %(recorded_at)s,
                    %(value_json)s, %(dag_run_id)s)
            ON CONFLICT DO NOTHING;
            """,
            parameters={
                "locomotive_id": row.get("locomotive_id"),
                "sensor_type":   row.get("sensor_type"),
                "recorded_at":   row.get("recorded_at"),
                "value_json":    row.get("value_json", "{}"),
                "dag_run_id":    context["run_id"],
            },
        )
        rows_inserted += 1

    log.info("Вставлено %d записей ГЛОНАСС в rzd_analytics.sensor_readings", rows_inserted)


# ------------------------------------------------------------------ #
#  Определение DAG                                                    #
# ------------------------------------------------------------------ #

default_args = {
    "owner":          "tche15-analytics",
    "retries":        2,
    "retry_delay":    timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="glonass_telemetry",
    description="Сбор телеметрии ГЛОНАСС локомотивов ТЧЭ-15 из Object Storage (каждые 15 мин)",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2024, 3, 1, tz=NSK_TZ),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args=default_args,
    tags=["telemetry", "realtime", "glonass", "tche15", "s3"],
) as dag:

    # S3KeySensor: ждём появления файла телеметрии в Object Storage
    wait_file = S3KeySensor(
        task_id="wait_for_telemetry_file",
        bucket_name="rzd-airflow-data",
        bucket_key="sensor_readings/{{ ds_nodash }}/data.csv",
        aws_conn_id=S3_CONN_ID,
        poke_interval=300,   # проверять каждые 5 минут
        timeout=7200,        # ждать не более 2 часов
        mode="reschedule",   # освобождать воркер между проверками
        soft_fail=True,      # продолжить если файл не появился (fallback на корневой)
    )

    t_load = PythonOperator(
        task_id="load_telemetry",
        python_callable=load_telemetry,
    )

    t_validate = PythonOperator(
        task_id="validate_telemetry",
        python_callable=validate_telemetry,
    )

    t_insert = PythonOperator(
        task_id="insert_to_postgres",
        python_callable=insert_to_postgres,
    )

    wait_file >> t_load >> t_validate >> t_insert
```

---

### DAG 2: Посменный отчёт по ОТД (каждые 8 часов)

Файл: `shift_adherence_dag.py`

```python
"""
DAG: shift_adherence_report
Назначение: посменный расчёт ОТД из trips.csv в Object Storage
Расписание: 3 раза в сутки (00:00 / 08:00 / 16:00 НСК = 17:00 / 01:00 / 09:00 UTC)
Платформа: Yandex Managed Airflow + Yandex Object Storage
"""

from __future__ import annotations

import logging
from datetime import timedelta
from io import StringIO

import pandas as pd
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

NSK_TZ        = pendulum.timezone("Asia/Novosibirsk")
S3_CONN_ID    = "yandex_s3"
POSTGRES_CONN = "rzd_postgres"


# ------------------------------------------------------------------ #
#  Вспомогательные функции S3                                         #
# ------------------------------------------------------------------ #

def read_csv_from_s3(bucket: str, key: str, conn_id: str = S3_CONN_ID) -> pd.DataFrame:
    """Читает CSV-файл из Yandex Object Storage через S3Hook."""
    hook = S3Hook(aws_conn_id=conn_id)
    obj = hook.get_key(key=key, bucket_name=bucket)
    content = obj.get()["Body"].read().decode("utf-8")
    return pd.read_csv(StringIO(content))


def write_csv_to_s3(
    df: pd.DataFrame,
    bucket: str,
    key: str,
    conn_id: str = S3_CONN_ID,
) -> None:
    """Записывает DataFrame как CSV в Yandex Object Storage."""
    hook = S3Hook(aws_conn_id=conn_id)
    buf = StringIO()
    df.to_csv(buf, index=False)
    hook.load_string(
        string_data=buf.getvalue(),
        key=key,
        bucket_name=bucket,
        replace=True,
    )


def get_shift_number(nsk_dt: pendulum.DateTime) -> int:
    """Определяет номер смены (1/2/3) по времени НСК."""
    h = nsk_dt.hour
    if 0 <= h < 8:
        return 1
    elif 8 <= h < 16:
        return 2
    return 3


# ------------------------------------------------------------------ #
#  Функции задач                                                      #
# ------------------------------------------------------------------ #

def read_trips_from_s3(**context) -> dict:
    """
    Читает trips.csv из rzd-airflow-data/ за текущую смену.
    Все операции с файлами — только через S3Hook.
    """
    bucket   = Variable.get("s3_bucket_data", default_var="rzd-airflow-data")
    ds       = context["ds"]            # YYYY-MM-DD
    ds_nodash = context["ds_nodash"]    # YYYYMMDD

    data_interval_start = context["data_interval_start"]
    nsk_dt = data_interval_start.in_timezone(NSK_TZ)
    shift_num = get_shift_number(nsk_dt)

    # Пробуем посуточный ключ; если нет — корневой файл
    key_daily = f"trips/{ds_nodash}/trips.csv"
    key_root  = "trips.csv"

    hook = S3Hook(aws_conn_id=S3_CONN_ID)
    key = key_daily if hook.check_for_key(key=key_daily, bucket_name=bucket) else key_root

    log.info(
        "Смена %d (%s): читаем рейсы из s3://%s/%s",
        shift_num, ds, bucket, key,
    )

    df = read_csv_from_s3(bucket=bucket, key=key)

    # Фильтр по дате и смене
    if "trip_date" in df.columns and "shift_number" in df.columns:
        df_shift = df[
            (df["trip_date"] == ds) & (df["shift_number"] == shift_num)
        ].copy()
    else:
        df_shift = df.copy()

    log.info("Найдено %d рейсов для смены %d", len(df_shift), shift_num)

    return {
        "shift_num":  shift_num,
        "trip_date":  ds,
        "ds_nodash":  ds_nodash,
        "trip_count": len(df_shift),
        "rows":       df_shift.to_dict(orient="records"),
    }


def calculate_otd(**context) -> dict:
    """
    Рассчитывает ОТД (On-Time Delivery) и средние опоздания по маршрутам.
    """
    ti    = context["ti"]
    data  = ti.xcom_pull(task_ids="read_trips_from_s3")
    rows  = data["rows"]

    if not rows:
        log.warning("Нет рейсов для смены %d — пустой расчёт", data["shift_num"])
        return {
            **data,
            "total_trips":   0,
            "delayed_trips": 0,
            "avg_delay_min": 0.0,
            "max_delay_min": 0.0,
            "otd_pct":       100.0,
            "route_stats":   [],
        }

    df = pd.DataFrame(rows)

    delays        = pd.to_numeric(df.get("delay_minutes", pd.Series(dtype=float)), errors="coerce").fillna(0)
    total_trips   = len(df)
    delayed_trips = int((delays > 0).sum())
    avg_delay     = float(delays.mean())
    max_delay     = float(delays.max())
    otd_pct       = (total_trips - delayed_trips) / total_trips * 100 if total_trips > 0 else 100.0

    # Статистика по маршрутам
    route_stats = []
    if "route_from" in df.columns and "route_to" in df.columns:
        df["delay_minutes_num"] = delays
        for (rf, rt), grp in df.groupby(["route_from", "route_to"]):
            route_stats.append({
                "route":         f"{rf} → {rt}",
                "trips":         len(grp),
                "avg_delay_min": round(grp["delay_minutes_num"].mean(), 2),
            })

    metrics = {
        **data,
        "total_trips":   total_trips,
        "delayed_trips": delayed_trips,
        "avg_delay_min": round(avg_delay, 2),
        "max_delay_min": round(max_delay, 2),
        "otd_pct":       round(otd_pct, 2),
        "route_stats":   route_stats,
    }

    log.info(
        "Смена %d | Рейсов: %d | OTD: %.1f%% | avg_delay: %.1f мин | max_delay: %.1f мин",
        data["shift_num"], total_trips, otd_pct, avg_delay, max_delay,
    )

    return metrics


def check_otd_threshold(**context) -> str:
    """Ветвление: нужно ли уведомление диспетчеру?"""
    ti      = context["ti"]
    metrics = ti.xcom_pull(task_ids="calculate_otd")
    threshold = float(Variable.get("delay_threshold_min", default_var="15"))

    if metrics["max_delay_min"] > threshold or metrics["otd_pct"] < 85.0:
        log.warning(
            "Смена %d: max_delay=%.1f мин > %.1f мин или OTD=%.1f%% < 85%% — ALERT",
            metrics["shift_num"], metrics["max_delay_min"], threshold, metrics["otd_pct"],
        )
        return "send_alert"

    log.info("Смена %d: OTD=%.1f%% — в норме", metrics["shift_num"], metrics["otd_pct"])
    return "skip_alert"


def send_alert(**context) -> None:
    """Уведомление дежурному диспетчеру ТЧЭ-15 (имитация)."""
    ti      = context["ti"]
    metrics = ti.xcom_pull(task_ids="calculate_otd")
    log.warning(
        "[ТЧЭ-15 ALERT] Смена %d (%s) | OTD=%.1f%% | max_delay=%.1f мин | рейсов: %d",
        metrics["shift_num"], metrics["trip_date"],
        metrics["otd_pct"], metrics["max_delay_min"], metrics["total_trips"],
    )


def write_report_to_s3(**context) -> None:
    """
    Записывает посменный отчёт по ОТД в rzd-airflow-results/shift_reports/.
    Ключ: shift_reports/{ds_nodash}_shift{N}_{run_id}.csv
    Все операции — только через S3Hook (hook.load_string).
    """
    ti      = context["ti"]
    metrics = ti.xcom_pull(task_ids="calculate_otd")
    bucket  = Variable.get("s3_bucket_results", default_var="rzd-airflow-results")

    # Формируем безопасный run_id для имени файла
    run_id_safe = context["run_id"].replace(":", "_").replace("+", "_")
    key = (
        f"shift_reports/{metrics['ds_nodash']}"
        f"_shift{metrics['shift_num']}"
        f"_{run_id_safe}.csv"
    )

    # Итоговый DataFrame отчёта
    report_df = pd.DataFrame([{
        "report_date":   metrics["trip_date"],
        "shift_number":  metrics["shift_num"],
        "total_trips":   metrics["total_trips"],
        "delayed_trips": metrics["delayed_trips"],
        "avg_delay_min": metrics["avg_delay_min"],
        "max_delay_min": metrics["max_delay_min"],
        "otd_pct":       metrics["otd_pct"],
        "dag_run_id":    context["run_id"],
    }])

    write_csv_to_s3(df=report_df, bucket=bucket, key=key)

    log.info(
        "Отчёт смены %d записан в s3://%s/%s",
        metrics["shift_num"], bucket, key,
    )

    # Дополнительно: статистика по маршрутам
    if metrics.get("route_stats"):
        routes_df = pd.DataFrame(metrics["route_stats"])
        routes_key = (
            f"shift_reports/{metrics['ds_nodash']}"
            f"_shift{metrics['shift_num']}_routes.csv"
        )
        write_csv_to_s3(df=routes_df, bucket=bucket, key=routes_key)
        log.info("Статистика по маршрутам записана в s3://%s/%s", bucket, routes_key)


# ------------------------------------------------------------------ #
#  Определение DAG                                                    #
# ------------------------------------------------------------------ #

default_args = {
    "owner":          "tche15-analytics",
    "retries":        1,
    "retry_delay":    timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="shift_adherence_report",
    description="Посменный отчёт по ОТД ТЧЭ-15: trips.csv из S3 → расчёт → отчёт в S3",
    schedule="0 17,1,9 * * *",
    start_date=pendulum.datetime(2024, 3, 1, tz=NSK_TZ),
    catchup=True,
    max_active_runs=3,
    dagrun_timeout=timedelta(minutes=20),
    default_args=default_args,
    tags=["report", "schedule", "adherence", "s3", "tche15"],
) as dag:

    t_read = PythonOperator(
        task_id="read_trips_from_s3",
        python_callable=read_trips_from_s3,
    )

    t_calc = PythonOperator(
        task_id="calculate_otd",
        python_callable=calculate_otd,
    )

    t_branch = BranchPythonOperator(
        task_id="check_otd_threshold",
        python_callable=check_otd_threshold,
    )

    t_alert = PythonOperator(
        task_id="send_alert",
        python_callable=send_alert,
    )

    t_skip = EmptyOperator(task_id="skip_alert")

    t_write = PythonOperator(
        task_id="write_report_to_s3",
        python_callable=write_report_to_s3,
        trigger_rule="none_failed_min_one_success",
    )

    t_read >> t_calc >> t_branch >> [t_alert, t_skip] >> t_write
```

---

## Контрольные вопросы

1. Почему для DAG телеметрии ГЛОНАСС выбрано `catchup=False`, а для посменного отчёта — `catchup=True`? В чём практическая разница при работе с данными в Object Storage?

2. S3KeySensor настроен с параметром `mode='reschedule'`. Что это означает и чем `reschedule` отличается от `poke`? Какой режим экономит ресурсы воркера Managed Airflow?

3. Cron-выражение `0 17,1,9 * * *` указано в UTC. Переведите в НСК (UTC+7) и объясните, почему расписание смен ТЧЭ-15 задаётся именно в UTC, а не в локальном времени.

4. Вы выполнили backfill за 7 дней для DAG с расписанием `*/15 * * * *`. Сколько DAG runs будет создано? Как это скажется на Managed Airflow и как ограничить нагрузку?

5. Функция `write_csv_to_s3` использует `hook.load_string()` с параметром `replace=True`. Что произойдёт при повторном запуске backfill за ту же дату? Является ли такой DAG идемпотентным?
