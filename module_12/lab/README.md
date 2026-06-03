# Лабораторная работа №12: Lockbox для S3 и PostgreSQL credentials в Managed Airflow

**Модуль:** 12 — Безопасность Airflow  
**Продолжительность:** 60 минут  
**Платформа:** Yandex Managed Service for Apache Airflow™  
**Схема БД:** `rzd_analytics`

> **Важно.** Среда выполнения — Managed Airflow. Прямого доступа к файловой системе нет.
> `airflow.cfg` не редактируется вручную — конфигурация передаётся через переменные окружения
> в настройках сервиса. DAG-файлы деплоятся только через Object Storage.

---

## Цель

Полностью убрать credentials из Airflow Connections UI: перенести секреты для S3 и
PostgreSQL в Yandex Lockbox, настроить Managed Airflow использовать Lockbox как
Secrets Backend, проверить аудит-лог и убедиться, что DAG продолжает работать.

---

## Предварительные условия

| Ресурс | Ожидаемое состояние |
|---|---|
| Managed Airflow кластер | Активен в Yandex Cloud |
| Бакет `rzd-airflow-dags` | Привязан к Managed Airflow, папка `dags/` существует |
| Бакет `rzd-airflow-data` | Содержит `locomotives.csv`, `sensor_readings.csv`, `trips.csv`, `schedule_adherence.csv`, `maintenance.csv` |
| Бакет `rzd-airflow-results` | Создан |
| Managed PostgreSQL | Активен, база `rzd_analytics` и таблицы инициализированы |
| Сервисный аккаунт Managed Airflow | Имеет роли: `lockbox.payloadViewer`, `storage.editor`, `managed-postgresql.editor` |
| YC CLI | Установлен, выполнен `yc init`, выбран нужный каталог |

### Переменные Airflow (Admin → Variables)

Убедитесь, что в Airflow UI заданы:

| Ключ | Значение |
|---|---|
| `s3_bucket_data` | `rzd-airflow-data` |
| `s3_bucket_results` | `rzd-airflow-results` |
| `depot_code` | `TCH-15` |
| `delay_threshold_min` | `15` |

---

## Задание

### Шаг 1. Создать Lockbox-секрет `rzd-s3-connection` со структурой Airflow Connection

Секрет должен содержать все поля Connection, которые Airflow ожидает для типа `aws`.

```bash
yc lockbox secret create \
  --name rzd-s3-connection \
  --description "Airflow Connection yandex_s3: Object Storage ТЧЭ-15" \
  --payload '[
    {"key": "conn_type", "text_value": "aws"},
    {"key": "login",     "text_value": "<Access Key ID>"},
    {"key": "password",  "text_value": "<Secret Access Key>"},
    {"key": "extra",     "text_value": "{\"endpoint_url\": \"https://storage.yandexcloud.net\", \"region_name\": \"ru-central1\"}"}
  ]'
```

Проверьте, что секрет создан:

```bash
yc lockbox secret get --name rzd-s3-connection
yc lockbox secret payload get --name rzd-s3-connection
```

Поле `password` (Secret Access Key) в выводе должно отображаться как `[Protected]`.

---

### Шаг 2. Создать Lockbox-секрет `rzd-postgres-connection`

```bash
yc lockbox secret create \
  --name rzd-postgres-connection \
  --description "Airflow Connection rzd_postgres: Managed PostgreSQL ТЧЭ-15" \
  --payload '[
    {"key": "conn_type", "text_value": "postgres"},
    {"key": "host",      "text_value": "<FQDN кластера>.mdb.yandexcloud.net"},
    {"key": "port",      "text_value": "5432"},
    {"key": "login",     "text_value": "airflow_svc"},
    {"key": "password",  "text_value": "<пароль>"},
    {"key": "schema",    "text_value": "rzd_analytics"}
  ]'
```

---

### Шаг 3. Настроить Managed Airflow использовать Lockbox как Secrets Backend

Конфигурация передаётся через переменные окружения кластера.

**Путь в консоли:** Managed Service for Apache Airflow → ваш кластер → Изменить →
Дополнительные настройки → Переменные окружения.

Добавьте две переменные:

```
AIRFLOW__SECRETS__BACKEND
= airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend

AIRFLOW__SECRETS__BACKEND_KWARGS
= {"folder_id": "<ID каталога>", "connections_prefix": "rzd", "sep": "-"}
```

> **Как работает сопоставление имён.**
> При `connections_prefix = "rzd"` и `sep = "-"` Connection `yandex_s3` будет искаться
> в секрете Lockbox с именем `rzd-yandex-s3`, а `rzd_postgres` — в `rzd-rzd-postgres`.
> Чтобы избежать двойного префикса, используйте `connections_prefix = ""` и имена
> секретов точно совпадающие с `conn_id`, либо переименуйте секреты по шаблону.
>
> В данной лабораторной работе для простоты рекомендуется задать:
> `connections_prefix = "airflow-connections"`, `sep = "-"`, и назвать секреты:
> `airflow-connections-yandex-s3` и `airflow-connections-rzd-postgres`.

Переименуйте секреты (или создайте с правильными именами):

```bash
# Создать с именами, соответствующими шаблону
yc lockbox secret create \
  --name airflow-connections-yandex-s3 \
  --description "Airflow Connection yandex_s3 via Lockbox" \
  --payload '[
    {"key": "conn_type", "text_value": "aws"},
    {"key": "login",     "text_value": "<Access Key ID>"},
    {"key": "password",  "text_value": "<Secret Access Key>"},
    {"key": "extra",     "text_value": "{\"endpoint_url\": \"https://storage.yandexcloud.net\", \"region_name\": \"ru-central1\"}"}
  ]'

yc lockbox secret create \
  --name airflow-connections-rzd-postgres \
  --description "Airflow Connection rzd_postgres via Lockbox" \
  --payload '[
    {"key": "conn_type", "text_value": "postgres"},
    {"key": "host",      "text_value": "<FQDN>.mdb.yandexcloud.net"},
    {"key": "port",      "text_value": "5432"},
    {"key": "login",     "text_value": "airflow_svc"},
    {"key": "password",  "text_value": "<пароль>"},
    {"key": "schema",    "text_value": "rzd_analytics"}
  ]'
```

После сохранения настроек Managed Airflow перезапустит планировщик и веб-сервер автоматически.

---

### Шаг 4. Удалить Connection `yandex_s3` из Airflow UI и убедиться, что DAG читает S3 через Lockbox

1. В Airflow UI откройте Admin → Connections.
2. Найдите `yandex_s3` и удалите запись.
3. Задеплойте DAG из Шага 5 (или используйте уже задеплоенный).
4. Запустите DAG вручную (Trigger DAG).
5. Откройте лог задачи `load_sensor_data` — убедитесь, что файл прочитан успешно.

Если задача завершилась успешно после удаления Connection из UI — Lockbox работает корректно.

---

### Шаг 5. Задеплоить DAG-файл в Object Storage

```bash
# Загрузить через YC CLI
yc storage cp lockbox_lab_dag.py s3://rzd-airflow-dags/dags/lockbox_lab_dag.py
```

Или через Yandex Cloud Console: Object Storage → rzd-airflow-dags → dags/ → Загрузить объект.

Через 30–60 секунд DAG `lockbox_lab` появится в Airflow UI.

---

### Шаг 6. Запустить DAG и проверить выполнение

1. В Airflow UI найдите DAG `lockbox_lab`.
2. Нажмите Trigger DAG (кнопка ▶).
3. Перейдите в Graph View — отследите выполнение задач.
4. Откройте логи задачи `verify_lockbox` — проверьте строки вида:
   ```
   Connection 'yandex_s3': conn_type=aws  [источник: Lockbox]
   Connection 'rzd_postgres': conn_type=postgres  [источник: Lockbox]
   ```
5. Откройте логи `load_sensor_data` — убедитесь, что загрузка завершилась без ошибок.
6. Проверьте, что результирующий файл появился в `rzd-airflow-results/`:
   ```bash
   yc storage ls s3://rzd-airflow-results/results/
   ```

---

### Шаг 7. Проверить аудит-лог: кто и когда запрашивал секрет

```bash
# Просмотр событий чтения payload секрета yandex_s3
yc audit-trails event list \
  --filter "event_type=yandex.cloud.audit.lockbox.GetPayload AND \
            details.secret_name=airflow-connections-yandex-s3" \
  --limit 20

# Просмотр событий для PostgreSQL-секрета
yc audit-trails event list \
  --filter "event_type=yandex.cloud.audit.lockbox.GetPayload AND \
            details.secret_name=airflow-connections-rzd-postgres" \
  --limit 20
```

В каждом событии зафиксированы:

- `subject.id` — ID сервисного аккаунта, который запрашивал секрет.
- `event_time` — время запроса.
- `details.secret_name` — имя секрета.

Убедитесь, что субъект совпадает с сервисным аккаунтом Managed Airflow,
а время событий соответствует моменту выполнения задач DAG.

---

### Шаг 8. Настроить ротацию секрета S3

```bash
# Добавить новую версию секрета (старая остаётся доступной до деактивации)
yc lockbox secret add-version \
  --name airflow-connections-yandex-s3 \
  --payload '[
    {"key": "conn_type", "text_value": "aws"},
    {"key": "login",     "text_value": "<новый Access Key ID>"},
    {"key": "password",  "text_value": "<новый Secret Access Key>"},
    {"key": "extra",     "text_value": "{\"endpoint_url\": \"https://storage.yandexcloud.net\", \"region_name\": \"ru-central1\"}"}
  ]'

# Посмотреть список версий
yc lockbox secret list-versions --name airflow-connections-yandex-s3

# Деактивировать старую версию
OLD_VERSION_ID="<id-старой-версии>"
yc lockbox secret deactivate-version \
  --name airflow-connections-yandex-s3 \
  --version-id "$OLD_VERSION_ID"
```

После ротации запустите DAG ещё раз и убедитесь, что задача `load_sensor_data`
завершается успешно — Managed Airflow автоматически использует текущую активную версию.

---

## Полный код DAG

Файл: `lockbox_lab_dag.py` — деплоить в `rzd-airflow-dags/dags/`.

```python
"""
lockbox_lab_dag.py
Лабораторная работа №12: Lockbox для S3 и PostgreSQL credentials.

Платформа: Yandex Managed Service for Apache Airflow™.
Все файловые операции — через S3Hook (aws_conn_id='yandex_s3').
Connection 'yandex_s3' и 'rzd_postgres' разрешаются из Yandex Lockbox.
Прямой доступ к локальной файловой системе не используется.

Деплой:
  yc storage cp lockbox_lab_dag.py s3://rzd-airflow-dags/dags/
"""

from __future__ import annotations

from datetime import datetime, timedelta
from io import StringIO
from typing import Any

import logging
import pandas as pd

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

# ─── настройки ───────────────────────────────────────────────────────────────
S3_CONN_ID  = "yandex_s3"       # берётся из Lockbox
PG_CONN_ID  = "rzd_postgres"    # берётся из Lockbox

BUCKET_DATA    = "rzd-airflow-data"
BUCKET_RESULTS = "rzd-airflow-results"

KEY_SENSORS      = "sensor_readings.csv"
KEY_LOCOMOTIVES  = "locomotives.csv"
KEY_TRIPS        = "trips.csv"
KEY_MAINTENANCE  = "maintenance.csv"

DEFAULT_ARGS: dict[str, Any] = {
    "owner":            "ivanov_ae",
    "depends_on_past":  False,
    "email":            ["ivanov@tceh15.rzd-sib.ru"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
}

# ─── вспомогательные функции S3 ──────────────────────────────────────────────

def read_csv_from_s3(
    bucket: str,
    key: str,
    conn_id: str = S3_CONN_ID,
) -> pd.DataFrame:
    """
    Прочитать CSV из Object Storage через S3Hook.
    Connection разрешается через Lockbox Secrets Backend.
    """
    hook = S3Hook(aws_conn_id=conn_id)
    obj  = hook.get_key(key=key, bucket_name=bucket)
    content = obj.get()["Body"].read().decode("utf-8")
    df = pd.read_csv(StringIO(content))
    log.info("Прочитан файл s3://%s/%s: %d строк", bucket, key, len(df))
    return df


def write_csv_to_s3(
    df: pd.DataFrame,
    bucket: str,
    key: str,
    conn_id: str = S3_CONN_ID,
) -> None:
    """
    Записать DataFrame в Object Storage как CSV через S3Hook.
    Никакой локальной файловой системы не используется.
    """
    hook = S3Hook(aws_conn_id=conn_id)
    buf  = StringIO()
    df.to_csv(buf, index=False)
    hook.load_string(
        string_data=buf.getvalue(),
        key=key,
        bucket_name=bucket,
        replace=True,
    )
    log.info("Записан файл s3://%s/%s", bucket, key)


# ─── задача 1: проверка Lockbox ───────────────────────────────────────────────

def verify_lockbox(**context) -> None:
    """
    Убедиться, что оба Connection разрешаются через Lockbox.
    Connection 'yandex_s3' удалён из Airflow UI — значит, источник Lockbox.
    """
    for conn_id in [S3_CONN_ID, PG_CONN_ID]:
        conn = BaseHook.get_connection(conn_id)
        log.info(
            "Connection '%s': conn_type=%s, host=%s  [Lockbox OK]",
            conn_id, conn.conn_type, conn.host or "—",
        )


# ─── задача 2: загрузка данных телеметрии из S3 ───────────────────────────────

def load_sensor_data(**context) -> None:
    """Загрузить показания датчиков из S3, рассчитать агрегаты."""
    df = read_csv_from_s3(BUCKET_DATA, KEY_SENSORS)

    stats: dict[str, Any] = {"total_records": len(df)}

    if "buxa_temp_max" in df.columns:
        stats["avg_buxa_temp"] = round(float(df["buxa_temp_max"].mean()), 2)
        stats["max_buxa_temp"] = round(float(df["buxa_temp_max"].max()), 2)
        over_threshold = int((df["buxa_temp_max"] > 80).sum())
        stats["critical_overheats"] = over_threshold
        log.info(
            "Буксы: avg=%.1f°C, max=%.1f°C, критических=%d",
            stats["avg_buxa_temp"],
            stats["max_buxa_temp"],
            stats["critical_overheats"],
        )

    if "speed_kmh" in df.columns:
        stats["avg_speed"] = round(float(df["speed_kmh"].mean()), 2)
        log.info("Средняя скорость: %.1f км/ч", stats["avg_speed"])

    context["ti"].xcom_push(key="sensor_stats", value=stats)


# ─── задача 3: загрузка реестра локомотивов из S3 ────────────────────────────

def load_locomotives(**context) -> None:
    """Загрузить реестр локомотивов из S3, передать через XCom."""
    df = read_csv_from_s3(BUCKET_DATA, KEY_LOCOMOTIVES)

    loco_info: dict[str, Any] = {
        "total": len(df),
        "series": df["series"].unique().tolist() if "series" in df.columns else [],
    }

    if "status" in df.columns:
        active_count = int((df["status"] == "active").sum())
        loco_info["active"] = active_count
        log.info(
            "Локомотивов всего: %d, активных: %d",
            loco_info["total"], active_count,
        )

    context["ti"].xcom_push(key="loco_info", value=loco_info)


# ─── задача 4: загрузка данных о рейсах из S3 ────────────────────────────────

def load_trips(**context) -> None:
    """Загрузить данные о рейсах из S3, рассчитать задержки."""
    df = read_csv_from_s3(BUCKET_DATA, KEY_TRIPS)

    threshold = int(Variable.get("delay_threshold_min", default_var="15"))
    trips_info: dict[str, Any] = {"total_trips": len(df)}

    if "delay_min" in df.columns:
        delayed = int((df["delay_min"] > threshold).sum())
        trips_info["delayed_trips"] = delayed
        trips_info["on_time_trips"] = len(df) - delayed
        trips_info["otd_pct"] = round(
            (len(df) - delayed) / len(df) * 100, 2
        ) if len(df) > 0 else 0.0
        log.info(
            "Рейсов: %d, задержанных (>%d мин): %d, OTD: %.1f%%",
            len(df), threshold, delayed, trips_info["otd_pct"],
        )

    context["ti"].xcom_push(key="trips_info", value=trips_info)


# ─── задача 5: сохранение агрегированного отчёта в S3 ─────────────────────────

def save_report_to_s3(**context) -> None:
    """
    Собрать агрегаты из XCom и сохранить сводный отчёт в Object Storage.
    Все файловые операции — исключительно через S3Hook.
    """
    ti           = context["ti"]
    sensor_stats = ti.xcom_pull(task_ids="load_sensor_data", key="sensor_stats") or {}
    loco_info    = ti.xcom_pull(task_ids="load_locomotives",  key="loco_info")    or {}
    trips_info   = ti.xcom_pull(task_ids="load_trips",        key="trips_info")   or {}

    report = {
        "report_date":        [context["ds"]],
        "depot_code":         [Variable.get("depot_code", default_var="TCH-15")],
        "total_sensor_recs":  [sensor_stats.get("total_records", 0)],
        "avg_buxa_temp":      [sensor_stats.get("avg_buxa_temp", None)],
        "max_buxa_temp":      [sensor_stats.get("max_buxa_temp", None)],
        "critical_overheats": [sensor_stats.get("critical_overheats", 0)],
        "avg_speed_kmh":      [sensor_stats.get("avg_speed", None)],
        "total_locos":        [loco_info.get("total", 0)],
        "active_locos":       [loco_info.get("active", 0)],
        "total_trips":        [trips_info.get("total_trips", 0)],
        "delayed_trips":      [trips_info.get("delayed_trips", 0)],
        "otd_pct":            [trips_info.get("otd_pct", None)],
        "secrets_source":     ["Yandex Lockbox"],
        "s3_conn_id":         [S3_CONN_ID],
        "pg_conn_id":         [PG_CONN_ID],
    }

    df_report  = pd.DataFrame(report)
    result_key = f"results/module12_lab_{context['ds_nodash']}.csv"
    write_csv_to_s3(df_report, BUCKET_RESULTS, result_key)


# ─── задача 6: запись агрегатов в PostgreSQL ─────────────────────────────────

def write_to_postgres(**context) -> None:
    """
    Записать ключевые метрики в таблицу rzd_analytics.schedule_adherence.
    Connection 'rzd_postgres' разрешается через Lockbox.
    """
    ti         = context["ti"]
    trips_info = ti.xcom_pull(task_ids="load_trips", key="trips_info") or {}

    otd_pct      = trips_info.get("otd_pct", 0.0)
    total_trips  = trips_info.get("total_trips", 0)
    delayed      = trips_info.get("delayed_trips", 0)
    on_time      = trips_info.get("on_time_trips", 0)

    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    insert_sql = """
        INSERT INTO rzd_analytics.schedule_adherence
            (report_date, route, total_trips, on_time_trips,
             delayed_trips, otd_pct)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """
    pg_hook.run(insert_sql, parameters=(
        context["ds"],
        Variable.get("depot_code", default_var="TCH-15"),
        total_trips,
        on_time,
        delayed,
        otd_pct,
    ))
    log.info(
        "Записано в rzd_analytics.schedule_adherence: "
        "дата=%s, рейсов=%d, OTD=%.1f%%",
        context["ds"], total_trips, otd_pct,
    )


# ─── определение DAG ─────────────────────────────────────────────────────────

with DAG(
    dag_id="lockbox_lab",
    description=(
        "Лабораторная работа №12: Lockbox + S3 + PostgreSQL для ТЧЭ-15. "
        "Credentials из Lockbox, файлы через S3Hook."
    ),
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    access_control={
        "DataEngineer": {"can_dag_read", "can_dag_edit"},
        "DataAnalyst":  {"can_dag_read"},
        "DutyOperator": {"can_dag_read", "can_dag_edit"},
    },
    tags=["security", "lockbox", "s3", "postgres", "tceh15", "lab"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Ждём появления файла с датчиками в Object Storage
    wait_for_sensors = S3KeySensor(
        task_id="wait_for_sensors",
        bucket_name=BUCKET_DATA,
        bucket_key=KEY_SENSORS,
        aws_conn_id=S3_CONN_ID,
        poke_interval=300,
        timeout=7200,
        mode="reschedule",
    )

    verify = PythonOperator(
        task_id="verify_lockbox",
        python_callable=verify_lockbox,
    )

    load_sensor_task = PythonOperator(
        task_id="load_sensor_data",
        python_callable=load_sensor_data,
    )

    load_loco_task = PythonOperator(
        task_id="load_locomotives",
        python_callable=load_locomotives,
    )

    load_trips_task = PythonOperator(
        task_id="load_trips",
        python_callable=load_trips,
    )

    save_report = PythonOperator(
        task_id="save_report_to_s3",
        python_callable=save_report_to_s3,
    )

    write_pg = PythonOperator(
        task_id="write_to_postgres",
        python_callable=write_to_postgres,
    )

    end = EmptyOperator(task_id="end")

    (
        start
        >> wait_for_sensors
        >> verify
        >> [load_sensor_task, load_loco_task, load_trips_task]
        >> save_report
        >> write_pg
        >> end
    )
```

---

## Деплой: загрузка .py в rzd-airflow-dags/ и проверка в UI

### Загрузка через YC CLI

```bash
yc storage cp lockbox_lab_dag.py s3://rzd-airflow-dags/dags/lockbox_lab_dag.py
```

### Загрузка через Yandex Cloud Console

1. Откройте Object Storage → rzd-airflow-dags.
2. Перейдите в папку `dags/`.
3. Нажмите «Загрузить объект» и выберите файл `lockbox_lab_dag.py`.

### Проверка появления DAG в UI

1. В Airflow UI откройте вкладку DAGs.
2. Через 30–60 секунд появится DAG `lockbox_lab`.
3. Если DAG не появился — перейдите в Browse → Import Errors и проверьте ошибки синтаксиса.

### Проверка правильности пути к бакету в настройках сервиса

```bash
# Убедиться, что Managed Airflow указывает на правильный бакет
yc managed-airflow cluster get <cluster-name> \
  --format json | jq '.code_sync.s3.bucket'
```

Ожидаемый вывод: `"rzd-airflow-dags"`.

---

## Ожидаемый результат

| Проверка | Результат |
|---|---|
| Connection `yandex_s3` удалён из Airflow UI | DAG всё равно успешно читает S3 |
| Логи задачи `verify_lockbox` | Строки `[Lockbox OK]` для обоих connections |
| Задача `load_sensor_data` | Завершилась успешно, статистика в XCom |
| Задача `save_report_to_s3` | Файл `results/module12_lab_<date>.csv` в `rzd-airflow-results/` |
| Задача `write_to_postgres` | Запись в `rzd_analytics.schedule_adherence` |
| Аудит-лог Lockbox | События `GetPayload` с ID сервисного аккаунта Managed Airflow |

---

## Задания повышенной сложности

### Задание 1. Ротация S3-ключей без остановки DAG

Реализуйте DAG `rotate_s3_credentials`, который:

1. Генерирует новый статический ключ для сервисного аккаунта через YC CLI (`yc iam access-key create`).
2. Добавляет новую версию секрета `airflow-connections-yandex-s3` в Lockbox через `yc lockbox secret add-version`.
3. Запускает DAG `lockbox_lab` (через `TriggerDagRunOperator`) и убеждается, что он завершился успешно.
4. Только после успешного завершения деактивирует старую версию секрета в Lockbox.

Все шаги реализуются как задачи DAG с `PythonOperator` + `subprocess.run(["yc", ...])`.
Никакого обращения к файловой системе — результаты между шагами передаются через XCom.

### Задание 2. Кастомный SecretsBackend с кэшированием

Стандартный `LockboxSecretBackend` делает HTTP-запрос к Lockbox при каждом обращении к
Connection. При высокой частоте запусков это создаёт лишнюю нагрузку.

Напишите класс `CachedLockboxBackend(LockboxSecretBackend)`, который:

1. Кэширует значения Connection в памяти на 5 минут (используйте `functools.lru_cache` или
   словарь с TTL).
2. При обращении к устаревшей записи обновляет кэш из Lockbox.
3. Поддерживает принудительный сброс кэша через Airflow Variable `lockbox_cache_flush = true`.
4. Включает метрику: логирует `cache_hit` / `cache_miss` для каждого обращения.

Задеплойте класс как Python-файл в `rzd-airflow-dags/plugins/` и зарегистрируйте через
`AIRFLOW__SECRETS__BACKEND`.

### Задание 3. Автоматическая проверка целостности секретов

Напишите DAG `lockbox_secrets_audit`, который запускается ежедневно в 06:00 и:

1. Читает список ожидаемых секретов из Airflow Variable `required_secrets`
   (JSON-список: `["airflow-connections-yandex-s3", "airflow-connections-rzd-postgres"]`).
2. Для каждого секрета вызывает `yc lockbox secret get` и проверяет, что:
   - секрет существует и активен,
   - дата последнего изменения не старше 90 дней (требование ротации),
   - набор ключей в payload соответствует ожидаемому шаблону.
3. Формирует отчёт в формате CSV и сохраняет в `rzd-airflow-results/audit/lockbox_<date>.csv`
   через `write_csv_to_s3`.
4. Если хотя бы один секрет просрочен — отправляет email-уведомление через
   `EmailOperator` (адрес берётся из Variable `security_email`).

---

## Проверочные вопросы

1. Что произойдёт, если сервисный аккаунт Managed Airflow не имеет роли `lockbox.payloadViewer`?
   На каком этапе запуска DAG возникнет ошибка?

2. Почему нельзя использовать `pd.read_csv("s3://rzd-airflow-data/sensor_readings.csv")`
   напрямую в Managed Airflow? Какие библиотеки для этого потребовались бы и чем `S3Hook`
   отличается от прямого обращения к S3?

3. В аудит-логе Lockbox вы видите обращения с незнакомого `subject.id`. Опишите алгоритм
   расследования: какие инструменты Yandex Cloud использовать, чтобы установить источник
   несанкционированного доступа?

4. Managed Airflow перезапускает планировщик при изменении переменных окружения. Как это
   влияет на уже выполняющиеся DAG? Как минимизировать риск прерывания работающих задач
   при ротации Secrets Backend?

5. Роль `DataAnalyst` имеет доступ к просмотру Variables, но не к Connections. Объясните,
   почему это важно с точки зрения безопасности: какую информацию содержат Connections,
   которую не должен видеть аналитик?

---

## Связанные файлы

- Практическая работа: `../practice/README.md`
- Презентация модуля: `../presentation.html`
- Входные данные: `s3://rzd-airflow-data/` (CSV-файлы датасета)
- Результаты: `s3://rzd-airflow-results/results/module12_lab_<YYYYMMDD>.csv`
- Аудит: `s3://rzd-airflow-results/audit/lockbox_<YYYYMMDD>.csv`
