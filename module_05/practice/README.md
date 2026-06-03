# Практическая работа №05: S3KeySensor и зависимости между пайплайнами

**Модуль:** 05 — Зависимости задач в Airflow
**Продолжительность:** 45–60 минут
**Платформа:** Yandex Managed Service for Apache Airflow™

---

## Цель и задачи

Научиться строить пайплайны с явными зависимостями задач в среде Yandex Managed Airflow:

- Заменить `FileSensor` на `S3KeySensor` для ожидания файлов в Object Storage
- Применять паттерны fan-out (параллелизация) и fan-in (объединение)
- Настраивать `ExternalTaskSensor` для зависимости между двумя DAG
- Выполнять все операции с файлами через `S3Hook` (без обращений к локальной файловой системе)
- Деплоить DAG-файлы через бакет, связанный с Managed Airflow

---

## Необходимые ресурсы

| Ресурс | Описание |
|---|---|
| Yandex Managed Service for Apache Airflow | Кластер запущен и доступен |
| Yandex Object Storage | Бакеты `rzd-airflow-dags`, `rzd-airflow-data`, `rzd-airflow-results` |
| Yandex Managed Service for PostgreSQL | Кластер `rzd_analytics`, схема `rzd_analytics` |
| Сервисный аккаунт | Роли `storage.uploader`, `storage.viewer` |
| Yandex Cloud CLI (`yc`) | Установлен и настроен |

---

## Подготовка Object Storage

### Шаг 1. Создание бакетов через Yandex Cloud Console

1. Откройте [console.cloud.yandex.ru](https://console.cloud.yandex.ru) и перейдите в **Object Storage**.
2. Создайте три бакета (кнопка **Создать бакет**):

| Имя бакета | Назначение | Класс хранилища |
|---|---|---|
| `rzd-airflow-dags` | DAG-файлы Managed Airflow | Стандартный |
| `rzd-airflow-data` | Входные CSV-файлы | Стандартный |
| `rzd-airflow-results` | Результаты обработки | Стандартный |

Для каждого бакета установите **Доступ: Приватный**.

### Шаг 2. Создание сервисного аккаунта и ключей доступа

```bash
# Создать сервисный аккаунт
yc iam service-account create --name airflow-s3-sa

# Получить ID аккаунта
SA_ID=$(yc iam service-account get airflow-s3-sa --format json | jq -r '.id')
FOLDER_ID=$(yc config get folder-id)

# Выдать роли на бакеты
yc resource-manager folder add-access-binding $FOLDER_ID \
    --role storage.uploader \
    --subject serviceAccount:$SA_ID

yc resource-manager folder add-access-binding $FOLDER_ID \
    --role storage.viewer \
    --subject serviceAccount:$SA_ID

# Создать статический ключ доступа (Access Key ID + Secret Access Key)
yc iam access-key create --service-account-name airflow-s3-sa
```

Сохраните вывод команды — `key_id` (Access Key ID) и `secret` (Secret Access Key).
Они понадобятся при настройке Connection в Airflow.

### Шаг 3. Загрузка CSV-файлов в Object Storage

Загрузите исходные датасеты в бакет `rzd-airflow-data`:

```bash
# Загрузка файлов датасета
yc storage cp locomotives.csv        s3://rzd-airflow-data/locomotives.csv
yc storage cp sensor_readings.csv    s3://rzd-airflow-data/sensor_readings.csv
yc storage cp trips.csv              s3://rzd-airflow-data/trips.csv
yc storage cp schedule_adherence.csv s3://rzd-airflow-data/schedule_adherence.csv
yc storage cp maintenance.csv        s3://rzd-airflow-data/maintenance.csv
```

Или через Yandex Cloud Console: откройте бакет `rzd-airflow-data` → **Загрузить объекты** → выберите CSV-файлы.

### Шаг 4. Имитация файла ГЛОНАСС для тестирования S3KeySensor

Создайте тестовый файл и загрузите его в нужный путь:

```bash
# Создать заглушку файла ГЛОНАСС
echo "timestamp,loco_id,lat,lon,speed_kmh,beacon_id" > glonass_20260601.csv
echo "2026-06-01 06:00:00,VL80-001,55.0302,82.9204,0.0,TCH15-A01" >> glonass_20260601.csv

# Загрузить в бакет по пути, который ожидает S3KeySensor
yc storage cp glonass_20260601.csv \
    s3://rzd-airflow-data/glonass/20260601/glonass_20260601.csv
```

---

## Настройка Airflow Connections и Variables

Все настройки выполняются в Airflow UI: откройте кластер Managed Airflow → **Открыть Airflow UI**.

### Connection: yandex_s3

Перейдите **Admin → Connections → Add Connection**:

| Поле | Значение |
|---|---|
| Conn Id | `yandex_s3` |
| Conn Type | `Amazon Web Services` |
| Login | `<Access Key ID>` (из шага 2) |
| Password | `<Secret Access Key>` (из шага 2) |
| Extra | `{"endpoint_url": "https://storage.yandexcloud.net", "region_name": "ru-central1"}` |

Нажмите **Save** и затем **Test** — должно появиться сообщение `Connection tested successfully`.

### Connection: rzd_postgres

Перейдите **Admin → Connections → Add Connection**:

| Поле | Значение |
|---|---|
| Conn Id | `rzd_postgres` |
| Conn Type | `Postgres` |
| Host | `<FQDN кластера>.mdb.yandexcloud.net` |
| Schema | `rzd_analytics` |
| Login | `airflow_user` |
| Password | `<пароль из Yandex Lockbox>` |
| Port | `6432` |

### Variables

Перейдите **Admin → Variables → Add Variable** и создайте:

| Key | Value |
|---|---|
| `s3_bucket_data` | `rzd-airflow-data` |
| `s3_bucket_results` | `rzd-airflow-results` |
| `depot_code` | `TCH-15` |
| `delay_threshold_min` | `15` |

---

## Деплой DAG-файла в Managed Airflow

В Yandex Managed Airflow DAG-файлы **не копируются по SSH** и **не кладутся в локальную папку `dags/`**. Они загружаются в бакет Object Storage, который связан с кластером.

### Привязка бакета к кластеру (выполняется один раз)

1. Откройте кластер Managed Airflow в консоли.
2. Перейдите в **DAG-файлы**.
3. Укажите бакет `rzd-airflow-dags` и папку `dags/`.
4. Сохраните изменения.

### Загрузка DAG-файла

```bash
# Через Yandex Cloud CLI
yc storage cp practice_05_rzd_dag.py \
    s3://rzd-airflow-dags/dags/practice_05_rzd_dag.py

# Или через AWS CLI (если настроен endpoint)
aws s3 cp practice_05_rzd_dag.py \
    s3://rzd-airflow-dags/dags/practice_05_rzd_dag.py \
    --endpoint-url https://storage.yandexcloud.net
```

После загрузки DAG появится в Airflow UI в течение 1–3 минут. Проверьте в разделе **DAGs** — новый DAG должен отобразиться в списке без ошибок импорта.

---

## Шаги выполнения

### Шаг 1. Понять архитектуру пайплайна

Пайплайн состоит из двух DAG, связанных через `ExternalTaskSensor`:

```
rzd-airflow-data/glonass/{{ ds_nodash }}/
        ↓  S3KeySensor
ingestion_dag:
  wait_glonass → read_sensor_readings ─┐
                 read_trips            ─┤ fan-out / fan-in
                                       ↓
                              merge_and_load_postgres
                                       ↓
                              write_summary_to_s3

        ↓  ExternalTaskSensor
kpi_dag:
  wait_ingestion_complete → calc_kpi → write_kpi_report_s3
```

### Шаг 2. Убедиться, что Connection yandex_s3 работает

В Airflow UI: **Admin → Connections → yandex_s3 → Test**.
Ожидаемый результат: `Connection tested successfully`.

### Шаг 3. Загрузить тестовый файл ГЛОНАСС в S3

Выполните команду из раздела "Подготовка Object Storage" → Шаг 4.
Файл должен находиться по пути `glonass/{{ ds_nodash }}/glonass_{{ ds_nodash }}.csv`
в бакете `rzd-airflow-data`.

### Шаг 4. Задеплоить DAG-файл

Скопируйте код из раздела "Полный код DAG" в файл `practice_05_rzd_dag.py` и загрузите:

```bash
yc storage cp practice_05_rzd_dag.py \
    s3://rzd-airflow-dags/dags/practice_05_rzd_dag.py
```

Дождитесь появления DAG в Airflow UI (1–3 мин).

### Шаг 5. Запустить DAG ingestion вручную

В Airflow UI найдите DAG `practice_05_ingestion_dag` → кнопка **Trigger DAG**.
В параметрах укажите дату логического запуска, соответствующую дате файла ГЛОНАСС.

Наблюдайте в **Graph View**:
- `wait_for_glonass_file` — статус `running` (режим `reschedule`: периодически `up_for_reschedule`)
- После появления файла в S3 — переход в `success`
- `read_sensor_readings` и `read_trips` запускаются **параллельно** (fan-out)
- `merge_and_load_postgres` запускается только после обеих параллельных задач (fan-in)

### Шаг 6. Запустить DAG KPI и проследить ожидание

Запустите DAG `practice_05_kpi_dag` в Airflow UI.
Задача `wait_ingestion_complete` должна ждать в режиме `reschedule`
пока `merge_and_load_postgres` в `practice_05_ingestion_dag` не завершится успешно.

### Шаг 7. Проверить результаты в Object Storage

После успешного выполнения обоих DAG проверьте наличие файлов в бакете результатов:

```bash
yc storage ls s3://rzd-airflow-results/
# Ожидается:
# merged/20260601/merged_data.csv
# kpi/20260601/kpi_report.csv
```

---

## Полный код DAG

Сохраните как `practice_05_rzd_dag.py` и загрузите в `rzd-airflow-dags/dags/`:

```python
"""
Практическая работа №05 — Модуль 05.
Депо ТЧЭ-15 Новосибирск: S3KeySensor, fan-out/fan-in, ExternalTaskSensor.

Среда: Yandex Managed Service for Apache Airflow™
Все операции с файлами — через S3Hook (Yandex Object Storage).
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
from airflow.sensors.external_task import ExternalTaskSensor

log = logging.getLogger(__name__)

# ── Вспомогательные функции для работы с S3 ─────────────────────────

def read_csv_from_s3(bucket: str, key: str, conn_id: str = "yandex_s3") -> pd.DataFrame:
    """Читает CSV-файл из Yandex Object Storage через S3Hook."""
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
    """Записывает DataFrame в CSV и сохраняет в Yandex Object Storage."""
    hook = S3Hook(aws_conn_id=conn_id)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=key,
        bucket_name=bucket,
        replace=True,
    )
    log.info("Записан файл s3://%s/%s (%d строк)", bucket, key, len(df))


# ── Задачи DAG 1: ingestion ──────────────────────────────────────────

def read_sensor_readings(**ctx) -> dict:
    """
    Читает sensor_readings.csv из S3.
    FAN-OUT ветка 1: выполняется параллельно с read_trips.
    """
    bucket = Variable.get("s3_bucket_data")
    key = "sensor_readings.csv"

    df = read_csv_from_s3(bucket=bucket, key=key)
    log.info("[sensor] Прочитано %d строк из s3://%s/%s", len(df), bucket, key)

    # Базовая статистика по температуре двигателя
    stats = {
        "rows": len(df),
        "locos": df["loco_id"].nunique() if "loco_id" in df.columns else 0,
        "avg_temp": round(df["temp_engine_c"].mean(), 2)
        if "temp_engine_c" in df.columns
        else None,
    }
    log.info("[sensor] Статистика: %s", stats)
    return stats


def read_trips(**ctx) -> dict:
    """
    Читает trips.csv из S3.
    FAN-OUT ветка 2: выполняется параллельно с read_sensor_readings.
    """
    bucket = Variable.get("s3_bucket_data")
    key = "trips.csv"

    df = read_csv_from_s3(bucket=bucket, key=key)
    log.info("[trips] Прочитано %d строк из s3://%s/%s", len(df), bucket, key)

    stats = {
        "rows": len(df),
        "locos": df["loco_id"].nunique() if "loco_id" in df.columns else 0,
        "total_distance_km": round(df["distance_km"].sum(), 2)
        if "distance_km" in df.columns
        else None,
    }
    log.info("[trips] Статистика: %s", stats)
    return stats


def merge_and_load_postgres(**ctx) -> dict:
    """
    FAN-IN: объединяет результаты обеих параллельных веток,
    записывает сводный CSV в rzd-airflow-results и имитирует
    загрузку в PostgreSQL (rzd_analytics).
    """
    ti = ctx["ti"]
    ds_nodash = ctx["ds_nodash"]

    sensor_stats = ti.xcom_pull(task_ids="read_sensor_readings")
    trips_stats = ti.xcom_pull(task_ids="read_trips")

    log.info("[merge] Sensor stats: %s", sensor_stats)
    log.info("[merge] Trips stats:  %s", trips_stats)

    # Формируем сводную строку результата
    summary = {
        "run_date": [ctx["ds"]],
        "depot_code": [Variable.get("depot_code", default_var="TCH-15")],
        "sensor_rows": [sensor_stats.get("rows", 0) if sensor_stats else 0],
        "trips_rows": [trips_stats.get("rows", 0) if trips_stats else 0],
        "sensor_locos": [sensor_stats.get("locos", 0) if sensor_stats else 0],
        "trips_locos": [trips_stats.get("locos", 0) if trips_stats else 0],
        "avg_temp_c": [sensor_stats.get("avg_temp") if sensor_stats else None],
        "total_distance_km": [
            trips_stats.get("total_distance_km") if trips_stats else None
        ],
    }
    df_summary = pd.DataFrame(summary)

    # Запись в Object Storage
    bucket_results = Variable.get("s3_bucket_results")
    key_out = f"merged/{ds_nodash}/merged_data.csv"
    write_csv_to_s3(df=df_summary, bucket=bucket_results, key=key_out)

    # В production здесь выполняется загрузка в PostgreSQL через PostgresHook:
    # hook_pg = PostgresHook(postgres_conn_id="rzd_postgres")
    # hook_pg.run("INSERT INTO rzd_analytics.ingestion_log ...")
    log.info("[merge] Загрузка в PostgreSQL rzd_analytics (имитация)")

    return {
        "merged_rows": len(df_summary),
        "s3_key": key_out,
        "bucket": bucket_results,
    }


# ── Задачи DAG 2: KPI ────────────────────────────────────────────────

def calc_kpi(**ctx) -> dict:
    """
    Читает сводный файл из S3 (результат ingestion DAG)
    и рассчитывает базовые KPI по депо ТЧЭ-15.
    """
    ds_nodash = ctx["ds_nodash"]
    bucket_results = Variable.get("s3_bucket_results")
    key_in = f"merged/{ds_nodash}/merged_data.csv"

    df = read_csv_from_s3(bucket=bucket_results, key=key_in)
    log.info("[kpi] Прочитан сводный файл: %d строк", len(df))

    delay_threshold = int(
        Variable.get("delay_threshold_min", default_var="15")
    )

    kpi = {
        "run_date": ctx["ds"],
        "depot_code": Variable.get("depot_code", default_var="TCH-15"),
        "sensor_rows": int(df["sensor_rows"].sum()),
        "trips_rows": int(df["trips_rows"].sum()),
        "avg_temp_c": float(df["avg_temp_c"].mean())
        if "avg_temp_c" in df.columns
        else None,
        "total_distance_km": float(df["total_distance_km"].sum())
        if "total_distance_km" in df.columns
        else None,
        "delay_threshold_min": delay_threshold,
    }

    log.info("[kpi] Рассчитаны KPI: %s", kpi)
    return kpi


def write_kpi_report_s3(**ctx) -> None:
    """
    Записывает отчёт KPI в CSV в rzd-airflow-results.
    """
    ti = ctx["ti"]
    ds_nodash = ctx["ds_nodash"]
    kpi = ti.xcom_pull(task_ids="calc_kpi")

    df_kpi = pd.DataFrame([kpi])
    bucket_results = Variable.get("s3_bucket_results")
    key_out = f"kpi/{ds_nodash}/kpi_report.csv"

    write_csv_to_s3(df=df_kpi, bucket=bucket_results, key=key_out)
    log.info("[kpi] Отчёт записан: s3://%s/%s", bucket_results, key_out)


# ── DAG 1: ingestion ─────────────────────────────────────────────────

default_args_ingestion = {
    "owner": "rzd_data_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="practice_05_ingestion_dag",
    description="Практика 05: S3KeySensor → fan-out → fan-in → PostgreSQL + S3",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=default_args_ingestion,
    tags=["practice", "module_05", "ingestion", "rzd"],
) as ingestion_dag:

    # S3KeySensor: ждём файл телеметрии ГЛОНАСС в Object Storage
    # mode="reschedule" — воркер не занят между проверками
    wait_for_glonass_file = S3KeySensor(
        task_id="wait_for_glonass_file",
        bucket_name="rzd-airflow-data",
        bucket_key="glonass/{{ ds_nodash }}/glonass_{{ ds_nodash }}.csv",
        aws_conn_id="yandex_s3",
        poke_interval=300,     # проверка каждые 5 минут
        timeout=7200,          # таймаут 2 часа
        mode="reschedule",     # не держит воркер
        soft_fail=False,
    )

    # FAN-OUT: два источника читаются параллельно
    task_read_sensors = PythonOperator(
        task_id="read_sensor_readings",
        python_callable=read_sensor_readings,
    )

    task_read_trips = PythonOperator(
        task_id="read_trips",
        python_callable=read_trips,
    )

    # FAN-IN: объединение и загрузка
    task_merge = PythonOperator(
        task_id="merge_and_load_postgres",
        python_callable=merge_and_load_postgres,
    )

    # Граф: сенсор → fan-out → fan-in
    wait_for_glonass_file >> [task_read_sensors, task_read_trips]
    [task_read_sensors, task_read_trips] >> task_merge


# ── DAG 2: KPI ───────────────────────────────────────────────────────

default_args_kpi = {
    "owner": "rzd_analytics_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="practice_05_kpi_dag",
    description="Практика 05: ExternalTaskSensor → расчёт KPI → отчёт в S3",
    schedule_interval="30 6 * * *",   # на 30 минут позже ingestion DAG
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=default_args_kpi,
    tags=["practice", "module_05", "kpi", "rzd"],
) as kpi_dag:

    # ExternalTaskSensor: ждём завершения merge_and_load_postgres в ingestion DAG
    # ingestion стартует в 06:00, kpi в 06:30 → execution_delta = 30 мин
    wait_ingestion_complete = ExternalTaskSensor(
        task_id="wait_ingestion_complete",
        external_dag_id="practice_05_ingestion_dag",
        external_task_id="merge_and_load_postgres",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        execution_delta=timedelta(minutes=30),
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    task_calc_kpi = PythonOperator(
        task_id="calc_kpi",
        python_callable=calc_kpi,
    )

    task_write_kpi = PythonOperator(
        task_id="write_kpi_report_s3",
        python_callable=write_kpi_report_s3,
    )

    # Граф DAG 2
    wait_ingestion_complete >> task_calc_kpi >> task_write_kpi
```

---

## Контрольные вопросы

1. Почему в Yandex Managed Airflow нельзя использовать `FileSensor`, и чем его заменяет `S3KeySensor`?

2. Что произойдёт, если файл `glonass/{{ ds_nodash }}/glonass_{{ ds_nodash }}.csv` уже существует в бакете в момент старта S3KeySensor? Изменится ли поведение сенсора?

3. Объясните, зачем в `ExternalTaskSensor` DAG KPI указан `execution_delta=timedelta(minutes=30)`. Что случится, если убрать этот параметр?

4. В функции `merge_and_load_postgres` используется `hook.load_string()`. Можно ли вместо него использовать `pd.read_csv("/tmp/file.csv")` + `open()` в Managed Airflow? Почему?

5. Как изменить `S3KeySensor`, чтобы он ждал появления **любого** файла в папке `glonass/{{ ds_nodash }}/` (wildcard), а не конкретного имени файла?
