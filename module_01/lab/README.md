# Лабораторная работа №01: Первый DAG через Object Storage: чтение sensor_readings

**Модуль:** 01 — Введение в Apache Airflow
**Организация:** Западно-Сибирская дирекция тяги, ТЧЭ-15 (депо Новосибирск-Главный)
**Расчётное время:** 60 минут
**Платформа:** Yandex Managed Service for Apache Airflow™
**Хранилище данных:** Yandex Object Storage (S3-совместимый)

---

## Цель

Разработать DAG `locomotive_telemetry_dag`, который:
- Читает файл `sensor_readings.csv` из Yandex Object Storage через `S3Hook`
- Валидирует данные (температура букс, флаг качества `quality_flag`)
- Загружает валидные записи в таблицу `rzd_analytics.sensor_readings` (Managed PostgreSQL)
- Записывает отчёт валидации обратно в Object Storage
- Деплоится через бакет `rzd-airflow-dags` (без доступа к локальной файловой системе)

---

## Предварительные условия

- Managed Airflow запущен и доступен (выполнена Практическая работа №01)
- Бакеты `rzd-airflow-data`, `rzd-airflow-results`, `rzd-airflow-dags` созданы
- Файл `sensor_readings.csv` загружен в бакет `rzd-airflow-data`
- Connection `yandex_s3` настроен в Airflow UI (тип Amazon S3, endpoint Yandex Object Storage)
- Connection `rzd_postgres` настроен в Airflow UI (Managed PostgreSQL FQDN, порт 6432)
- Переменные `s3_bucket_data` и `s3_bucket_results` заданы через Admin → Variables

---

## Задание

Разработайте и задеплойте DAG `locomotive_telemetry_dag`, выполняющий следующие шаги:

1. **Ожидание файла** — `S3KeySensor` ожидает появления `sensor_readings.csv` в бакете `rzd-airflow-data`. Режим `reschedule`, таймаут 2 часа.

2. **Чтение данных** — читает `sensor_readings.csv` из Object Storage через `S3Hook`. Использует функцию `read_csv_from_s3(bucket, key)`. Передаёт DataFrame через XCom (в виде JSON).

3. **Валидация данных** — проверяет:
   - Наличие обязательных столбцов (`loco_id`, `timestamp`, `temperature`, `quality_flag`)
   - Значение `quality_flag` (допустимые: `ok`, `warning`, `alarm`)
   - Диапазон температуры буксы: 0–150°C
   - Отклоняет записи с `quality_flag = 'error'` или значением за пределами диапазона

4. **Загрузка в PostgreSQL** — вставляет валидные строки в таблицу `rzd_analytics.sensor_readings` через `PostgresHook`. Использует `ON CONFLICT DO NOTHING` для идемпотентности.

5. **Формирование отчёта валидации** — создаёт DataFrame с итогами (`total_read`, `valid`, `rejected`, `run_date`) и записывает его в Object Storage через `S3Hook.load_string()`.

6. **Итоговое сообщение** — логирует сводку в Airflow UI (количество записей, количество аномалий с температурой > 80°C).

7. **Деплой** — файл `locomotive_telemetry_dag.py` загружается в бакет `rzd-airflow-dags/dags/` через CLI или консоль. Проверить появление DAG в UI через 1–3 минуты.

---

## Полный код DAG

Сохраните код в файл `locomotive_telemetry_dag.py` и загрузите в бакет `rzd-airflow-dags/dags/`.

```python
"""
DAG: locomotive_telemetry_dag
Назначение: Чтение, валидация и загрузка телеметрии локомотивов ТЧЭ-15.
Источник:   Yandex Object Storage, бакет rzd-airflow-data, ключ sensor_readings.csv
Приёмник:   Managed PostgreSQL, таблица rzd_analytics.sensor_readings
Отчёт:      Object Storage, бакет rzd-airflow-results
Расписание: каждые 15 минут
Платформа:  Yandex Managed Service for Apache Airflow™

ВАЖНО: Все операции с файлами — ТОЛЬКО через S3Hook.
       Прямой доступ к файловой системе недоступен в Managed Airflow.
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

logger = logging.getLogger(__name__)

# --- Константы ---
S3_CONN_ID = "yandex_s3"
PG_CONN_ID = "rzd_postgres"
SOURCE_KEY = "sensor_readings.csv"
BUXA_TEMP_MIN = 0.0
BUXA_TEMP_MAX = 150.0
BUXA_TEMP_ALARM = 80.0
VALID_QUALITY_FLAGS = {"ok", "warning", "alarm"}

# --- Вспомогательные функции ---

def read_csv_from_s3(bucket: str, key: str, conn_id: str = S3_CONN_ID) -> pd.DataFrame:
    """
    Читает CSV-файл из Yandex Object Storage через S3Hook.
    Не использует локальную файловую систему.
    """
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
    """
    Записывает DataFrame как CSV в Yandex Object Storage через S3Hook.
    Использует hook.load_string() — без записи на диск.
    """
    hook = S3Hook(aws_conn_id=conn_id)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=key,
        bucket_name=bucket,
        replace=True,
    )
    logger.info("Записано в Object Storage: s3://%s/%s", bucket, key)


# ------------------------------------------------------------------ #
#  ЗАДАЧА 1: S3KeySensor — ожидание файла (объявляется в теле DAG)    #
# ------------------------------------------------------------------ #
# Объявляется ниже как оператор внутри блока with DAG


# ------------------------------------------------------------------ #
#  ЗАДАЧА 2: Чтение sensor_readings.csv из Object Storage             #
# ------------------------------------------------------------------ #
def read_sensor_data(**context) -> int:
    """
    Читает sensor_readings.csv из Object Storage через S3Hook.
    Передаёт данные через XCom в формате JSON.
    Возвращает количество прочитанных строк.
    """
    bucket = Variable.get("s3_bucket_data", default_var="rzd-airflow-data")
    df = read_csv_from_s3(bucket=bucket, key=SOURCE_KEY)

    record_count = len(df)
    logger.info("Прочитано записей из sensor_readings.csv: %d", record_count)

    loco_ids = df["loco_id"].unique().tolist() if "loco_id" in df.columns else []
    logger.info("Локомотивы в выборке: %s", ", ".join(str(x) for x in loco_ids))

    # Передаём через XCom как JSON-строку
    context["ti"].xcom_push(key="raw_data_json", value=df.to_json(orient="records"))
    context["ti"].xcom_push(key="record_count", value=record_count)
    return record_count


# ------------------------------------------------------------------ #
#  ЗАДАЧА 3: Валидация данных                                         #
# ------------------------------------------------------------------ #
def validate_sensor_data(**context) -> int:
    """
    Валидирует показания датчиков:
    - Обязательные столбцы: loco_id, timestamp, temperature, quality_flag
    - quality_flag должен быть одним из: ok, warning, alarm
    - temperature в диапазоне 0–150°C
    Возвращает количество валидных записей.
    """
    raw_json = context["ti"].xcom_pull(
        task_ids="read_sensor_data", key="raw_data_json"
    )
    df = pd.read_json(StringIO(raw_json), orient="records")

    total = len(df)
    required_columns = {"loco_id", "timestamp", "temperature", "quality_flag"}
    missing_cols = required_columns - set(df.columns)
    if missing_cols:
        raise ValueError(
            f"В sensor_readings.csv отсутствуют обязательные столбцы: {missing_cols}"
        )

    # Фильтрация по quality_flag
    invalid_flag_mask = ~df["quality_flag"].isin(VALID_QUALITY_FLAGS)
    rejected_flag = int(invalid_flag_mask.sum())
    if rejected_flag > 0:
        logger.warning(
            "Отклонено записей с недопустимым quality_flag: %d", rejected_flag
        )

    # Фильтрация по диапазону температуры
    df["temperature"] = pd.to_numeric(df["temperature"], errors="coerce")
    invalid_temp_mask = (
        df["temperature"].isna()
        | (df["temperature"] < BUXA_TEMP_MIN)
        | (df["temperature"] > BUXA_TEMP_MAX)
    )
    rejected_temp = int(invalid_temp_mask.sum())
    if rejected_temp > 0:
        logger.warning(
            "Отклонено записей с некорректной температурой: %d", rejected_temp
        )

    combined_invalid = invalid_flag_mask | invalid_temp_mask
    df_valid = df[~combined_invalid].copy()
    rejected_total = total - len(df_valid)

    logger.info(
        "Валидация завершена: всего=%d, валидных=%d, отклонённых=%d",
        total, len(df_valid), rejected_total,
    )

    if df_valid.empty:
        raise ValueError("После валидации не осталось корректных записей!")

    context["ti"].xcom_push(
        key="valid_data_json", value=df_valid.to_json(orient="records")
    )
    context["ti"].xcom_push(key="valid_count", value=len(df_valid))
    context["ti"].xcom_push(key="rejected_count", value=rejected_total)
    return len(df_valid)


# ------------------------------------------------------------------ #
#  ЗАДАЧА 4: Загрузка в Managed PostgreSQL                            #
# ------------------------------------------------------------------ #
def load_to_postgres(**context) -> int:
    """
    Загружает валидированные показания датчиков в таблицу
    rzd_analytics.sensor_readings через PostgresHook.
    Использует ON CONFLICT DO NOTHING для идемпотентности.
    """
    valid_json = context["ti"].xcom_pull(
        task_ids="validate_sensor_data", key="valid_data_json"
    )
    df = pd.read_json(StringIO(valid_json), orient="records")
    run_id = context["run_id"]

    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

    # Создать таблицу, если не существует
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS rzd_analytics.sensor_readings (
            id              SERIAL PRIMARY KEY,
            loco_id         VARCHAR(30)  NOT NULL,
            recorded_at     TIMESTAMP    NOT NULL,
            temperature     NUMERIC(6,2),
            quality_flag    VARCHAR(20),
            speed_kmh       NUMERIC(6,1),
            fuel_lh         NUMERIC(7,2),
            latitude        NUMERIC(9,6),
            longitude       NUMERIC(9,6),
            dag_run_id      VARCHAR(100),
            loaded_at       TIMESTAMP DEFAULT NOW(),
            UNIQUE (loco_id, recorded_at)
        );
        CREATE SCHEMA IF NOT EXISTS rzd_analytics;
    """
    hook.run(create_table_sql)

    insert_sql = """
        INSERT INTO rzd_analytics.sensor_readings (
            loco_id, recorded_at, temperature, quality_flag,
            speed_kmh, fuel_lh, latitude, longitude, dag_run_id
        ) VALUES (
            %(loco_id)s,
            %(timestamp)s::TIMESTAMP,
            %(temperature)s,
            %(quality_flag)s,
            NULLIF(%(speed_kmh)s, 'nan')::NUMERIC,
            NULLIF(%(fuel_lh)s, 'nan')::NUMERIC,
            NULLIF(%(latitude)s, 'nan')::NUMERIC,
            NULLIF(%(longitude)s, 'nan')::NUMERIC,
            %(dag_run_id)s
        )
        ON CONFLICT (loco_id, recorded_at) DO NOTHING
    """

    loaded = 0
    for _, row in df.iterrows():
        params = row.to_dict()
        params["dag_run_id"] = run_id
        # Привести NaN к строке для NULLIF
        for field in ("speed_kmh", "fuel_lh", "latitude", "longitude"):
            if field in params and pd.isna(params[field]):
                params[field] = "nan"
            elif field in params:
                params[field] = str(params[field])
        try:
            hook.run(insert_sql, parameters=params)
            loaded += 1
        except Exception as exc:
            logger.error(
                "Ошибка загрузки записи loco_id=%s ts=%s: %s",
                params.get("loco_id"), params.get("timestamp"), exc,
            )

    logger.info("Загружено в PostgreSQL: %d / %d записей", loaded, len(df))
    context["ti"].xcom_push(key="loaded_count", value=loaded)
    return loaded


# ------------------------------------------------------------------ #
#  ЗАДАЧА 5: Запись отчёта валидации в Object Storage                 #
# ------------------------------------------------------------------ #
def write_validation_report(**context) -> str:
    """
    Формирует DataFrame с итогами валидации и записывает его
    в Object Storage через S3Hook.load_string().
    Ключ содержит дату запуска (ds_nodash) для партиционирования.
    """
    ti = context["ti"]
    total_read = ti.xcom_pull(task_ids="read_sensor_data",   key="record_count") or 0
    valid      = ti.xcom_pull(task_ids="validate_sensor_data", key="valid_count") or 0
    rejected   = ti.xcom_pull(task_ids="validate_sensor_data", key="rejected_count") or 0
    loaded     = ti.xcom_pull(task_ids="load_to_postgres",    key="loaded_count") or 0

    bucket_results = Variable.get("s3_bucket_results", default_var="rzd-airflow-results")
    ds_nodash = context["ds_nodash"]
    result_key = f"validation_reports/sensor_readings/{ds_nodash}_validation.csv"

    report_df = pd.DataFrame([{
        "run_date": context["ds"],
        "dag_run_id": context["run_id"],
        "depot": Variable.get("depot_code", default_var="TCH-15"),
        "total_read": total_read,
        "valid": valid,
        "rejected": rejected,
        "loaded_to_pg": loaded,
    }])

    write_csv_to_s3(df=report_df, bucket=bucket_results, key=result_key)
    logger.info("Отчёт валидации: s3://%s/%s", bucket_results, result_key)
    return f"s3://{bucket_results}/{result_key}"


# ------------------------------------------------------------------ #
#  ЗАДАЧА 6: Итоговое сообщение в лог Airflow                        #
# ------------------------------------------------------------------ #
def log_run_summary(**context) -> str:
    """
    Читает итоги из XCom и формирует сводку в логах Airflow UI.
    Выделяет аномалии с температурой > BUXA_TEMP_ALARM.
    """
    ti = context["ti"]
    total  = ti.xcom_pull(task_ids="read_sensor_data",     key="record_count") or 0
    valid  = ti.xcom_pull(task_ids="validate_sensor_data", key="valid_count") or 0
    loaded = ti.xcom_pull(task_ids="load_to_postgres",     key="loaded_count") or 0

    # Подсчёт аномалий по температуре из валидных данных
    valid_json = ti.xcom_pull(task_ids="validate_sensor_data", key="valid_data_json")
    alarm_count = 0
    if valid_json:
        df_valid = pd.read_json(StringIO(valid_json), orient="records")
        if "temperature" in df_valid.columns:
            alarm_count = int((df_valid["temperature"] > BUXA_TEMP_ALARM).sum())

    status = "ВНИМАНИЕ: АНОМАЛИИ ТЕМПЕРАТУРЫ!" if alarm_count > 0 else "OK"
    summary = (
        f"\n{'='*58}\n"
        f"ТЕЛЕМЕТРИЯ ТЧЭ-15 | {context['ds']} | {status}\n"
        f"{'='*58}\n"
        f"Прочитано записей:          {total}\n"
        f"Прошли валидацию:           {valid}\n"
        f"Загружено в PostgreSQL:     {loaded}\n"
        f"Аномалий (темп > {BUXA_TEMP_ALARM}°C): {alarm_count}\n"
        f"{'─'*58}\n"
        f"Результаты: rzd-airflow-results/validation_reports/\n"
        f"{'='*58}"
    )
    logger.info(summary)
    if alarm_count > 0:
        logger.warning(
            "Обнаружено %d записей с температурой буксы > %.0f°C! "
            "Проверьте rzd_analytics.sensor_readings.",
            alarm_count, BUXA_TEMP_ALARM,
        )
    return summary


# ------------------------------------------------------------------ #
#  ОПРЕДЕЛЕНИЕ DAG                                                    #
# ------------------------------------------------------------------ #
default_args = {
    "owner": "rzd-de-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "depends_on_past": False,
}

with DAG(
    dag_id="locomotive_telemetry_dag",
    description="Телеметрия локомотивов ТЧЭ-15: S3 → валидация → PostgreSQL → отчёт S3",
    schedule="*/15 * * * *",
    start_date=datetime(2024, 3, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["rzd", "telemetry", "tche15", "s3", "sensor"],
    doc_md="""
    ## locomotive_telemetry_dag

    Пайплайн обработки телеметрии локомотивов ТЧЭ-15.
    Запускается каждые 15 минут.

    **Поток данных:**
    ```
    S3KeySensor → read_sensor_data → validate_sensor_data
        → load_to_postgres → write_validation_report → log_run_summary
    ```

    **Источник:** `rzd-airflow-data/sensor_readings.csv`
    **Приёмник:** `rzd_analytics.sensor_readings` (Managed PostgreSQL)
    **Отчёт:** `rzd-airflow-results/validation_reports/sensor_readings/<date>_validation.csv`

    **Connections:** `yandex_s3`, `rzd_postgres`
    **Variables:** `s3_bucket_data`, `s3_bucket_results`, `depot_code`
    """,
) as dag:

    # Шаг 1: Ожидание файла в Object Storage
    wait_for_sensor_file = S3KeySensor(
        task_id="wait_for_sensor_file",
        bucket_name="{{ var.value.s3_bucket_data }}",
        bucket_key=SOURCE_KEY,
        aws_conn_id=S3_CONN_ID,
        poke_interval=60,
        timeout=7200,
        mode="reschedule",
    )

    # Шаг 2: Чтение данных из S3
    t2_read = PythonOperator(
        task_id="read_sensor_data",
        python_callable=read_sensor_data,
    )

    # Шаг 3: Валидация
    t3_validate = PythonOperator(
        task_id="validate_sensor_data",
        python_callable=validate_sensor_data,
    )

    # Шаг 4: Загрузка в PostgreSQL
    t4_load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    # Шаг 5: Запись отчёта в S3
    t5_report = PythonOperator(
        task_id="write_validation_report",
        python_callable=write_validation_report,
    )

    # Шаг 6: Итоговый лог
    t6_summary = PythonOperator(
        task_id="log_run_summary",
        python_callable=log_run_summary,
    )

    # Цепочка зависимостей
    (
        wait_for_sensor_file
        >> t2_read
        >> t3_validate
        >> t4_load
        >> t5_report
        >> t6_summary
    )
```

---

## Деплой и тестирование

### Загрузка DAG в Object Storage

```bash
# Через Яндекс CLI
yc storage cp locomotive_telemetry_dag.py \
    s3://rzd-airflow-dags/dags/locomotive_telemetry_dag.py

# Проверить, что файл появился в бакете
yc storage ls s3://rzd-airflow-dags/dags/
```

Через Yandex Cloud Console:
1. **Object Storage → rzd-airflow-dags → Загрузить объект**
2. Файл: `locomotive_telemetry_dag.py`, путь объекта: `dags/locomotive_telemetry_dag.py`
3. Нажать **Загрузить**

### Проверка в Airflow UI

1. Открыть Airflow UI (URL из карточки кластера Managed Airflow)
2. Через 1–3 минуты DAG `locomotive_telemetry_dag` появится в списке
3. Убедиться, что DAG не содержит ошибок импорта: **DAGs → Import Errors** (список должен быть пуст)
4. Включить DAG (переключатель слева от имени)
5. Нажать **Trigger DAG** для ручного запуска

### Ожидаемый результат

После успешного запуска:
- Все 6 задач в статусе `success` (зелёный цвет в Grid View)
- Задача `wait_for_sensor_file` завершается быстро (файл уже есть в S3)
- Задача `validate_sensor_data` выводит в лог количество валидных и отклонённых записей
- В Managed PostgreSQL появляются строки в таблице `rzd_analytics.sensor_readings`
- В Object Storage появляется файл: `rzd-airflow-results/validation_reports/sensor_readings/<ds_nodash>_validation.csv`
- Задача `log_run_summary` выводит итоговую сводку, включая количество аномалий температуры

Проверка результатов через Airflow UI → **Browse → XComs**: убедитесь, что между задачами передаются значения `record_count`, `valid_count`, `loaded_count`.

---

## Задания повышенной сложности

### Задание 1. S3KeySensor с шаблоном даты и партиционированием

Измените DAG так, чтобы `S3KeySensor` ожидал файл по ключу с датой запуска:

```
sensor_readings/{{ ds_nodash }}/data.csv
```

Добавьте в задачу `read_sensor_data` логику: если файл за текущую дату не найден, прочитать файл за предыдущую дату как запасной вариант (`fallback_key = f"sensor_readings/{prev_ds_nodash}/data.csv"`). Реализуйте проверку через `S3Hook.check_for_key()`.

### Задание 2. Динамическая генерация задач валидации по локомотивам

Перепишите этап валидации с использованием Dynamic Task Mapping (Airflow 2.3+): для каждого уникального `loco_id` в файле создайте отдельную задачу валидации через `.expand()`. Запись в PostgreSQL должна выполняться одной общей задачей после завершения всех задач валидации.

```python
# Подсказка: структура с Dynamic Task Mapping
@task
def validate_single_loco(loco_data: dict) -> dict:
    ...

# В теле DAG:
loco_groups = split_by_loco_task(raw_data)          # возвращает список dict
validated = validate_single_loco.expand(loco_data=loco_groups)
```

### Задание 3. Запись аномалий в отдельный объект Object Storage

Добавьте задачу `write_anomalies_to_s3`, которая выполняется параллельно с `write_validation_report`. Задача должна:
- Отфильтровать из валидных данных строки с `temperature > 80.0`
- Записать их в `rzd-airflow-results/anomalies/sensor_readings/<ds_nodash>_alarms.csv` через `S3Hook.load_string()`
- Если аномалий нет — записать пустой CSV с заголовками (не пропускать запись)

Структура итогового графа:

```
wait_for_sensor_file → read_sensor_data → validate_sensor_data → load_to_postgres
                                                                ↙               ↘
                                              write_validation_report    write_anomalies_to_s3
                                                                ↘               ↙
                                                              log_run_summary
```
