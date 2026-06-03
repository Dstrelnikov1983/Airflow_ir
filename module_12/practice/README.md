# Практическая работа №12: Безопасность: Yandex Lockbox вместо Connections и S3-credentials

**Модуль:** 12 — Безопасность Airflow  
**Продолжительность:** 45 минут  
**Платформа:** Yandex Managed Service for Apache Airflow™  
**Схема БД:** `rzd_analytics`

> **Важно.** Среда выполнения — Managed Airflow. Прямого доступа к файловой системе нет.
> Все файлы хранятся в Yandex Object Storage (S3-совместимый).
> DAG-файлы деплоятся через бакет `rzd-airflow-dags/`, а не через `airflow dags` CLI или SSH.

---

## Цель и задачи

**Цель:** перевести все credentials аналитической платформы ТЧЭ-15 из Airflow Connections UI в
Yandex Lockbox, а чтение CSV-файлов — исключительно через S3Hook (`aws_conn_id='yandex_s3'`).

**Задачи:**

1. Создать секреты в Yandex Lockbox: `rzd-postgres-creds`, `yandex-s3-keys`.
2. Настроить Secrets Backend в Managed Airflow так, чтобы Connections читались из Lockbox автоматически.
3. Убедиться, что DAG читает данные из Object Storage через `S3Hook` без хардкода ключей.
4. Настроить RBAC: роли `DataEngineer`, `DataAnalyst`, `DutyOperator` — только нужные DAG и Variables.
5. Убедиться, что Fernet Key управляется Managed Airflow (не вручную).
6. Задокументировать результат и загрузить DAG-файл в `rzd-airflow-dags/`.

---

## Предварительные условия

Перед началом работы убедитесь, что выполнены следующие условия.

| Ресурс | Статус |
|---|---|
| Managed Airflow создан в Yandex Cloud Console | Активен |
| Бакет `rzd-airflow-dags` создан и указан в настройках Managed Airflow | Привязан |
| Бакет `rzd-airflow-data` содержит CSV-файлы датасета | Загружены |
| Бакет `rzd-airflow-results` создан для результатов обработки | Создан |
| Managed PostgreSQL кластер создан, база `rzd_analytics` инициализирована | Активен |
| Сервисный аккаунт с ролями `lockbox.payloadViewer` и `storage.editor` создан | Готов |
| YC CLI установлен и настроен (`yc init`) | Настроен |

### Структура бакетов

```
rzd-airflow-dags/          — DAG-файлы (связан с Managed Airflow)
rzd-airflow-data/          — входные данные
  ├── sensor_readings.csv
  ├── locomotives.csv
  ├── trips.csv
  ├── schedule_adherence.csv
  └── maintenance.csv
rzd-airflow-results/       — результаты обработки (выходные CSV, отчёты)
```

### Настройка Connection yandex_s3 в Airflow UI

Перейдите: Admin → Connections → Add a new record.

| Поле | Значение |
|---|---|
| Conn Id | `yandex_s3` |
| Conn Type | `Amazon Web Services` |
| Login | `<Access Key ID сервисного аккаунта>` |
| Password | `<Secret Access Key>` |
| Extra | `{"endpoint_url": "https://storage.yandexcloud.net", "region_name": "ru-central1"}` |

---

## Шаги выполнения

### Шаг 1. Создание секретов в Yandex Lockbox

Секреты создаются через YC CLI. Managed Airflow читает их через Secrets Backend автоматически
— Connection в UI для этих подключений создавать не нужно.

**1.1. Секрет `rzd-postgres-creds` — Managed PostgreSQL:**

```bash
yc lockbox secret create \
  --name rzd-postgres-creds \
  --description "Airflow Connection: rzd_postgres (Managed PostgreSQL ТЧЭ-15)" \
  --payload '[
    {"key": "conn_type", "text_value": "postgres"},
    {"key": "host",      "text_value": "<FQDN>.mdb.yandexcloud.net"},
    {"key": "port",      "text_value": "5432"},
    {"key": "login",     "text_value": "airflow_svc"},
    {"key": "password",  "text_value": "<пароль>"},
    {"key": "schema",    "text_value": "rzd_analytics"}
  ]'
```

**1.2. Секрет `yandex-s3-keys` — Object Storage:**

```bash
yc lockbox secret create \
  --name yandex-s3-keys \
  --description "Airflow Connection: yandex_s3 (Object Storage ТЧЭ-15)" \
  --payload '[
    {"key": "conn_type",     "text_value": "aws"},
    {"key": "login",         "text_value": "<Access Key ID>"},
    {"key": "password",      "text_value": "<Secret Access Key>"},
    {"key": "extra",         "text_value": "{\"endpoint_url\": \"https://storage.yandexcloud.net\", \"region_name\": \"ru-central1\"}"}
  ]'
```

**1.3. Проверить созданные секреты:**

```bash
yc lockbox secret list
yc lockbox secret get --name rzd-postgres-creds
yc lockbox secret get --name yandex-s3-keys
```

---

### Шаг 2. Настройка Secrets Backend в Managed Airflow

В Managed Airflow конфигурация передаётся через параметры сервиса в Yandex Cloud Console —
редактировать `airflow.cfg` напрямую нельзя.

**Путь:** Yandex Cloud Console → Managed Service for Apache Airflow → ваш кластер → Изменить → Дополнительные настройки → Переменные окружения.

Добавьте переменные:

| Переменная | Значение |
|---|---|
| `AIRFLOW__SECRETS__BACKEND` | `airflow.providers.yandex.secrets.lockbox.LockboxSecretBackend` |
| `AIRFLOW__SECRETS__BACKEND_KWARGS` | `{"folder_id": "<ID каталога>", "connections_prefix": "airflow-connections", "variables_prefix": "airflow-variables", "sep": "-"}` |

После сохранения Managed Airflow автоматически перезапустит планировщик и веб-сервер.

> **Соглашение об именовании.** При `connections_prefix = "airflow-connections"` и `sep = "-"`
> Connection с `conn_id = "rzd_postgres"` будет искаться в секрете Lockbox с именем
> `airflow-connections-rzd-postgres`. Убедитесь, что имена секретов соответствуют этому шаблону.

---

### Шаг 3. Проверка — Connections читаются из Lockbox

После настройки Secrets Backend Connection `rzd_postgres` и `yandex_s3` должны разрешаться
через Lockbox без записи в Airflow UI.

Удалите Connection `yandex_s3` из Airflow UI (Admin → Connections → удалить строку `yandex_s3`),
затем запустите тестовый DAG (см. Шаг 5). Если DAG успешно прочитал файл из S3 — Lockbox
работает корректно.

Проверку можно также выполнить через Airflow REST API (запрос из любой задачи DAG):

```python
from airflow.hooks.base import BaseHook
import logging

log = logging.getLogger(__name__)

def verify_lockbox_connections(**context):
    """Проверить, что Connections разрешаются через Lockbox."""
    for conn_id in ["rzd_postgres", "yandex_s3"]:
        try:
            conn = BaseHook.get_connection(conn_id)
            log.info(
                "Connection '%s': host=%s, conn_type=%s — источник: Lockbox",
                conn_id, conn.host, conn.conn_type,
            )
        except Exception as exc:
            log.error("Не удалось получить Connection '%s': %s", conn_id, exc)
            raise
```

---

### Шаг 4. Настройка RBAC: роли DataEngineer / DataAnalyst / DutyOperator

Роли настраиваются через Airflow UI: Security → Roles.

| Право | DataEngineer | DataAnalyst | DutyOperator |
|---|:---:|:---:|:---:|
| Просмотр DAG | Да | Да | Да |
| Запуск DAG (trigger) | Да | Да | Да |
| Редактирование DAG (pause/unpause) | Да | Нет | Да |
| Просмотр Variables | Да | Да | Нет |
| Создание/редактирование Variables | Да | Нет | Нет |
| Просмотр Connections | Да | Нет | Нет |
| Редактирование Connections | Да | Нет | Нет |
| Просмотр логов задач | Да | Да | Да |
| Просмотр Audit Logs | Да | Нет | Нет |
| Доступ к Admin-панели | Нет | Нет | Нет |

Создайте тестовых пользователей через Security → List Users → Add a new record:

| Пользователь | Роль | Email |
|---|---|---|
| `ivanov_ae` | DataEngineer | ivanov@tceh15.rzd-sib.ru |
| `sidorova_mn` | DataAnalyst | sidorova@tceh15.rzd-sib.ru |
| `petrov_vv` | DutyOperator | petrov@tceh15.rzd-sib.ru |

DAG-level RBAC задаётся параметром `access_control` прямо в коде DAG (см. Шаг 5).

---

### Шаг 5. Деплой DAG-файла через Object Storage

В Managed Airflow DAG-файлы загружаются в бакет, связанный с сервисом. Никакого SSH или
прямого доступа к серверу не требуется.

**Загрузка через YC CLI:**

```bash
yc storage cp lockbox_security_dag.py s3://rzd-airflow-dags/dags/lockbox_security_dag.py
```

**Загрузка через Yandex Cloud Console:**

Перейдите: Object Storage → rzd-airflow-dags → папка `dags/` → Загрузить объект.

**Проверка в Airflow UI:**

Через 30–60 секунд DAG появится в списке DAGs. Если DAG не появился — проверьте:

1. Правильность пути в бакете (должна быть папка `dags/`).
2. Синтаксис Python-файла (ошибки парсинга скрыты в Import Errors).
3. Настройку бакета в параметрах Managed Airflow кластера.

---

### Шаг 6. Проверка аудит-лога Lockbox

После запуска DAG убедитесь, что обращения к секретам фиксируются.

```bash
# Просмотр аудит-лога обращений к секрету rzd-postgres-creds
yc audit-trails event list \
  --filter "event_type=yandex.cloud.audit.lockbox.GetPayload AND \
            details.secret_name=rzd-postgres-creds" \
  --limit 20

# Просмотр аудит-лога обращений к yandex-s3-keys
yc audit-trails event list \
  --filter "event_type=yandex.cloud.audit.lockbox.GetPayload AND \
            details.secret_name=yandex-s3-keys" \
  --limit 20
```

В выводе должны быть события с `subject` = ID сервисного аккаунта Managed Airflow,
временем чуть позже запуска DAG.

---

## Полный код DAG

Файл: `lockbox_security_dag.py` — деплоить в `rzd-airflow-dags/dags/`.

```python
"""
lockbox_security_dag.py
Практическая работа №12: Безопасность — Yandex Lockbox + S3.

Демонстрирует:
  - Чтение CSV из Object Storage через S3Hook (aws_conn_id='yandex_s3').
  - Connection 'yandex_s3' и 'rzd_postgres' разрешаются через Lockbox.
  - RBAC: access_control по ролям ТЧЭ-15.
  - Запись результата обратно в Object Storage.

Деплой:
  yc storage cp lockbox_security_dag.py s3://rzd-airflow-dags/dags/
"""

from __future__ import annotations

from datetime import datetime, timedelta
from io import StringIO

import logging
import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

log = logging.getLogger(__name__)

# ─── константы ───────────────────────────────────────────────────────────────
S3_CONN_ID     = "yandex_s3"
BUCKET_DATA    = "rzd-airflow-data"
BUCKET_RESULTS = "rzd-airflow-results"
KEY_LOCOS      = "locomotives.csv"
KEY_SENSORS    = "sensor_readings.csv"
KEY_RESULT     = "results/module12_lockbox_check_{{ ds_nodash }}.csv"

DEFAULT_ARGS = {
    "owner":            "ivanov_ae",
    "depends_on_past":  False,
    "email":            ["ivanov@tceh15.rzd-sib.ru"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# ─── вспомогательные функции S3 ──────────────────────────────────────────────

def read_csv_from_s3(
    bucket: str,
    key: str,
    conn_id: str = S3_CONN_ID,
) -> pd.DataFrame:
    """Прочитать CSV из Object Storage через S3Hook."""
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
    """Записать DataFrame в Object Storage как CSV через S3Hook."""
    hook = S3Hook(aws_conn_id=conn_id)
    buf = StringIO()
    df.to_csv(buf, index=False)
    hook.load_string(
        string_data=buf.getvalue(),
        key=key,
        bucket_name=bucket,
        replace=True,
    )
    log.info("Записан файл: s3://%s/%s", bucket, key)


# ─── задачи DAG ──────────────────────────────────────────────────────────────

def verify_lockbox_connections(**context) -> None:
    """
    Убедиться, что Connections разрешаются через Lockbox.
    Connection 'yandex_s3' удалён из UI — он берётся из Lockbox.
    """
    from airflow.hooks.base import BaseHook

    for conn_id in ["yandex_s3", "rzd_postgres"]:
        try:
            conn = BaseHook.get_connection(conn_id)
            log.info(
                "Connection '%s': conn_type=%s, host=%s  [источник: Lockbox]",
                conn_id, conn.conn_type, conn.host,
            )
        except Exception as exc:
            log.error(
                "Connection '%s' не найден ни в Lockbox, "
                "ни в Airflow UI: %s",
                conn_id, exc,
            )
            raise


def load_locomotives(**context) -> None:
    """Загрузить реестр локомотивов из S3 и передать статистику через XCom."""
    df = read_csv_from_s3(BUCKET_DATA, KEY_LOCOS)
    log.info("Загружено локомотивов: %d", len(df))
    context["ti"].xcom_push(key="loco_count", value=len(df))
    context["ti"].xcom_push(key="series_list", value=df["series"].unique().tolist())


def load_sensors(**context) -> None:
    """Загрузить показания датчиков из S3 и рассчитать агрегаты."""
    df = read_csv_from_s3(BUCKET_DATA, KEY_SENSORS)
    log.info("Загружено записей телеметрии: %d", len(df))

    depot_code = Variable.get("depot_code", default_var="TCH-15")
    threshold  = int(Variable.get("delay_threshold_min", default_var="15"))
    log.info(
        "Депо: %s, порог задержки: %d мин",
        depot_code, threshold,
    )

    # Агрегаты по температуре буксы
    if "buxa_temp_max" in df.columns:
        avg_temp = df["buxa_temp_max"].mean()
        max_temp = df["buxa_temp_max"].max()
        log.info(
            "Температура буксы — средняя: %.1f°C, максимум: %.1f°C",
            avg_temp, max_temp,
        )
        context["ti"].xcom_push(key="avg_buxa_temp", value=round(avg_temp, 2))
        context["ti"].xcom_push(key="max_buxa_temp", value=round(max_temp, 2))

    context["ti"].xcom_push(key="sensor_count", value=len(df))


def build_security_report(**context) -> None:
    """
    Сформировать отчёт по безопасности и сохранить в S3.
    Все операции с файлами — через S3Hook.
    """
    ti           = context["ti"]
    loco_count   = ti.xcom_pull(task_ids="load_locomotives",  key="loco_count")
    sensor_count = ti.xcom_pull(task_ids="load_sensors",      key="sensor_count")
    avg_temp     = ti.xcom_pull(task_ids="load_sensors",      key="avg_buxa_temp")
    max_temp     = ti.xcom_pull(task_ids="load_sensors",      key="max_buxa_temp")
    series_list  = ti.xcom_pull(task_ids="load_locomotives",  key="series_list")

    report_data = {
        "report_date":     [context["ds"]],
        "depot_code":      [Variable.get("depot_code", default_var="TCH-15")],
        "loco_count":      [loco_count],
        "sensor_records":  [sensor_count],
        "avg_buxa_temp":   [avg_temp],
        "max_buxa_temp":   [max_temp],
        "series_count":    [len(series_list) if series_list else 0],
        "secrets_backend": ["Yandex Lockbox"],
        "s3_conn_source":  ["Lockbox: yandex-s3-keys"],
        "pg_conn_source":  ["Lockbox: rzd-postgres-creds"],
    }

    df_report = pd.DataFrame(report_data)

    # Ключ с подстановкой даты
    result_key = f"results/module12_lockbox_check_{context['ds_nodash']}.csv"
    write_csv_to_s3(df_report, BUCKET_RESULTS, result_key)
    log.info(
        "Отчёт сохранён: s3://%s/%s",
        BUCKET_RESULTS, result_key,
    )


def check_rbac_variables(**context) -> None:
    """
    Проверить доступность Variables (читаются из Airflow Variables,
    могут также храниться в Lockbox при наличии variables_prefix).
    """
    variables_to_check = [
        "s3_bucket_data",
        "s3_bucket_results",
        "depot_code",
        "delay_threshold_min",
    ]

    for var_key in variables_to_check:
        value = Variable.get(var_key, default_var=None)
        if value is not None:
            log.info("Variable '%s' = '%s'  [OK]", var_key, value)
        else:
            log.warning(
                "Variable '%s' не задана — используется значение по умолчанию",
                var_key,
            )


# ─── определение DAG ─────────────────────────────────────────────────────────

with DAG(
    dag_id="lockbox_security_practice",
    description=(
        "Практическая работа №12: Lockbox + S3 для ТЧЭ-15. "
        "Credentials из Lockbox, файлы через S3Hook."
    ),
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    # RBAC: разграничение доступа по ролям депо ТЧЭ-15
    access_control={
        "DataEngineer": {"can_dag_read", "can_dag_edit"},
        "DataAnalyst":  {"can_dag_read"},
        "DutyOperator": {"can_dag_read", "can_dag_edit"},
    },
    tags=["security", "lockbox", "s3", "tceh15", "practice"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Ждём появления файла с датчиками в S3
    wait_for_sensors = S3KeySensor(
        task_id="wait_for_sensors_file",
        bucket_name=BUCKET_DATA,
        bucket_key=KEY_SENSORS,
        aws_conn_id=S3_CONN_ID,
        poke_interval=60,
        timeout=1800,
        mode="reschedule",
    )

    verify_lockbox = PythonOperator(
        task_id="verify_lockbox_connections",
        python_callable=verify_lockbox_connections,
    )

    check_vars = PythonOperator(
        task_id="check_rbac_variables",
        python_callable=check_rbac_variables,
    )

    load_locos = PythonOperator(
        task_id="load_locomotives",
        python_callable=load_locomotives,
    )

    load_sensor_data = PythonOperator(
        task_id="load_sensors",
        python_callable=load_sensors,
    )

    build_report = PythonOperator(
        task_id="build_security_report",
        python_callable=build_security_report,
    )

    end = EmptyOperator(task_id="end")

    (
        start
        >> wait_for_sensors
        >> verify_lockbox
        >> check_vars
        >> [load_locos, load_sensor_data]
        >> build_report
        >> end
    )
```

---

## Контрольные вопросы

1. В каком порядке Airflow ищет Connection: сначала Secrets Backend (Lockbox) или встроенную
   базу данных? Что произойдёт, если секрет найден в Lockbox, но Connection с тем же именем
   также существует в Airflow UI?

2. Почему в Managed Airflow нельзя редактировать `airflow.cfg` напрямую? Каким способом
   передаётся конфигурация `AIRFLOW__SECRETS__BACKEND_KWARGS` в Managed Airflow?

3. Почему все операции с файлами выполняются через `S3Hook`, а не через `open()` или
   `pd.read_csv('s3://...')`? Какое преимущество даёт использование `aws_conn_id`?

4. Роль `DataAnalyst` имеет право просматривать Variables, но не может их редактировать.
   Как это ограничение защищает production-среду от случайных изменений конфигурации?

5. Как убедиться, что Secret Key и Access Key ID для Object Storage не хранятся открытым
   текстом ни в коде DAG, ни в Airflow UI? Опишите путь credentials от Lockbox до момента
   выполнения задачи `load_sensors`.

---

## Связанные файлы

- Лабораторная работа: `../lab/README.md`
- Презентация модуля: `../presentation.html`
- Входные данные: `s3://rzd-airflow-data/locomotives.csv`, `s3://rzd-airflow-data/sensor_readings.csv`
- Результаты: `s3://rzd-airflow-results/results/module12_lockbox_check_<YYYYMMDD>.csv`
