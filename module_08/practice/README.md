# Практическая работа №08: Тестирование DAG с mock S3Hook и PostgreSQL

**Модуль:** 08 — Тестирование
**Продолжительность:** 45 минут
**Уровень:** базовый
**Организация:** Западно-Сибирская дирекция тяги, ТЧЭ-15, депо Новосибирск-Главный
**Платформа:** Yandex Managed Service for Apache Airflow™

---

## Цель и задачи

После выполнения практической работы вы будете уметь:

- Устанавливать и настраивать pytest для Airflow-проекта на Managed Airflow
- Писать тесты структуры DAG с помощью `DagBag`
- Тестировать бизнес-логику классификации температуры букс в изоляции
- Мокировать `S3Hook` (Yandex Object Storage) с помощью `unittest.mock`
- Мокировать `psycopg2.connect` (Yandex Managed PostgreSQL) без реального кластера
- Деплоить DAG-файлы через бакет `rzd-airflow-dags/` в Object Storage

---

## Предварительные условия

### Yandex Managed Service for Apache Airflow™

- Кластер Managed Airflow создан и запущен
- DAG-бакет привязан: `rzd-airflow-dags/` → Managed Airflow → DAG-файлы
- Airflow UI доступен по HTTPS

### Yandex Object Storage

Созданы бакеты:

| Бакет | Назначение |
|---|---|
| `rzd-airflow-dags` | DAG-файлы (связан с Managed Airflow) |
| `rzd-airflow-data` | Входные CSV (sensor_readings.csv и др.) |
| `rzd-airflow-results` | Результаты обработки |

### Connection в Airflow UI

Перейдите: **Admin → Connections → Add**

| Поле | Значение |
|---|---|
| Conn Id | `yandex_s3` |
| Conn Type | Amazon Web Services |
| Login | `<Access Key ID сервисного аккаунта>` |
| Password | `<Secret Access Key>` |
| Extra | `{"endpoint_url": "https://storage.yandexcloud.net", "region_name": "ru-central1"}` |

### Connection для PostgreSQL

| Поле | Значение |
|---|---|
| Conn Id | `rzd_postgres` |
| Conn Type | Postgres |
| Host | `<FQDN>.mdb.yandexcloud.net` |
| Schema | `rzd_analytics` |
| Login / Password | из Yandex Lockbox |

### Переменные Airflow (Admin → Variables)

| Ключ | Значение |
|---|---|
| `s3_bucket_data` | `rzd-airflow-data` |
| `s3_bucket_results` | `rzd-airflow-results` |
| `depot_code` | `TCH-15` |

---

## Шаг 1. Структура проекта

Все файлы разрабатываются локально, затем деплоятся в Object Storage.

```
rzd_airflow/
├── dags/
│   └── buxa_monitor.py          # DAG мониторинга букс
├── tests/
│   ├── __init__.py
│   ├── conftest.py              # общие фикстуры
│   ├── test_dag_structure.py    # тесты структуры DAG
│   └── test_buxa_logic.py       # тесты бизнес-логики
├── pytest.ini
└── requirements-test.txt
```

Создайте директории локально:

```bash
mkdir -p rzd_airflow/dags
mkdir -p rzd_airflow/tests
touch rzd_airflow/tests/__init__.py
```

---

## Шаг 2. Установка зависимостей

Создайте файл `requirements-test.txt`:

```text
pytest==7.4.4
pytest-mock==3.12.0
pytest-cov==4.1.0
coverage==7.4.1
apache-airflow==2.9.0
apache-airflow-providers-amazon==8.19.0
psycopg2-binary==2.9.9
pandas==2.1.4
boto3==1.34.0
```

Установите зависимости:

```bash
pip install -r requirements-test.txt
```

Проверьте установку:

```bash
pytest --version
# pytest 7.4.4

python -c "from airflow.providers.amazon.aws.hooks.s3 import S3Hook; print('S3Hook OK')"
# S3Hook OK
```

---

## Шаг 3. Создание DAG buxa_monitor с S3Hook

Создайте файл `dags/buxa_monitor.py`. DAG читает телеметрию из CSV в Object Storage, классифицирует температуры букс и записывает результаты обратно в S3 и PostgreSQL.

```python
# dags/buxa_monitor.py
"""
DAG мониторинга температуры букс локомотивного парка ТЧЭ-15.

Платформа: Yandex Managed Service for Apache Airflow™
Хранилище: Yandex Object Storage (S3-совместимый), conn_id='yandex_s3'
БД:        Yandex Managed PostgreSQL, conn_id='rzd_postgres'

Пайплайн:
  1. wait_for_telemetry   — S3KeySensor: ждать CSV-файл за дату в Object Storage
  2. read_telemetry       — S3Hook: считать sensor_readings.csv
  3. check_buxa_temp      — классифицировать температуры по порогам ТЧЭ-15
  4. branch_alert         — ветвление: normal / warning / critical
  5. send_alert           — уведомить ТЧМИ при warning/critical
  6. log_summary          — записать итоги в PostgreSQL rzd_analytics
  7. save_results_to_s3   — сохранить обработанные алерты в rzd-airflow-results/

Пороги ТЧЭ-15:
  normal:   temp_c < 80
  warning:  80 <= temp_c < 90
  critical: temp_c >= 90
"""

from __future__ import annotations

from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import psycopg2

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# ── Константы ────────────────────────────────────────────────────
S3_CONN_ID      = "yandex_s3"
BUCKET_DATA     = "rzd-airflow-data"
BUCKET_RESULTS  = "rzd-airflow-results"
TEMP_WARNING    = 80.0
TEMP_CRITICAL   = 90.0

default_args = {
    "owner":            "tche15-analytics",
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
    "email_on_failure": True,
    "email":            ["disp@tche15.rzd.ru"],
}


# ────────────────────────────────────────────────────────────────
#  Вспомогательные функции для работы с S3
# ────────────────────────────────────────────────────────────────

def read_csv_from_s3(
    bucket: str,
    key: str,
    conn_id: str = S3_CONN_ID,
) -> pd.DataFrame:
    """Прочитать CSV из Yandex Object Storage в DataFrame."""
    hook = S3Hook(aws_conn_id=conn_id)
    obj  = hook.get_key(key=key, bucket_name=bucket)
    content = obj.get()["Body"].read().decode("utf-8")
    return pd.read_csv(StringIO(content))


def write_csv_to_s3(
    df: pd.DataFrame,
    bucket: str,
    key: str,
    conn_id: str = S3_CONN_ID,
) -> None:
    """Записать DataFrame как CSV в Yandex Object Storage."""
    hook       = S3Hook(aws_conn_id=conn_id)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=key,
        bucket_name=bucket,
        replace=True,
    )


# ────────────────────────────────────────────────────────────────
#  Бизнес-логика (тестируемые функции)
# ────────────────────────────────────────────────────────────────

def classify_temperature(temp_c: float) -> str:
    """
    Классифицировать температуру буксы по порогам ТЧЭ-15.

    Returns:
        'normal'   — temp_c < 80
        'warning'  — 80 <= temp_c < 90
        'critical' — temp_c >= 90
    """
    if temp_c >= TEMP_CRITICAL:
        return "critical"
    elif temp_c >= TEMP_WARNING:
        return "warning"
    return "normal"


def count_alerts(readings: list) -> dict:
    """
    Подсчитать количество показаний по статусам.

    Raises:
        ValueError: если список пуст
    """
    if not readings:
        raise ValueError("Список показаний пуст — нет данных для анализа")

    counts = {"normal": 0, "warning": 0, "critical": 0}
    for r in readings:
        counts[classify_temperature(r["temp_c"])] += 1

    counts["total"] = len(readings)
    return counts


def get_worst_status(readings: list) -> str:
    """Определить наихудший статус среди всех показаний."""
    statuses = {classify_temperature(r["temp_c"]) for r in readings}
    if "critical" in statuses:
        return "critical"
    elif "warning" in statuses:
        return "warning"
    return "normal"


# ────────────────────────────────────────────────────────────────
#  Таски DAG
# ────────────────────────────────────────────────────────────────

def read_telemetry(**context) -> None:
    """
    Считать sensor_readings.csv из Object Storage за дату запуска.

    Ключ S3: rzd-airflow-data/sensor_readings/<ds_nodash>.csv
    """
    ds_nodash = context["ds_nodash"]
    key       = f"sensor_readings/{ds_nodash}.csv"

    df = read_csv_from_s3(bucket=BUCKET_DATA, key=key)

    # Фильтровать только показания букс
    df_buxa = df[df["sensor_type"] == "BUXA_TEMP"].copy()
    readings = df_buxa.to_dict(orient="records")

    context["ti"].xcom_push(key="buxa_readings", value=readings)


def check_buxa_temp(**context) -> None:
    """Классифицировать каждое показание и сформировать список алертов."""
    readings = context["ti"].xcom_pull(
        task_ids="read_telemetry", key="buxa_readings"
    )
    if readings is None:
        raise ValueError("buxa_readings XCom not found — нет данных от read_telemetry")

    alerts = [
        {**r, "status": classify_temperature(r["temp_c"])}
        for r in readings
        if classify_temperature(r["temp_c"]) != "normal"
    ]
    summary = count_alerts(readings) if readings else {"total": 0}

    context["ti"].xcom_push(key="buxa_alerts", value=alerts)
    context["ti"].xcom_push(key="summary",     value=summary)


def branch_by_status(**context) -> str:
    """Ветвление по наихудшему статусу из всех показаний."""
    readings = context["ti"].xcom_pull(
        task_ids="read_telemetry", key="buxa_readings"
    )
    if not readings:
        return "no_alerts"

    worst = get_worst_status(readings)
    return {
        "critical": "send_critical_alert",
        "warning":  "send_warning_alert",
        "normal":   "no_alerts",
    }.get(worst, "no_alerts")


def save_results_to_s3(**context) -> None:
    """
    Сохранить список алертов в Object Storage.

    Ключ S3: rzd-airflow-results/buxa_alerts/<ds_nodash>.csv
    """
    ds_nodash = context["ds_nodash"]
    alerts    = context["ti"].xcom_pull(
        task_ids="check_buxa_temp", key="buxa_alerts"
    )
    if not alerts:
        print("Алертов нет — файл результатов не создаётся.")
        return

    df  = pd.DataFrame(alerts)
    key = f"buxa_alerts/{ds_nodash}.csv"
    write_csv_to_s3(df=df, bucket=BUCKET_RESULTS, key=key)
    print(f"Сохранено {len(alerts)} алертов → s3://{BUCKET_RESULTS}/{key}")


def log_summary(**context) -> None:
    """Записать итоги проверки в Yandex Managed PostgreSQL rzd_analytics."""
    summary  = context["ti"].xcom_pull(
        task_ids="check_buxa_temp", key="summary"
    )
    conn_str = context["params"].get(
        "conn_str",
        "postgresql://airflow:airflow@<FQDN>.mdb.yandexcloud.net/rzd_analytics",
    )

    insert_sql = """
        INSERT INTO buxa_daily_summary
            (check_date, total, normal_cnt, warning_cnt, critical_cnt)
        VALUES (%s, %s, %s, %s, %s)
    """
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(insert_sql, (
                context["ds"],
                summary.get("total",    0),
                summary.get("normal",   0),
                summary.get("warning",  0),
                summary.get("critical", 0),
            ))
        conn.commit()


# ────────────────────────────────────────────────────────────────
#  Определение DAG
# ────────────────────────────────────────────────────────────────

with DAG(
    dag_id="buxa_temp_monitor",
    default_args=default_args,
    description="Мониторинг температуры букс — ТЧЭ-15 (S3 + Managed PG)",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["rzd", "tche15", "buxa", "safety", "s3"],
    params={
        "conn_str": (
            "postgresql://airflow:airflow"
            "@<FQDN>.mdb.yandexcloud.net/rzd_analytics"
        ),
    },
) as dag:

    wait_task = S3KeySensor(
        task_id="wait_for_telemetry",
        bucket_name=BUCKET_DATA,
        bucket_key="sensor_readings/{{ ds_nodash }}.csv",
        aws_conn_id=S3_CONN_ID,
        poke_interval=300,
        timeout=7200,
        mode="reschedule",
    )

    read_task = PythonOperator(
        task_id="read_telemetry",
        python_callable=read_telemetry,
    )
    check_task = PythonOperator(
        task_id="check_buxa_temp",
        python_callable=check_buxa_temp,
    )
    branch_task = BranchPythonOperator(
        task_id="branch_alert",
        python_callable=branch_by_status,
    )
    send_critical = PythonOperator(
        task_id="send_critical_alert",
        python_callable=lambda **ctx: print("[CRITICAL] Экстренная остановка!"),
    )
    send_warning = PythonOperator(
        task_id="send_warning_alert",
        python_callable=lambda **ctx: print("[WARNING] Уведомление ТЧМИ"),
    )
    no_alerts = EmptyOperator(task_id="no_alerts")

    save_task = PythonOperator(
        task_id="save_results_to_s3",
        python_callable=save_results_to_s3,
        trigger_rule="none_failed_min_one_success",
    )
    summary_task = PythonOperator(
        task_id="log_summary",
        python_callable=log_summary,
        trigger_rule="none_failed_min_one_success",
    )

    wait_task >> read_task >> check_task >> branch_task
    branch_task >> [send_critical, send_warning, no_alerts]
    [send_critical, send_warning, no_alerts] >> save_task >> summary_task
```

---

## Шаг 4. Настройка conftest.py

Создайте файл `tests/conftest.py`:

```python
# tests/conftest.py
"""
Общие фикстуры pytest для тестов DAG buxa_temp_monitor.
Тематика: ТЧЭ-15, Западно-Сибирская дирекция тяги.
Платформа: Yandex Managed Service for Apache Airflow™
"""

import os
import pytest
from unittest.mock import MagicMock
from airflow.models import DagBag

# ── Тестовое окружение Airflow ───────────────────────────────────
os.environ["AIRFLOW__CORE__SQL_ALCHEMY_CONN"] = "sqlite:///./test_airflow.db"
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"]   = "True"
os.environ["AIRFLOW__CORE__DAGS_FOLDER"]      = os.path.join(
    os.path.dirname(__file__), "..", "dags"
)


@pytest.fixture(scope="session")
def dagbag():
    """
    DagBag — загружаем все DAG один раз для всей тест-сессии.

    scope="session": загрузка DagBag — дорогая операция,
    повторная загрузка при каждом тесте замедлит CI в 10-20 раз.
    """
    return DagBag(
        dag_folder=os.environ["AIRFLOW__CORE__DAGS_FOLDER"],
        include_examples=False,
    )


@pytest.fixture(scope="session")
def buxa_dag(dagbag):
    """Объект DAG buxa_temp_monitor."""
    dag = dagbag.get_dag("buxa_temp_monitor")
    assert dag is not None, (
        "DAG 'buxa_temp_monitor' не найден. "
        "Проверьте DAGS_FOLDER и dag_id в buxa_monitor.py."
    )
    return dag


@pytest.fixture
def mock_ti():
    """
    Мок объекта TaskInstance Airflow для XCom-тестов.

    Позволяет тестировать xcom_push/xcom_pull без запуска Airflow.
    """
    return MagicMock()


@pytest.fixture
def mock_s3_hook():
    """
    Мок S3Hook для имитации работы с Yandex Object Storage
    без реального подключения.

    Возвращает настроенный MagicMock, имитирующий get_key() и load_string().
    """
    mock_hook = MagicMock()

    # Имитировать структуру ответа S3: obj.get()['Body'].read()
    mock_body    = MagicMock()
    mock_obj     = MagicMock()
    mock_obj.get.return_value = {"Body": mock_body}
    mock_hook.get_key.return_value = mock_obj

    return mock_hook, mock_body


@pytest.fixture
def sample_readings():
    """
    Типовой набор показаний датчиков букс за смену ТЧЭ-15.
    Содержит все три статуса: normal, warning, critical.
    """
    return [
        {
            "loco_id":    "ВЛ80С-731",
            "section_id": "А",
            "buxa_id":    "Л1",
            "temp_c":     65.2,
            "recorded_at": "2024-06-01 08:00:00",
        },
        {
            "loco_id":    "ВЛ80С-731",
            "section_id": "А",
            "buxa_id":    "П3",
            "temp_c":     84.7,
            "recorded_at": "2024-06-01 08:05:00",
        },
        {
            "loco_id":    "2ТЭ116-927",
            "section_id": "1",
            "buxa_id":    "Л2",
            "temp_c":     92.1,
            "recorded_at": "2024-06-01 08:10:00",
        },
        {
            "loco_id":    "ЭП2К-100",
            "section_id": "-",
            "buxa_id":    "П4",
            "temp_c":     71.0,
            "recorded_at": "2024-06-01 08:12:00",
        },
        {
            "loco_id":    "ЧМЭ3-4412",
            "section_id": "-",
            "buxa_id":    "Л3",
            "temp_c":     58.5,
            "recorded_at": "2024-06-01 08:15:00",
        },
    ]


@pytest.fixture
def all_normal_readings():
    """Все буксы в норме — ожидаем маршрут 'no_alerts'."""
    return [
        {"loco_id": "ЭП2К-100",  "buxa_id": "Л1", "temp_c": 55.0},
        {"loco_id": "ЭД4М-0001", "buxa_id": "П2", "temp_c": 61.3},
        {"loco_id": "ЭС2Г-0045", "buxa_id": "Л3", "temp_c": 70.0},
    ]


@pytest.fixture
def critical_readings():
    """Критический перегрев — ожидаем маршрут 'send_critical_alert'."""
    return [
        {"loco_id": "2ТЭ116-927", "buxa_id": "Л1", "temp_c": 95.0},
        {"loco_id": "2ТЭ116-927", "buxa_id": "П2", "temp_c": 101.3},
    ]


@pytest.fixture
def sample_csv_content():
    """
    Содержимое тестового CSV-файла sensor_readings для имитации S3.
    Используется вместе с mock_s3_hook для тестирования read_telemetry.
    """
    return (
        "loco_id,section_id,sensor_type,buxa_id,temp_c,recorded_at\n"
        "ВЛ80С-731,А,BUXA_TEMP,Л1,65.2,2024-06-01 08:00:00\n"
        "ВЛ80С-731,А,BUXA_TEMP,П3,84.7,2024-06-01 08:05:00\n"
        "2ТЭ116-927,1,BUXA_TEMP,Л2,92.1,2024-06-01 08:10:00\n"
        "ЭП2К-100,-,TRACTION_AMP,,-,2024-06-01 08:11:00\n"
    )
```

---

## Шаг 5. Тесты структуры DAG

Создайте файл `tests/test_dag_structure.py`:

```python
# tests/test_dag_structure.py
"""
Тесты структуры DAG buxa_temp_monitor.

Проверяют корректность загрузки, конфигурации и графа
зависимостей без фактического запуска тасков.
"""

import pytest


class TestDagLoading:
    """Проверка загрузки DAG через DagBag."""

    def test_no_import_errors(self, dagbag):
        """Ни один DAG-файл не должен содержать ошибок импорта."""
        assert dagbag.import_errors == {}, (
            f"Ошибки импорта DAG: {dagbag.import_errors}"
        )

    def test_buxa_monitor_dag_exists(self, dagbag):
        """DAG 'buxa_temp_monitor' должен присутствовать в DagBag."""
        assert "buxa_temp_monitor" in dagbag.dags, (
            "DAG 'buxa_temp_monitor' не найден. "
            "Проверьте dag_id и DAGS_FOLDER."
        )


class TestDagConfiguration:
    """Проверка параметров конфигурации DAG."""

    def test_schedule_interval(self, buxa_dag):
        """Мониторинг букс должен запускаться каждые 30 минут."""
        assert buxa_dag.schedule_interval == "*/30 * * * *", (
            "Расписание изменилось — проверьте совместимость с OTD-витриной!"
        )

    def test_catchup_disabled(self, buxa_dag):
        """catchup должен быть отключён для safety-critical DAG."""
        assert buxa_dag.catchup is False

    def test_rzd_tag_present(self, buxa_dag):
        """DAG должен иметь тег 'rzd' для идентификации в UI."""
        assert "rzd" in buxa_dag.tags

    def test_tche15_tag_present(self, buxa_dag):
        """DAG должен иметь тег 'tche15' для фильтрации по депо."""
        assert "tche15" in buxa_dag.tags

    def test_s3_tag_present(self, buxa_dag):
        """DAG должен иметь тег 's3' — признак использования Object Storage."""
        assert "s3" in buxa_dag.tags

    def test_retries_at_least_2(self, buxa_dag):
        """Все таски должны иметь не менее 2 попыток повтора."""
        for task in buxa_dag.tasks:
            assert task.retries >= 2, (
                f"Таск '{task.task_id}' имеет только "
                f"{task.retries} retries — недостаточно для prod"
            )


class TestDagDependencies:
    """Проверка графа зависимостей тасков."""

    def test_required_tasks_present(self, buxa_dag):
        """Все ключевые таски должны присутствовать в DAG."""
        required = {
            "wait_for_telemetry",
            "read_telemetry",
            "check_buxa_temp",
            "branch_alert",
            "save_results_to_s3",
            "log_summary",
        }
        actual  = {t.task_id for t in buxa_dag.tasks}
        missing = required - actual
        assert not missing, f"Отсутствующие таски: {missing}"

    def test_wait_before_read(self, buxa_dag):
        """S3KeySensor должен выполняться перед read_telemetry."""
        wait_task = buxa_dag.get_task("wait_for_telemetry")
        read_task = buxa_dag.get_task("read_telemetry")
        assert read_task in wait_task.downstream_list

    def test_read_before_check(self, buxa_dag):
        """read_telemetry должен выполняться перед check_buxa_temp."""
        read_task  = buxa_dag.get_task("read_telemetry")
        check_task = buxa_dag.get_task("check_buxa_temp")
        assert check_task in read_task.downstream_list

    def test_check_before_branch(self, buxa_dag):
        """check_buxa_temp должен выполняться перед branch_alert."""
        check_task  = buxa_dag.get_task("check_buxa_temp")
        branch_task = buxa_dag.get_task("branch_alert")
        assert branch_task in check_task.downstream_list
```

---

## Шаг 6. Тесты бизнес-логики с mock S3Hook и psycopg2

Создайте файл `tests/test_buxa_logic.py`:

```python
# tests/test_buxa_logic.py
"""
Unit-тесты бизнес-логики DAG buxa_temp_monitor.

Тестируем функции в изоляции:
  - classify_temperature  — без внешних зависимостей
  - count_alerts          — без внешних зависимостей
  - get_worst_status      — без внешних зависимостей
  - read_telemetry        — mock S3Hook (Yandex Object Storage)
  - log_summary           — mock psycopg2 (Yandex Managed PostgreSQL)
  - check_buxa_temp       — mock XCom (TaskInstance)
  - branch_by_status      — mock XCom (TaskInstance)
"""

import pytest
from unittest.mock import MagicMock, patch

from dags.buxa_monitor import (
    classify_temperature,
    count_alerts,
    get_worst_status,
    check_buxa_temp,
    branch_by_status,
    read_csv_from_s3,
    write_csv_to_s3,
    log_summary,
)


class TestClassifyTemperature:
    """Тесты классификации температуры буксы по порогам ТЧЭ-15."""

    @pytest.mark.parametrize("temp_c, expected", [
        # Нормальный диапазон
        (20.0,  "normal"),    # буксы в холодный день
        (55.0,  "normal"),    # типичный рабочий режим ВЛ80С
        (79.9,  "normal"),    # максимальная нормальная температура
        # Предупреждение (80-89 C)
        (80.0,  "warning"),   # нижняя граница warning включительно
        (85.0,  "warning"),   # середина диапазона
        (89.9,  "warning"),   # верхняя граница warning
        # Критический перегрев (>= 90 C)
        (90.0,  "critical"),  # нижняя граница critical включительно
        (95.0,  "critical"),  # типичный перегрев при неисправности
        (120.0, "critical"),  # экстремальный перегрев
    ])
    def test_classify_temperature(self, temp_c, expected):
        """Классификация возвращает правильный статус для всех диапазонов."""
        result = classify_temperature(temp_c)
        assert result == expected, (
            f"temp_c={temp_c} C: ожидали '{expected}', получили '{result}'"
        )


class TestCountAlerts:
    """Тесты подсчёта показаний по статусам."""

    def test_correct_counts(self, sample_readings):
        """Подсчёт верен для смешанного набора показаний."""
        # sample_readings: 3 normal (65.2, 71.0, 58.5) + 1 warning (84.7) + 1 critical (92.1)
        result = count_alerts(sample_readings)
        assert result["warning"]  == 1
        assert result["critical"] == 1
        assert result["total"]    == 5
        assert result["normal"]   == 3

    def test_total_equals_len(self, sample_readings):
        """total должен совпадать с len(readings)."""
        result = count_alerts(sample_readings)
        assert result["total"] == len(sample_readings)

    def test_empty_list_raises(self):
        """Пустой список показаний — ValueError с понятным сообщением."""
        with pytest.raises(ValueError, match="пуст"):
            count_alerts([])

    def test_all_normal(self, all_normal_readings):
        """Все в норме — warning и critical равны 0."""
        result = count_alerts(all_normal_readings)
        assert result["warning"]  == 0
        assert result["critical"] == 0
        assert result["normal"]   == len(all_normal_readings)


class TestGetWorstStatus:
    """Тесты определения наихудшего статуса."""

    def test_critical_dominates(self, sample_readings):
        """При наличии critical — возвращаем critical."""
        assert get_worst_status(sample_readings) == "critical"

    def test_warning_without_critical(self):
        """При наличии только warning — возвращаем warning."""
        readings = [
            {"temp_c": 65.0},
            {"temp_c": 82.0},  # warning
        ]
        assert get_worst_status(readings) == "warning"

    def test_all_normal(self, all_normal_readings):
        """Все в норме — возвращаем normal."""
        assert get_worst_status(all_normal_readings) == "normal"


class TestReadCsvFromS3:
    """Тесты чтения CSV из Yandex Object Storage через mock S3Hook."""

    @patch("dags.buxa_monitor.S3Hook")
    def test_read_csv_calls_get_key(self, mock_s3hook_cls, sample_csv_content):
        """read_csv_from_s3 вызывает hook.get_key с правильными аргументами."""
        mock_hook = MagicMock()
        mock_body = MagicMock()
        mock_body.read.return_value = sample_csv_content.encode("utf-8")
        mock_hook.get_key.return_value.get.return_value = {"Body": mock_body}
        mock_s3hook_cls.return_value = mock_hook

        df = read_csv_from_s3(
            bucket="rzd-airflow-data",
            key="sensor_readings/20240601.csv",
        )

        mock_hook.get_key.assert_called_once_with(
            key="sensor_readings/20240601.csv",
            bucket_name="rzd-airflow-data",
        )
        assert len(df) > 0

    @patch("dags.buxa_monitor.S3Hook")
    def test_read_csv_filters_buxa_temp(self, mock_s3hook_cls, sample_csv_content):
        """read_csv_from_s3 возвращает все строки CSV (фильтрация — в таске)."""
        mock_hook = MagicMock()
        mock_body = MagicMock()
        mock_body.read.return_value = sample_csv_content.encode("utf-8")
        mock_hook.get_key.return_value.get.return_value = {"Body": mock_body}
        mock_s3hook_cls.return_value = mock_hook

        df = read_csv_from_s3(
            bucket="rzd-airflow-data",
            key="sensor_readings/20240601.csv",
        )
        # CSV содержит 4 строки (3 BUXA_TEMP + 1 TRACTION_AMP)
        assert len(df) == 4


class TestWriteCsvToS3:
    """Тесты записи CSV в Yandex Object Storage через mock S3Hook."""

    @patch("dags.buxa_monitor.S3Hook")
    def test_write_csv_calls_load_string(self, mock_s3hook_cls):
        """write_csv_to_s3 вызывает hook.load_string с replace=True."""
        import pandas as pd

        mock_hook = MagicMock()
        mock_s3hook_cls.return_value = mock_hook

        df = pd.DataFrame([
            {"loco_id": "ВЛ80С-731", "temp_c": 92.1, "status": "critical"},
        ])
        write_csv_to_s3(
            df=df,
            bucket="rzd-airflow-results",
            key="buxa_alerts/20240601.csv",
        )

        mock_hook.load_string.assert_called_once()
        call_kwargs = mock_hook.load_string.call_args.kwargs
        assert call_kwargs["key"]         == "buxa_alerts/20240601.csv"
        assert call_kwargs["bucket_name"] == "rzd-airflow-results"
        assert call_kwargs["replace"]     is True


class TestCheckBuxaTempXCom:
    """Тесты XCom-передачи в таске check_buxa_temp."""

    def test_xcom_push_buxa_alerts_key(self, mock_ti, sample_readings):
        """check_buxa_temp должен записать 'buxa_alerts' в XCom."""
        mock_ti.xcom_pull.return_value = sample_readings

        check_buxa_temp(ti=mock_ti)

        push_keys = {
            call.kwargs.get("key") or call.args[0]
            for call in mock_ti.xcom_push.call_args_list
        }
        assert "buxa_alerts" in push_keys

    def test_xcom_push_summary_key(self, mock_ti, sample_readings):
        """check_buxa_temp должен записать 'summary' в XCom."""
        mock_ti.xcom_pull.return_value = sample_readings

        check_buxa_temp(ti=mock_ti)

        push_keys = {
            call.kwargs.get("key") or call.args[0]
            for call in mock_ti.xcom_push.call_args_list
        }
        assert "summary" in push_keys

    def test_xcom_none_raises(self, mock_ti):
        """Если XCom вернул None — должна быть ValueError."""
        mock_ti.xcom_pull.return_value = None

        with pytest.raises(ValueError, match="buxa_readings XCom not found"):
            check_buxa_temp(ti=mock_ti)


class TestBranchByStatus:
    """Тесты ветвления по наихудшему статусу."""

    def test_critical_route(self, mock_ti, critical_readings):
        """critical показания → маршрут send_critical_alert."""
        mock_ti.xcom_pull.return_value = critical_readings
        assert branch_by_status(ti=mock_ti) == "send_critical_alert"

    def test_warning_route(self, mock_ti):
        """warning показания (без critical) → send_warning_alert."""
        mock_ti.xcom_pull.return_value = [
            {"loco_id": "ВЛ80С-731", "buxa_id": "Л1", "temp_c": 83.5},
        ]
        assert branch_by_status(ti=mock_ti) == "send_warning_alert"

    def test_normal_route(self, mock_ti, all_normal_readings):
        """Все в норме → маршрут no_alerts."""
        mock_ti.xcom_pull.return_value = all_normal_readings
        assert branch_by_status(ti=mock_ti) == "no_alerts"

    def test_empty_readings_no_alerts(self, mock_ti):
        """Пустые данные → no_alerts (безопасное поведение)."""
        mock_ti.xcom_pull.return_value = []
        assert branch_by_status(ti=mock_ti) == "no_alerts"


class TestLogSummaryMockPg:
    """Тесты записи сводки в Yandex Managed PostgreSQL через mock psycopg2."""

    @patch("dags.buxa_monitor.psycopg2.connect")
    def test_insert_called_once(self, mock_connect):
        """log_summary вызывает INSERT ровно один раз."""
        mock_cursor = MagicMock()
        mock_conn   = MagicMock()
        mock_conn.__enter__ = lambda s: mock_conn
        mock_conn.__exit__  = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__  = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = {
            "total": 5, "normal": 3, "warning": 1, "critical": 1
        }

        log_summary(
            ti=mock_ti,
            ds="2024-06-01",
            params={"conn_str": "postgresql://test/rzd_analytics"},
        )

        mock_cursor.execute.assert_called_once()

    @patch("dags.buxa_monitor.psycopg2.connect")
    def test_commit_called_once(self, mock_connect):
        """commit должен вызываться ровно один раз."""
        mock_cursor = MagicMock()
        mock_conn   = MagicMock()
        mock_conn.__enter__ = lambda s: mock_conn
        mock_conn.__exit__  = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__  = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = {
            "total": 3, "normal": 3, "warning": 0, "critical": 0
        }

        log_summary(
            ti=mock_ti,
            ds="2024-06-01",
            params={"conn_str": "postgresql://test/rzd_analytics"},
        )

        mock_conn.commit.assert_called_once()
```

---

## Шаг 7. Настройка pytest.ini

Создайте файл `pytest.ini` в корне проекта:

```ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts =
    -v
    --tb=short
    --strict-markers
markers =
    unit: unit-тесты (без внешних зависимостей)
    integration: требует Airflow DB или внешних сервисов
    slow: выполняется более 5 секунд
    safety: критичные тесты безопасности (classify_temperature)
    s3: тесты взаимодействия с Yandex Object Storage
```

---

## Шаг 8. Запуск тестов

### Запуск всех тестов

```bash
cd rzd_airflow
pytest tests/ -v
```

### Ожидаемый результат

```
tests/test_dag_structure.py::TestDagLoading::test_no_import_errors                  PASSED
tests/test_dag_structure.py::TestDagLoading::test_buxa_monitor_dag_exists           PASSED
tests/test_dag_structure.py::TestDagConfiguration::test_schedule_interval           PASSED
tests/test_dag_structure.py::TestDagConfiguration::test_catchup_disabled            PASSED
tests/test_dag_structure.py::TestDagConfiguration::test_rzd_tag_present             PASSED
tests/test_dag_structure.py::TestDagConfiguration::test_tche15_tag_present          PASSED
tests/test_dag_structure.py::TestDagConfiguration::test_s3_tag_present              PASSED
tests/test_dag_structure.py::TestDagConfiguration::test_retries_at_least_2         PASSED
tests/test_dag_structure.py::TestDagDependencies::test_required_tasks_present       PASSED
tests/test_dag_structure.py::TestDagDependencies::test_wait_before_read             PASSED
tests/test_dag_structure.py::TestDagDependencies::test_read_before_check            PASSED
tests/test_dag_structure.py::TestDagDependencies::test_check_before_branch          PASSED
tests/test_buxa_logic.py::TestClassifyTemperature::test_classify_temperature[...]   PASSED
tests/test_buxa_logic.py::TestCountAlerts::test_correct_counts                      PASSED
tests/test_buxa_logic.py::TestReadCsvFromS3::test_read_csv_calls_get_key            PASSED
tests/test_buxa_logic.py::TestWriteCsvToS3::test_write_csv_calls_load_string        PASSED
tests/test_buxa_logic.py::TestCheckBuxaTempXCom::test_xcom_push_buxa_alerts_key     PASSED
tests/test_buxa_logic.py::TestBranchByStatus::test_critical_route                   PASSED
tests/test_buxa_logic.py::TestLogSummaryMockPg::test_insert_called_once             PASSED
tests/test_buxa_logic.py::TestLogSummaryMockPg::test_commit_called_once             PASSED
========== 28 passed in 3.9s ==========
```

### Запуск с отчётом покрытия

```bash
pytest tests/ --cov=dags --cov-report=term-missing --cov-report=html
# Откройте htmlcov/index.html в браузере
```

### Только safety-critical тесты

```bash
pytest tests/ -m "safety" -v
```

---

## Шаг 9. Деплой DAG в Yandex Managed Airflow через Object Storage

После прохождения всех тестов деплоим DAG-файл в бакет, привязанный к Managed Airflow.

### Через Yandex Cloud CLI

```bash
# Установить YC CLI (если не установлен)
curl -sSL https://storage.yandexcloud.net/yandexcloud-yc/install.sh | bash

# Загрузить DAG-файл в Object Storage
yc storage cp dags/buxa_monitor.py s3://rzd-airflow-dags/dags/buxa_monitor.py

# Проверить, что файл появился
yc storage ls s3://rzd-airflow-dags/dags/
```

### Через AWS CLI (совместимый с Object Storage)

```bash
aws s3 cp dags/buxa_monitor.py s3://rzd-airflow-dags/dags/buxa_monitor.py \
    --endpoint-url https://storage.yandexcloud.net
```

### Проверка в Airflow UI

1. Открыть Airflow UI: **Managed Airflow → Открыть Airflow UI**
2. Перейти в **DAGs** → найти `buxa_temp_monitor`
3. DAG должен появиться в течение 1-2 минут после загрузки файла
4. Убедиться, что нет ошибок импорта (значок без красного восклицательного знака)

---

## Шаг 10. Ручная проверка таска

```bash
# Запустить один таск без записи в метабазу
airflow tasks test buxa_temp_monitor read_telemetry 2024-06-01

# Проверить рендер шаблонов S3-ключей
airflow tasks render buxa_temp_monitor wait_for_telemetry 2024-06-01

# Проверить граф зависимостей
airflow dags show buxa_temp_monitor
```

---

## Контрольные вопросы

1. Почему в тесте `test_read_csv_calls_get_key` мы патчим `dags.buxa_monitor.S3Hook`, а не просто `airflow.providers.amazon.aws.hooks.s3.S3Hook`?

2. Что произойдёт, если в mock S3Hook не настроить цепочку `get_key().get()['Body'].read()`? Какую ошибку вернёт тест?

3. Чем `S3KeySensor` в режиме `mode='reschedule'` отличается от `mode='poke'`? Почему для production DAG предпочтителен `reschedule`?

4. Объясните, почему `scope="session"` в фикстуре `dagbag` ускоряет прогон тестов в CI в 10-20 раз.

5. Как добавить тест, проверяющий, что при отсутствии файла в Object Storage `S3KeySensor` не пропускает пайплайн дальше? Какой mock нужен?

---

*Практическая работа №08 | Курс: Apache Airflow | Yandex Managed Service for Apache Airflow™ | Западно-Сибирская дирекция тяги, ТЧЭ-15*
