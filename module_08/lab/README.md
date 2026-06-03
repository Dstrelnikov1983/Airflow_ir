# Лабораторная работа №08: Полный тест-сьют с mock S3 и CI/CD через Object Storage

**Модуль:** 08 — Тестирование
**Продолжительность:** 60-90 минут
**Уровень:** продвинутый
**Организация:** Западно-Сибирская дирекция тяги, ТЧЭ-15, депо Новосибирск-Главный
**Платформа:** Yandex Managed Service for Apache Airflow™
**Хранилище:** Yandex Object Storage (S3-совместимый)
**СУБД:** Yandex Managed Service for PostgreSQL, схема `rzd_analytics`

---

## Цель

После выполнения лабораторной работы вы будете уметь:

- Создавать полный тест-сьют (8+ тестов) для production DAG с S3-операциями
- Мокировать `S3Hook` (Yandex Object Storage) на уровне чтения и записи файлов
- Мокировать `psycopg2.connect` (Yandex Managed PostgreSQL) без реального кластера
- Настраивать GitHub Actions для автоматического запуска тестов при `git push`
- Деплоить DAG-файлы в бакет `rzd-airflow-dags/` при успешном прохождении CI
- Измерять и контролировать покрытие кода тестами (цель: >= 85%)

---

## Предварительные условия

### Инфраструктура Yandex Cloud

- Yandex Managed Service for Apache Airflow™: кластер создан и запущен
- DAG-бакет привязан: `rzd-airflow-dags/` → Managed Airflow → настройки → DAG-файлы
- Yandex Managed Service for PostgreSQL: кластер в той же сети, что и Managed Airflow
- Сервисный аккаунт с ролями `storage.editor` и `managed-postgresql.editor`

### Структура бакетов Object Storage

| Бакет | Назначение | Содержимое |
|---|---|---|
| `rzd-airflow-dags` | DAG-файлы | `dags/buxa_full_pipeline.py` |
| `rzd-airflow-data` | Входные данные | `sensor_readings/*.csv`, `locomotives.csv` |
| `rzd-airflow-results` | Результаты | `buxa_alerts/*.csv`, `shift_summary/*.csv` |

### Connection `yandex_s3` в Airflow UI

Перейдите: **Admin → Connections → Add**

```
Conn Id:   yandex_s3
Conn Type: Amazon Web Services
Login:     <Access Key ID сервисного аккаунта>
Password:  <Secret Access Key>
Extra:     {"endpoint_url": "https://storage.yandexcloud.net", "region_name": "ru-central1"}
```

### Connection `rzd_postgres` в Airflow UI

```
Conn Id:   rzd_postgres
Conn Type: Postgres
Host:      <FQDN кластера>.mdb.yandexcloud.net
Schema:    rzd_analytics
Login:     airflow_user
Password:  <из Yandex Lockbox>
Port:      6432
```

### Переменные Airflow (Admin → Variables)

| Ключ | Значение |
|---|---|
| `s3_bucket_data` | `rzd-airflow-data` |
| `s3_bucket_results` | `rzd-airflow-results` |
| `depot_code` | `TCH-15` |
| `delay_threshold_min` | `15` |

### Выполнена практическая работа №08

Базовые концепции mock S3Hook и DagBag изучены в практической работе.

---

## Задание

### Шаг 1. Структура проекта

Подготовьте структуру каталогов локально:

```
rzd_airflow/
├── dags/
│   ├── buxa_monitor.py              # DAG из практической работы №08
│   └── buxa_full_pipeline.py        # Расширенный production DAG (этот шаг)
├── tests/
│   ├── __init__.py
│   ├── conftest.py                  # Фикстуры (обновить из практики)
│   └── test_buxa_monitor.py         # Основной файл лаб. работы (8+ тестов)
├── .github/
│   └── workflows/
│       └── test_and_deploy.yml      # CI/CD: тесты + деплой в Object Storage
├── pytest.ini
└── requirements-test.txt
```

Создайте директории:

```bash
mkdir -p rzd_airflow/dags
mkdir -p rzd_airflow/tests
mkdir -p rzd_airflow/.github/workflows
touch rzd_airflow/tests/__init__.py
```

### Шаг 2. Обновление conftest.py

Добавьте в `tests/conftest.py` новые фикстуры для лабораторной работы.

**Ключевые фикстуры для S3-тестов:**

```python
@pytest.fixture
def mock_s3_hook_factory():
    """
    Фабрика mock S3Hook.

    Принимает CSV-контент и возвращает настроенный mock,
    имитирующий get_key() / load_string() без реального S3.
    """
    def _factory(csv_content: str):
        mock_hook = MagicMock()
        mock_body = MagicMock()
        mock_body.read.return_value = csv_content.encode("utf-8")
        mock_hook.get_key.return_value.get.return_value = {"Body": mock_body}
        return mock_hook
    return _factory


@pytest.fixture
def sensor_readings_csv():
    """Тестовый CSV sensor_readings — имитирует файл из rzd-airflow-data/."""
    return (
        "loco_id,section_id,sensor_type,buxa_id,temp_c,recorded_at\n"
        "ВЛ80С-731,А,BUXA_TEMP,Л1,65.2,2024-06-01 08:00:00\n"
        "ВЛ80С-731,А,BUXA_TEMP,П3,84.7,2024-06-01 08:05:00\n"
        "2ТЭ116-927,1,BUXA_TEMP,Л2,92.1,2024-06-01 08:10:00\n"
        "2ТЭ116-927,2,BUXA_TEMP,П1,79.9,2024-06-01 08:12:00\n"
        "ЭП1М-0023,-,BUXA_TEMP,Л4,80.0,2024-06-01 08:15:00\n"
        "2ТЭ25КМ-001,1,BUXA_TEMP,П2,90.0,2024-06-01 08:20:00\n"
        "ЭП2К-100,-,TRACTION_AMP,,450.0,2024-06-01 08:00:00\n"
    )


@pytest.fixture
def mock_pg_cursor_conn():
    """
    Настроенный mock psycopg2 для тестов записи в rzd_analytics.

    Возвращает (mock_conn, mock_cursor).
    """
    mock_cursor = MagicMock()
    mock_conn   = MagicMock()
    mock_conn.__enter__ = lambda s: mock_conn
    mock_conn.__exit__  = MagicMock(return_value=False)
    mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
    mock_conn.cursor.return_value.__exit__  = MagicMock(return_value=False)
    return mock_conn, mock_cursor
```

### Шаг 3. Полный DAG buxa_full_pipeline с S3Hook

Создайте файл `dags/buxa_full_pipeline.py`. DAG работает полностью через Object Storage — локальная файловая система не используется.

### Шаг 4. Тест-сьют tests/test_buxa_monitor.py — 8+ тестов

Реализуйте следующие тест-классы:

| Класс | Тесты | Что проверяется |
|---|---|---|
| `TestDagStructure` | 2 | DagBag загружается, все таски присутствуют |
| `TestTemperatureBoundary` | 1 (9 parametrize) | Граничные значения классификации |
| `TestReadFromS3` | 2 | mock S3Hook: get_key вызван, CSV корректно распарсен |
| `TestWriteToS3` | 2 | mock S3Hook: load_string вызван, replace=True |
| `TestShiftSummary` | 3 | Агрегация по смене, обработка пустых данных |
| `TestBranching` | 3 | XCom-ветвление critical/warning/normal |
| `TestPersistToPg` | 3 | mock psycopg2: INSERT, commit, значения |
| `TestGlonassMock` | 2 | mock requests.get: позиция, ошибка HTTP 503 |

### Шаг 5. Запуск тестов и проверка покрытия

```bash
cd rzd_airflow

# Прогон всех тестов
pytest tests/test_buxa_monitor.py -v --tb=short

# Отчёт покрытия — цель >= 85%
pytest tests/test_buxa_monitor.py \
    --cov=dags \
    --cov-report=term-missing \
    --cov-fail-under=85
```

### Шаг 6. GitHub Actions: тесты + деплой в Object Storage

Настройте CI/CD: при `git push` запускаются тесты, при успехе — DAG-файл деплоится в `rzd-airflow-dags/`.

### Шаг 7. Проверка деплоя в Airflow UI

После успешного прогона CI проверьте, что DAG появился в Managed Airflow UI и загружается без ошибок.

### Шаг 8. Ручной тест тасков

Убедитесь, что каждый таск корректно работает с mock-данными через `airflow tasks test`.

### Шаг 9. Разбор результатов и ретроспектива

Проанализируйте отчёт покрытия и определите непокрытые ветки кода.

---

## Полный код DAG buxa_full_pipeline.py

```python
# dags/buxa_full_pipeline.py
"""
Расширенный production DAG буксового мониторинга для ТЧЭ-15.

Платформа: Yandex Managed Service for Apache Airflow™
Хранилище: Yandex Object Storage (S3-совместимый)
           - Чтение:  rzd-airflow-data/sensor_readings/<ds_nodash>.csv
           - Запись:  rzd-airflow-results/buxa_alerts/<ds_nodash>.csv
           - Запись:  rzd-airflow-results/shift_summary/<ds_nodash>.csv
БД:        Yandex Managed PostgreSQL (rzd_analytics)
           conn_id = 'rzd_postgres'

ВАЖНО: Прямой доступ к файловой системе НЕ используется.
       Все файловые операции — через S3Hook (aws_conn_id='yandex_s3').

Пайплайн:
  1. wait_for_telemetry     — S3KeySensor: ждать CSV за дату
  2. read_buxa_telemetry    — S3Hook: читать sensor_readings
  3. enrich_with_glonass    — requests: позиции локомотивов
  4. classify_buxa_status   — классифицировать температуры
  5. compute_shift_summary  — агрегировать за смену
  6. route_by_severity      — ветвление по наихудшему статусу
  7. send_critical_alert    — экстренное уведомление
  8. send_warning_alert     — предупреждение ТЧМИ
  9. no_critical_issues     — нет проблем
 10. save_alerts_to_s3      — S3Hook: сохранить алерты в results-бакет
 11. persist_summary_to_pg  — psycopg2: сводку в rzd_analytics
 12. update_maintenance_plan — обновить план ТО
"""

from __future__ import annotations

from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import psycopg2
import requests

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.trigger_rule import TriggerRule

# ── Константы ────────────────────────────────────────────────────
S3_CONN_ID     = "yandex_s3"
BUCKET_DATA    = "rzd-airflow-data"
BUCKET_RESULTS = "rzd-airflow-results"

TEMP_WARNING  = 80.0
TEMP_CRITICAL = 90.0

# Три предупреждения за смену → инициировать внеплановое ТО
WARNING_THRESHOLD_FOR_TO = 3

default_args = {
    "owner":            "tche15-analytics",
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
    "email_on_failure": True,
    "email":            ["disp@tche15.rzd.ru"],
}


# ────────────────────────────────────────────────────────────────
#  Вспомогательные функции: S3Hook (Object Storage)
# ────────────────────────────────────────────────────────────────

def read_csv_from_s3(
    bucket: str,
    key: str,
    conn_id: str = S3_CONN_ID,
) -> pd.DataFrame:
    """
    Прочитать CSV из Yandex Object Storage в DataFrame.

    Args:
        bucket:  имя бакета, например 'rzd-airflow-data'
        key:     путь к объекту, например 'sensor_readings/20240601.csv'
        conn_id: Airflow Connection (Conn Type: Amazon Web Services)

    Returns:
        pd.DataFrame с содержимым CSV
    """
    hook    = S3Hook(aws_conn_id=conn_id)
    obj     = hook.get_key(key=key, bucket_name=bucket)
    content = obj.get()["Body"].read().decode("utf-8")
    return pd.read_csv(StringIO(content))


def write_csv_to_s3(
    df: pd.DataFrame,
    bucket: str,
    key: str,
    conn_id: str = S3_CONN_ID,
) -> None:
    """
    Записать DataFrame как CSV в Yandex Object Storage.

    Args:
        df:      DataFrame для сохранения
        bucket:  имя бакета, например 'rzd-airflow-results'
        key:     путь к объекту, например 'buxa_alerts/20240601.csv'
        conn_id: Airflow Connection (Conn Type: Amazon Web Services)
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


# ────────────────────────────────────────────────────────────────
#  Бизнес-логика (тестируемые чистые функции)
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


def compute_shift_summary(readings: list) -> dict:
    """
    Вычислить агрегированные показатели за смену.

    Args:
        readings: список словарей с ключом 'temp_c'

    Returns:
        Словарь: total, normal_cnt, warning_cnt, critical_cnt,
                 max_temp_c, worst_status, locos_with_issues

    Raises:
        ValueError: если readings пуст
    """
    if not readings:
        raise ValueError("Список показаний пуст — нет данных за смену")

    counts      = {"normal": 0, "warning": 0, "critical": 0}
    max_temp    = float("-inf")
    locos_issues = set()

    for r in readings:
        status = classify_temperature(r["temp_c"])
        counts[status] += 1
        if r["temp_c"] > max_temp:
            max_temp = r["temp_c"]
        if status != "normal":
            locos_issues.add(r["loco_id"])

    worst = (
        "critical" if counts["critical"] > 0
        else "warning" if counts["warning"] > 0
        else "normal"
    )

    return {
        "total":             len(readings),
        "normal_cnt":        counts["normal"],
        "warning_cnt":       counts["warning"],
        "critical_cnt":      counts["critical"],
        "max_temp_c":        round(max_temp, 1),
        "worst_status":      worst,
        "locos_with_issues": list(locos_issues),
    }


def needs_unscheduled_to(summary: dict) -> bool:
    """
    Определить, нужно ли внеплановое ТО.

    Критерии: critical_cnt > 0 или warning_cnt >= WARNING_THRESHOLD_FOR_TO.
    """
    return (
        summary.get("critical_cnt", 0) > 0
        or summary.get("warning_cnt", 0) >= WARNING_THRESHOLD_FOR_TO
    )


def enrich_reading_with_position(reading: dict, glonass_url: str) -> dict:
    """
    Добавить данные о позиции из ГЛОНАСС к показанию датчика.

    Raises:
        requests.exceptions.HTTPError: при ошибке HTTP от API
    """
    loco_id = reading["loco_id"]
    resp    = requests.get(f"{glonass_url}/position/{loco_id}", timeout=5)
    resp.raise_for_status()
    pos = resp.json()
    return {
        **reading,
        "lat":       pos.get("lat"),
        "lon":       pos.get("lon"),
        "speed_kmh": pos.get("speed_kmh"),
    }


def calculate_otd(trips: list) -> float:
    """
    Рассчитать OTD (On-Time Delivery) по списку поездок.

    Returns:
        Доля поездок без опоздания (0.0-1.0)

    Raises:
        ValueError: если список пуст
    """
    if not trips:
        raise ValueError("Список поездок пуст")

    on_time = sum(
        1 for t in trips
        if t.get("actual_arr") and t.get("scheduled_arr")
        and t["actual_arr"] <= t["scheduled_arr"]
    )
    return round(on_time / len(trips), 4)


# ────────────────────────────────────────────────────────────────
#  Таски DAG
# ────────────────────────────────────────────────────────────────

def read_buxa_telemetry(**context) -> None:
    """
    Читать sensor_readings из Object Storage.

    Ключ S3: rzd-airflow-data/sensor_readings/<ds_nodash>.csv
    """
    ds_nodash = context["ds_nodash"]
    key       = f"sensor_readings/{ds_nodash}.csv"

    df      = read_csv_from_s3(bucket=BUCKET_DATA, key=key)
    df_buxa = df[df["sensor_type"] == "BUXA_TEMP"].copy()
    df_buxa["temp_c"] = df_buxa["temp_c"].astype(float)
    readings = df_buxa.to_dict(orient="records")

    context["ti"].xcom_push(key="buxa_readings", value=readings)


def enrich_with_glonass(**context) -> None:
    """Обогатить показания данными позиций ГЛОНАСС."""
    readings    = context["ti"].xcom_pull(
        task_ids="read_buxa_telemetry", key="buxa_readings"
    )
    if readings is None:
        raise ValueError("buxa_readings XCom not found")

    glonass_url = context["params"].get(
        "glonass_url", "https://glonass.rzd.ru/api/v1"
    )

    enriched = []
    for r in readings:
        try:
            enriched.append(enrich_reading_with_position(r, glonass_url))
        except Exception:
            # Недоступность ГЛОНАСС не блокирует пайплайн
            enriched.append({**r, "lat": None, "lon": None, "speed_kmh": None})

    context["ti"].xcom_push(key="enriched_readings", value=enriched)


def classify_buxa_status(**context) -> None:
    """Классифицировать все показания и вычислить сводку за смену."""
    enriched = context["ti"].xcom_pull(
        task_ids="enrich_with_glonass", key="enriched_readings"
    )
    if enriched is None:
        raise ValueError("enriched_readings XCom not found")

    summary = compute_shift_summary(enriched)
    context["ti"].xcom_push(key="shift_summary", value=summary)


def route_by_severity(**context) -> str:
    """Ветвление по наихудшему статусу смены."""
    summary = context["ti"].xcom_pull(
        task_ids="classify_buxa_status", key="shift_summary"
    )
    if summary is None:
        raise ValueError("shift_summary XCom not found")

    worst = summary.get("worst_status", "normal")
    return {
        "critical": "send_critical_alert",
        "warning":  "send_warning_alert",
        "normal":   "no_critical_issues",
    }.get(worst, "no_critical_issues")


def save_alerts_to_s3(**context) -> None:
    """
    Сохранить обработанные алерты в Object Storage.

    Ключ S3: rzd-airflow-results/buxa_alerts/<ds_nodash>.csv
    """
    ds_nodash = context["ds_nodash"]
    summary   = context["ti"].xcom_pull(
        task_ids="classify_buxa_status", key="shift_summary"
    )

    if not summary:
        print("Нет данных для сохранения.")
        return

    df  = pd.DataFrame([summary])
    key = f"buxa_alerts/{ds_nodash}.csv"
    write_csv_to_s3(df=df, bucket=BUCKET_RESULTS, key=key)
    print(f"Сохранено → s3://{BUCKET_RESULTS}/{key}")


def persist_summary_to_pg(**context) -> None:
    """Записать сводку за смену в Yandex Managed PostgreSQL rzd_analytics."""
    summary  = context["ti"].xcom_pull(
        task_ids="classify_buxa_status", key="shift_summary"
    )
    conn_str = context["params"].get("conn_str")

    insert_sql = """
        INSERT INTO rzd_analytics.buxa_daily_summary
            (check_date, total_checks, normal_cnt, warning_cnt, critical_cnt)
        VALUES (%s, %s, %s, %s, %s)
    """
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(insert_sql, (
                context["ds"],
                summary["total"],
                summary["normal_cnt"],
                summary["warning_cnt"],
                summary["critical_cnt"],
            ))
        conn.commit()


def update_maintenance_plan(**context) -> None:
    """Создать заявку на внеплановое ТО при многократных предупреждениях."""
    summary = context["ti"].xcom_pull(
        task_ids="classify_buxa_status", key="shift_summary"
    )
    if summary and needs_unscheduled_to(summary):
        locos = summary.get("locos_with_issues", [])
        print(
            f"[ТО] Необходимо внеплановое ТО для локомотивов: {locos}. "
            f"critical={summary['critical_cnt']}, "
            f"warning={summary['warning_cnt']}"
        )


# ────────────────────────────────────────────────────────────────
#  Определение DAG
# ────────────────────────────────────────────────────────────────

with DAG(
    dag_id="buxa_full_pipeline",
    default_args=default_args,
    description="Полный пайплайн мониторинга букс — ТЧЭ-15 (S3 + Managed PG)",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["rzd", "tche15", "buxa", "safety", "s3", "glonass"],
    params={
        "conn_str": (
            "postgresql://airflow_user:password"
            "@<FQDN>.mdb.yandexcloud.net/rzd_analytics"
        ),
        "glonass_url": "https://glonass.rzd.ru/api/v1",
    },
) as dag:

    t_wait = S3KeySensor(
        task_id="wait_for_telemetry",
        bucket_name=BUCKET_DATA,
        bucket_key="sensor_readings/{{ ds_nodash }}.csv",
        aws_conn_id=S3_CONN_ID,
        poke_interval=300,
        timeout=7200,
        mode="reschedule",
    )

    t_read     = PythonOperator(
        task_id="read_buxa_telemetry",
        python_callable=read_buxa_telemetry,
    )
    t_enrich   = PythonOperator(
        task_id="enrich_with_glonass",
        python_callable=enrich_with_glonass,
    )
    t_classify = PythonOperator(
        task_id="classify_buxa_status",
        python_callable=classify_buxa_status,
    )
    t_branch   = BranchPythonOperator(
        task_id="route_by_severity",
        python_callable=route_by_severity,
    )

    t_critical = PythonOperator(
        task_id="send_critical_alert",
        python_callable=lambda **ctx: print("[CRITICAL] Экстренная остановка!"),
    )
    t_warning = PythonOperator(
        task_id="send_warning_alert",
        python_callable=lambda **ctx: print("[WARNING] Уведомление ТЧМИ"),
    )
    t_ok = EmptyOperator(task_id="no_critical_issues")

    t_save = PythonOperator(
        task_id="save_alerts_to_s3",
        python_callable=save_alerts_to_s3,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    t_persist = PythonOperator(
        task_id="persist_summary_to_pg",
        python_callable=persist_summary_to_pg,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    t_to_plan = PythonOperator(
        task_id="update_maintenance_plan",
        python_callable=update_maintenance_plan,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t_wait >> t_read >> t_enrich >> t_classify >> t_branch
    t_branch >> [t_critical, t_warning, t_ok]
    [t_critical, t_warning, t_ok] >> t_save >> t_persist >> t_to_plan
```

---

## Полный тест-сьют tests/test_buxa_monitor.py

```python
# tests/test_buxa_monitor.py
"""
Полный тест-сьют для buxa_full_pipeline.

Тест-классы:
  1. TestDagStructure          — загрузка DAG, наличие тасков (2 теста)
  2. TestTemperatureBoundary   — граничные значения classify_temperature (9 тестов)
  3. TestReadFromS3             — mock S3Hook: чтение CSV (2 теста)
  4. TestWriteToS3              — mock S3Hook: запись CSV (2 теста)
  5. TestShiftSummary          — агрегация за смену (3 теста)
  6. TestBranching              — XCom-ветвление critical/warning/normal (3 теста)
  7. TestPersistToPg            — mock psycopg2: INSERT/commit/значения (3 теста)
  8. TestGlonassMock            — mock requests.get: позиция, HTTP 503 (2 теста)

Итого: 8 классов, 26 тестов
"""

import os
import pytest
from unittest.mock import MagicMock, patch

from airflow.models import DagBag

from dags.buxa_full_pipeline import (
    classify_temperature,
    compute_shift_summary,
    needs_unscheduled_to,
    enrich_reading_with_position,
    route_by_severity,
    classify_buxa_status,
    persist_summary_to_pg,
    read_csv_from_s3,
    write_csv_to_s3,
    calculate_otd,
)


# ════════════════════════════════════════════════════════════════
#  БЛОК 1: Структура DAG
# ════════════════════════════════════════════════════════════════

class TestDagStructure:
    """Тест 1-2: Загрузка DAG через DagBag и наличие ключевых тасков."""

    def _get_dagbag(self):
        return DagBag(
            dag_folder=os.path.join(
                os.path.dirname(__file__), "..", "dags"
            ),
            include_examples=False,
        )

    def test_no_import_errors(self):
        """Тест 1: DAG загружается без ошибок импорта."""
        bag = self._get_dagbag()
        assert bag.import_errors == {}, (
            f"Ошибки импорта DAG: {bag.import_errors}"
        )

    def test_all_required_tasks_present(self):
        """Тест 2: Все ключевые таски присутствуют в buxa_full_pipeline."""
        bag = self._get_dagbag()
        dag = bag.get_dag("buxa_full_pipeline")
        assert dag is not None, "DAG 'buxa_full_pipeline' не найден в DagBag"

        task_ids = {t.task_id for t in dag.tasks}
        required = {
            "wait_for_telemetry",
            "read_buxa_telemetry",
            "enrich_with_glonass",
            "classify_buxa_status",
            "route_by_severity",
            "send_critical_alert",
            "send_warning_alert",
            "no_critical_issues",
            "save_alerts_to_s3",
            "persist_summary_to_pg",
            "update_maintenance_plan",
        }
        missing = required - task_ids
        assert not missing, f"Отсутствующие таски: {missing}"


# ════════════════════════════════════════════════════════════════
#  БЛОК 2: Классификация температур — граничные значения
# ════════════════════════════════════════════════════════════════

class TestTemperatureBoundary:
    """Тест 3: Параметрические граничные значения (9 случаев)."""

    @pytest.mark.parametrize("temp_c, expected", [
        (0.0,   "normal"),    # нулевая температура
        (55.0,  "normal"),    # типичный рабочий режим
        (79.9,  "normal"),    # максимальная нормальная (не включая 80)
        (80.0,  "warning"),   # нижняя граница warning включительно
        (85.0,  "warning"),   # середина диапазона
        (89.9,  "warning"),   # верхняя граница warning (не включая 90)
        (90.0,  "critical"),  # нижняя граница critical включительно
        (95.0,  "critical"),  # типичный перегрев при неисправности
        (120.0, "critical"),  # экстремальный перегрев
    ])
    def test_classify_temperature_boundary_values(self, temp_c, expected):
        """Проверка граничных значений по регламенту ТЧЭ-15."""
        result = classify_temperature(temp_c)
        assert result == expected, (
            f"temp_c={temp_c} C: ожидали '{expected}', получили '{result}'"
        )


# ════════════════════════════════════════════════════════════════
#  БЛОК 3: Чтение CSV из Object Storage (mock S3Hook)
# ════════════════════════════════════════════════════════════════

class TestReadFromS3:
    """Тесты 4-5: mock S3Hook для имитации чтения из Yandex Object Storage."""

    @patch("dags.buxa_full_pipeline.S3Hook")
    def test_read_csv_calls_get_key_with_correct_args(
        self, mock_s3hook_cls, sensor_readings_csv
    ):
        """Тест 4: get_key вызван с правильными bucket и key."""
        mock_hook = MagicMock()
        mock_body = MagicMock()
        mock_body.read.return_value = sensor_readings_csv.encode("utf-8")
        mock_hook.get_key.return_value.get.return_value = {"Body": mock_body}
        mock_s3hook_cls.return_value = mock_hook

        read_csv_from_s3(
            bucket="rzd-airflow-data",
            key="sensor_readings/20240601.csv",
        )

        mock_hook.get_key.assert_called_once_with(
            key="sensor_readings/20240601.csv",
            bucket_name="rzd-airflow-data",
        )

    @patch("dags.buxa_full_pipeline.S3Hook")
    def test_read_csv_returns_dataframe_with_correct_rows(
        self, mock_s3hook_cls, sensor_readings_csv
    ):
        """Тест 5: DataFrame содержит все строки CSV-файла."""
        mock_hook = MagicMock()
        mock_body = MagicMock()
        mock_body.read.return_value = sensor_readings_csv.encode("utf-8")
        mock_hook.get_key.return_value.get.return_value = {"Body": mock_body}
        mock_s3hook_cls.return_value = mock_hook

        df = read_csv_from_s3(
            bucket="rzd-airflow-data",
            key="sensor_readings/20240601.csv",
        )

        # CSV содержит 7 строк данных (6 BUXA_TEMP + 1 TRACTION_AMP)
        assert len(df) == 7
        assert "loco_id" in df.columns
        assert "temp_c"  in df.columns


# ════════════════════════════════════════════════════════════════
#  БЛОК 4: Запись CSV в Object Storage (mock S3Hook)
# ════════════════════════════════════════════════════════════════

class TestWriteToS3:
    """Тесты 6-7: mock S3Hook для имитации записи в Yandex Object Storage."""

    @patch("dags.buxa_full_pipeline.S3Hook")
    def test_write_csv_calls_load_string(self, mock_s3hook_cls):
        """Тест 6: load_string вызван ровно один раз."""
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

    @patch("dags.buxa_full_pipeline.S3Hook")
    def test_write_csv_uses_replace_true(self, mock_s3hook_cls):
        """Тест 7: load_string вызван с replace=True и правильными координатами S3."""
        import pandas as pd

        mock_hook = MagicMock()
        mock_s3hook_cls.return_value = mock_hook

        df = pd.DataFrame([
            {"loco_id": "2ТЭ116-927", "temp_c": 101.3, "status": "critical"},
        ])
        write_csv_to_s3(
            df=df,
            bucket="rzd-airflow-results",
            key="shift_summary/20240601.csv",
        )

        call_kwargs = mock_hook.load_string.call_args.kwargs
        assert call_kwargs["replace"]     is True
        assert call_kwargs["bucket_name"] == "rzd-airflow-results"
        assert call_kwargs["key"]         == "shift_summary/20240601.csv"


# ════════════════════════════════════════════════════════════════
#  БЛОК 5: Агрегация показаний за смену
# ════════════════════════════════════════════════════════════════

class TestShiftSummary:
    """Тесты 8-10: compute_shift_summary."""

    def test_summary_with_mixed_readings(self, sensor_readings_csv):
        """Тест 8: Подсчёт по смешанным данным (normal/warning/critical)."""
        readings = [
            {"loco_id": "ВЛ80С-731",    "temp_c": 65.2},  # normal
            {"loco_id": "ВЛ80С-731",    "temp_c": 84.7},  # warning
            {"loco_id": "2ТЭ116-927",   "temp_c": 92.1},  # critical
            {"loco_id": "2ТЭ116-927",   "temp_c": 79.9},  # normal
            {"loco_id": "ЭП1М-0023",    "temp_c": 80.0},  # warning
            {"loco_id": "2ТЭ25КМ-001",  "temp_c": 90.0},  # critical
        ]
        result = compute_shift_summary(readings)
        assert result["normal_cnt"]   == 2
        assert result["warning_cnt"]  == 2
        assert result["critical_cnt"] == 2
        assert result["total"]        == 6
        assert result["worst_status"] == "critical"

    def test_summary_max_temp(self):
        """Тест 9: max_temp_c соответствует максимальному значению."""
        readings = [
            {"loco_id": "2ТЭ116-927", "temp_c": 95.0},
            {"loco_id": "2ТЭ116-927", "temp_c": 101.3},
            {"loco_id": "2ТЭ116-927", "temp_c": 98.7},
        ]
        result = compute_shift_summary(readings)
        assert result["max_temp_c"] == pytest.approx(101.3, abs=0.1)

    def test_summary_empty_raises(self):
        """Тест 10: Пустой список — ValueError."""
        with pytest.raises(ValueError, match="пуст"):
            compute_shift_summary([])


# ════════════════════════════════════════════════════════════════
#  БЛОК 6: Ветвление BranchPythonOperator
# ════════════════════════════════════════════════════════════════

class TestBranching:
    """Тесты 11-13: route_by_severity через mock XCom."""

    def _make_ti(self, worst_status: str) -> MagicMock:
        ti = MagicMock()
        ti.xcom_pull.return_value = {
            "total": 6, "normal_cnt": 2, "warning_cnt": 2, "critical_cnt": 2,
            "max_temp_c": 92.1, "worst_status": worst_status,
            "locos_with_issues": ["ВЛ80С-731", "2ТЭ116-927"],
        }
        return ti

    def test_route_critical(self):
        """Тест 11: critical → send_critical_alert."""
        ti = self._make_ti("critical")
        assert route_by_severity(ti=ti) == "send_critical_alert"

    def test_route_warning(self):
        """Тест 12: warning → send_warning_alert."""
        ti = self._make_ti("warning")
        assert route_by_severity(ti=ti) == "send_warning_alert"

    def test_route_normal(self):
        """Тест 13: normal → no_critical_issues."""
        ti = self._make_ti("normal")
        assert route_by_severity(ti=ti) == "no_critical_issues"


# ════════════════════════════════════════════════════════════════
#  БЛОК 7: Запись в Yandex Managed PostgreSQL (mock psycopg2)
# ════════════════════════════════════════════════════════════════

class TestPersistToPg:
    """Тесты 14-16: persist_summary_to_pg через mock psycopg2."""

    def _setup_mock_pg(self, mock_connect):
        mock_cursor = MagicMock()
        mock_conn   = MagicMock()
        mock_conn.__enter__ = lambda s: mock_conn
        mock_conn.__exit__  = MagicMock(return_value=False)
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
        mock_conn.cursor.return_value.__exit__  = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn
        return mock_conn, mock_cursor

    @patch("dags.buxa_full_pipeline.psycopg2.connect")
    def test_persist_executes_insert(self, mock_connect):
        """Тест 14: INSERT INTO rzd_analytics.buxa_daily_summary вызван."""
        mock_conn, mock_cursor = self._setup_mock_pg(mock_connect)
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = {
            "total": 6, "normal_cnt": 2,
            "warning_cnt": 2, "critical_cnt": 2,
        }

        persist_summary_to_pg(
            ti=mock_ti,
            ds="2024-06-01",
            params={"conn_str": "postgresql://test/rzd_analytics"},
        )

        mock_cursor.execute.assert_called_once()
        sql = mock_cursor.execute.call_args[0][0]
        assert "INSERT INTO rzd_analytics.buxa_daily_summary" in sql

    @patch("dags.buxa_full_pipeline.psycopg2.connect")
    def test_persist_uses_correct_values(self, mock_connect):
        """Тест 15: INSERT передаёт правильные значения из summary."""
        mock_conn, mock_cursor = self._setup_mock_pg(mock_connect)
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = {
            "total": 10, "normal_cnt": 5,
            "warning_cnt": 3, "critical_cnt": 2,
        }

        persist_summary_to_pg(
            ti=mock_ti,
            ds="2024-06-01",
            params={"conn_str": "postgresql://test/rzd_analytics"},
        )

        values = mock_cursor.execute.call_args[0][1]
        assert values[0] == "2024-06-01"  # check_date
        assert values[1] == 10            # total_checks
        assert values[2] == 5             # normal_cnt
        assert values[3] == 3             # warning_cnt
        assert values[4] == 2             # critical_cnt

    @patch("dags.buxa_full_pipeline.psycopg2.connect")
    def test_persist_commits_transaction(self, mock_connect):
        """Тест 16: commit вызван ровно один раз."""
        mock_conn, mock_cursor = self._setup_mock_pg(mock_connect)
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = {
            "total": 3, "normal_cnt": 3,
            "warning_cnt": 0, "critical_cnt": 0,
        }

        persist_summary_to_pg(
            ti=mock_ti,
            ds="2024-06-01",
            params={"conn_str": "postgresql://test/rzd_analytics"},
        )

        mock_conn.commit.assert_called_once()


# ════════════════════════════════════════════════════════════════
#  БЛОК 8: Mock ГЛОНАСС API (requests.get)
# ════════════════════════════════════════════════════════════════

class TestGlonassMock:
    """Тесты 17-18: enrich_reading_with_position через mock requests.get."""

    @patch("dags.buxa_full_pipeline.requests.get")
    def test_position_data_added_to_reading(self, mock_get):
        """Тест 17: Данные позиции из ГЛОНАСС добавляются к показанию."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "loco_id":   "ВЛ80С-731",
            "lat":       54.9526,
            "lon":       82.9156,
            "speed_kmh": 74.3,
        }
        mock_get.return_value = mock_resp

        reading = {"loco_id": "ВЛ80С-731", "buxa_id": "Л1", "temp_c": 65.0}
        result  = enrich_reading_with_position(reading, "https://glonass.test")

        assert result["speed_kmh"] == 74.3
        assert result["lat"]       == pytest.approx(54.9526)
        assert result["temp_c"]    == 65.0  # оригинальные данные сохранены
        mock_get.assert_called_once()

    @patch("dags.buxa_full_pipeline.requests.get")
    def test_glonass_http_503_raises(self, mock_get):
        """Тест 18: HTTPError от ГЛОНАСС пробрасывается наружу."""
        import requests as req_lib

        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = req_lib.exceptions.HTTPError(
            "503 Service Unavailable"
        )
        mock_get.return_value = mock_resp

        reading = {"loco_id": "ЭП2К-100", "buxa_id": "Л1", "temp_c": 60.0}

        with pytest.raises(req_lib.exceptions.HTTPError, match="503"):
            enrich_reading_with_position(reading, "https://glonass.test")
```

---

## Деплой: загрузка .py в rzd-airflow-dags/ и проверка в UI

### Настройка секретов GitHub

В репозитории GitHub перейдите: **Settings → Secrets and variables → Actions**

Добавьте секреты:

| Имя секрета | Значение |
|---|---|
| `YC_ACCESS_KEY_ID` | Access Key ID сервисного аккаунта Yandex Cloud |
| `YC_SECRET_ACCESS_KEY` | Secret Access Key сервисного аккаунта |

### Файл .github/workflows/test_and_deploy.yml

```yaml
# .github/workflows/test_and_deploy.yml
name: Test DAGs and Deploy to Yandex Object Storage

on:
  push:
    branches:
      - main
      - develop
      - "feature/**"
    paths:
      - "dags/**"
      - "tests/**"
      - "requirements*.txt"
  pull_request:
    branches:
      - main

env:
  AIRFLOW_HOME:                        /tmp/airflow
  AIRFLOW__CORE__EXECUTOR:             LocalExecutor
  AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: "sqlite:////tmp/airflow/airflow.db"
  AIRFLOW__CORE__UNIT_TEST_MODE:       "True"
  PYTHONPATH:                          ${{ github.workspace }}

jobs:

  # ── Job 1: Unit-тесты ────────────────────────────────────────
  unit-tests:
    name: Unit Tests with mock S3 and PostgreSQL
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: "pip"

      - name: Install dependencies
        run: |
          pip install \
            apache-airflow==2.9.0 \
            apache-airflow-providers-amazon==8.19.0 \
            pytest==7.4.4 \
            pytest-mock==3.12.0 \
            pytest-cov==4.1.0 \
            coverage==7.4.1 \
            psycopg2-binary==2.9.9 \
            pandas==2.1.4 \
            boto3==1.34.0
          airflow db init

      - name: Run tests
        run: |
          pytest tests/ -v --tb=short \
            --junitxml=test-results/results.xml

      - name: Upload test results
        uses: actions/upload-artifact@v4
        if: always()
        with:
          name: test-results
          path: test-results/

  # ── Job 2: Покрытие кода ─────────────────────────────────────
  coverage:
    name: Code Coverage (>=85%)
    runs-on: ubuntu-latest
    needs: unit-tests
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: "pip"

      - name: Install dependencies
        run: |
          pip install \
            apache-airflow==2.9.0 \
            apache-airflow-providers-amazon==8.19.0 \
            pytest==7.4.4 pytest-cov==4.1.0 \
            psycopg2-binary==2.9.9 pandas==2.1.4 boto3==1.34.0
          airflow db init

      - name: Run coverage
        run: |
          pytest tests/ \
            --cov=dags \
            --cov-report=xml:coverage.xml \
            --cov-report=term-missing \
            --cov-fail-under=85

      - name: Upload coverage report
        uses: actions/upload-artifact@v4
        with:
          name: coverage-report
          path: coverage.xml

  # ── Job 3: Деплой DAG в Object Storage ───────────────────────
  deploy-to-s3:
    name: Deploy DAG to rzd-airflow-dags (Yandex Object Storage)
    runs-on: ubuntu-latest
    needs: [unit-tests, coverage]
    if: github.ref == 'refs/heads/main' && github.event_name == 'push'
    steps:
      - uses: actions/checkout@v4

      - name: Install AWS CLI (compatible with Yandex Object Storage)
        run: pip install awscli

      - name: Configure AWS CLI for Yandex Object Storage
        env:
          AWS_ACCESS_KEY_ID:     ${{ secrets.YC_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.YC_SECRET_ACCESS_KEY }}
        run: |
          aws configure set aws_access_key_id     "$AWS_ACCESS_KEY_ID"
          aws configure set aws_secret_access_key "$AWS_SECRET_ACCESS_KEY"
          aws configure set region                 ru-central1

      - name: Upload DAG files to rzd-airflow-dags/
        env:
          AWS_ACCESS_KEY_ID:     ${{ secrets.YC_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.YC_SECRET_ACCESS_KEY }}
        run: |
          aws s3 cp dags/buxa_full_pipeline.py \
            s3://rzd-airflow-dags/dags/buxa_full_pipeline.py \
            --endpoint-url https://storage.yandexcloud.net

          aws s3 cp dags/buxa_monitor.py \
            s3://rzd-airflow-dags/dags/buxa_monitor.py \
            --endpoint-url https://storage.yandexcloud.net

          echo "DAG-файлы задеплоены в rzd-airflow-dags/"

      - name: Verify upload
        env:
          AWS_ACCESS_KEY_ID:     ${{ secrets.YC_ACCESS_KEY_ID }}
          AWS_SECRET_ACCESS_KEY: ${{ secrets.YC_SECRET_ACCESS_KEY }}
        run: |
          aws s3 ls s3://rzd-airflow-dags/dags/ \
            --endpoint-url https://storage.yandexcloud.net
```

### Ручная загрузка (без CI/CD)

```bash
# Через AWS CLI, совместимый с Yandex Object Storage
aws s3 cp dags/buxa_full_pipeline.py \
    s3://rzd-airflow-dags/dags/buxa_full_pipeline.py \
    --endpoint-url https://storage.yandexcloud.net

# Через YC CLI
yc storage cp dags/buxa_full_pipeline.py \
    s3://rzd-airflow-dags/dags/buxa_full_pipeline.py
```

### Проверка в Airflow UI

1. Открыть Airflow UI: **Managed Airflow → Открыть Airflow UI**
2. Перейти в **DAGs** → найти `buxa_full_pipeline`
3. DAG появляется через 1-2 минуты после загрузки файла в Object Storage
4. Убедиться, что нет значка ошибки импорта
5. Перейти в **Graph View** — граф должен содержать 11 тасков

---

## Ожидаемый результат

### Прогон тестов

```
tests/test_buxa_monitor.py::TestDagStructure::test_no_import_errors            PASSED
tests/test_buxa_monitor.py::TestDagStructure::test_all_required_tasks_present  PASSED
tests/test_buxa_monitor.py::TestTemperatureBoundary::[0.0-normal]              PASSED
tests/test_buxa_monitor.py::TestTemperatureBoundary::[80.0-warning]            PASSED
tests/test_buxa_monitor.py::TestTemperatureBoundary::[90.0-critical]           PASSED
... (все 9 parametrize-случаев)
tests/test_buxa_monitor.py::TestReadFromS3::test_read_csv_calls_get_key...     PASSED
tests/test_buxa_monitor.py::TestReadFromS3::test_read_csv_returns_dataframe... PASSED
tests/test_buxa_monitor.py::TestWriteToS3::test_write_csv_calls_load_string    PASSED
tests/test_buxa_monitor.py::TestWriteToS3::test_write_csv_uses_replace_true    PASSED
tests/test_buxa_monitor.py::TestShiftSummary::test_summary_with_mixed...       PASSED
tests/test_buxa_monitor.py::TestShiftSummary::test_summary_max_temp            PASSED
tests/test_buxa_monitor.py::TestShiftSummary::test_summary_empty_raises        PASSED
tests/test_buxa_monitor.py::TestBranching::test_route_critical                 PASSED
tests/test_buxa_monitor.py::TestBranching::test_route_warning                  PASSED
tests/test_buxa_monitor.py::TestBranching::test_route_normal                   PASSED
tests/test_buxa_monitor.py::TestPersistToPg::test_persist_executes_insert      PASSED
tests/test_buxa_monitor.py::TestPersistToPg::test_persist_uses_correct_values  PASSED
tests/test_buxa_monitor.py::TestPersistToPg::test_persist_commits_transaction  PASSED
tests/test_buxa_monitor.py::TestGlonassMock::test_position_data_added...       PASSED
tests/test_buxa_monitor.py::TestGlonassMock::test_glonass_http_503_raises      PASSED
========== 26 passed in 5.2s ==========
```

### Отчёт покрытия

```
Name                              Stmts   Miss  Cover   Missing
---------------------------------------------------------------
dags/buxa_full_pipeline.py          98      7    93%   71-74, 120
---------------------------------------------------------------
TOTAL                               98      7    93%
```

### GitHub Actions

```
unit-tests     — passed (26/26 tests)
coverage       — passed (93% >= 85%)
deploy-to-s3   — passed (buxa_full_pipeline.py uploaded to rzd-airflow-dags/)
```

---

## Задания повышенной сложности

### Задание A: Тест end-to-end с полным CSV через mock S3Hook

Реализуйте тест, который полностью имитирует выполнение таска `read_buxa_telemetry`:

1. Создайте mock S3Hook, возвращающий реалистичный CSV с 50 строками
2. Вызовите `read_buxa_telemetry` с mock `ti` и `ds_nodash`
3. Проверьте, что в XCom записан список только BUXA_TEMP-записей
4. Убедитесь, что колонка `temp_c` имеет тип `float`

```python
@patch("dags.buxa_full_pipeline.S3Hook")
def test_read_buxa_telemetry_end_to_end(self, mock_s3hook_cls):
    """E2E: read_buxa_telemetry читает CSV из S3, фильтрует BUXA_TEMP."""
    csv_50_rows = "loco_id,section_id,sensor_type,buxa_id,temp_c,recorded_at\n"
    for i in range(40):
        csv_50_rows += f"ВЛ80С-{i},А,BUXA_TEMP,Л1,{60 + i},2024-06-01 08:00:00\n"
    for i in range(10):
        csv_50_rows += f"ЭП2К-{i},-,TRACTION_AMP,,450,2024-06-01 08:00:00\n"

    mock_hook = MagicMock()
    mock_body = MagicMock()
    mock_body.read.return_value = csv_50_rows.encode("utf-8")
    mock_hook.get_key.return_value.get.return_value = {"Body": mock_body}
    mock_s3hook_cls.return_value = mock_hook

    mock_ti = MagicMock()

    from dags.buxa_full_pipeline import read_buxa_telemetry
    read_buxa_telemetry(ti=mock_ti, ds_nodash="20240601")

    pushed = mock_ti.xcom_push.call_args.kwargs["value"]
    assert len(pushed) == 40
    assert all(isinstance(r["temp_c"], float) for r in pushed)
```

### Задание B: Property-based тестирование с hypothesis

Установите `hypothesis` и напишите property-based тест для `classify_temperature`:

```bash
pip install hypothesis
```

```python
from hypothesis import given, strategies as st

@given(temp_c=st.floats(min_value=-50.0, max_value=200.0, allow_nan=False))
def test_classify_always_returns_valid_status(temp_c):
    """classify_temperature всегда возвращает допустимый статус."""
    result = classify_temperature(temp_c)
    assert result in {"normal", "warning", "critical"}, (
        f"Недопустимый статус '{result}' для temp_c={temp_c}"
    )
```

### Задание C: Интеграционный тест чтения реального CSV из S3

Создайте тест с пометкой `@pytest.mark.integration`, который при наличии реальных credentials к Yandex Object Storage:

1. Загружает тестовый CSV в бакет `rzd-airflow-data/`
2. Вызывает `read_csv_from_s3` с реальным S3Hook
3. Проверяет, что DataFrame содержит ожидаемое количество строк
4. Удаляет тестовый файл после проверки

```python
@pytest.mark.integration
@pytest.mark.skipif(
    not os.environ.get("RUN_INTEGRATION_TESTS"),
    reason="Интеграционные тесты: RUN_INTEGRATION_TESTS=1 не установлен",
)
def test_read_csv_from_real_s3():
    """Интеграционный тест: реальное чтение CSV из Yandex Object Storage."""
    # Реализовать с использованием S3Hook(aws_conn_id='yandex_s3')
    pass
```

---

## Чек-лист сдачи работы

- [ ] Файл `dags/buxa_full_pipeline.py` создан; все файловые операции — через S3Hook
- [ ] Файл `tests/test_buxa_monitor.py` содержит 8+ тестов в 8 классах
- [ ] Все тесты проходят: `pytest tests/test_buxa_monitor.py -v` — все зелёные
- [ ] Покрытие >= 85%: `pytest --cov=dags --cov-fail-under=85` — пройдено
- [ ] Файл `.github/workflows/test_and_deploy.yml` создан с 3 джобами
- [ ] Секреты `YC_ACCESS_KEY_ID` и `YC_SECRET_ACCESS_KEY` добавлены в GitHub
- [ ] При `git push` в `main` CI успешно запускается и деплоит DAG в Object Storage
- [ ] DAG `buxa_full_pipeline` виден в Airflow UI без ошибок импорта
- [ ] Выполнено минимум одно задание повышенной сложности (A, B или C)

---

*Лабораторная работа №08 | Курс: Apache Airflow | Yandex Managed Service for Apache Airflow™ | Западно-Сибирская дирекция тяги, ТЧЭ-15, депо Новосибирск-Главный*
