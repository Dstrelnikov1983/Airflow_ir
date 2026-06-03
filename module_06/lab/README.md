# Лабораторная работа №06: Dynamic Task Mapping — параллельная обработка маршрутов из S3

**Модуль:** 06 — Условные потоки и параллельная обработка
**Продолжительность:** 60–90 минут
**Уровень:** Базовый + задания повышенной сложности
**Платформа:** Yandex Managed Service for Apache Airflow
**Контекст:** Депо ТЧЭ-15 Новосибирск — расчёт ОТД (отклонения от расписания) по маршрутам

---

## Цель

Разработать DAG `route_analysis_dag`, который:

- Читает список маршрутов из `trips.csv` в Yandex Object Storage
- Параллельно обрабатывает каждый маршрут с помощью `Dynamic Task Mapping` (`expand()`)
- Рассчитывает показатель ОТД (процент рейсов без опоздания) для каждого маршрута
- Объединяет результаты в сводный отчёт и записывает его в S3
- Применяет `BranchPythonOperator`: если ОТД < 90% — создаёт алерт, иначе — стандартный отчёт

Все операции с файлами — исключительно через `S3Hook`. Прямой доступ к локальной файловой системе не используется.

---

## Предварительные условия

- Yandex Managed Service for Apache Airflow запущен и доступен через Web UI
- Бакеты и данные настроены согласно практической работе №06:
  - `rzd-airflow-dags/` — бакет для DAG-файлов, связан с кластером Managed Airflow
  - `rzd-airflow-data/` — содержит `trips.csv`, `locomotives.csv`, `sensor_readings.csv`
  - `rzd-airflow-results/` — создан для записи результатов
- Connection `yandex_s3` настроен в Airflow UI (тип Amazon Web Services, endpoint_url Yandex)
- Connection `rzd_postgres` настроен в Airflow UI (Managed PostgreSQL FQDN)
- Variables `s3_bucket_data`, `s3_bucket_results`, `depot_code`, `delay_threshold_min` созданы

---

## Задание

Разработайте DAG `route_analysis_dag` со следующей структурой задач:

1. **`wait_for_trips_file`** — `S3KeySensor`: ждёт появления файла `trips.csv` в бакете `rzd-airflow-data`. Использует `mode='reschedule'` для экономии слотов воркера.

2. **`fetch_routes`** — `@task`: читает `trips.csv` из S3 через `S3Hook`, возвращает список уникальных `route_id`. Результат передаётся в следующую задачу через XCom.

3. **`process_route.expand(route_id=routes)`** — `@task` с Dynamic Mapping: для каждого маршрута из списка выполняется отдельный параллельный экземпляр. Каждый экземпляр:
   - Читает `trips.csv` из S3 через `S3Hook`
   - Фильтрует строки по текущему `route_id`
   - Рассчитывает ОТД = (количество рейсов без опоздания / общее количество рейсов) × 100%
   - Записывает результат маршрута в `s3://rzd-airflow-results/routes/<ds_nodash>/<route_id>/result.csv`

4. **`merge_results`** — `@task`: читает все файлы результатов из папки `s3://rzd-airflow-results/routes/<ds_nodash>/`, объединяет их в единый DataFrame, записывает сводный отчёт в `s3://rzd-airflow-results/summary/<ds_nodash>/report.csv`.

5. **`check_otd_threshold`** — `BranchPythonOperator`: читает сводный отчёт из S3, вычисляет средний ОТД. Если средний ОТД < 90% — возвращает `alert_branch`, иначе — `normal_reporting`.

6. **`alert_branch`** — `@task`: логирует предупреждение о низком ОТД, записывает алерт в `s3://rzd-airflow-results/alerts/<ds_nodash>/alert.csv`.

7. **`normal_reporting`** — `@task`: логирует успешный результат, записывает финальный статус в `s3://rzd-airflow-results/status/<ds_nodash>/ok.csv`.

8. **`finalize`** — `PythonOperator` с `trigger_rule="none_failed_min_one_success"`: завершающая задача, выполняется всегда независимо от результата ветвления.

---

## Полный код DAG

Сохраните файл как `route_analysis_dag.py` и загрузите в `rzd-airflow-dags/dags/`:

```python
"""
DAG: route_analysis_dag
Депо ТЧЭ-15 Новосибирск.

Параллельный расчёт ОТД (отклонения от расписания) по маршрутам.
Данные читаются из Yandex Object Storage через S3Hook.
Результаты записываются обратно в Object Storage через hook.load_string().

Прямой доступ к локальной файловой системе НЕ используется.
"""

from __future__ import annotations

from io import StringIO
from datetime import datetime, timedelta
from typing import Any

import pandas as pd

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# ─────────────────────────────────────────────────────────────────────────────
# Константы
# ─────────────────────────────────────────────────────────────────────────────

S3_CONN_ID      = "yandex_s3"
OTD_THRESHOLD   = 90.0   # % — порог ОТД, ниже которого создаётся алерт


# ─────────────────────────────────────────────────────────────────────────────
# Вспомогательные функции работы с S3
# ─────────────────────────────────────────────────────────────────────────────

def read_csv_from_s3(
    bucket: str,
    key: str,
    conn_id: str = S3_CONN_ID,
) -> pd.DataFrame:
    """
    Читает CSV из Yandex Object Storage через S3Hook.
    Не использует pd.read_csv с локальным путём.
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
    Записывает DataFrame в CSV в Yandex Object Storage через S3Hook.
    Использует hook.load_string() — прямой доступ к ФС не требуется.
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


def list_keys_in_s3_prefix(
    bucket: str,
    prefix: str,
    conn_id: str = S3_CONN_ID,
) -> list[str]:
    """Возвращает список ключей S3 с заданным префиксом."""
    hook = S3Hook(aws_conn_id=conn_id)
    keys = hook.list_keys(bucket_name=bucket, prefix=prefix)
    return keys or []


# ─────────────────────────────────────────────────────────────────────────────
# Функции задач (callable для PythonOperator / BranchPythonOperator)
# ─────────────────────────────────────────────────────────────────────────────

def check_otd_threshold_fn(**context: Any) -> str:
    """
    BranchPythonOperator callable.
    Читает сводный отчёт из S3, вычисляет средний ОТД.
    Возвращает task_id следующей задачи.
    """
    bucket = Variable.get("s3_bucket_results")
    ds_nd  = context["ds_nodash"]
    key    = f"summary/{ds_nd}/report.csv"

    print(f"Чтение сводного отчёта: s3://{bucket}/{key}")

    hook = S3Hook(aws_conn_id=S3_CONN_ID)
    if not hook.check_for_key(key=key, bucket_name=bucket):
        print("Сводный отчёт не найден — переходим в alert_branch")
        return "alert_branch"

    df = read_csv_from_s3(bucket=bucket, key=key)

    if df.empty or "otd_pct" not in df.columns:
        print("Отчёт пуст или не содержит колонки otd_pct — alert_branch")
        return "alert_branch"

    avg_otd = df["otd_pct"].mean()
    print(f"Средний ОТД по всем маршрутам: {avg_otd:.2f}% (порог: {OTD_THRESHOLD}%)")

    if avg_otd < OTD_THRESHOLD:
        print(f"ОТД ниже порога {OTD_THRESHOLD}% — создаём алерт")
        return "alert_branch"
    else:
        print(f"ОТД в норме ({avg_otd:.2f}% >= {OTD_THRESHOLD}%) — стандартная отчётность")
        return "normal_reporting"


def finalize_fn(**context: Any) -> None:
    """Завершающая задача: логирует итог прогона."""
    ds    = context["ds"]
    depot = Variable.get("depot_code")
    print(f"Анализ маршрутов депо {depot} за {ds} завершён.")
    print("Результаты доступны в s3://rzd-airflow-results/")


# ─────────────────────────────────────────────────────────────────────────────
# Определение DAG
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="route_analysis_dag",
    description="ТЧЭ-15: расчёт ОТД по маршрутам с Dynamic Task Mapping из S3",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_tasks=8,
    default_args={
        "retries":           1,
        "retry_delay":       timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["rzd", "dynamic-mapping", "branch", "otd", "module-06"],
) as dag:

    # ── Шаг 1: Ждём файл trips.csv в S3 ─────────────────────────────────────
    wait_for_trips_file = S3KeySensor(
        task_id="wait_for_trips_file",
        bucket_name="{{ var.value.s3_bucket_data }}",
        bucket_key="trips.csv",
        aws_conn_id=S3_CONN_ID,
        poke_interval=300,
        timeout=7200,
        mode="reschedule",
    )

    # ── Шаг 2: Читаем список уникальных маршрутов из trips.csv ───────────────
    @task
    def fetch_routes(**context: Any) -> list[str]:
        """
        Читает trips.csv из S3 через S3Hook.
        Возвращает список уникальных route_id для Dynamic Task Mapping.
        """
        bucket = Variable.get("s3_bucket_data")
        key    = "trips.csv"

        print(f"Чтение маршрутов из s3://{bucket}/{key}")
        df     = read_csv_from_s3(bucket=bucket, key=key)
        routes = df["route_id"].dropna().unique().tolist()

        print(f"Найдено маршрутов: {len(routes)} — {routes}")
        return routes

    # ── Шаг 3: Параллельная обработка каждого маршрута ───────────────────────
    @task
    def process_route(route_id: str, **context: Any) -> dict:
        """
        Обрабатывает один маршрут: читает trips.csv из S3,
        фильтрует по route_id, рассчитывает ОТД, пишет результат в S3.

        Вызывается параллельно для каждого маршрута через expand().
        """
        bucket_data    = Variable.get("s3_bucket_data")
        bucket_results = Variable.get("s3_bucket_results")
        ds_nd          = context["ds_nodash"]
        threshold_min  = int(Variable.get("delay_threshold_min", default_var="15"))

        # Читаем полный файл маршрутов из S3
        print(f"[{route_id}] Чтение trips.csv из s3://{bucket_data}/trips.csv")
        df = read_csv_from_s3(bucket=bucket_data, key="trips.csv")

        # Фильтруем строки по текущему маршруту
        route_df = df[df["route_id"] == route_id].copy()
        if route_df.empty:
            print(f"[{route_id}] Нет данных — пропускаем")
            return {"route_id": route_id, "otd_pct": 0.0, "trips_count": 0}

        # Рассчитываем опоздание в минутах
        route_df["planned_arrival"] = pd.to_datetime(route_df["planned_arrival"])
        route_df["actual_arrival"]  = pd.to_datetime(route_df["actual_arrival"])
        route_df["delay_min"] = (
            (route_df["actual_arrival"] - route_df["planned_arrival"])
            .dt.total_seconds() / 60
        )

        trips_count = len(route_df)
        on_time     = (route_df["delay_min"] <= threshold_min).sum()
        late_count  = trips_count - on_time
        otd_pct     = round((on_time / trips_count) * 100, 2) if trips_count > 0 else 0.0
        avg_delay   = round(route_df["delay_min"].mean(), 2)

        print(
            f"[{route_id}] Рейсов: {trips_count} | "
            f"Вовремя: {on_time} | Опоздали: {late_count} | "
            f"ОТД: {otd_pct}% | Среднее опоздание: {avg_delay} мин"
        )

        result_df = pd.DataFrame([{
            "route_id":      route_id,
            "date":          context["ds"],
            "depot_code":    Variable.get("depot_code"),
            "trips_count":   trips_count,
            "on_time":       int(on_time),
            "late_count":    int(late_count),
            "avg_delay_min": avg_delay,
            "otd_pct":       otd_pct,
        }])

        # Пишем результат маршрута в отдельную папку S3
        result_key = f"routes/{ds_nd}/{route_id}/result.csv"
        print(f"[{route_id}] Запись в s3://{bucket_results}/{result_key}")
        write_csv_to_s3(df=result_df, bucket=bucket_results, key=result_key)

        return {"route_id": route_id, "otd_pct": otd_pct, "trips_count": trips_count}

    # ── Шаг 4: Объединение результатов всех маршрутов ─────────────────────────
    @task(trigger_rule="none_failed_min_one_success")
    def merge_results(**context: Any) -> None:
        """
        Читает все файлы результатов из папки routes/<ds_nodash>/ в S3,
        объединяет в единый DataFrame, записывает сводный отчёт.
        """
        bucket = Variable.get("s3_bucket_results")
        ds_nd  = context["ds_nodash"]
        prefix = f"routes/{ds_nd}/"

        print(f"Поиск результатов с префиксом s3://{bucket}/{prefix}")
        keys = list_keys_in_s3_prefix(bucket=bucket, prefix=prefix)
        result_keys = [k for k in keys if k.endswith("result.csv")]

        if not result_keys:
            raise ValueError(f"Файлы результатов не найдены по префиксу {prefix}")

        print(f"Найдено файлов результатов: {len(result_keys)}")
        frames = []
        for key in result_keys:
            print(f"  Чтение: s3://{bucket}/{key}")
            frames.append(read_csv_from_s3(bucket=bucket, key=key))

        summary_df = pd.concat(frames, ignore_index=True)
        summary_df = summary_df.sort_values("otd_pct", ascending=True)

        avg_otd = summary_df["otd_pct"].mean()
        print(f"\nСводный ОТД по депо: {avg_otd:.2f}%")
        print(summary_df[["route_id", "trips_count", "otd_pct", "avg_delay_min"]].to_string(index=False))

        # Записываем сводный отчёт в S3
        report_key = f"summary/{ds_nd}/report.csv"
        print(f"\nЗапись сводного отчёта в s3://{bucket}/{report_key}")
        write_csv_to_s3(df=summary_df, bucket=bucket, key=report_key)

    # ── Шаг 5: Ветвление по значению ОТД ────────────────────────────────────
    check_otd = BranchPythonOperator(
        task_id="check_otd_threshold",
        python_callable=check_otd_threshold_fn,
    )

    # ── Шаг 6а: Ветка АЛЕРТ — ОТД ниже порога 90% ────────────────────────────
    @task
    def alert_branch(**context: Any) -> None:
        """
        ОТД ниже порога: логирует предупреждение,
        записывает алерт в s3://rzd-airflow-results/alerts/<ds_nodash>/alert.csv.
        """
        bucket = Variable.get("s3_bucket_results")
        ds_nd  = context["ds_nodash"]

        # Читаем сводный отчёт для получения деталей
        report_key = f"summary/{ds_nd}/report.csv"
        hook = S3Hook(aws_conn_id=S3_CONN_ID)

        avg_otd = None
        worst_route = None
        if hook.check_for_key(key=report_key, bucket_name=bucket):
            df = read_csv_from_s3(bucket=bucket, key=report_key)
            if not df.empty and "otd_pct" in df.columns:
                avg_otd     = round(df["otd_pct"].mean(), 2)
                worst_route = df.loc[df["otd_pct"].idxmin(), "route_id"]

        print(f"АЛЕРТ: Средний ОТД = {avg_otd}% (ниже порога {OTD_THRESHOLD}%)")
        print(f"Наиболее проблемный маршрут: {worst_route}")

        alert_df = pd.DataFrame([{
            "date":        context["ds"],
            "depot_code":  Variable.get("depot_code"),
            "avg_otd_pct": avg_otd,
            "threshold":   OTD_THRESHOLD,
            "worst_route": worst_route,
            "action":      "Направить уведомление начальнику депо. Провести разбор опозданий.",
        }])

        alert_key = f"alerts/{ds_nd}/alert.csv"
        print(f"Запись алерта в s3://{bucket}/{alert_key}")
        write_csv_to_s3(df=alert_df, bucket=bucket, key=alert_key)

    # ── Шаг 6б: Ветка НОРМА — ОТД в норме ───────────────────────────────────
    @task
    def normal_reporting(**context: Any) -> None:
        """
        ОТД в норме: логирует успех,
        записывает статус в s3://rzd-airflow-results/status/<ds_nodash>/ok.csv.
        """
        bucket = Variable.get("s3_bucket_results")
        ds_nd  = context["ds_nodash"]

        report_key = f"summary/{ds_nd}/report.csv"
        hook = S3Hook(aws_conn_id=S3_CONN_ID)

        avg_otd = None
        if hook.check_for_key(key=report_key, bucket_name=bucket):
            df = read_csv_from_s3(bucket=bucket, key=report_key)
            if not df.empty and "otd_pct" in df.columns:
                avg_otd = round(df["otd_pct"].mean(), 2)

        print(f"ОТД в норме: {avg_otd}% >= {OTD_THRESHOLD}%")

        status_df = pd.DataFrame([{
            "date":        context["ds"],
            "depot_code":  Variable.get("depot_code"),
            "avg_otd_pct": avg_otd,
            "status":      "OK",
        }])

        status_key = f"status/{ds_nd}/ok.csv"
        print(f"Запись статуса OK в s3://{bucket}/{status_key}")
        write_csv_to_s3(df=status_df, bucket=bucket, key=status_key)

    # ── Шаг 7: Финализация ───────────────────────────────────────────────────
    finalize = PythonOperator(
        task_id="finalize",
        python_callable=finalize_fn,
        trigger_rule="none_failed_min_one_success",
    )

    # ─────────────────────────────────────────────────────────────────────────
    # Сборка зависимостей DAG
    # ─────────────────────────────────────────────────────────────────────────

    routes      = fetch_routes()
    route_tasks = process_route.expand(route_id=routes)
    merged      = merge_results()
    alert_task  = alert_branch()
    normal_task = normal_reporting()

    # Цепочка:
    # wait → fetch_routes → process_route[N] → merge_results → check_otd
    # check_otd → alert_branch → finalize
    # check_otd → normal_reporting → finalize
    wait_for_trips_file >> routes
    route_tasks >> merged >> check_otd
    check_otd >> [alert_task, normal_task]
    [alert_task, normal_task] >> finalize
```

---

## Деплой и тестирование

### Загрузка DAG в Object Storage

**Через Yandex Cloud CLI:**

```bash
yc storage cp route_analysis_dag.py s3://rzd-airflow-dags/dags/route_analysis_dag.py
```

**Через Yandex Cloud Console:**

1. Откройте Object Storage → бакет `rzd-airflow-dags`
2. Перейдите в папку `dags/`
3. Нажмите **Загрузить** → выберите файл `route_analysis_dag.py`

### Проверка в Airflow UI

1. Откройте Airflow Web UI → вкладка **DAGs**
2. Подождите 1–3 минуты (Scheduler сканирует бакет с интервалом)
3. Найдите `route_analysis_dag` в списке. Если DAG не появился — проверьте раздел **Import Errors** на главной странице

Диагностика ошибок импорта:
- Нажмите на красный значок Import Errors
- Убедитесь, что провайдер `apache-airflow-providers-amazon` установлен в кластере
- Проверьте правильность Connection `yandex_s3`

### Запуск и наблюдение

1. Включите DAG (Toggle ON)
2. Нажмите **Trigger DAG** → **Trigger**
3. Откройте **Graph View** для наблюдения за ходом выполнения

### Ожидаемый результат

| Задача | Ожидаемый статус |
|---|---|
| `wait_for_trips_file` | success (файл найден в S3) |
| `fetch_routes` | success, XCom содержит список маршрутов |
| `process_route[0..N]` | success, помечена `[N mapped tasks]` |
| `merge_results` | success, создан файл `summary/<ds>/report.csv` |
| `check_otd_threshold` | success, выбирает одну ветку |
| `alert_branch` или `normal_reporting` | success (одна), skipped (другая) |
| `finalize` | success всегда |

После успешного выполнения DAG в бакете `rzd-airflow-results` появятся файлы:

```
rzd-airflow-results/
├── routes/<ds_nodash>/
│   ├── route_001/result.csv
│   ├── route_002/result.csv
│   └── ...
├── summary/<ds_nodash>/
│   └── report.csv
└── alerts/<ds_nodash>/alert.csv  (или status/<ds_nodash>/ok.csv)
```

---

## Задания повышенной сложности

### Задание 1: Чтение списка маршрутов из Airflow Variable

Измените `fetch_routes()` так, чтобы список маршрутов для обработки читался из переменной Airflow, а не вычислялся из всего `trips.csv`. Это позволит оперативно исключать маршруты без изменения кода DAG.

Шаги:
1. В Airflow UI → Admin → Variables → создайте переменную `active_routes` со значением `["route_001", "route_002", "route_003"]`
2. Измените `fetch_routes()`:

```python
import json
from airflow.models import Variable

@task
def fetch_routes(**context):
    raw_var = Variable.get("active_routes", default_var=None)
    if raw_var:
        routes = json.loads(raw_var)
        print(f"Маршруты из Variable: {routes}")
        return routes

    # Fallback: читаем из S3
    bucket = Variable.get("s3_bucket_data")
    df     = read_csv_from_s3(bucket=bucket, key="trips.csv")
    routes = df["route_id"].dropna().unique().tolist()
    print(f"Маршруты из S3: {routes}")
    return routes
```

3. Добавьте новый маршрут в переменную через UI — без изменения кода DAG количество mapped-задач должно измениться.

### Задание 2: Ограничение параллелизма через пул Airflow

Задачи `process_route` могут нагружать S3 при большом числе маршрутов. Ограничьте одновременное выполнение через Airflow Pool.

Шаги:
1. Airflow UI → Admin → Pools → **+ Add a new record**:
   - Pool Name: `s3_read_pool`
   - Slots: `3`
2. Добавьте параметр `pool` в декоратор задачи:

```python
@task(pool="s3_read_pool")
def process_route(route_id: str, **context) -> dict:
    ...
```

3. Запустите DAG и в разделе **Grid View** убедитесь, что одновременно выполняются не более 3 экземпляров `process_route`.

### Задание 3: Сохранение сводного отчёта в PostgreSQL

Дополните задачу `merge_results()` записью итоговых данных в таблицу `rzd_analytics.route_performance` через `PostgresHook`.

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

@task(trigger_rule="none_failed_min_one_success")
def merge_results(**context):
    # ... (существующий код чтения и объединения) ...

    # Запись в PostgreSQL через Managed PostgreSQL (rzd_postgres Connection)
    pg_hook = PostgresHook(postgres_conn_id="rzd_postgres")
    conn    = pg_hook.get_conn()
    cursor  = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS route_performance (
            id           SERIAL PRIMARY KEY,
            route_id     TEXT,
            date         DATE,
            depot_code   TEXT,
            trips_count  INTEGER,
            on_time      INTEGER,
            late_count   INTEGER,
            avg_delay_min NUMERIC(6,2),
            otd_pct      NUMERIC(5,2),
            loaded_at    TIMESTAMP DEFAULT NOW()
        )
    """)

    for _, row in summary_df.iterrows():
        cursor.execute("""
            INSERT INTO route_performance
                (route_id, date, depot_code, trips_count, on_time, late_count, avg_delay_min, otd_pct)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row["route_id"],
            row["date"],
            row["depot_code"],
            int(row["trips_count"]),
            int(row["on_time"]),
            int(row["late_count"]),
            float(row["avg_delay_min"]),
            float(row["otd_pct"]),
        ))

    conn.commit()
    cursor.close()
    print(f"Записано {len(summary_df)} строк в rzd_analytics.route_performance")
```

---

*Лабораторная работа №06 | Модуль 06: Условные потоки и параллельная обработка | Депо ТЧЭ-15 Новосибирск | Apache Airflow 2.8 | Yandex Managed Service for Apache Airflow*
