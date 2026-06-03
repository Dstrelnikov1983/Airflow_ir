# Практическая работа №06: Ветвление и Dynamic Mapping с данными из Object Storage

**Модуль:** 06 — Условные потоки и параллельная обработка
**Продолжительность:** 45–60 минут
**Платформа:** Yandex Managed Service for Apache Airflow
**Контекст:** Депо ТЧЭ-15 Новосибирск — анализ показаний датчиков локомотивов

---

## Цель и задачи

**Цель:** Разработать DAG, который читает данные о состоянии локомотивов из Yandex Object Storage, принимает решения о маршруте обработки на основе температуры букс и параллельно обрабатывает каждый маршрут через Dynamic Task Mapping.

**Задачи:**

1. Настроить подключение к Yandex Object Storage через Connection `yandex_s3` в Airflow UI
2. Реализовать чтение CSV-файлов из S3 через `S3Hook` (без локальной файловой системы)
3. Применить `BranchPythonOperator` для ветвления по максимальной температуре букс
4. Использовать `expand()` для параллельной обработки маршрутов из `trips.csv`
5. Записывать результаты каждой ветки в отдельную папку S3 через `hook.load_string()`

---

## Необходимые ресурсы

| Ресурс | Описание |
|---|---|
| Yandex Managed Airflow | Кластер запущен, Web UI доступен |
| Yandex Object Storage | Бакеты `rzd-airflow-data`, `rzd-airflow-results`, `rzd-airflow-dags` |
| Managed PostgreSQL | Кластер `rzd_analytics`, доступен по FQDN |
| Сервисный аккаунт | Роли `storage.viewer` + `storage.uploader` + `managed-airflow.integrationProvider` |
| CSV-файлы датасета | `sensor_readings.csv`, `trips.csv`, `locomotives.csv` загружены в `rzd-airflow-data` |

---

## Подготовка Object Storage

### Шаг 1. Создание бакетов через Yandex Cloud Console

1. Откройте [console.cloud.yandex.ru](https://console.cloud.yandex.ru) → **Object Storage**
2. Нажмите **Создать бакет** и создайте три бакета:

| Бакет | Назначение | Доступ |
|---|---|---|
| `rzd-airflow-dags` | DAG-файлы (связан с Managed Airflow) | Приватный |
| `rzd-airflow-data` | Входные CSV-файлы датасета | Приватный |
| `rzd-airflow-results` | Результаты обработки | Приватный |

> Имена бакетов должны быть уникальны глобально. При необходимости добавьте суффикс, например `rzd-airflow-data-tceh15`.

### Шаг 2. Загрузка CSV-файлов датасета

Загрузите файлы в бакет `rzd-airflow-data` через консоль или CLI:

```bash
# Через Yandex Cloud CLI
yc storage cp sensor_readings.csv     s3://rzd-airflow-data/sensor_readings.csv
yc storage cp trips.csv               s3://rzd-airflow-data/trips.csv
yc storage cp locomotives.csv         s3://rzd-airflow-data/locomotives.csv
yc storage cp schedule_adherence.csv  s3://rzd-airflow-data/schedule_adherence.csv
yc storage cp maintenance.csv         s3://rzd-airflow-data/maintenance.csv
```

Ожидаемая структура бакета `rzd-airflow-data`:

```
rzd-airflow-data/
├── sensor_readings.csv
├── trips.csv
├── locomotives.csv
├── schedule_adherence.csv
└── maintenance.csv
```

### Шаг 3. Создание сервисного аккаунта

1. В Yandex Cloud Console → **IAM** → **Сервисные аккаунты** → **Создать**
2. Имя: `airflow-s3-sa`
3. Назначьте роли:
   - `storage.viewer` — чтение из бакетов
   - `storage.uploader` — запись в бакеты
   - `managed-airflow.integrationProvider` — интеграция с Managed Airflow

### Шаг 4. Создание ключей доступа для S3

1. Перейдите в сервисный аккаунт `airflow-s3-sa`
2. Вкладка **Ключи доступа** → **Создать статический ключ**
3. Сохраните **Access Key ID** и **Secret Access Key** — они понадобятся для Connection

> Ключи отображаются только один раз. Сохраните их в надёжном месте или в Yandex Lockbox.

---

## Настройка Airflow Connections и Variables

### Connection: yandex_s3

Откройте Airflow UI → **Admin** → **Connections** → **+ Add a new record**:

| Поле | Значение |
|---|---|
| Conn Id | `yandex_s3` |
| Conn Type | `Amazon Web Services` |
| Login | `<Access Key ID сервисного аккаунта>` |
| Password | `<Secret Access Key>` |
| Extra | `{"endpoint_url": "https://storage.yandexcloud.net", "region_name": "ru-central1"}` |

Нажмите **Save**, затем **Test** — должно появиться сообщение `Connection successfully tested`.

### Connection: rzd_postgres

Откройте Airflow UI → **Admin** → **Connections** → **+ Add a new record**:

| Поле | Значение |
|---|---|
| Conn Id | `rzd_postgres` |
| Conn Type | `Postgres` |
| Host | `<FQDN кластера>.mdb.yandexcloud.net` |
| Schema | `rzd_analytics` |
| Login | `airflow_user` |
| Password | `<пароль из Yandex Lockbox>` |
| Port | `6432` |

> FQDN кластера PostgreSQL можно найти в Yandex Cloud Console → Managed Service for PostgreSQL → ваш кластер → вкладка **Хосты**.

### Variables

Откройте Airflow UI → **Admin** → **Variables** → **+ Add a new record**:

| Key | Value | Описание |
|---|---|---|
| `s3_bucket_data` | `rzd-airflow-data` | Бакет с входными данными |
| `s3_bucket_results` | `rzd-airflow-results` | Бакет для результатов |
| `depot_code` | `TCH-15` | Код депо |
| `delay_threshold_min` | `15` | Порог опоздания в минутах |

---

## Деплой DAG-файла в Managed Airflow

### Метод 1: Через Yandex Cloud Console

1. Откройте Managed Service for Apache Airflow → ваш кластер
2. Перейдите в раздел **DAG-файлы**
3. Убедитесь, что бакет `rzd-airflow-dags` подключён к кластеру
4. Перейдите в Object Storage → `rzd-airflow-dags`
5. Нажмите **Загрузить** → выберите файл `branch_mapping_dag.py`

### Метод 2: Через Yandex Cloud CLI

```bash
yc storage cp branch_mapping_dag.py s3://rzd-airflow-dags/dags/branch_mapping_dag.py
```

### Проверка появления DAG

1. Откройте Airflow UI → вкладка **DAGs**
2. Подождите 1–3 минуты (Airflow сканирует бакет с интервалом)
3. Найдите `rzd_branch_mapping` в списке
4. Если DAG не появился — проверьте **Import Errors** на главной странице UI

> Никогда не используйте `airflow dags`, SSH, SCP или локальную папку `dags/` — в Managed Airflow прямой доступ к файловой системе отсутствует.

---

## Шаги выполнения

### Шаг 1. Изучить структуру входных данных

Перед написанием DAG убедитесь, что CSV-файлы загружены корректно. Проверьте содержимое через Airflow UI → Admin → Connections → Test для `yandex_s3`, либо запустите тестовый DAG с задачей `S3ListOperator`.

Ожидаемые колонки файлов:

**sensor_readings.csv:**
```
reading_id, locomotive_id, timestamp, axle_box_temp_1, axle_box_temp_2,
axle_box_temp_3, axle_box_temp_4, engine_temp, oil_pressure, vibration_level
```

**trips.csv:**
```
trip_id, locomotive_id, route_id, departure_station, arrival_station,
planned_departure, actual_departure, planned_arrival, actual_arrival, status
```

### Шаг 2. Загрузить DAG-файл в бакет

Сохраните код из раздела **Полный код DAG** в файл `branch_mapping_dag.py` и загрузите в бакет `rzd-airflow-dags/dags/`.

### Шаг 3. Включить DAG и выполнить тестовый запуск

1. В Airflow UI найдите `rzd_branch_mapping`
2. Переключите Toggle в положение **ON**
3. Нажмите **Trigger DAG** → **Trigger** (без параметров)
4. Откройте **Graph View** и наблюдайте выполнение задач

### Шаг 4. Проверить ветвление по температуре букс

1. В Graph View найдите задачу `check_axle_temp`
2. Одна из трёх веток (`normal_branch`, `warning_branch`, `critical_branch`) будет **success** (зелёная)
3. Остальные ветки будут **skipped** (серые)
4. Нажмите на активную ветку → **Log** — убедитесь, что в логах видно решение о маршруте

### Шаг 5. Проверить Dynamic Task Mapping

1. Найдите задачу `process_route` в Graph View
2. Она должна отображаться как `process_route [N mapped tasks]`, где N — число уникальных маршрутов из `trips.csv`
3. Нажмите на задачу → откроется список отдельных экземпляров по маршрутам
4. Каждый экземпляр выполняется независимо и параллельно

### Шаг 6. Проверить результаты в Object Storage

1. Откройте Yandex Cloud Console → Object Storage → `rzd-airflow-results`
2. Убедитесь, что созданы папки:
   - `normal/` — результаты нормального маршрута
   - `warning/` — результаты при предупреждении
   - `critical/` — результаты при критическом состоянии
3. В каждой папке должен быть CSV-файл с результатами за текущую дату

### Шаг 7. Проверить переменные и XCom

1. Airflow UI → **Admin** → **XComs** — найдите записи для `dag_id=rzd_branch_mapping`
2. Убедитесь, что задача `fetch_sensor_data` записала `max_axle_temp` в XCom
3. Задача `fetch_routes` должна иметь XCom-запись со списком маршрутов

---

## Полный код DAG

Сохраните файл как `branch_mapping_dag.py`:

```python
"""
DAG: rzd_branch_mapping
Депо ТЧЭ-15 Новосибирск — ветвление по температуре букс
и параллельная обработка маршрутов из Object Storage.

Все операции с файлами — через S3Hook (Yandex Object Storage).
Прямой доступ к локальной файловой системе не используется.
"""

from __future__ import annotations

from io import StringIO
from datetime import datetime, timedelta

import pandas as pd

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# ─────────────────────────────────────────────────────────────────────────────
# Константы
# ─────────────────────────────────────────────────────────────────────────────

S3_CONN_ID       = "yandex_s3"
TEMP_WARN_LIMIT  = 80.0   # °C — порог предупреждения температуры буксы
TEMP_CRIT_LIMIT  = 95.0   # °C — критический порог


# ─────────────────────────────────────────────────────────────────────────────
# Вспомогательные функции работы с S3
# ─────────────────────────────────────────────────────────────────────────────

def read_csv_from_s3(bucket: str, key: str, conn_id: str = S3_CONN_ID) -> pd.DataFrame:
    """
    Читает CSV-файл из Yandex Object Storage через S3Hook.
    Прямой доступ к локальной файловой системе НЕ используется.
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
    Записывает DataFrame в CSV-файл в Yandex Object Storage через S3Hook.
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


# ─────────────────────────────────────────────────────────────────────────────
# Функции задач
# ─────────────────────────────────────────────────────────────────────────────

def fetch_sensor_data(**context) -> None:
    """
    Читает sensor_readings.csv из S3, вычисляет максимальную температуру букс.
    Сохраняет результат в XCom для задачи ветвления.
    """
    bucket = Variable.get("s3_bucket_data")
    key    = "sensor_readings.csv"

    print(f"Чтение показаний датчиков: s3://{bucket}/{key}")
    df = read_csv_from_s3(bucket=bucket, key=key)

    temp_cols = [c for c in df.columns if "axle_box_temp" in c]
    if not temp_cols:
        raise ValueError(f"Колонки температуры букс не найдены. Колонки: {df.columns.tolist()}")

    max_temp = df[temp_cols].max().max()
    print(f"Максимальная температура буксы: {max_temp:.1f} °C (порог предупреждения: {TEMP_WARN_LIMIT}°C)")

    context["ti"].xcom_push(key="max_axle_temp", value=float(max_temp))


def decide_temp_branch(**context) -> str:
    """
    BranchPythonOperator: возвращает task_id ветки на основе температуры.
    """
    max_temp = context["ti"].xcom_pull(
        task_ids="fetch_sensor_data",
        key="max_axle_temp",
    )

    print(f"Решение о ветвлении: max_axle_temp = {max_temp:.1f} °C")

    if max_temp is None:
        print("Данные температуры отсутствуют — аварийный алерт")
        return "critical_branch"
    elif max_temp >= TEMP_CRIT_LIMIT:
        print(f"КРИТИЧНО: {max_temp:.1f} >= {TEMP_CRIT_LIMIT} °C — немедленное ТО")
        return "critical_branch"
    elif max_temp >= TEMP_WARN_LIMIT:
        print(f"ПРЕДУПРЕЖДЕНИЕ: {max_temp:.1f} >= {TEMP_WARN_LIMIT} °C — плановый осмотр")
        return "warning_branch"
    else:
        print(f"НОРМА: {max_temp:.1f} °C — стандартная обработка")
        return "normal_branch"


def handle_normal(**context) -> None:
    """Ветка НОРМА: сохраняет статус в папку normal/ бакета результатов."""
    bucket  = Variable.get("s3_bucket_results")
    ds_nd   = context["ds_nodash"]
    key     = f"normal/{ds_nd}/status.csv"

    max_temp = context["ti"].xcom_pull(task_ids="fetch_sensor_data", key="max_axle_temp")
    result_df = pd.DataFrame([{
        "depot_code": Variable.get("depot_code"),
        "date":       context["ds"],
        "max_axle_temp": max_temp,
        "status":     "NORMAL",
        "action":     "Стандартная обработка, ТО по расписанию",
    }])

    print(f"Запись результата НОРМА в s3://{bucket}/{key}")
    write_csv_to_s3(df=result_df, bucket=bucket, key=key)


def handle_warning(**context) -> None:
    """Ветка ПРЕДУПРЕЖДЕНИЕ: сохраняет статус в папку warning/."""
    bucket  = Variable.get("s3_bucket_results")
    ds_nd   = context["ds_nodash"]
    key     = f"warning/{ds_nd}/status.csv"

    max_temp = context["ti"].xcom_pull(task_ids="fetch_sensor_data", key="max_axle_temp")
    result_df = pd.DataFrame([{
        "depot_code": Variable.get("depot_code"),
        "date":       context["ds"],
        "max_axle_temp": max_temp,
        "status":     "WARNING",
        "action":     "Направить локомотив на внеплановый осмотр буксового узла",
    }])

    print(f"Запись результата ПРЕДУПРЕЖДЕНИЕ в s3://{bucket}/{key}")
    write_csv_to_s3(df=result_df, bucket=bucket, key=key)


def handle_critical(**context) -> None:
    """Ветка КРИТИЧНО: сохраняет статус в папку critical/ и логирует аварию."""
    bucket  = Variable.get("s3_bucket_results")
    ds_nd   = context["ds_nodash"]
    key     = f"critical/{ds_nd}/status.csv"

    max_temp = context["ti"].xcom_pull(task_ids="fetch_sensor_data", key="max_axle_temp")
    result_df = pd.DataFrame([{
        "depot_code": Variable.get("depot_code"),
        "date":       context["ds"],
        "max_axle_temp": max_temp,
        "status":     "CRITICAL",
        "action":     "НЕМЕДЛЕННАЯ ОСТАНОВКА. Осмотр буксового узла. Уведомить ревизора.",
    }])

    print(f"КРИТИЧЕСКИЙ АЛЕРТ: max_axle_temp = {max_temp:.1f} °C")
    print(f"Запись аварийного статуса в s3://{bucket}/{key}")
    write_csv_to_s3(df=result_df, bucket=bucket, key=key)


def fetch_routes(**context) -> list[str]:
    """
    Читает trips.csv из S3 и возвращает список уникальных маршрутов.
    Результат используется для Dynamic Task Mapping (expand).
    """
    bucket = Variable.get("s3_bucket_data")
    key    = "trips.csv"

    print(f"Чтение маршрутов: s3://{bucket}/{key}")
    df     = read_csv_from_s3(bucket=bucket, key=key)
    routes = df["route_id"].dropna().unique().tolist()

    print(f"Найдено уникальных маршрутов: {len(routes)} — {routes}")
    return routes


def process_route(route_id: str, **context) -> None:
    """
    Обрабатывает данные одного маршрута: фильтрует trips.csv,
    рассчитывает среднее опоздание, записывает результат в S3.
    Вызывается параллельно для каждого маршрута через expand().
    """
    bucket_data    = Variable.get("s3_bucket_data")
    bucket_results = Variable.get("s3_bucket_results")
    ds_nd          = context["ds_nodash"]
    threshold      = int(Variable.get("delay_threshold_min", default_var="15"))

    print(f"[{route_id}] Чтение данных маршрута из s3://{bucket_data}/trips.csv")
    df = read_csv_from_s3(bucket=bucket_data, key="trips.csv")

    # Фильтруем по маршруту
    route_df = df[df["route_id"] == route_id].copy()
    if route_df.empty:
        print(f"[{route_id}] Данных нет — пропускаем")
        return

    # Рассчитываем опоздание в минутах
    route_df["planned_arrival"] = pd.to_datetime(route_df["planned_arrival"])
    route_df["actual_arrival"]  = pd.to_datetime(route_df["actual_arrival"])
    route_df["delay_min"] = (
        (route_df["actual_arrival"] - route_df["planned_arrival"])
        .dt.total_seconds() / 60
    )

    avg_delay   = route_df["delay_min"].mean()
    trips_count = len(route_df)
    late_trips  = (route_df["delay_min"] > threshold).sum()

    print(f"[{route_id}] Рейсов: {trips_count} | Среднее опоздание: {avg_delay:.1f} мин | Опоздавших: {late_trips}")

    result_df = pd.DataFrame([{
        "route_id":      route_id,
        "date":          context["ds"],
        "depot_code":    Variable.get("depot_code"),
        "trips_count":   trips_count,
        "avg_delay_min": round(avg_delay, 2),
        "late_trips":    int(late_trips),
        "otd_pct":       round((1 - late_trips / trips_count) * 100, 2) if trips_count > 0 else 0,
    }])

    # Каждый маршрут пишет в свою папку
    result_key = f"routes/{ds_nd}/{route_id}/summary.csv"
    print(f"[{route_id}] Запись результата в s3://{bucket_results}/{result_key}")
    write_csv_to_s3(df=result_df, bucket=bucket_results, key=result_key)


def finalize(**context) -> None:
    """
    Завершающая задача: выполняется после всех веток и маппинга.
    Логирует итог прогона.
    """
    ds = context["ds"]
    depot = Variable.get("depot_code")
    print(f"Прогон за {ds} завершён. Депо {depot}.")
    print("Результаты записаны в s3://rzd-airflow-results/")


# ─────────────────────────────────────────────────────────────────────────────
# Определение DAG
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="rzd_branch_mapping",
    description="ТЧЭ-15: ветвление по температуре букс + parallel mapping маршрутов",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries":         1,
        "retry_delay":     timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["rzd", "branch", "dynamic-mapping", "module-06"],
) as dag:

    # ── Сенсор: ждём появления файла sensor_readings.csv в S3 ────────────────
    wait_for_sensors = S3KeySensor(
        task_id="wait_for_sensor_data",
        bucket_name="{{ var.value.s3_bucket_data }}",
        bucket_key="sensor_readings.csv",
        aws_conn_id=S3_CONN_ID,
        poke_interval=60,
        timeout=1800,
        mode="reschedule",
    )

    # ── Задача 1: читаем данные датчиков из S3 ────────────────────────────────
    from airflow.operators.python import PythonOperator

    read_sensors = PythonOperator(
        task_id="fetch_sensor_data",
        python_callable=fetch_sensor_data,
    )

    # ── Задача 2: ветвление по температуре буксы ──────────────────────────────
    branch = BranchPythonOperator(
        task_id="check_axle_temp",
        python_callable=decide_temp_branch,
    )

    # ── Ветки обработки ───────────────────────────────────────────────────────
    normal_task   = PythonOperator(task_id="normal_branch",   python_callable=handle_normal)
    warning_task  = PythonOperator(task_id="warning_branch",  python_callable=handle_warning)
    critical_task = PythonOperator(task_id="critical_branch", python_callable=handle_critical)

    # ── Задача 3: читаем список маршрутов ─────────────────────────────────────
    @task(trigger_rule="none_failed_min_one_success")
    def get_routes(**context) -> list[str]:
        return fetch_routes(**context)

    # ── Задача 4: обработка каждого маршрута (Dynamic Task Mapping) ───────────
    @task(trigger_rule="none_failed_min_one_success")
    def handle_route(route_id: str, **context) -> None:
        process_route(route_id=route_id, **context)

    # ── Задача 5: финализация ─────────────────────────────────────────────────
    done = PythonOperator(
        task_id="finalize",
        python_callable=finalize,
        trigger_rule="none_failed_min_one_success",
    )

    # ── Зависимости ───────────────────────────────────────────────────────────
    routes_list = get_routes()

    wait_for_sensors >> read_sensors >> branch
    branch >> [normal_task, warning_task, critical_task]
    [normal_task, warning_task, critical_task] >> routes_list

    # Dynamic Task Mapping: process_route запускается параллельно по каждому маршруту
    mapped_routes = handle_route.expand(route_id=routes_list)
    mapped_routes >> done
```

---

## Контрольные вопросы

1. **Почему нельзя использовать `pd.read_csv("sensor_readings.csv")` в Managed Airflow?**
   Объясните, что происходит с локальной файловой системой при горизонтальном масштабировании воркеров.

2. **Что вернёт `decide_temp_branch()`, если `max_axle_temp = 88.5`? Какая задача выполнится, а какие получат статус SKIPPED?**

3. **Почему задача `finalize` имеет `trigger_rule="none_failed_min_one_success"`? Что произойдёт, если убрать этот параметр?**

4. **Сколько параллельных экземпляров задачи `handle_route` создастся, если `trips.csv` содержит 7 уникальных маршрутов?**

5. **В чём разница между `mode='reschedule'` и `mode='poke'` у `S3KeySensor`? Какой режим предпочтительнее в Managed Airflow и почему?**

---

*Практическая работа №06 | Модуль 06: Условные потоки и параллельная обработка | Депо ТЧЭ-15 Новосибирск | Apache Airflow 2.8 | Yandex Managed Service for Apache Airflow*
