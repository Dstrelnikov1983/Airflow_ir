# Лабораторная работа №03: Посменный отчёт по опозданиям: backfill из Object Storage

**Организация:** РЖД, Западно-Сибирская дирекция тяги, депо Новосибирск-Главный (ТЧЭ-15)
**Платформа:** Yandex Managed Service for Apache Airflow™ + Yandex Object Storage
**Продолжительность:** 60–90 минут
**Уровень:** Средний

---

## Цель

Разработать production-ориентированный DAG `shift_delay_report` для аналитической платформы ТЧЭ-15, который:

- Читает `schedule_adherence.csv` из Yandex Object Storage (S3) за текущую смену — без доступа к локальной файловой системе.
- Рассчитывает средние опоздания по маршрутам и метрики OTD.
- Записывает посменный отчёт в `rzd-airflow-results/shift_reports/` через `S3Hook.load_string()`.
- Корректно обрабатывает backfill за 7 дней (данные уже в Object Storage).
- Является идемпотентным: повторный запуск за ту же смену перезаписывает файл отчёта.

---

## Предварительные условия

- Managed Airflow запущен и доступен (адрес предоставлен преподавателем).
- Бакеты `rzd-airflow-data`, `rzd-airflow-dags`, `rzd-airflow-results` созданы (из практики №03).
- Файл `schedule_adherence.csv` загружен в бакет `rzd-airflow-data/`.
- Connection `yandex_s3` настроен в Airflow UI:
  - Conn Type: `Amazon Web Services`
  - Login: Access Key ID сервисного аккаунта
  - Password: Secret Access Key
  - Extra: `{"endpoint_url": "https://storage.yandexcloud.net", "region_name": "ru-central1"}`
- Connection `rzd_postgres` настроен (Managed PostgreSQL, база `rzd_analytics`).
- Variables `s3_bucket_data`, `s3_bucket_results`, `delay_threshold_min` созданы.

---

## Задание

### Шаг 1. Проверьте наличие данных в Object Storage

Откройте Yandex Cloud Console → Object Storage → бакет `rzd-airflow-data`.
Убедитесь, что файл `schedule_adherence.csv` присутствует.

Структура CSV-файла:

```
trip_date,shift_number,train_number,route_from,route_to,scheduled_arr,actual_arr,delay_minutes
2024-03-01,1,2703,Новосибирск-Гл,Барнаул,2024-03-01 02:30:00,2024-03-01 02:45:00,15.0
2024-03-01,2,7501,Инская,Алтайская,2024-03-01 11:00:00,2024-03-01 11:42:00,42.0
...
```

Если файл ещё не загружен — используйте Yandex Cloud Console или CLI:

```bash
yc storage cp data/schedule_adherence.csv s3://rzd-airflow-data/schedule_adherence.csv
```

### Шаг 2. Создайте DAG-файл

Разработайте файл `shift_delay_report_dag.py` согласно коду в разделе «Полный код DAG».

Ключевые требования:
- Расписание: `schedule="0 0,8,16 * * *"` (НСК), или `"0 17,1,9 * * *"` (UTC).
- Чтение данных: **только через** `S3Hook` — функция `read_csv_from_s3(bucket, key)`.
- Запись отчётов: **только через** `hook.load_string()` в бакет `rzd-airflow-results`.

### Шаг 3. Разместите DAG в Object Storage

```bash
yc storage cp shift_delay_report_dag.py \
    s3://rzd-airflow-dags/dags/shift_delay_report_dag.py
```

Через Yandex Cloud Console: бакет `rzd-airflow-dags` → **Загрузить объекты** → выбрать файл.

### Шаг 4. Проверьте DAG в Airflow UI

1. Откройте Airflow UI → список DAG. Подождите 1–3 минуты.
2. Убедитесь, что `shift_delay_report` появился без ошибок импорта.
3. Проверьте: **Admin → Import Errors** — список должен быть пустым.

### Шаг 5. Протестируйте отдельную задачу

В Airflow UI выберите DAG `shift_delay_report` → **Graph View** → задача `read_schedule_adherence` → **Clear** (запуск в режиме test).

Убедитесь в логах: задача успешно прочитала `schedule_adherence.csv` из S3.

### Шаг 6. Выполните backfill за 2024-03-01 — 2024-03-07

Данные за эту неделю уже находятся в `rzd-airflow-data/`. Запустите backfill:

```bash
airflow dags backfill \
    --dag-id shift_delay_report \
    --start-date 2024-03-01 \
    --end-date   2024-03-07 \
    --reset-dagruns
```

Ожидаемый результат: 21 DAG run (7 дней × 3 смены в сутки).

### Шаг 7. Проверьте результаты в Object Storage

1. Откройте бакет `rzd-airflow-results` → папку `shift_reports/`.
2. Убедитесь, что появились файлы вида:

```
rzd-airflow-results/
└── shift_reports/
    ├── 20240301_shift1_<run_id>.csv
    ├── 20240301_shift2_<run_id>.csv
    ├── 20240301_shift3_<run_id>.csv
    ├── 20240302_shift1_<run_id>.csv
    └── ...
```

3. Скачайте один из файлов и проверьте содержимое — должны присутствовать столбцы `route`, `total_trips`, `avg_delay_min`, `otd_pct`.

### Шаг 8. Проверьте идемпотентность

Запустите backfill за одну дату повторно:

```bash
airflow dags backfill \
    --dag-id shift_delay_report \
    --start-date 2024-03-03 \
    --end-date   2024-03-03 \
    --reset-dagruns
```

Ожидаемый результат: файлы отчётов за 2024-03-03 перезаписаны (не задвоены), так как `write_csv_to_s3` использует `replace=True`.

### Шаг 9. Проверьте Grid View в Airflow UI

1. Откройте DAG `shift_delay_report` → вкладка **Grid**.
2. Убедитесь: все 21 run зелёные.
3. Кликните на один run → задача `calculate_route_delays` → вкладка **XCom**: проверьте метрики (`avg_delay_min`, `otd_pct`, `route_stats`).

---

## Полный код DAG

Файл: `shift_delay_report_dag.py`

```python
"""
DAG: shift_delay_report
Назначение: посменный отчёт по опозданиям на маршрутах ТЧЭ-15
Расписание: 0 0,8,16 * * * (НСК) = 0 17,1,9 * * * (UTC)
Источник данных: rzd-airflow-data/schedule_adherence.csv (Yandex Object Storage)
Результаты: rzd-airflow-results/shift_reports/ (Yandex Object Storage)
Платформа: Yandex Managed Airflow — локальная файловая система НЕ используется
"""

from __future__ import annotations

import logging
from datetime import timedelta
from io import StringIO

import pandas as pd
import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

log = logging.getLogger(__name__)

NSK_TZ     = pendulum.timezone("Asia/Novosibirsk")
S3_CONN_ID = "yandex_s3"


# ------------------------------------------------------------------ #
#  Вспомогательные функции: ВСЕ операции с файлами через S3Hook      #
# ------------------------------------------------------------------ #

def read_csv_from_s3(
    bucket: str,
    key: str,
    conn_id: str = S3_CONN_ID,
) -> pd.DataFrame:
    """
    Читает CSV-файл из Yandex Object Storage.
    Прямой доступ к локальной ФС не используется.
    """
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
    """
    Записывает DataFrame как CSV в Yandex Object Storage.
    Использует hook.load_string() — без записи на диск.
    """
    hook = S3Hook(aws_conn_id=conn_id)
    buf  = StringIO()
    df.to_csv(buf, index=False)
    hook.load_string(
        string_data=buf.getvalue(),
        key=key,
        bucket_name=bucket,
        replace=True,   # идемпотентность: перезаписать при повторном запуске
    )
    log.info("Записан файл s3://%s/%s (%d строк)", bucket, key, len(df))


def get_shift_number(nsk_hour: int) -> int:
    """Определяет номер смены по часу НСК."""
    if 0 <= nsk_hour < 8:
        return 1
    elif 8 <= nsk_hour < 16:
        return 2
    return 3


# ================================================================== #
#  ОПРЕДЕЛЕНИЕ DAG                                                   #
# ================================================================== #

default_args = {
    "owner":          "tche15-analytics",
    "retries":        2,
    "retry_delay":    timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="shift_delay_report",
    description=(
        "Посменный отчёт по опозданиям ТЧЭ-15: "
        "schedule_adherence.csv из S3 → расчёт → отчёт в S3"
    ),
    # 00:00 / 08:00 / 16:00 НСК = 17:00 / 01:00 / 09:00 UTC пред./тек. дня
    schedule="0 17,1,9 * * *",
    start_date=pendulum.datetime(2024, 3, 1, tz=NSK_TZ),
    catchup=True,          # восстанавливаем пропущенные смены через backfill
    max_active_runs=3,     # до 3 параллельных runs (по числу смен в сутки)
    dagrun_timeout=timedelta(minutes=30),
    default_args=default_args,
    tags=["report", "delays", "s3", "backfill", "tche15"],
) as dag:

    # ---------------------------------------------------------------- #
    #  ЗАДАЧА 1: Читаем schedule_adherence.csv из S3 за текущую смену  #
    # ---------------------------------------------------------------- #

    @task(task_id="read_schedule_adherence")
    def read_schedule_adherence(**context) -> dict:
        """
        Читает schedule_adherence.csv из rzd-airflow-data/.
        Фильтрует строки по дате {{ ds }} и номеру смены.
        Возвращает dict с метаданными и строками данных.
        """
        bucket    = Variable.get("s3_bucket_data",    default_var="rzd-airflow-data")
        ds        = context["ds"]         # YYYY-MM-DD
        ds_nodash = context["ds_nodash"]  # YYYYMMDD

        data_interval_start = context["data_interval_start"]
        nsk_dt    = data_interval_start.in_timezone(NSK_TZ)
        shift_num = get_shift_number(nsk_dt.hour)

        # Пробуем посуточный ключ, fallback на корневой файл
        key_dated = f"schedule_adherence/{ds_nodash}/data.csv"
        key_root  = "schedule_adherence.csv"

        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        key  = key_dated if hook.check_for_key(key=key_dated, bucket_name=bucket) else key_root

        log.info(
            "Смена %d (%s): читаем schedule_adherence из s3://%s/%s",
            shift_num, ds, bucket, key,
        )

        df = read_csv_from_s3(bucket=bucket, key=key)

        # Фильтрация по дате и смене (если поля присутствуют)
        if "trip_date" in df.columns and "shift_number" in df.columns:
            df_shift = df[
                (df["trip_date"] == ds) & (df["shift_number"] == shift_num)
            ].copy()
        else:
            df_shift = df.copy()

        log.info(
            "Загружено %d записей для смены %d из %d строк файла",
            len(df_shift), shift_num, len(df),
        )

        return {
            "shift_num":  shift_num,
            "trip_date":  ds,
            "ds_nodash":  ds_nodash,
            "row_count":  len(df_shift),
            "rows":       df_shift.to_dict(orient="records"),
        }

    # ---------------------------------------------------------------- #
    #  ЗАДАЧА 2: Рассчитываем средние опоздания по маршрутам           #
    # ---------------------------------------------------------------- #

    @task(task_id="calculate_route_delays")
    def calculate_route_delays(input_data: dict) -> dict:
        """
        Рассчитывает OTD, средние и максимальные опоздания
        в разрезе маршрутов за текущую смену.
        """
        rows      = input_data["rows"]
        shift_num = input_data["shift_num"]

        if not rows:
            log.warning("Нет данных для смены %d — возвращаем нулевые метрики.", shift_num)
            return {
                **input_data,
                "total_trips":   0,
                "delayed_trips": 0,
                "avg_delay_min": 0.0,
                "max_delay_min": 0.0,
                "otd_pct":       100.0,
                "route_stats":   [],
            }

        df    = pd.DataFrame(rows)
        delay = pd.to_numeric(df.get("delay_minutes", pd.Series(dtype=float)),
                              errors="coerce").fillna(0)

        total_trips   = len(df)
        delayed_trips = int((delay > 0).sum())
        avg_delay     = float(delay.mean())
        max_delay     = float(delay.max())
        otd_pct       = (total_trips - delayed_trips) / total_trips * 100 \
                        if total_trips > 0 else 100.0

        # Агрегат по маршрутам
        route_stats = []
        if "route_from" in df.columns and "route_to" in df.columns:
            df["_delay"] = delay
            for (rf, rt), grp in df.groupby(["route_from", "route_to"]):
                delayed_on_route = int((grp["_delay"] > 0).sum())
                route_stats.append({
                    "route":              f"{rf} → {rt}",
                    "total_trips":        len(grp),
                    "delayed_trips":      delayed_on_route,
                    "avg_delay_min":      round(float(grp["_delay"].mean()), 2),
                    "max_delay_min":      round(float(grp["_delay"].max()), 2),
                    "otd_pct":            round(
                        (len(grp) - delayed_on_route) / len(grp) * 100, 2
                    ),
                })

        log.info(
            "Смена %d | Рейсов: %d | Задержано: %d | OTD: %.1f%% | "
            "avg_delay: %.1f мин | max_delay: %.1f мин",
            shift_num, total_trips, delayed_trips,
            otd_pct, avg_delay, max_delay,
        )

        return {
            **input_data,
            "total_trips":   total_trips,
            "delayed_trips": delayed_trips,
            "avg_delay_min": round(avg_delay, 2),
            "max_delay_min": round(max_delay, 2),
            "otd_pct":       round(otd_pct, 2),
            "route_stats":   route_stats,
        }

    # ---------------------------------------------------------------- #
    #  ЗАДАЧА 3: Проверяем пороговые значения                          #
    # ---------------------------------------------------------------- #

    @task.branch(task_id="check_delay_threshold")
    def check_delay_threshold(metrics: dict) -> str:
        """
        Ветвление: нужно ли уведомить дежурного диспетчера?
        Возвращает task_id следующей задачи.
        """
        threshold = float(Variable.get("delay_threshold_min", default_var="15"))

        needs_alert = (
            metrics["max_delay_min"] > threshold
            or metrics["otd_pct"] < 85.0
        )

        if needs_alert:
            log.warning(
                "Смена %d: max_delay=%.1f > %.1f или OTD=%.1f%% < 85%% — ALERT",
                metrics["shift_num"],
                metrics["max_delay_min"],
                threshold,
                metrics["otd_pct"],
            )
            return "send_dispatcher_alert"

        log.info(
            "Смена %d: OTD=%.1f%% — норма, уведомление не требуется.",
            metrics["shift_num"], metrics["otd_pct"],
        )
        return "skip_alert"

    # ---------------------------------------------------------------- #
    #  ЗАДАЧА 4a: Уведомление диспетчеру                               #
    # ---------------------------------------------------------------- #

    @task(task_id="send_dispatcher_alert")
    def send_dispatcher_alert(metrics: dict) -> None:
        """
        Уведомление дежурному диспетчеру ТЧЭ-15.
        В production: webhook в корпоративную систему оповещения РЖД.
        """
        log.warning(
            "[ТЧЭ-15 ALERT] Смена %d | Дата: %s | OTD: %.1f%% | "
            "max_delay: %.1f мин | Рейсов с задержкой: %d из %d",
            metrics["shift_num"],
            metrics["trip_date"],
            metrics["otd_pct"],
            metrics["max_delay_min"],
            metrics["delayed_trips"],
            metrics["total_trips"],
        )
        # В production:
        # requests.post(WEBHOOK_URL, json={"text": message, "chat": "dispatch-tche15"})

    # ---------------------------------------------------------------- #
    #  ЗАДАЧА 4b: Пропуск уведомления                                  #
    # ---------------------------------------------------------------- #

    from airflow.operators.empty import EmptyOperator
    skip = EmptyOperator(task_id="skip_alert")

    # ---------------------------------------------------------------- #
    #  ЗАДАЧА 5: Записываем отчёт в Object Storage                     #
    # ---------------------------------------------------------------- #

    @task(
        task_id="write_shift_report_to_s3",
        trigger_rule="none_failed_min_one_success",
    )
    def write_shift_report_to_s3(metrics: dict, **context) -> None:
        """
        Записывает итоговый отчёт смены в rzd-airflow-results/shift_reports/.
        Ключ S3: shift_reports/{ds_nodash}_{run_id}.csv
        Использует hook.load_string() — запись на диск не производится.
        """
        bucket     = Variable.get("s3_bucket_results", default_var="rzd-airflow-results")
        run_id     = context["run_id"]
        run_id_safe = run_id.replace(":", "_").replace("+", "_")[:40]

        # Сводный отчёт смены
        summary_key = (
            f"shift_reports/{metrics['ds_nodash']}"
            f"_shift{metrics['shift_num']}"
            f"_{run_id_safe}.csv"
        )
        summary_df = pd.DataFrame([{
            "report_date":   metrics["trip_date"],
            "shift_number":  metrics["shift_num"],
            "total_trips":   metrics["total_trips"],
            "delayed_trips": metrics["delayed_trips"],
            "avg_delay_min": metrics["avg_delay_min"],
            "max_delay_min": metrics["max_delay_min"],
            "otd_pct":       metrics["otd_pct"],
            "dag_run_id":    run_id,
        }])
        write_csv_to_s3(df=summary_df, bucket=bucket, key=summary_key)

        # Детализация по маршрутам (отдельный файл)
        if metrics.get("route_stats"):
            routes_key = (
                f"shift_reports/{metrics['ds_nodash']}"
                f"_shift{metrics['shift_num']}_routes.csv"
            )
            routes_df = pd.DataFrame(metrics["route_stats"])
            write_csv_to_s3(df=routes_df, bucket=bucket, key=routes_key)

        log.info(
            "Отчёт смены %d (%s) сохранён в s3://%s/%s",
            metrics["shift_num"], metrics["trip_date"], bucket, summary_key,
        )

    # ---------------------------------------------------------------- #
    #  ГРАФ ЗАВИСИМОСТЕЙ                                               #
    # ---------------------------------------------------------------- #

    raw_data = read_schedule_adherence()
    metrics  = calculate_route_delays(raw_data)
    branch   = check_delay_threshold(metrics)

    alert_task = send_dispatcher_alert(metrics)
    report     = write_shift_report_to_s3(metrics)

    branch >> [alert_task, skip] >> report
```

---

## Деплой и тестирование

### Загрузка DAG в Object Storage

**Через Yandex CLI:**

```bash
yc storage cp shift_delay_report_dag.py \
    s3://rzd-airflow-dags/dags/shift_delay_report_dag.py
```

**Через Yandex Cloud Console:**

1. Object Storage → бакет `rzd-airflow-dags` → **Загрузить объекты**.
2. Выберите файл `shift_delay_report_dag.py`.
3. Нажмите **Загрузить**.

### Проверка в Airflow UI

1. Откройте Airflow UI → подождите 1–3 минуты.
2. DAG `shift_delay_report` должен появиться в списке.
3. Проверьте: **Admin → Import Errors** — пусто.
4. DAG → **Graph View**: убедитесь, что все 5 задач видны и граф корректен.
5. Активируйте DAG (переключатель слева от имени).

### Запуск backfill

```bash
# Поставить DAG на паузу перед backfill (рекомендуется)
airflow dags pause shift_delay_report

# Выполнить backfill за неделю
airflow dags backfill \
    --dag-id shift_delay_report \
    --start-date 2024-03-01 \
    --end-date   2024-03-07 \
    --reset-dagruns

# Снять паузу
airflow dags unpause shift_delay_report
```

### Ожидаемый результат

После успешного backfill:

| Что проверять | Ожидаемый результат |
|---|---|
| Grid View в Airflow UI | 21 зелёная ячейка (7 дней × 3 смены) |
| Бакет `rzd-airflow-results/shift_reports/` | 21 CSV-файл сводного отчёта + файлы маршрутов |
| XCom задачи `calculate_route_delays` | поля `otd_pct`, `avg_delay_min`, `route_stats` |
| Повторный backfill за 2024-03-03 | файлы перезаписаны, не задвоены |

**Пример содержимого файла отчёта** (`20240301_shift2_<run_id>.csv`):

```
report_date,shift_number,total_trips,delayed_trips,avg_delay_min,max_delay_min,otd_pct,dag_run_id
2024-03-01,2,2,1,21.0,42.0,50.0,backfill__2024-03-01T01:00:00+00:00
```

---

## Задания повышенной сложности

### Задание 1. Динамические ключи S3 по партициям дат

Модифицируйте DAG так, чтобы данные читались по иерархическому ключу S3:

```
schedule_adherence/year=2024/month=03/day=01/data.csv
```

Реализуйте функцию `build_s3_key(ds: str) -> str`, которая формирует ключ по шаблону с партиционированием. Если партиционированный файл отсутствует — использовать `schedule_adherence.csv` как fallback с логированием предупреждения.

### Задание 2. Запись итогового отчёта также в PostgreSQL

Добавьте в DAG задачу `write_report_to_postgres` (после `write_shift_report_to_s3`), которая выполняет UPSERT-вставку метрик смены в таблицу `rzd_analytics.shift_reports`:

```sql
INSERT INTO rzd_analytics.shift_reports
    (report_date, shift_number, total_trips, delayed_trips,
     avg_delay_min, max_delay_min, otd_pct, dag_run_id)
VALUES (%(report_date)s, ...)
ON CONFLICT (report_date, shift_number)
DO UPDATE SET
    avg_delay_min = EXCLUDED.avg_delay_min,
    otd_pct       = EXCLUDED.otd_pct,
    updated_at    = now();
```

Убедитесь, что задача является идемпотентной при повторном backfill.

### Задание 3. S3KeySensor перед чтением файла

Перед задачей `read_schedule_adherence` добавьте `S3KeySensor`, который ожидает появления файла телеметрии в бакете:

```python
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

wait_for_file = S3KeySensor(
    task_id="wait_for_schedule_file",
    bucket_name="rzd-airflow-data",
    bucket_key="schedule_adherence/{{ ds_nodash }}/data.csv",
    aws_conn_id="yandex_s3",
    poke_interval=300,
    timeout=7200,
    mode="reschedule",
    soft_fail=True,
)
```

Объясните, почему `mode='reschedule'` предпочтительнее `mode='poke'` в Managed Airflow при большом числе параллельных DAG runs.
