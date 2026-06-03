# Лабораторная работа №11: Три алерта для ТЧЭ-15 через S3 и Telegram

**Модуль 11 — Эксплуатация Airflow**
**Организация:** РЖД, Западно-Сибирская дирекция тяги, депо ТЧЭ-15 Новосибирск-Главный
**Платформа:** Yandex Managed Service for Apache Airflow™ + Yandex Object Storage
**Продолжительность:** 90–120 минут

---

## Цель

Разработать файл `alert_callbacks.py` и DAG `tche15_three_alerts`, которые реализуют три производственных сценария алертинга для дежурного инженера ТЧЭ-15 без доступа к локальной файловой системе — все файловые операции выполняются через Yandex Object Storage (S3).

**Три алерта:**

1. **`on_buxa_critical`** — читает последний файл телеметрии из S3; если `buxa_temp_max > 80°C` → отправляет Telegram-алерт.
2. **`on_otd_below_threshold`** — делает запрос к PostgreSQL; если ОТД (on-time delivery) < 90% → отправляет Telegram + записывает отчёт в `rzd-airflow-results/alerts/`.
3. **`on_pipeline_failure`** — стандартный `on_failure_callback`; при любой ошибке задачи → Telegram + запись в `rzd-airflow-results/errors/`.

---

## Предварительные условия

### Connections (Admin → Connections в UI Managed Airflow)

| Conn Id | Conn Type | Login | Password | Extra |
|---|---|---|---|---|
| `yandex_s3` | Amazon Web Services | `<Access Key ID>` | `<Secret Access Key>` | `{"endpoint_url": "https://storage.yandexcloud.net", "region_name": "ru-central1"}` |
| `rzd_postgres` | Postgres | `rzd_user` | `<пароль>` | — |

Для `rzd_postgres` укажите:
- **Host:** `<FQDN>.mdb.yandexcloud.net`
- **Schema:** `rzd_analytics`
- **Port:** `6432`

### Variables (Admin → Variables)

| Key | Value |
|---|---|
| `telegram_bot_token` | `<токен бота>` |
| `telegram_chat_id` | `<chat_id дежурного>` |
| `s3_bucket_data` | `rzd-airflow-data` |
| `s3_bucket_results` | `rzd-airflow-results` |
| `otd_threshold_pct` | `90` |

### Структура бакетов

```
rzd-airflow-dags/
├── alert_callbacks.py      ← вспомогательный модуль (загрузить первым)
└── tche15_three_alerts.py  ← DAG-файл

rzd-airflow-data/
└── sensor_readings/
    └── <YYYYMMDD>_telemetry.csv

rzd-airflow-results/
├── alerts/   ← JSON-отчёты по ОТД и перегреву
└── errors/   ← JSON-описания ошибок пайплайна
```

---

## Задание

### Шаг 1. Получить токен Telegram-бота

1. Откройте Telegram → найдите **@BotFather** → `/newbot`.
2. Введите название бота: `ТЧЭ-15 Дежурный`.
3. Введите username: `tche15_duty_bot` (уникальный).
4. Сохраните токен вида `7123456789:AAFxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`.
5. Напишите боту любое сообщение, затем получите chat_id:

```bash
curl "https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates"
# Найдите поле "id" в объекте "chat" — это и есть chat_id
```

6. Добавьте токен и chat_id в Airflow Variables (`Admin → Variables`).

---

### Шаг 2. Создать вспомогательный модуль `alert_callbacks.py`

Создайте файл `alert_callbacks.py` со всеми тремя callback-функциями:

```python
"""
alert_callbacks.py — три алерта для депо ТЧЭ-15.

Деплой:
    yc storage cp alert_callbacks.py \
        s3://rzd-airflow-dags/alert_callbacks.py

Импорт в DAG:
    from alert_callbacks import (
        on_buxa_critical,
        on_otd_below_threshold,
        on_pipeline_failure,
    )
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from io import StringIO
from typing import Any

import pandas as pd
import psycopg2
import requests
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

log = logging.getLogger(__name__)

S3_CONN_ID     = "yandex_s3"
BUCKET_DATA    = "rzd-airflow-data"
BUCKET_RESULTS = "rzd-airflow-results"


# ─── Внутренние утилиты ───────────────────────────────────────────────────

def _telegram_send(text: str) -> bool:
    """Отправить HTML-сообщение в Telegram чат дежурного ТЧЭ-15."""
    token   = Variable.get("telegram_bot_token")
    chat_id = Variable.get("telegram_chat_id")
    url     = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={
                "chat_id":                  chat_id,
                "text":                     text,
                "parse_mode":               "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        resp.raise_for_status()
        log.info("Telegram-алерт отправлен успешно.")
        return True
    except requests.RequestException as exc:
        log.error("Ошибка Telegram API: %s", exc)
        return False


def _s3_write_json(
    prefix: str,
    payload: dict[str, Any],
    conn_id: str = S3_CONN_ID,
) -> str:
    """
    Записать payload как JSON в Object Storage.

    Возвращает полный S3-ключ объекта.
    """
    hook = S3Hook(aws_conn_id=conn_id)
    ts   = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    tag  = payload.get("dag_id", "unknown")
    key  = f"{prefix}/{ts}_{tag}.json"
    hook.load_string(
        string_data=json.dumps(payload, ensure_ascii=False, indent=2),
        key=key,
        bucket_name=BUCKET_RESULTS,
        replace=True,
    )
    log.info("Записано в s3://%s/%s", BUCKET_RESULTS, key)
    return key


def _s3_read_csv(bucket: str, key: str, conn_id: str = S3_CONN_ID) -> pd.DataFrame:
    """Прочитать CSV из Object Storage в DataFrame."""
    hook    = S3Hook(aws_conn_id=conn_id)
    obj     = hook.get_key(key=key, bucket_name=bucket)
    content = obj.get()["Body"].read().decode("utf-8")
    return pd.read_csv(StringIO(content))


def _s3_write_csv(
    df: pd.DataFrame,
    bucket: str,
    key: str,
    conn_id: str = S3_CONN_ID,
) -> None:
    """Записать DataFrame как CSV в Object Storage."""
    hook = S3Hook(aws_conn_id=conn_id)
    buf  = StringIO()
    df.to_csv(buf, index=False)
    hook.load_string(
        string_data=buf.getvalue(),
        key=key,
        bucket_name=bucket,
        replace=True,
    )
    log.info("CSV записан: s3://%s/%s (%d строк)", bucket, key, len(df))


def _pg_connect():
    """Создать подключение к Yandex Managed PostgreSQL rzd_analytics."""
    conn = BaseHook.get_connection("rzd_postgres")
    return psycopg2.connect(
        host=conn.host,
        port=conn.port or 6432,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
        connect_timeout=10,
    )


# ─── Алерт 1: on_buxa_critical ───────────────────────────────────────────

def on_buxa_critical(**context) -> None:
    """
    Алерт 1: перегрев буксы.

    Читает CSV телеметрии из S3 (ключ строится из ds_nodash).
    Если buxa_temp_max > 80°C → отправляет Telegram + записывает JSON
    в rzd-airflow-results/alerts/.
    Вызывается как python_callable в PythonOperator.
    """
    ds_nodash = context["ds_nodash"]
    key       = f"sensor_readings/{ds_nodash}_telemetry.csv"

    log.info("Читаем телеметрию из s3://%s/%s", BUCKET_DATA, key)
    df = _s3_read_csv(bucket=BUCKET_DATA, key=key)

    if "buxa_temp_max" not in df.columns:
        log.warning("Колонка buxa_temp_max не найдена в файле %s", key)
        return

    critical = df[df["buxa_temp_max"] > 80.0].copy()
    log.info(
        "Всего строк: %d, критических (>80°C): %d",
        len(df), len(critical),
    )

    if critical.empty:
        log.info("Перегрева букс не обнаружено. ТЧЭ-15 в норме.")
        return

    # Берём строку с максимальной температурой для алерта
    worst = critical.loc[critical["buxa_temp_max"].idxmax()]
    temp  = float(worst["buxa_temp_max"])
    loco  = str(worst.get("loco_id", "неизвестен"))

    payload = {
        "dag_id":      context["dag"].dag_id,
        "alert_type":  "buxa_overheat",
        "severity":    "critical",
        "loco_id":     loco,
        "buxa_temp_c": temp,
        "total_critical_rows": len(critical),
        "source_key":  key,
        "depot":       "TCH-15",
        "ts":          datetime.now(timezone.utc).isoformat(),
    }
    _s3_write_json(prefix="alerts", payload=payload)

    text = (
        f"🚨 <b>КРИТИЧНО: ПЕРЕГРЕВ БУКСЫ — ТЧЭ-15</b>\n"
        f"Локомотив ID: <code>{loco}</code>\n"
        f"Температура: <b>{temp:.1f}°C</b> (норма &lt;60°C)\n"
        f"Строк с перегревом: {len(critical)}\n"
        f"Файл: <code>{key}</code>\n"
        f"Время: {datetime.now(timezone.utc).strftime('%d.%m.%Y %H:%M UTC')}\n"
        f"Депо: ТЧЭ-15 НГ"
    )
    _telegram_send(text)
    context["ti"].xcom_push(key="buxa_alerts", value=len(critical))


# ─── Алерт 2: on_otd_below_threshold ─────────────────────────────────────

def on_otd_below_threshold(**context) -> None:
    """
    Алерт 2: ОТД ниже порога.

    Делает запрос к PostgreSQL rzd_analytics.schedule_adherence.
    Если ОТД < otd_threshold_pct (по умолчанию 90%) за последние 7 дней →
        - отправляет Telegram
        - записывает CSV-отчёт в rzd-airflow-results/alerts/
        - записывает JSON-описание алерта в rzd-airflow-results/alerts/
    Вызывается как python_callable в PythonOperator.
    """
    threshold = float(Variable.get("otd_threshold_pct", default_var=90))

    pg = _pg_connect()
    try:
        with pg.cursor() as cur:
            cur.execute("""
                SELECT
                    ROUND(
                        100.0
                        * SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END)
                        / NULLIF(COUNT(*), 0),
                        2
                    ) AS otd_pct,
                    COUNT(*)                              AS total_trips,
                    SUM(CASE WHEN NOT is_on_time THEN 1 ELSE 0 END)
                                                          AS delayed_trips,
                    AVG(CASE WHEN NOT is_on_time
                             THEN delay_minutes END)      AS avg_delay_min
                FROM rzd_analytics.schedule_adherence
                WHERE check_date >= CURRENT_DATE - INTERVAL '7 days'
            """)
            row = cur.fetchone()
    finally:
        pg.close()

    if row is None or row[0] is None:
        log.warning("Нет данных в schedule_adherence за последние 7 дней.")
        return

    otd_pct, total, delayed, avg_delay = (
        float(row[0]), int(row[1]), int(row[2]),
        float(row[3]) if row[3] else 0.0,
    )
    log.info(
        "ОТД за 7 дней: %.2f%% (порог: %.0f%%), поездов: %d, опоздавших: %d",
        otd_pct, threshold, total, delayed,
    )

    if otd_pct >= threshold:
        log.info("ОТД в норме (%.2f%% >= %.0f%%). Алерт не нужен.",
                 otd_pct, threshold)
        context["ti"].xcom_push(key="otd_pct", value=otd_pct)
        return

    # OTD упал ниже порога — формируем алерт
    ds_nodash  = context["ds_nodash"]
    dag_id     = context["dag"].dag_id

    # Записать CSV-отчёт с деталями опозданий
    pg2 = _pg_connect()
    try:
        with pg2.cursor() as cur:
            cur.execute("""
                SELECT
                    t.train_number,
                    t.origin,
                    t.destination,
                    sa.check_date,
                    sa.delay_minutes
                FROM rzd_analytics.schedule_adherence sa
                JOIN rzd_analytics.trips t ON t.trip_id = sa.trip_id
                WHERE sa.check_date >= CURRENT_DATE - INTERVAL '7 days'
                  AND NOT sa.is_on_time
                ORDER BY sa.delay_minutes DESC
                LIMIT 50
            """)
            rows = cur.fetchall()
    finally:
        pg2.close()

    detail_df = pd.DataFrame(
        rows,
        columns=["train_number", "origin", "destination",
                 "check_date", "delay_minutes"],
    )
    csv_key = f"alerts/{ds_nodash}_otd_delayed_trips.csv"
    _s3_write_csv(df=detail_df, bucket=BUCKET_RESULTS, key=csv_key)

    payload = {
        "dag_id":        dag_id,
        "alert_type":    "otd_below_threshold",
        "severity":      "warning",
        "otd_pct":       otd_pct,
        "threshold_pct": threshold,
        "total_trips":   total,
        "delayed_trips": delayed,
        "avg_delay_min": round(avg_delay, 1),
        "detail_csv":    f"s3://{BUCKET_RESULTS}/{csv_key}",
        "depot":         "TCH-15",
        "ts":            datetime.now(timezone.utc).isoformat(),
    }
    _s3_write_json(prefix="alerts", payload=payload)

    text = (
        f"⚠️ <b>ОТД НИЖЕ НОРМЫ — ТЧЭ-15</b>\n"
        f"ОТД за 7 дней: <b>{otd_pct:.1f}%</b> "
        f"(порог: {threshold:.0f}%)\n"
        f"Всего поездок: {total} | Опоздавших: {delayed}\n"
        f"Среднее опоздание: {avg_delay:.1f} мин\n"
        f"Детальный отчёт: <code>s3://{BUCKET_RESULTS}/{csv_key}</code>\n"
        f"Время: {datetime.now(timezone.utc).strftime('%d.%m.%Y %H:%M UTC')}\n"
        f"Депо: ТЧЭ-15 НГ"
    )
    _telegram_send(text)
    context["ti"].xcom_push(key="otd_pct", value=otd_pct)


# ─── Алерт 3: on_pipeline_failure ────────────────────────────────────────

def on_pipeline_failure(context: dict[str, Any]) -> None:
    """
    Алерт 3: стандартный on_failure_callback.

    Вызывается Airflow автоматически при сбое любой задачи DAG.
    Действия:
      1. Записывает JSON с описанием ошибки в rzd-airflow-results/errors/.
      2. Отправляет Telegram-алерт дежурному инженеру.
    """
    dag_id    = context["dag"].dag_id
    task_id   = context["task_instance"].task_id
    exec_date = context["logical_date"].strftime("%d.%m.%Y %H:%M UTC")
    exception = str(context.get("exception", "неизвестная ошибка"))[:500]
    log_url   = context["task_instance"].log_url or "—"

    payload = {
        "dag_id":     dag_id,
        "task_id":    task_id,
        "alert_type": "pipeline_failure",
        "severity":   "critical",
        "exec_date":  exec_date,
        "exception":  exception,
        "log_url":    log_url,
        "depot":      "TCH-15",
        "ts":         datetime.now(timezone.utc).isoformat(),
    }
    _s3_write_json(prefix="errors", payload=payload)

    text = (
        f"🔥 <b>ОТКАЗ ПАЙПЛАЙНА — ТЧЭ-15</b>\n"
        f"DAG: <code>{dag_id}</code>\n"
        f"Task: <code>{task_id}</code>\n"
        f"Запуск: {exec_date}\n"
        f"Ошибка:\n<pre>{exception[:300]}</pre>\n"
        f"Лог: {log_url}"
    )
    _telegram_send(text)
```

---

### Шаг 3. Создать DAG `tche15_three_alerts.py`

Создайте основной файл DAG:

```python
"""
DAG: tche15_three_alerts
Описание: три производственных алерта депо ТЧЭ-15.
          Все файловые операции через Yandex Object Storage (S3).

Деплой:
    yc storage cp tche15_three_alerts.py \
        s3://rzd-airflow-dags/tche15_three_alerts.py
"""
from __future__ import annotations

import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.dates import days_ago

from alert_callbacks import (
    on_buxa_critical,
    on_otd_below_threshold,
    on_pipeline_failure,
)

log = logging.getLogger(__name__)

S3_CONN_ID  = "yandex_s3"
BUCKET_DATA = "rzd-airflow-data"

DEFAULT_ARGS = {
    "owner":               "tche15-ops",
    "retries":             1,
    "retry_delay":         timedelta(minutes=2),
    "on_failure_callback": on_pipeline_failure,
    "email_on_failure":    False,
}


def summarize_run(**context) -> None:
    """Итоговый лог: сводка результатов запуска DAG."""
    ti = context["ti"]

    buxa_alerts = ti.xcom_pull(
        task_ids="alert_buxa_critical", key="buxa_alerts"
    ) or 0
    otd_pct = ti.xcom_pull(
        task_ids="alert_otd_below_threshold", key="otd_pct"
    )

    log.info("=== ИТОГ ЗАПУСКА tche15_three_alerts ===")
    log.info("Алерт 1 (буксы): критических записей = %d", buxa_alerts)
    if otd_pct is not None:
        log.info("Алерт 2 (ОТД): %.2f%%", float(otd_pct))
    else:
        log.info("Алерт 2 (ОТД): данные не получены")
    log.info("Алерт 3 (pipeline_failure): настроен через on_failure_callback")
    log.info("=========================================")


with DAG(
    dag_id="tche15_three_alerts",
    default_args=DEFAULT_ARGS,
    description=(
        "Три алерта ТЧЭ-15: перегрев букс, ОТД < 90%, "
        "отказ пайплайна → Telegram + S3"
    ),
    schedule="0 */1 * * *",   # каждый час
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["tche15", "alerts", "s3", "production"],
) as dag:

    # Ждём входной файл телеметрии в S3
    wait_file = S3KeySensor(
        task_id="wait_for_telemetry_file",
        bucket_name=BUCKET_DATA,
        bucket_key="sensor_readings/{{ ds_nodash }}_telemetry.csv",
        aws_conn_id=S3_CONN_ID,
        poke_interval=300,     # проверять каждые 5 минут
        timeout=7200,          # не более 2 часов
        mode="reschedule",
        on_failure_callback=on_pipeline_failure,
    )

    # Алерт 1: перегрев буксы (читает CSV из S3)
    t_buxa = PythonOperator(
        task_id="alert_buxa_critical",
        python_callable=on_buxa_critical,
        pool="alert_pool",
        priority_weight=200,
        doc_md=(
            "Читает CSV телеметрии из S3. "
            "При buxa_temp_max > 80°C → Telegram + JSON в S3/alerts/."
        ),
    )

    # Алерт 2: ОТД ниже порога (запрос к PostgreSQL)
    t_otd = PythonOperator(
        task_id="alert_otd_below_threshold",
        python_callable=on_otd_below_threshold,
        pool="alert_pool",
        priority_weight=150,
        doc_md=(
            "Запрашивает ОТД из PostgreSQL. "
            "Если < 90% → Telegram + CSV-отчёт + JSON в S3/alerts/."
        ),
    )

    # Алерт 3 (on_pipeline_failure) подключён через DEFAULT_ARGS
    # и срабатывает автоматически при падении любой задачи DAG.

    # Итоговый шаг: сводка в логах
    t_summary = PythonOperator(
        task_id="summarize_run",
        python_callable=summarize_run,
        trigger_rule="all_done",
        doc_md="Итоговая сводка результатов запуска DAG в логах Airflow.",
    )

    # Граф зависимостей
    wait_file >> [t_buxa, t_otd] >> t_summary
```

---

### Шаг 4. Деплой файлов в Object Storage

Загрузите оба файла в бакет `rzd-airflow-dags/`, который связан с Managed Airflow:

```bash
# Загрузить вспомогательный модуль с callback-функциями
yc storage cp alert_callbacks.py \
    s3://rzd-airflow-dags/alert_callbacks.py

# Загрузить DAG-файл
yc storage cp tche15_three_alerts.py \
    s3://rzd-airflow-dags/tche15_three_alerts.py

# Проверить, что файлы загружены
yc storage ls rzd-airflow-dags/
```

Через **Yandex Cloud Console**:

1. Откройте **Object Storage** → бакет `rzd-airflow-dags`.
2. Нажмите **Загрузить объекты** → выберите оба `.py`-файла.
3. Подождите 1–2 минуты.
4. Откройте **Managed Airflow** → ваш кластер → **Интерфейс Airflow** (кнопка в консоли).
5. Убедитесь, что DAG `tche15_three_alerts` появился в списке со статусом `Active`.

> Никогда не используйте `airflow dags`, `ssh`, `scp` или локальную папку `dags/` — в Managed Airflow это недоступно.

---

### Шаг 5. Проверка в Airflow UI

После загрузки файлов:

1. Перейдите в **Airflow UI** → **DAGs**.
2. Найдите `tche15_three_alerts` — убедитесь, что нет ошибок импорта (Import Errors).
3. Переведите DAG в активный режим (тумблер On/Off).
4. Нажмите **Trigger DAG** для ручного запуска.
5. Откройте **Graph View** — убедитесь, что все задачи выполнились (`success` или `skipped`).
6. Откройте логи задачи `alert_buxa_critical` — убедитесь, что в логах нет трассировки исключений.

---

### Шаг 6. Тестирование алерта о перегреве (Алерт 1)

Подготовьте тестовый CSV с критической температурой и загрузите в S3:

```python
# Скрипт: generate_test_telemetry.py (запускать локально или в Cloud Shell)
import csv
import io
from datetime import datetime, timezone

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

rows = [
    {
        "loco_id":       "7",
        "recorded_at":   datetime.now(timezone.utc).isoformat(),
        "buxa_temp_max": "85.5",   # критическое значение
        "traction_amps": "1500",
        "voltage_kv":    "25.0",
        "fuel_rate":     "0",
        "speed_kmh":     "60.0",
        "lat":           "54.9833",
        "lon":           "82.8964",
    },
]

buf = io.StringIO()
writer = csv.DictWriter(buf, fieldnames=rows[0].keys())
writer.writeheader()
writer.writerows(rows)

hook = S3Hook(aws_conn_id="yandex_s3")
ds_nodash = datetime.now(timezone.utc).strftime("%Y%m%d")
key = f"sensor_readings/{ds_nodash}_telemetry.csv"

hook.load_string(
    string_data=buf.getvalue(),
    key=key,
    bucket_name="rzd-airflow-data",
    replace=True,
)
print(f"Тестовый файл загружен: s3://rzd-airflow-data/{key}")
```

После загрузки файла запустите DAG вручную через Airflow UI — ожидайте Telegram-сообщение.

---

### Шаг 7. Тестирование алерта ОТД (Алерт 2)

Временно установите порог выше текущего значения ОТД:

1. Откройте **Admin → Variables** в Airflow UI.
2. Измените `otd_threshold_pct` с `90` на `99`.
3. Запустите DAG через **Trigger DAG**.
4. Ожидайте Telegram-алерт об ОТД ниже порога.
5. Проверьте, что в `rzd-airflow-results/alerts/` появился CSV-файл с деталями опозданий:

```bash
yc storage ls rzd-airflow-results/alerts/
# Ожидаемый вывод:
# <YYYYMMDD>T<HHMMSS>Z_tche15_three_alerts.json
# <YYYYMMDD>_otd_delayed_trips.csv
```

6. Верните значение переменной `otd_threshold_pct` обратно на `90`.

---

### Шаг 8. Проверка алерта при сбое пайплайна (Алерт 3)

Временно сломайте DAG, изменив имя Connection:

1. В Airflow UI откройте **Trigger DAG with config**.
2. Передайте конфигурацию: `{"test_mode": true}` (для документации).
3. Если задача упала — `on_pipeline_failure` сработает автоматически.
4. Проверьте папку `rzd-airflow-results/errors/`:

```bash
yc storage ls rzd-airflow-results/errors/
# Должен появиться файл <timestamp>_tche15_three_alerts.json
```

5. Скачайте и просмотрите JSON-файл ошибки:

```bash
yc storage cp \
    s3://rzd-airflow-results/errors/<имя_файла>.json \
    ./error_report.json

cat error_report.json
```

---

### Шаг 9. Проверка итоговых результатов

```bash
# Список алертов в S3
yc storage ls rzd-airflow-results/alerts/

# Список ошибок пайплайна в S3
yc storage ls rzd-airflow-results/errors/

# Просмотр последнего алерта о перегреве
yc storage cp \
    s3://rzd-airflow-results/alerts/<последний_файл>.json \
    ./last_alert.json
```

В Airflow UI:
- **DAGs** → `tche15_three_alerts` → вкладка **Runs** — убедитесь, что последний запуск завершился со статусом `success`.
- **Browse** → **Task Instances** — найдите все три задачи алертов и убедитесь в отсутствии ошибок.

---

## Ожидаемый результат

После выполнения лабораторной работы должны выполняться следующие условия:

- DAG `tche15_three_alerts` развёрнут в Managed Airflow через бакет `rzd-airflow-dags/`.
- При CSV с `buxa_temp_max > 80°C` в `rzd-airflow-data/` — Telegram-алерт приходит в течение 5 минут.
- При ОТД < порога — Telegram-алерт + CSV-отчёт записываются в `rzd-airflow-results/alerts/`.
- При падении любой задачи DAG — JSON с описанием ошибки записывается в `rzd-airflow-results/errors/` + Telegram-алерт.
- В бакете `rzd-airflow-results/` есть файлы с результатами каждого запуска.
- Задачи `alert_buxa_critical` и `alert_otd_below_threshold` выполняются параллельно.
- Все операции с файлами выполняются через `S3Hook` с `aws_conn_id='yandex_s3'`, без обращений к локальной файловой системе.

---

## Задания повышенной сложности

### Задание A: Несколько получателей алертов

Реализуйте маршрутизацию Telegram-алертов по типу:
- `on_buxa_critical` → чат дежурного инженера + чат начальника депо.
- `on_otd_below_threshold` → чат планово-производственного отдела.
- `on_pipeline_failure` → чат DevOps-команды.

Требования:
- Добавьте Variables `telegram_chat_id_chief`, `telegram_chat_id_ppo`, `telegram_chat_id_devops`.
- Модифицируйте функцию `_telegram_send` так, чтобы принимать список `chat_ids`.
- Каждый алерт должен отправляться в свои чаты по отдельности, не в групповой.

### Задание B: Дедупликация алертов через S3

Реализуйте механизм антиспама: один и тот же алерт о перегреве буксы одного локомотива не должен отправляться чаще, чем раз в 30 минут.

Требования:
- Хранить состояние последних алертов в файле `s3://rzd-airflow-results/state/last_alerts.json`.
- Перед отправкой алерта читать этот файл из S3 и проверять время последнего алерта для данного `loco_id`.
- После отправки обновлять файл состояния в S3.
- Использовать только `S3Hook` — без PostgreSQL, Redis или Airflow Variables для хранения состояния.

### Задание C: Еженедельный сводный отчёт

Добавьте в DAG четвёртую задачу `send_weekly_summary`, которая запускается только по воскресеньям (используйте `ShortCircuitOperator` + проверку дня недели).

Требования:
- Читает все файлы из `rzd-airflow-results/alerts/` за последние 7 дней через `S3Hook.list_keys()`.
- Считает количество алертов по типам (`buxa_overheat`, `otd_below_threshold`, `pipeline_failure`).
- Формирует HTML-таблицу сводки и отправляет в Telegram.
- Записывает итоговый CSV в `rzd-airflow-results/weekly/<YYYYWW>_summary.csv`.
