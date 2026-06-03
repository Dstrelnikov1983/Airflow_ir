# Практическая работа №11: Мониторинг Managed Airflow и алерты для S3-пайплайнов

**Модуль 11 — Эксплуатация Airflow**
**Организация:** РЖД, Западно-Сибирская дирекция тяги, депо ТЧЭ-15 Новосибирск-Главный
**Платформа:** Yandex Managed Service for Apache Airflow™ + Yandex Object Storage
**Продолжительность:** 60–75 минут

---

## Цель и задачи

Настроить промышленный мониторинг и систему алертов для S3-пайплайнов депо ТЧЭ-15 на базе Yandex Managed Airflow без доступа к локальной файловой системе.

**Задачи:**

1. Подключить встроенный мониторинг Yandex Cloud Monitoring для Managed Airflow.
2. Реализовать `on_failure_callback`, который пишет ошибку в S3 и отправляет Telegram-алерт дежурному инженеру.
3. Настроить `S3KeySensor` для ожидания входного файла с таймаутом и callback при его отсутствии.
4. Добавить `SqlSensor` для контроля попадания данных в PostgreSQL после загрузки.
5. Загрузить DAG-файл в бакет `rzd-airflow-dags/` через Yandex Cloud Console или CLI и убедиться, что он подхвачен Managed Airflow.

---

## Предварительные условия

Перед началом работы должны быть выполнены следующие настройки:

### Managed Airflow

- Кластер Yandex Managed Service for Apache Airflow создан и работает.
- DAG-бакет (`rzd-airflow-dags/`) указан в настройках кластера как источник DAG-файлов.
- Сервисный аккаунт кластера имеет роль `storage.editor` на бакетах `rzd-airflow-dags`, `rzd-airflow-data`, `rzd-airflow-results`.

### Airflow Connections (Admin → Connections)

| Conn Id | Conn Type | Login | Password | Extra |
|---|---|---|---|---|
| `yandex_s3` | Amazon Web Services | `<Access Key ID>` | `<Secret Access Key>` | `{"endpoint_url": "https://storage.yandexcloud.net", "region_name": "ru-central1"}` |
| `rzd_postgres` | Postgres | `rzd_user` | `<пароль>` | — |

Для `rzd_postgres` также укажите:
- **Host:** `<FQDN кластера>.mdb.yandexcloud.net`
- **Schema:** `rzd_analytics`
- **Port:** `6432` (PgBouncer)

### Airflow Variables (Admin → Variables)

| Key | Value |
|---|---|
| `s3_bucket_data` | `rzd-airflow-data` |
| `s3_bucket_results` | `rzd-airflow-results` |
| `depot_code` | `TCH-15` |
| `delay_threshold_min` | `15` |
| `telegram_bot_token` | `<токен бота>` |
| `telegram_chat_id` | `<chat_id дежурного>` |

### Структура бакетов Object Storage

```
rzd-airflow-dags/           ← DAG-файлы (связан с Managed Airflow)
rzd-airflow-data/
├── sensor_readings.csv
├── locomotives.csv
├── trips.csv
├── schedule_adherence.csv
└── maintenance.csv
rzd-airflow-results/
├── alerts/                 ← JSON-описания алертов
└── errors/                 ← JSON-описания ошибок пайплайнов
```

---

## Шаг 1. Подключение Yandex Cloud Monitoring для Managed Airflow

Yandex Managed Airflow автоматически публикует метрики в **Yandex Cloud Monitoring** — никаких StatsD Exporter, Prometheus или Grafana устанавливать не нужно.

### 1.1. Просмотр метрик в консоли

1. Откройте **Yandex Cloud Console** → выберите каталог с кластером Airflow.
2. Перейдите: **Managed Service for Apache Airflow** → ваш кластер → вкладка **Мониторинг**.
3. Доступны встроенные графики:
   - `airflow.dag_processing.total_parse_time` — время парсинга DAG.
   - `airflow.executor.queued_tasks` — задачи в очереди.
   - `airflow.executor.running_tasks` — выполняющиеся задачи.
   - `airflow.scheduler.tasks.killed_externally` — задачи, прерванные снаружи.

### 1.2. Создание алерта в Cloud Monitoring

1. Откройте **Cloud Monitoring** → **Алерты** → **Создать алерт**.
2. Добавьте метрику:
   - Сервис: `Managed Service for Apache Airflow`
   - Метрика: `airflow.executor.queued_tasks`
   - Агрегация: `MAX`, окно 5 мин.
3. Условие срабатывания: `> 20` на протяжении 5 минут.
4. Канал уведомления: Email или Telegram через **Notification Channel**.
5. Нажмите **Сохранить**.

> Аналогично создайте алерт на `airflow.scheduler.heartbeat` — если значение не обновлялось более 2 минут, scheduler завис.

---

## Шаг 2. Создание вспомогательных функций для S3 и Telegram

Создайте файл `alert_helpers.py` и загрузите его в бакет `rzd-airflow-dags/`:

```python
# alert_helpers.py
# Вспомогательные функции для алертов ТЧЭ-15.
# Деплой: загрузить в rzd-airflow-dags/
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

import requests
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

log = logging.getLogger(__name__)

S3_CONN_ID   = "yandex_s3"
BUCKET_RESULTS = "rzd-airflow-results"


def _send_telegram(text: str) -> bool:
    """Отправить сообщение в Telegram дежурному ТЧЭ-15."""
    token   = Variable.get("telegram_bot_token")
    chat_id = Variable.get("telegram_chat_id")
    url     = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={
                "chat_id":   chat_id,
                "text":      text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        resp.raise_for_status()
        log.info("Telegram-алерт отправлен.")
        return True
    except requests.RequestException as exc:
        log.error("Ошибка отправки Telegram-алерта: %s", exc)
        return False


def write_alert_to_s3(
    prefix: str,
    payload: dict[str, Any],
    conn_id: str = S3_CONN_ID,
) -> str:
    """
    Записать JSON-алерт в Object Storage.

    prefix: 'alerts' или 'errors'
    Возвращает полный S3-ключ записанного объекта.
    """
    hook = S3Hook(aws_conn_id=conn_id)
    ts   = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key  = f"{prefix}/{ts}_{payload.get('dag_id', 'unknown')}.json"
    hook.load_string(
        string_data=json.dumps(payload, ensure_ascii=False, indent=2),
        key=key,
        bucket_name=BUCKET_RESULTS,
        replace=True,
    )
    log.info("Алерт записан в s3://%s/%s", BUCKET_RESULTS, key)
    return key


def on_failure_callback(context: dict[str, Any]) -> None:
    """
    Стандартный on_failure_callback для DAG ТЧЭ-15.

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
        "exec_date":  exec_date,
        "exception":  exception,
        "log_url":    log_url,
        "depot":      "TCH-15",
    }
    write_alert_to_s3(prefix="errors", payload=payload)

    text = (
        f"🔥 <b>ОТКАЗ ПАЙПЛАЙНА — ТЧЭ-15</b>\n"
        f"DAG: <code>{dag_id}</code>\n"
        f"Task: <code>{task_id}</code>\n"
        f"Запуск: {exec_date}\n"
        f"Ошибка:\n<pre>{exception[:300]}</pre>\n"
        f"Лог: {log_url}"
    )
    _send_telegram(text)
```

Загрузка через CLI:

```bash
yc storage cp alert_helpers.py s3://rzd-airflow-dags/alert_helpers.py
```

---

## Шаг 3. S3KeySensor — ожидание входного файла с таймаутом

`S3KeySensor` заменяет `FileSensor` при работе с Object Storage. При отсутствии файла дольше `timeout` секунд задача переходит в статус `failed` и срабатывает `on_failure_callback`.

```python
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

wait_for_sensor_file = S3KeySensor(
    task_id="wait_for_sensor_readings",
    bucket_name="rzd-airflow-data",
    bucket_key="sensor_readings/{{ ds_nodash }}_telemetry.csv",
    aws_conn_id="yandex_s3",
    poke_interval=300,   # проверять каждые 5 минут
    timeout=7200,        # ждать не более 2 часов
    mode="reschedule",   # освобождать слот воркера между проверками
    on_failure_callback=on_failure_callback,  # из alert_helpers.py
)
```

> Режим `mode='reschedule'` критически важен для Managed Airflow: воркер не блокируется в ожидании файла, а освобождает слот для других задач.

---

## Шаг 4. SqlSensor — проверка попадания данных в PostgreSQL

После загрузки CSV в PostgreSQL необходимо убедиться, что данные действительно появились в таблице. Используйте `SqlSensor`:

```python
from airflow.providers.common.sql.sensors.sql import SqlSensor

check_data_in_pg = SqlSensor(
    task_id="check_sensor_data_in_postgres",
    conn_id="rzd_postgres",
    sql="""
        SELECT COUNT(*)
        FROM rzd_analytics.sensor_readings
        WHERE recorded_at::date = '{{ ds }}'::date
    """,
    success=lambda count: int(count[0][0]) > 0,
    poke_interval=60,
    timeout=600,
    mode="reschedule",
)
```

Sensor вернёт `True`, когда COUNT > 0. Если данные не появились за 10 минут — задача падает и срабатывает `on_failure_callback`.

---

## Шаг 5. Полный DAG: мониторинг пайплайна ТЧЭ-15

Создайте файл `monitoring_pipeline_tche15.py` и загрузите в `rzd-airflow-dags/`:

```python
"""
DAG: monitoring_pipeline_tche15
Описание: Приём телеметрии ТЧЭ-15 из S3, загрузка в PostgreSQL,
          проверка данных через SqlSensor, алерты при сбоях.

Деплой: yc storage cp monitoring_pipeline_tche15.py \
            s3://rzd-airflow-dags/monitoring_pipeline_tche15.py
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from io import StringIO

import pandas as pd
import psycopg2
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.dates import days_ago

from alert_helpers import on_failure_callback, write_alert_to_s3

log = logging.getLogger(__name__)

S3_CONN_ID     = "yandex_s3"
BUCKET_DATA    = "rzd-airflow-data"
BUCKET_RESULTS = "rzd-airflow-results"

DEFAULT_ARGS = {
    "owner":               "tche15-ops",
    "retries":             2,
    "retry_delay":         timedelta(minutes=3),
    "on_failure_callback": on_failure_callback,
    "email_on_failure":    False,
}


# ── Утилиты S3 ────────────────────────────────────────────────────────────

def read_csv_from_s3(
    bucket: str,
    key: str,
    conn_id: str = S3_CONN_ID,
) -> pd.DataFrame:
    """Прочитать CSV из Object Storage в DataFrame."""
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
    """Записать DataFrame как CSV в Object Storage."""
    hook       = S3Hook(aws_conn_id=conn_id)
    buf        = StringIO()
    df.to_csv(buf, index=False)
    hook.load_string(
        string_data=buf.getvalue(),
        key=key,
        bucket_name=bucket,
        replace=True,
    )
    log.info("Записано в s3://%s/%s (%d строк)", bucket, key, len(df))


# ── Задачи DAG ────────────────────────────────────────────────────────────

def load_sensor_readings_to_pg(**context) -> None:
    """
    Читает CSV телеметрии из S3 и загружает в rzd_analytics.sensor_readings.
    Ключ файла строится из даты запуска DAG: sensor_readings/<ds_nodash>.csv
    """
    ds_nodash = context["ds_nodash"]
    key       = f"sensor_readings/{ds_nodash}_telemetry.csv"

    df = read_csv_from_s3(bucket=BUCKET_DATA, key=key)
    log.info("Прочитано %d строк телеметрии из s3://%s/%s",
             len(df), BUCKET_DATA, key)

    conn   = BaseHook.get_connection("rzd_postgres")
    pg     = psycopg2.connect(
        host=conn.host, port=conn.port or 6432,
        dbname=conn.schema, user=conn.login, password=conn.password,
        connect_timeout=10,
    )
    try:
        with pg.cursor() as cur:
            for _, row in df.iterrows():
                cur.execute(
                    """
                    INSERT INTO rzd_analytics.sensor_readings
                        (loco_id, recorded_at, buxa_temp_max,
                         traction_amps, voltage_kv,
                         fuel_rate, speed_kmh, lat, lon)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        row.get("loco_id"),
                        row.get("recorded_at"),
                        row.get("buxa_temp_max"),
                        row.get("traction_amps"),
                        row.get("voltage_kv"),
                        row.get("fuel_rate"),
                        row.get("speed_kmh"),
                        row.get("lat"),
                        row.get("lon"),
                    ),
                )
        pg.commit()
        log.info("Загрузка завершена: %d строк → rzd_analytics.sensor_readings",
                 len(df))
        context["ti"].xcom_push(key="rows_loaded", value=len(df))
    finally:
        pg.close()


def check_critical_temperatures(**context) -> None:
    """
    Читает показания за последние 10 минут из PostgreSQL.
    При buxa_temp_max >= 80°C → пишет алерт в S3/alerts/ + Telegram.
    """
    conn = BaseHook.get_connection("rzd_postgres")
    pg   = psycopg2.connect(
        host=conn.host, port=conn.port or 6432,
        dbname=conn.schema, user=conn.login, password=conn.password,
        connect_timeout=10,
    )
    try:
        with pg.cursor() as cur:
            cur.execute("""
                SELECT
                    l.series,
                    l.loco_number,
                    s.loco_id,
                    MAX(s.buxa_temp_max) AS max_temp,
                    AVG(s.lat)           AS lat,
                    AVG(s.lon)           AS lon
                FROM rzd_analytics.sensor_readings s
                JOIN rzd_analytics.locomotives l
                     ON l.loco_id = s.loco_id
                WHERE s.recorded_at >= NOW() - INTERVAL '10 minutes'
                  AND s.buxa_temp_max >= 80
                GROUP BY l.series, l.loco_number, s.loco_id
                ORDER BY max_temp DESC
            """)
            rows = cur.fetchall()
    finally:
        pg.close()

    alerts_sent = 0
    for series, number, loco_id, temp, lat, lon in rows:
        payload = {
            "dag_id":      context["dag"].dag_id,
            "alert_type":  "buxa_overheat",
            "severity":    "critical",
            "loco_id":     loco_id,
            "locomotive":  f"{series}-{number}",
            "buxa_temp_c": float(temp),
            "lat":         float(lat) if lat else None,
            "lon":         float(lon) if lon else None,
            "depot":       "TCH-15",
        }
        write_alert_to_s3(prefix="alerts", payload=payload)
        alerts_sent += 1
        log.error(
            "КРИТИЧНО: перегрев буксы %s-%s: %.1f°C",
            series, number, float(temp),
        )

    log.info("Проверка температур завершена. Алертов: %d", alerts_sent)
    context["ti"].xcom_push(key="critical_alerts", value=alerts_sent)


def save_daily_summary(**context) -> None:
    """
    Формирует итоговый CSV за сутки и записывает в S3/results/.
    """
    ds        = context["ds"]
    ds_nodash = context["ds_nodash"]
    rows_loaded = (
        context["ti"].xcom_pull(
            task_ids="load_sensor_readings_to_pg",
            key="rows_loaded",
        ) or 0
    )
    critical_alerts = (
        context["ti"].xcom_pull(
            task_ids="check_critical_temperatures",
            key="critical_alerts",
        ) or 0
    )

    summary_df = pd.DataFrame([{
        "date":            ds,
        "depot":           "TCH-15",
        "rows_loaded":     rows_loaded,
        "critical_alerts": critical_alerts,
        "pipeline_status": "ok",
        "generated_at":    datetime.now(timezone.utc).isoformat(),
    }])

    key = f"summaries/{ds_nodash}_daily_summary.csv"
    write_csv_to_s3(df=summary_df, bucket=BUCKET_RESULTS, key=key)
    log.info("Итоговый отчёт записан: s3://%s/%s", BUCKET_RESULTS, key)


# ── Определение DAG ───────────────────────────────────────────────────────

with DAG(
    dag_id="monitoring_pipeline_tche15",
    default_args=DEFAULT_ARGS,
    description=(
        "Приём телеметрии ТЧЭ-15 из S3 → PostgreSQL, "
        "мониторинг температур, алерты"
    ),
    schedule="0 */1 * * *",   # каждый час
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["tche15", "monitoring", "s3", "production"],
) as dag:

    # Шаг 1: ждём появления файла телеметрии в S3
    wait_file = S3KeySensor(
        task_id="wait_for_sensor_file",
        bucket_name=BUCKET_DATA,
        bucket_key="sensor_readings/{{ ds_nodash }}_telemetry.csv",
        aws_conn_id=S3_CONN_ID,
        poke_interval=300,
        timeout=7200,
        mode="reschedule",
        on_failure_callback=on_failure_callback,
    )

    # Шаг 2: загружаем CSV в PostgreSQL
    load_data = PythonOperator(
        task_id="load_sensor_readings_to_pg",
        python_callable=load_sensor_readings_to_pg,
    )

    # Шаг 3: проверяем, что данные появились в PostgreSQL
    verify_pg = SqlSensor(
        task_id="verify_data_in_postgres",
        conn_id="rzd_postgres",
        sql="""
            SELECT COUNT(*)
            FROM rzd_analytics.sensor_readings
            WHERE recorded_at::date = '{{ ds }}'::date
        """,
        success=lambda result: int(result[0][0]) > 0,
        poke_interval=60,
        timeout=600,
        mode="reschedule",
        on_failure_callback=on_failure_callback,
    )

    # Шаг 4: проверяем критические температуры и пишем алерты в S3
    check_temps = PythonOperator(
        task_id="check_critical_temperatures",
        python_callable=check_critical_temperatures,
    )

    # Шаг 5: сохраняем итоговый отчёт за сутки в S3
    save_summary = PythonOperator(
        task_id="save_daily_summary",
        python_callable=save_daily_summary,
        trigger_rule="all_done",
    )

    wait_file >> load_data >> verify_pg >> check_temps >> save_summary
```

---

## Шаг 6. Деплой DAG в Managed Airflow

Managed Airflow читает DAG-файлы непосредственно из бакета Object Storage. Локальная папка `dags/` недоступна.

### Через Yandex Cloud CLI

```bash
# Загрузить вспомогательный модуль
yc storage cp alert_helpers.py \
    s3://rzd-airflow-dags/alert_helpers.py

# Загрузить основной DAG
yc storage cp monitoring_pipeline_tche15.py \
    s3://rzd-airflow-dags/monitoring_pipeline_tche15.py

# Проверить, что файлы появились в бакете
yc storage ls rzd-airflow-dags/
```

### Через Yandex Cloud Console

1. Откройте **Object Storage** → бакет `rzd-airflow-dags`.
2. Нажмите **Загрузить объекты** и выберите `.py`-файлы.
3. Подождите 1–2 минуты — Managed Airflow обнаружит новые DAG автоматически.
4. Откройте **Managed Airflow** → ваш кластер → **Интерфейс Airflow**.
5. Убедитесь, что DAG `monitoring_pipeline_tche15` появился в списке.

> Никогда не используйте `airflow dags`, `ssh`, `scp` или монтирование локальных папок — в Managed Airflow это недоступно.

---

## Контрольные вопросы

1. Чем `S3KeySensor` с `mode='reschedule'` отличается от `mode='poke'` при работе в Managed Airflow? Почему режим `reschedule` предпочтителен?

2. Какую роль выполняет `SqlSensor` в пайплайне после загрузки CSV? Что произойдёт, если таблица пуста из-за ошибки парсинга?

3. Объясните, почему `on_failure_callback` пишет ошибку в S3 (`rzd-airflow-results/errors/`), а не только отправляет Telegram. Какой это даёт операционный эффект?

4. В чём разница между метриками Yandex Cloud Monitoring (встроен в Managed Airflow) и стеком Prometheus + StatsD Exporter, который использовался бы на VM? Какие метрики недоступны в Cloud Monitoring?

5. Как изменить DAG, чтобы при отсутствии файла телеметрии более 4 часов алерт отправлялся в отдельный Telegram-чат начальника депо, а не только дежурному инженеру?
