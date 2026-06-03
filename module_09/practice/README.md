# Практическая работа №09: DockerOperator с данными из Object Storage

**Организация:** РЖД, Западно-Сибирская дирекция тяги, депо Новосибирск-Главный (ТЧЭ-15)
**Цель:** Написать Dockerfile для sklearn-модели прогнозирования перегрева букс,
собрать образ, загрузить в Yandex Container Registry и запустить через DockerOperator в Airflow DAG.
Все данные читаются из Yandex Object Storage (S3), результаты пишутся обратно в S3.
**Время выполнения:** 45–60 минут
**Уровень:** Средний
**Платформа:** Yandex Managed Service for Apache Airflow™

---

## Содержание

1. [Цель и задачи](#1-цель-и-задачи)
2. [Предварительные условия](#2-предварительные-условия)
3. [Шаг 1 — Настройка подключений в Airflow UI](#3-шаг-1--настройка-подключений-в-airflow-ui)
4. [Шаг 2 — Подготовка скрипта предсказания с доступом к S3](#4-шаг-2--подготовка-скрипта-предсказания-с-доступом-к-s3)
5. [Шаг 3 — Написание Dockerfile с boto3](#5-шаг-3--написание-dockerfile-с-boto3)
6. [Шаг 4 — Сборка образа и push в Yandex Container Registry](#6-шаг-4--сборка-образа-и-push-в-yandex-container-registry)
7. [Шаг 5 — DAG с DockerOperator (S3-паттерн)](#7-шаг-5--dag-с-dockeroperator-s3-паттерн)
8. [Шаг 6 — Деплой DAG через Object Storage](#8-шаг-6--деплой-dag-через-object-storage)
9. [Проверка выполнения](#9-проверка-выполнения)
10. [Контрольные вопросы](#10-контрольные-вопросы)

---

## 1. Цель и задачи

### Цель

Построить ML-пайплайн прогноза перегрева букс локомотивов ТЧЭ-15, где:

- входные данные модели читаются из Yandex Object Storage (бакет `rzd-airflow-data`);
- DockerOperator запускает образ из Yandex Container Registry;
- внутри контейнера скрипт читает данные непосредственно из S3 через `boto3`;
- результаты прогноза записываются обратно в S3 в бакет `rzd-airflow-results/predictions/`.

### Задачи

1. Настроить Connection `yandex_s3` и переменные Airflow.
2. Написать Python-скрипт `predict_buxa_failure.py`, который работает с S3 (без локальных файлов).
3. Собрать Docker-образ с `boto3` и `scikit-learn`.
4. Загрузить образ в Yandex Container Registry.
5. Написать DAG с `DockerOperator`, который передаёт путь S3 через переменные окружения.
6. Задеплоить DAG через бакет `rzd-airflow-dags/`.

---

## 2. Предварительные условия

Managed Airflow, S3 и PostgreSQL должны быть настроены и доступны.

### Инструменты

| Инструмент | Версия | Назначение |
|---|---|---|
| Yandex Managed Airflow | 2.8+ | Оркестрация DAG |
| apache-airflow-providers-amazon | 8.0+ | S3Hook, S3KeySensor |
| apache-airflow-providers-docker | 3.8+ | DockerOperator |
| Docker Engine (локально) | 24+ | Сборка образов |
| Yandex CLI (yc) | последняя | Container Registry, Object Storage |
| Python | 3.11 | Разработка скриптов |

### Структура бакетов (уже создана)

```
rzd-airflow-dags/          — DAG-файлы (связан с Managed Airflow)
rzd-airflow-data/          — входные данные
  └── sensor_readings.csv
  └── locomotives.csv
rzd-airflow-results/       — результаты обработки
  └── predictions/         — выходные прогнозы
```

### Тестовые данные

| Файл в S3 | Строк | Описание |
|---|---|---|
| `rzd-airflow-data/sensor_readings.csv` | 50 000 | Показания датчиков букс: температура, ток, скорость |
| `rzd-airflow-data/locomotives.csv` | 45 | Парк локомотивов ТЧЭ-15: серия, номер, дата ТО |

---

## 3. Шаг 1 — Настройка подключений в Airflow UI

### Connection: yandex_s3

Откройте **Admin → Connections → Add a new record**:

| Поле | Значение |
|---|---|
| Conn Id | `yandex_s3` |
| Conn Type | `Amazon Web Services` |
| Login | `<Access Key ID сервисного аккаунта>` |
| Password | `<Secret Access Key>` |
| Extra | `{"endpoint_url": "https://storage.yandexcloud.net", "region_name": "ru-central1"}` |

### Connection: rzd_postgres

| Поле | Значение |
|---|---|
| Conn Id | `rzd_postgres` |
| Conn Type | `Postgres` |
| Host | `<FQDN>.mdb.yandexcloud.net` |
| Schema | `rzd_analytics` |
| Login/Password | из Yandex Lockbox или напрямую |

### Переменные Airflow (Admin → Variables)

| Ключ | Значение |
|---|---|
| `s3_bucket_data` | `rzd-airflow-data` |
| `s3_bucket_results` | `rzd-airflow-results` |
| `depot_code` | `TCH-15` |
| `delay_threshold_min` | `15` |

---

## 4. Шаг 2 — Подготовка скрипта предсказания с доступом к S3

Создайте файл `predict_buxa_failure.py`. Скрипт будет запускаться **внутри Docker-контейнера** и обращается к S3 напрямую через `boto3`.

```python
#!/usr/bin/env python3
"""
predict_buxa_failure.py
Прогноз перегрева букс локомотивов ТЧЭ-15.

Запускается внутри Docker-контейнера (DockerOperator).
Все файловые операции — через Yandex Object Storage (S3-совместимый).
Переменные окружения передаются из DockerOperator.environment.

Модель: LinearRegression (scikit-learn) — прогноз температуры через 2 часа.
Критический порог: > 80°C.
"""

import os
import sys
import json
import logging
import boto3
from io import StringIO
from datetime import datetime

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler

# ── Конфигурация из переменных окружения (передаются из DockerOperator) ────────
S3_ENDPOINT     = os.environ.get("S3_ENDPOINT",     "https://storage.yandexcloud.net")
S3_REGION       = os.environ.get("S3_REGION",       "ru-central1")
S3_ACCESS_KEY   = os.environ.get("S3_ACCESS_KEY",   "")
S3_SECRET_KEY   = os.environ.get("S3_SECRET_KEY",   "")
S3_BUCKET_DATA  = os.environ.get("S3_BUCKET",       "rzd-airflow-data")
S3_KEY_INPUT    = os.environ.get("S3_KEY",          "sensor_readings.csv")
S3_BUCKET_OUT   = os.environ.get("S3_BUCKET_RESULTS","rzd-airflow-results")
TARGET_DATE     = os.environ.get("TARGET_DATE",     datetime.today().strftime("%Y-%m-%d"))
DEPOT_CODE      = os.environ.get("DEPOT_CODE",      "TCH-15")
THRESHOLD_TEMP  = float(os.environ.get("THRESHOLD_TEMP", "80"))

# ── Логирование ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Признаки модели ─────────────────────────────────────────────────────────────
FEATURE_COLS = [
    "buxa_temp_celsius",
    "speed_kmh",
    "traction_current_a",
    "temp_delta_30min",
    "engine_hours",
    "ambient_temp_celsius",
    "load_factor",
]


def get_s3_client():
    """Создание клиента boto3 для Yandex Object Storage."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        region_name=S3_REGION,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )


def read_csv_from_s3(s3_client, bucket: str, key: str) -> pd.DataFrame:
    """Чтение CSV-файла из S3 в DataFrame."""
    log.info(f"Чтение из S3: s3://{bucket}/{key}")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")
    df = pd.read_csv(StringIO(content))
    log.info(f"Загружено {len(df)} строк из s3://{bucket}/{key}")
    return df


def write_csv_to_s3(s3_client, df: pd.DataFrame, bucket: str, key: str):
    """Запись DataFrame в S3 в формате CSV."""
    log.info(f"Запись в S3: s3://{bucket}/{key} ({len(df)} строк)")
    buffer = StringIO()
    df.to_csv(buffer, index=False)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )
    log.info(f"Записано успешно: s3://{bucket}/{key}")


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Расчёт производных признаков."""
    df = df.copy()
    df = df.sort_values(["loco_number", "buxa_position", "timestamp"])

    # Изменение температуры за 30 минут (60 записей × 30 сек)
    df["temp_delta_30min"] = (
        df.groupby(["loco_number", "buxa_position"])["buxa_temp_celsius"]
        .diff(periods=60)
        .fillna(0)
    )

    # Коэффициент загрузки — нормализованный ток
    max_current = df["traction_current_a"].quantile(0.99).clip(lower=1)
    df["load_factor"] = (df["traction_current_a"] / max_current).clip(0, 1)

    # Заполнение пропусков медианой
    for col in FEATURE_COLS:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].median())
        else:
            df[col] = 0.0

    return df


def train_and_predict(df: pd.DataFrame) -> pd.DataFrame:
    """
    Обучение LinearRegression на исторических данных текущего дня
    и прогноз температуры буксы через 2 часа.
    Возвращает DataFrame с добавленными колонками failure_prob и risk_level.
    """
    df = df.copy()

    # Целевая переменная: температура через 2 часа (240 записей × 30 сек)
    df["target_temp"] = (
        df.groupby(["loco_number", "buxa_position"])["buxa_temp_celsius"]
        .shift(-240)
    )

    train_df = df.dropna(subset=["target_temp"])
    if len(train_df) < 100:
        log.warning("Недостаточно данных для обучения, используем эвристику")
        df["predicted_temp"] = df["buxa_temp_celsius"] + df["temp_delta_30min"] * 4
    else:
        scaler = StandardScaler()
        X_train = scaler.fit_transform(train_df[FEATURE_COLS])
        y_train = train_df["target_temp"].values

        model = LinearRegression()
        model.fit(X_train, y_train)

        X_all = scaler.transform(df[FEATURE_COLS])
        df["predicted_temp"] = model.predict(X_all)

    # failure_prob: нормализованное превышение порога (0–1)
    df["failure_prob"] = (
        (df["predicted_temp"] - THRESHOLD_TEMP) / THRESHOLD_TEMP
    ).clip(0, 1)

    df["risk_level"] = pd.cut(
        df["failure_prob"],
        bins=[0, 0.05, 0.15, 0.30, 1.01],
        labels=["low", "medium", "high", "critical"],
        include_lowest=True,
    )
    df["failure_flag"] = (df["failure_prob"] > 0.15).astype(int)

    return df


def main():
    log.info(f"=== Прогноз отказов букс ТЧЭ-15 ===")
    log.info(f"Дата: {TARGET_DATE}, Депо: {DEPOT_CODE}, Порог: {THRESHOLD_TEMP}°C")
    log.info(f"Источник данных: s3://{S3_BUCKET_DATA}/{S3_KEY_INPUT}")

    s3 = get_s3_client()

    # 1. Чтение данных из S3
    df = read_csv_from_s3(s3, S3_BUCKET_DATA, S3_KEY_INPUT)

    # 2. Фильтрация по дате (если колонка reading_date есть)
    if "reading_date" in df.columns:
        df = df[df["reading_date"] == TARGET_DATE]
        log.info(f"После фильтрации по дате {TARGET_DATE}: {len(df)} строк")

    if len(df) == 0:
        log.error(f"Нет данных датчиков за {TARGET_DATE}")
        sys.exit(1)

    # 3. Инженерия признаков
    df = engineer_features(df)

    # 4. Прогноз
    df = train_and_predict(df)

    # 5. Статистика
    critical_count = int((df["risk_level"] == "critical").sum())
    high_count     = int((df["risk_level"] == "high").sum())
    log.info(f"Критических: {critical_count}, высокий риск: {high_count}")

    # 6. Подготовка результата (только колонки с риском >= high)
    output_cols = [
        "loco_number", "loco_series", "buxa_position",
        "failure_prob", "risk_level", "failure_flag",
        "buxa_temp_celsius", "predicted_temp", "temp_delta_30min",
        "speed_kmh", "timestamp",
    ]
    available = [c for c in output_cols if c in df.columns]
    result_df = df[df["failure_flag"] == 1][available].copy()

    # 7. Запись результатов в S3
    date_nodash = TARGET_DATE.replace("-", "")
    out_key = f"predictions/{date_nodash}_buxa_predictions.csv"
    write_csv_to_s3(s3, result_df, S3_BUCKET_OUT, out_key)

    # 8. XCom: вывод JSON в stdout (Airflow читает последнюю строку)
    xcom_result = {
        "status":         "success",
        "date":           TARGET_DATE,
        "depot":          DEPOT_CODE,
        "total_records":  len(df),
        "critical_count": critical_count,
        "high_count":     high_count,
        "s3_output":      f"s3://{S3_BUCKET_OUT}/{out_key}",
    }
    print(json.dumps(xcom_result))


if __name__ == "__main__":
    main()
```

---

## 5. Шаг 3 — Написание Dockerfile с boto3

Создайте файл `Dockerfile` рядом со скриптом:

```dockerfile
# Dockerfile для ML-модели прогноза отказов букс ТЧЭ-15
# Все данные читаются/пишутся через Yandex Object Storage (boto3)

FROM python:3.11-slim

LABEL maintainer="rzd-tceh15-analytics@rzd.ru"
LABEL version="2.0"
LABEL description="Buxa failure prediction — S3 I/O, LinearRegression"

# Системные зависимости
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Зависимости Python — отдельный слой для кэширования
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Только код (данных в образе нет — они в S3)
COPY predict_buxa_failure.py .

# Переменные окружения по умолчанию (переопределяются из DockerOperator)
ENV DEPOT_CODE=TCH-15
ENV THRESHOLD_TEMP=80
ENV S3_ENDPOINT=https://storage.yandexcloud.net
ENV S3_REGION=ru-central1

# Не запускать от root
RUN useradd -m -u 1000 rzd_worker
USER rzd_worker

CMD ["python", "predict_buxa_failure.py"]
```

Создайте файл `requirements.txt`:

```text
scikit-learn==1.4.2
pandas==2.2.2
numpy==1.26.4
boto3==1.34.100
botocore==1.34.100
```

> **Важно:** `boto3` обязателен — контейнер читает данные напрямую из S3, без монтирования томов.

---

## 6. Шаг 4 — Сборка образа и push в Yandex Container Registry

Все команды выполняются локально (на рабочей станции с Docker и yc CLI).

```bash
# 1. Сборка образа
docker build -t buxa-predictor:v2.0 .

# 2. Проверка сборки
docker images | grep buxa-predictor

# 3. Локальное тестирование (передаём ключи S3 и путь к файлу)
docker run --rm \
  -e S3_ACCESS_KEY="<ваш_access_key>" \
  -e S3_SECRET_KEY="<ваш_secret_key>" \
  -e S3_BUCKET="rzd-airflow-data" \
  -e S3_KEY="sensor_readings.csv" \
  -e S3_BUCKET_RESULTS="rzd-airflow-results" \
  -e TARGET_DATE="2024-03-15" \
  -e DEPOT_CODE="TCH-15" \
  buxa-predictor:v2.0

# 4. Аутентификация в Yandex Container Registry
yc container registry configure-docker
# или через IAM-токен:
docker login \
  --username iam \
  --password $(yc iam create-token) \
  cr.yandex

# 5. Создать реестр (один раз для проекта)
yc container registry create --name rzd-tceh15-registry
# Запомните registry_id из вывода: crp1abc23defghijk

# 6. Тегирование и push
REGISTRY_ID="crp1abc23defghijk"
docker tag buxa-predictor:v2.0 cr.yandex/${REGISTRY_ID}/buxa-predictor:v2.0
docker tag buxa-predictor:v2.0 cr.yandex/${REGISTRY_ID}/buxa-predictor:latest

docker push cr.yandex/${REGISTRY_ID}/buxa-predictor:v2.0
docker push cr.yandex/${REGISTRY_ID}/buxa-predictor:latest

# 7. Проверка в реестре
yc container image list --repository-name rzd-tceh15-registry/buxa-predictor
```

---

## 7. Шаг 5 — DAG с DockerOperator (S3-паттерн)

Создайте файл `buxa_failure_prediction_dag.py`:

```python
"""
DAG: buxa_failure_prediction
Платформа: Yandex Managed Service for Apache Airflow™

Описание:
  ML-прогноз перегрева букс локомотивов ТЧЭ-15 через DockerOperator.
  Образ читает данные из S3 и пишет результаты в S3.
  Деплой DAG: загрузить файл в бакет rzd-airflow-dags/dags/

Архитектура:
  S3KeySensor (ждёт файл) → DockerOperator (читает S3 → пишет S3) → verify_results
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

log = logging.getLogger(__name__)

# ── Константы ──────────────────────────────────────────────────────────────────
REGISTRY        = "cr.yandex/crp1abc23defghijk"   # заменить на свой registry_id
IMAGE_TAG       = "v2.0"
S3_CONN_ID      = "yandex_s3"

# Бакеты читаются из Airflow Variables (Admin → Variables)
BUCKET_DATA     = "{{ var.value.s3_bucket_data }}"       # rzd-airflow-data
BUCKET_RESULTS  = "{{ var.value.s3_bucket_results }}"    # rzd-airflow-results
DEPOT_CODE      = "{{ var.value.depot_code }}"           # TCH-15

# Ключ S3 для входного файла датчиков
INPUT_S3_KEY    = "sensor_readings.csv"

# ── Функция верификации результатов в S3 ───────────────────────────────────────
def verify_predictions_in_s3(ds: str, **context) -> dict:
    """
    Проверяет, что файл прогнозов записан в S3.
    Использует S3Hook (aws_conn_id='yandex_s3') — стандартный паттерн.
    """
    hook = S3Hook(aws_conn_id=S3_CONN_ID)
    bucket_results = Variable.get("s3_bucket_results", default_var="rzd-airflow-results")

    date_nodash = ds.replace("-", "")
    expected_key = f"predictions/{date_nodash}_buxa_predictions.csv"

    key_exists = hook.check_for_key(key=expected_key, bucket_name=bucket_results)
    if not key_exists:
        raise FileNotFoundError(
            f"Файл прогноза не найден в S3: s3://{bucket_results}/{expected_key}"
        )

    # Получение размера файла для верификации
    obj = hook.get_key(key=expected_key, bucket_name=bucket_results)
    size_bytes = obj.get()["ContentLength"]
    log.info(
        f"Прогноз подтверждён: s3://{bucket_results}/{expected_key} "
        f"({size_bytes} байт)"
    )
    return {"s3_key": expected_key, "size_bytes": size_bytes, "date": ds}


# ── Параметры DAG по умолчанию ─────────────────────────────────────────────────
default_args = {
    "owner":             "rzd-tceh15-analytics",
    "depends_on_past":   False,
    "email":             ["analytics@rzd-tceh15.ru"],
    "email_on_failure":  True,
    "email_on_retry":    False,
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=60),
}

# ── DAG ────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="buxa_failure_prediction",
    description=(
        "ML-прогноз перегрева букс ТЧЭ-15 (DockerOperator + S3). "
        "Деплой: загрузить в rzd-airflow-dags/dags/"
    ),
    schedule="0 */4 * * *",
    start_date=datetime(2024, 3, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["rzd", "tceh15", "ml", "buxa", "docker", "s3"],
) as dag:

    # ── Задача 1: S3KeySensor — ожидание входного файла ────────────────────────
    # Заменяет FileSensor: ожидает появления CSV в бакете S3.
    wait_for_sensor_data = S3KeySensor(
        task_id="wait_for_sensor_data",
        bucket_name="rzd-airflow-data",
        bucket_key=INPUT_S3_KEY,
        aws_conn_id=S3_CONN_ID,
        poke_interval=300,     # проверять каждые 5 минут
        timeout=7200,          # таймаут 2 часа
        mode="reschedule",     # освобождать слот между проверками
    )

    # ── Задача 2: DockerOperator — ML-прогноз с чтением/записью S3 ────────────
    # Ключи S3 передаются через переменные окружения контейнера.
    # Контейнер сам работает с S3 через boto3 — томов не нужно.
    predict_failures = DockerOperator(
        task_id="predict_buxa_failures",
        image=f"{REGISTRY}/buxa-predictor:{IMAGE_TAG}",
        command="python predict_buxa_failure.py",
        environment={
            # Yandex Object Storage
            "S3_ENDPOINT":       "https://storage.yandexcloud.net",
            "S3_REGION":         "ru-central1",
            # Ключи доступа — из Yandex Lockbox или Airflow Connections
            # Для prod: использовать сервисный аккаунт с ролью storage.editor
            "S3_ACCESS_KEY":     "{{ conn.yandex_s3.login }}",
            "S3_SECRET_KEY":     "{{ conn.yandex_s3.password }}",
            # Бакеты и пути
            "S3_BUCKET":         "rzd-airflow-data",
            "S3_KEY":            INPUT_S3_KEY,
            "S3_BUCKET_RESULTS": "rzd-airflow-results",
            # Параметры прогноза
            "TARGET_DATE":       "{{ ds }}",
            "DEPOT_CODE":        "TCH-15",
            "THRESHOLD_TEMP":    "80",
        },
        # Не монтируем тома — данные в S3
        mounts=[],
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        auto_remove=True,
        mem_limit="4g",
        cpus=2.0,
        # XCom: последняя строка stdout (JSON из print() в скрипте)
        retrieve_output=True,
        retrieve_output_path="/tmp/xcom_result.json",
    )

    # ── Задача 3: Верификация результатов в S3 ─────────────────────────────────
    verify_results = PythonOperator(
        task_id="verify_results_in_s3",
        python_callable=verify_predictions_in_s3,
    )

    # ── Граф зависимостей ──────────────────────────────────────────────────────
    wait_for_sensor_data >> predict_failures >> verify_results
```

---

## 8. Шаг 6 — Деплой DAG через Object Storage

В Yandex Managed Airflow DAG-файлы **не копируются через SSH** — они загружаются в бакет S3, который привязан к Managed Airflow.

```bash
# Вариант 1: через yc CLI
yc storage cp buxa_failure_prediction_dag.py \
  s3://rzd-airflow-dags/dags/buxa_failure_prediction_dag.py

# Вариант 2: через AWS CLI (настроенный на Yandex Cloud)
aws s3 cp buxa_failure_prediction_dag.py \
  s3://rzd-airflow-dags/dags/buxa_failure_prediction_dag.py \
  --endpoint-url https://storage.yandexcloud.net

# Вариант 3: через Yandex Cloud Console
# Object Storage → rzd-airflow-dags → dags/ → Загрузить файл

# Проверка — файл виден в бакете
yc storage ls s3://rzd-airflow-dags/dags/

# Через 1-2 минуты DAG появится в Airflow UI:
# https://<managed-airflow-url>/dags/buxa_failure_prediction
```

> **Не использовать:** `airflow dags / ssh / scp / локальная папка dags/` — в Managed Airflow прямого доступа к файловой системе воркера нет.

---

## 9. Проверка выполнения

После запуска DAG проверьте результаты через S3Hook (или AWS CLI):

```bash
# 1. Проверить файл прогнозов в S3
aws s3 ls s3://rzd-airflow-results/predictions/ \
  --endpoint-url https://storage.yandexcloud.net

# Ожидаемый вывод:
# 2024-03-15 10:05:22    24576 20240315_buxa_predictions.csv

# 2. Скачать и просмотреть результат
aws s3 cp \
  s3://rzd-airflow-results/predictions/20240315_buxa_predictions.csv \
  ./buxa_predictions.csv \
  --endpoint-url https://storage.yandexcloud.net

head -5 buxa_predictions.csv

# 3. В Airflow UI:
#    DAGs → buxa_failure_prediction → Graph View
#    Все три задачи должны быть зелёными (success)

# 4. XCom результата задачи predict_buxa_failures:
#    DAGs → buxa_failure_prediction → <run> → predict_buxa_failures → XCom
#    Ожидаемый JSON:
# {
#   "status": "success",
#   "date": "2024-03-15",
#   "depot": "TCH-15",
#   "total_records": 48320,
#   "critical_count": 7,
#   "high_count": 19,
#   "s3_output": "s3://rzd-airflow-results/predictions/20240315_buxa_predictions.csv"
# }
```

### Типичные ошибки и решение

| Ошибка | Причина | Решение |
|---|---|---|
| `NoCredentialsError` | Нет ключей S3 в env контейнера | Проверить `conn.yandex_s3.login/password` в Airflow |
| `NoSuchKey` | Файл не загружен в бакет | Проверить `rzd-airflow-data/sensor_readings.csv` через `yc storage ls` |
| `EndpointResolutionError` | Неверный endpoint_url | Проверить Extra в Connection: `"endpoint_url": "https://storage.yandexcloud.net"` |
| `Cannot connect to Docker daemon` | Managed Airflow не имеет Docker | Использовать `KubernetesPodOperator` или `YandexCloudRunOperator` вместо Docker |
| `DAG not found in UI` | DAG не загружен в нужный бакет | Проверить путь: `rzd-airflow-dags/dags/<имя_файла>.py` |
| `Image not found` | Образ не в Container Registry | Выполнить `docker push cr.yandex/<id>/buxa-predictor:v2.0` |

---

## 10. Контрольные вопросы

1. **Почему в Managed Airflow нельзя использовать `FileSensor` и монтирование томов Docker?**
   Объясните, как `S3KeySensor` решает ту же задачу без доступа к локальной файловой системе.

2. **Как передать ключи доступа к S3 в Docker-контейнер безопасно?**
   Сравните три подхода: переменные окружения из Connection, Airflow Variables, Yandex Lockbox.
   Какой из них предпочтителен для production?

3. **Что произойдёт, если метод `print(json.dumps(result))` убрать из скрипта `predict_buxa_failure.py`?**
   Как Airflow читает XCom из DockerOperator? Что записывается и откуда?

4. **В задаче `verify_results_in_s3` используется `S3Hook(aws_conn_id='yandex_s3')`.
   Почему тип подключения `Amazon Web Services`, а не специальный тип Yandex или «Amazon S3»?**
   Подсказка: отдельного типа «Amazon S3» в Airflow больше нет — он слит с `Amazon Web Services`, а `S3Hook` наследует `AwsBaseHook`. Как параметр `endpoint_url` в Extra позволяет подключиться к Yandex Object Storage?

5. **Как изменить DAG, чтобы он обрабатывал сразу несколько дат (backfill)?**
   Что нужно поменять в параметрах DAG (`catchup`, `start_date`) и в логике формирования пути S3?
