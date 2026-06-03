# Лабораторная работа №09: ML-пайплайн: S3 данные → DockerOperator → результаты в S3

**Организация:** РЖД, Западно-Сибирская дирекция тяги, депо Новосибирск-Главный (ТЧЭ-15)
**Цель:** Собрать и запустить полноценный DAG `ml_ore_quality_pipeline`, в котором
Docker-контейнер читает данные из Yandex Object Storage, применяет ML-модель
(LinearRegression) и записывает прогнозы обратно в S3. Все файловые операции —
исключительно через S3, без доступа к локальной файловой системе.
**Время выполнения:** 90–120 минут
**Уровень:** Повышенный
**Платформа:** Yandex Managed Service for Apache Airflow™

---

## Содержание

1. [Цель](#1-цель)
2. [Предварительные условия](#2-предварительные-условия)
3. [Задание: пошаговое выполнение](#3-задание-пошаговое-выполнение)
   - [Шаг 1 — Структура бакетов и загрузка данных](#шаг-1--структура-бакетов-и-загрузка-данных)
   - [Шаг 2 — Dockerfile: python:3.11-slim + scikit-learn + boto3](#шаг-2--dockerfile-python311-slim--scikit-learn--boto3)
   - [Шаг 3 — Скрипт predict_buxa_failure.py: S3 → ML → S3](#шаг-3--скрипт-predict_buxa_failurepy-s3--ml--s3)
   - [Шаг 4 — Сборка образа и push в Container Registry](#шаг-4--сборка-образа-и-push-в-container-registry)
   - [Шаг 5 — DAG ml_ore_quality_pipeline.py](#шаг-5--dag-ml_ore_quality_pipelinepy)
   - [Шаг 6 — Деплой DAG в rzd-airflow-dags/](#шаг-6--деплой-dag-в-rzd-airflow-dags)
   - [Шаг 7 — Запуск и тестирование в Airflow UI](#шаг-7--запуск-и-тестирование-в-airflow-ui)
   - [Шаг 8 — Проверка результатов в S3](#шаг-8--проверка-результатов-в-s3)
   - [Шаг 9 — Верификация через S3Hook в Python](#шаг-9--верификация-через-s3hook-в-python)
4. [Полный код DAG](#4-полный-код-dag)
5. [Деплой: загрузка в rzd-airflow-dags/ и проверка в UI](#5-деплой-загрузка-в-rzd-airflow-dags-и-проверка-в-ui)
6. [Ожидаемый результат](#6-ожидаемый-результат)
7. [Задания повышенной сложности](#7-задания-повышенной-сложности)

---

## 1. Цель

Реализовать ML-пайплайн для анализа риска перегрева букс локомотивов ТЧЭ-15,
полностью работающий в Yandex Managed Airflow без доступа к локальной файловой системе:

```
rzd-airflow-data/sensor_readings.csv
          │
          ▼  (S3KeySensor ждёт файл)
   extract_s3_path (PythonOperator)
   — формирует путь S3, передаёт в XCom
          │
          ▼  (DockerOperator)
   predict (buxa-predictor:v2.0)
   — контейнер читает CSV из S3 через boto3
   — обучает LinearRegression
   — пишет predictions.csv в S3
          │
          ▼  (PythonOperator)
   verify_results_in_s3
   — S3Hook проверяет наличие файла
   — логирует размер и количество строк
          │
          ▼
rzd-airflow-results/predictions/<date>_predictions.csv
```

---

## 2. Предварительные условия

### Инструменты и версии

| Инструмент | Версия | Назначение |
|---|---|---|
| Yandex Managed Airflow | 2.8+ | Оркестрация DAG |
| apache-airflow-providers-amazon | 8.0+ | S3Hook, S3KeySensor |
| apache-airflow-providers-docker | 3.8+ | DockerOperator |
| Docker Engine (локально) | 24+ | Сборка образов |
| Yandex CLI (yc) | последняя | Container Registry, Object Storage |
| Python | 3.11 | Разработка |

### Connection и переменные (уже настроены из практической работы)

| Параметр | Значение |
|---|---|
| Conn Id | `yandex_s3` |
| Conn Type | `Amazon Web Services` |
| Login | `<Access Key ID>` |
| Password | `<Secret Access Key>` |
| Extra | `{"endpoint_url": "https://storage.yandexcloud.net", "region_name": "ru-central1"}` |

| Variable | Значение |
|---|---|
| `s3_bucket_data` | `rzd-airflow-data` |
| `s3_bucket_results` | `rzd-airflow-results` |
| `depot_code` | `TCH-15` |

### Тестовые данные в S3

| Ключ в S3 | Строк | Описание |
|---|---|---|
| `rzd-airflow-data/sensor_readings.csv` | 50 000 | Показания датчиков букс |
| `rzd-airflow-data/locomotives.csv` | 45 | Парк локомотивов ТЧЭ-15 |

---

## 3. Задание: пошаговое выполнение

### Шаг 1 — Структура бакетов и загрузка данных

Убедитесь, что входные данные загружены в S3. Если ещё нет:

```bash
# Создать бакеты (если не созданы)
yc storage bucket create --name rzd-airflow-data
yc storage bucket create --name rzd-airflow-results
yc storage bucket create --name rzd-airflow-dags

# Загрузить тестовые данные
yc storage cp sensor_readings.csv \
  s3://rzd-airflow-data/sensor_readings.csv

yc storage cp locomotives.csv \
  s3://rzd-airflow-data/locomotives.csv

# Проверить
yc storage ls s3://rzd-airflow-data/
```

Структура бакетов для лабораторной работы:

```
rzd-airflow-dags/
  └── dags/
      └── ml_ore_quality_pipeline.py    ← DAG-файл

rzd-airflow-data/
  ├── sensor_readings.csv               ← входные данные
  └── locomotives.csv

rzd-airflow-results/
  └── predictions/
      └── <YYYYMMDD>_predictions.csv    ← результаты (создаёт контейнер)
```

---

### Шаг 2 — Dockerfile: python:3.11-slim + scikit-learn + boto3

Создайте файл `Dockerfile`:

```dockerfile
# Dockerfile для ML-прогноза букс ТЧЭ-15
# I/O: Yandex Object Storage (boto3), без локальных файлов

FROM python:3.11-slim

LABEL maintainer="rzd-tceh15@rzd.ru"
LABEL version="2.0"
LABEL description="Buxa failure predictor — S3 data source, LinearRegression"

# Системные зависимости
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Зависимости отдельным слоем (кэшируется при пересборке)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Только код — данных в образе нет (они в S3)
COPY predict_buxa_failure.py .

# Переменные окружения по умолчанию
ENV S3_ENDPOINT=https://storage.yandexcloud.net
ENV S3_REGION=ru-central1
ENV DEPOT_CODE=TCH-15
ENV THRESHOLD_TEMP=80

# Непривилегированный пользователь
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

---

### Шаг 3 — Скрипт predict_buxa_failure.py: S3 → ML → S3

Создайте файл `predict_buxa_failure.py`:

```python
#!/usr/bin/env python3
"""
predict_buxa_failure.py
Прогноз перегрева букс локомотивов ТЧЭ-15.

Входные данные:  читает sensor_readings.csv из S3 (env S3_BUCKET / S3_KEY)
Модель:          LinearRegression (scikit-learn) — прогноз темп. через 2 часа
Выходные данные: пишет predictions.csv в S3 (env S3_BUCKET_RESULTS)

Запускается внутри Docker-контейнера через DockerOperator.
Локальная файловая система НЕ используется.
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
from sklearn.metrics import mean_absolute_error

# ── Конфигурация (все значения передаются из DockerOperator.environment) ────────
S3_ENDPOINT      = os.environ.get("S3_ENDPOINT",      "https://storage.yandexcloud.net")
S3_REGION        = os.environ.get("S3_REGION",        "ru-central1")
S3_ACCESS_KEY    = os.environ.get("S3_ACCESS_KEY",    "")
S3_SECRET_KEY    = os.environ.get("S3_SECRET_KEY",    "")
S3_BUCKET        = os.environ.get("S3_BUCKET",        "rzd-airflow-data")
S3_KEY           = os.environ.get("S3_KEY",           "sensor_readings.csv")
S3_BUCKET_RESULTS = os.environ.get("S3_BUCKET_RESULTS", "rzd-airflow-results")
TARGET_DATE      = os.environ.get("TARGET_DATE",      datetime.today().strftime("%Y-%m-%d"))
DEPOT_CODE       = os.environ.get("DEPOT_CODE",       "TCH-15")
THRESHOLD_TEMP   = float(os.environ.get("THRESHOLD_TEMP", "80"))

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

OUTPUT_COLS = [
    "loco_number", "loco_series", "buxa_position",
    "predicted_temp", "failure_prob", "risk_level", "failure_flag",
    "buxa_temp_celsius", "temp_delta_30min", "speed_kmh",
    "timestamp", "prediction_date",
]


# ── S3-операции ────────────────────────────────────────────────────────────────

def get_s3_client():
    """Создание boto3-клиента для Yandex Object Storage."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        region_name=S3_REGION,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )


def read_csv_from_s3(s3_client, bucket: str, key: str) -> pd.DataFrame:
    """Чтение CSV-файла из S3 в DataFrame (без локального файла)."""
    log.info(f"Чтение из S3: s3://{bucket}/{key}")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")
    df = pd.read_csv(StringIO(content))
    log.info(f"Загружено строк: {len(df)}, колонок: {len(df.columns)}")
    return df


def write_csv_to_s3(s3_client, df: pd.DataFrame, bucket: str, key: str):
    """Запись DataFrame в S3 в формате CSV (без локального файла)."""
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


# ── Feature engineering ────────────────────────────────────────────────────────

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Добавление производных признаков для LinearRegression."""
    df = df.copy()

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.sort_values(["loco_number", "buxa_position", "timestamp"])

    # Изменение температуры за 30 мин (60 записей × 30 сек)
    df["temp_delta_30min"] = (
        df.groupby(["loco_number", "buxa_position"])["buxa_temp_celsius"]
        .diff(periods=60)
        .fillna(0)
    )

    # Коэффициент загрузки — нормализованный ток
    max_current = df["traction_current_a"].quantile(0.99).clip(lower=1)
    df["load_factor"] = (df["traction_current_a"] / max_current).clip(0, 1)

    # Заполнение пропусков медианой / нулями
    for col in FEATURE_COLS:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].median())
        else:
            df[col] = 0.0

    return df


# ── ML: обучение и прогноз ─────────────────────────────────────────────────────

def train_and_predict(df: pd.DataFrame) -> pd.DataFrame:
    """
    Обучение LinearRegression на данных текущего дня:
      - целевая переменная: температура буксы через 2 часа (shift -240)
      - признаки: FEATURE_COLS
      - оценка: MAE на отложенной выборке
    """
    df = df.copy()

    # Целевая переменная: температура через 2 часа
    df["target_temp"] = (
        df.groupby(["loco_number", "buxa_position"])["buxa_temp_celsius"]
        .shift(-240)
    )

    train_df = df.dropna(subset=["target_temp"])
    n_train = len(train_df)

    if n_train < 200:
        log.warning(
            f"Мало данных для обучения ({n_train} строк), "
            "применяем линейную экстраполяцию"
        )
        df["predicted_temp"] = (
            df["buxa_temp_celsius"] + df["temp_delta_30min"] * 4
        )
        mae = None
    else:
        # Разбивка train/test: последние 20% — тест
        split_idx  = int(n_train * 0.8)
        train_part = train_df.iloc[:split_idx]
        test_part  = train_df.iloc[split_idx:]

        scaler = StandardScaler()
        X_train = scaler.fit_transform(train_part[FEATURE_COLS])
        y_train = train_part["target_temp"].values

        model = LinearRegression()
        model.fit(X_train, y_train)

        # Оценка качества на тесте
        X_test  = scaler.transform(test_part[FEATURE_COLS])
        y_test  = test_part["target_temp"].values
        y_pred  = model.predict(X_test)
        mae     = float(mean_absolute_error(y_test, y_pred))
        log.info(f"Качество модели — MAE: {mae:.2f}°C (тест: {len(test_part)} строк)")

        # Прогноз на всех данных
        X_all = scaler.transform(df[FEATURE_COLS])
        df["predicted_temp"] = model.predict(X_all)

    # Оценка риска
    df["failure_prob"] = (
        (df["predicted_temp"] - THRESHOLD_TEMP) / THRESHOLD_TEMP
    ).clip(0, 1)

    df["risk_level"] = pd.cut(
        df["failure_prob"],
        bins=[0, 0.05, 0.15, 0.30, 1.01],
        labels=["low", "medium", "high", "critical"],
        include_lowest=True,
    )
    df["failure_flag"]     = (df["failure_prob"] > 0.15).astype(int)
    df["prediction_date"]  = TARGET_DATE

    return df, mae


# ── Основная логика ─────────────────────────────────────────────────────────────

def main():
    log.info("=== ML-прогноз отказов букс ТЧЭ-15 (S3 I/O) ===")
    log.info(f"Дата: {TARGET_DATE} | Депо: {DEPOT_CODE} | Порог: {THRESHOLD_TEMP}°C")
    log.info(f"Источник: s3://{S3_BUCKET}/{S3_KEY}")

    s3 = get_s3_client()

    # 1. Чтение данных из S3
    df = read_csv_from_s3(s3, S3_BUCKET, S3_KEY)

    # 2. Фильтрация по дате
    if "reading_date" in df.columns:
        df = df[df["reading_date"] == TARGET_DATE]
        log.info(f"После фильтрации по дате {TARGET_DATE}: {len(df)} строк")

    if len(df) == 0:
        log.error(f"Нет данных датчиков за {TARGET_DATE}, депо {DEPOT_CODE}")
        sys.exit(1)

    # 3. Инженерия признаков
    df = engineer_features(df)

    # 4. Обучение и прогноз
    df, mae = train_and_predict(df)

    # 5. Статистика
    critical_count = int((df["risk_level"] == "critical").sum())
    high_count     = int((df["risk_level"] == "high").sum())
    log.info(f"Критических: {critical_count}, высокий риск: {high_count}")

    # 6. Формирование выходного датасета (только failure_flag == 1)
    available_cols = [c for c in OUTPUT_COLS if c in df.columns]
    predictions_df = df[df["failure_flag"] == 1][available_cols].copy()

    # 7. Запись прогнозов в S3
    date_nodash = TARGET_DATE.replace("-", "")
    out_key = f"predictions/{date_nodash}_predictions.csv"
    write_csv_to_s3(s3, predictions_df, S3_BUCKET_RESULTS, out_key)

    # 8. XCom: JSON в stdout (DockerOperator читает последнюю строку)
    xcom_result = {
        "status":          "success",
        "date":            TARGET_DATE,
        "depot":           DEPOT_CODE,
        "total_records":   len(df),
        "critical_count":  critical_count,
        "high_count":      high_count,
        "predictions_rows": len(predictions_df),
        "model_mae":       round(mae, 3) if mae is not None else None,
        "s3_output":       f"s3://{S3_BUCKET_RESULTS}/{out_key}",
    }
    print(json.dumps(xcom_result))


if __name__ == "__main__":
    main()
```

---

### Шаг 4 — Сборка образа и push в Container Registry

```bash
# Сборка
docker build -t buxa-predictor:v2.0 .

# Локальное тестирование (передаём ключи S3)
docker run --rm \
  -e S3_ACCESS_KEY="<ваш_access_key>" \
  -e S3_SECRET_KEY="<ваш_secret_key>" \
  -e S3_BUCKET="rzd-airflow-data" \
  -e S3_KEY="sensor_readings.csv" \
  -e S3_BUCKET_RESULTS="rzd-airflow-results" \
  -e TARGET_DATE="2024-03-15" \
  -e DEPOT_CODE="TCH-15" \
  buxa-predictor:v2.0

# Аутентификация в Yandex Container Registry
yc container registry configure-docker

# Push в реестр (заменить REGISTRY_ID на реальный)
REGISTRY_ID="crp1abc23defghijk"
docker tag buxa-predictor:v2.0 \
  cr.yandex/${REGISTRY_ID}/buxa-predictor:v2.0
docker push cr.yandex/${REGISTRY_ID}/buxa-predictor:v2.0

# Проверка
yc container image list \
  --repository-name rzd-tceh15-registry/buxa-predictor
```

---

### Шаг 5 — DAG ml_ore_quality_pipeline.py

Полный код DAG приведён в разделе 4.

---

### Шаг 6 — Деплой DAG в rzd-airflow-dags/

```bash
# Загрузить DAG в бакет Managed Airflow
yc storage cp ml_ore_quality_pipeline.py \
  s3://rzd-airflow-dags/dags/ml_ore_quality_pipeline.py

# Проверить загрузку
yc storage ls s3://rzd-airflow-dags/dags/

# Через 1-2 минуты DAG появится в Airflow UI
```

---

### Шаг 7 — Запуск и тестирование в Airflow UI

1. Откройте **Airflow UI → DAGs → ml_ore_quality_pipeline**.
2. Включите DAG (переключатель слева от имени).
3. Нажмите **Trigger DAG** → укажите `conf: {"target_date": "2024-03-15"}`.
4. Откройте **Graph View** — наблюдайте за выполнением задач.
5. Кликните на задачу `predict` → **Logs** — убедитесь, что контейнер читает S3 и пишет результат.

---

### Шаг 8 — Проверка результатов в S3

```bash
# Список файлов прогнозов
yc storage ls s3://rzd-airflow-results/predictions/

# Скачать и проверить содержимое
yc storage cp \
  s3://rzd-airflow-results/predictions/20240315_predictions.csv \
  ./predictions_check.csv

# Просмотр первых строк
head -10 predictions_check.csv

# Ожидаемые колонки:
# loco_number, loco_series, buxa_position, predicted_temp,
# failure_prob, risk_level, failure_flag, buxa_temp_celsius,
# temp_delta_30min, speed_kmh, timestamp, prediction_date
```

---

### Шаг 9 — Верификация через S3Hook в Python

Дополнительная проверка через S3Hook (можно запустить локально или в задаче Airflow):

```python
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from io import StringIO
import pandas as pd

def verify_predictions(bucket: str, key: str, conn_id: str = "yandex_s3"):
    """Чтение и верификация файла прогнозов из S3 через S3Hook."""
    hook = S3Hook(aws_conn_id=conn_id)

    # Проверка существования файла
    if not hook.check_for_key(key=key, bucket_name=bucket):
        raise FileNotFoundError(f"Не найден: s3://{bucket}/{key}")

    # Чтение содержимого
    obj = hook.get_key(key=key, bucket_name=bucket)
    content = obj.get()["Body"].read().decode("utf-8")
    df = pd.read_csv(StringIO(content))

    print(f"Файл: s3://{bucket}/{key}")
    print(f"Строк: {len(df)}")
    print(f"Распределение risk_level:\n{df['risk_level'].value_counts()}")
    print(f"Средняя predicted_temp: {df['predicted_temp'].mean():.1f}°C")

    return df


# Использование
df = verify_predictions(
    bucket="rzd-airflow-results",
    key="predictions/20240315_predictions.csv",
)
```

---

## 4. Полный код DAG

Создайте файл `ml_ore_quality_pipeline.py`:

```python
"""
DAG: ml_ore_quality_pipeline
Платформа: Yandex Managed Service for Apache Airflow™

Описание:
  ML-пайплайн прогноза перегрева букс локомотивов ТЧЭ-15.
  Все файловые операции — через Yandex Object Storage (S3).
  Нет доступа к локальной файловой системе воркера.

Пайплайн:
  wait_for_data (S3KeySensor)
    → extract_s3_path (PythonOperator)
    → predict (DockerOperator: читает S3, пишет S3)
    → verify_results_in_s3 (PythonOperator)

Деплой:
  yc storage cp ml_ore_quality_pipeline.py s3://rzd-airflow-dags/dags/
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.docker.operators.docker import DockerOperator

log = logging.getLogger(__name__)

# ── Константы ──────────────────────────────────────────────────────────────────
REGISTRY        = "cr.yandex/crp1abc23defghijk"   # заменить на реальный registry_id
IMAGE_NAME      = "buxa-predictor"
IMAGE_TAG       = "v2.0"
S3_CONN_ID      = "yandex_s3"
INPUT_S3_KEY    = "sensor_readings.csv"


# ── Функция 1: формирование пути S3 и передача в XCom ─────────────────────────
def extract_s3_path(ds: str, **context) -> dict:
    """
    Формирует пути S3 для входного файла и файла результатов.
    Результат передаётся в XCom для использования downstream-задачами.
    """
    bucket_data    = Variable.get("s3_bucket_data",    default_var="rzd-airflow-data")
    bucket_results = Variable.get("s3_bucket_results", default_var="rzd-airflow-results")
    depot_code     = Variable.get("depot_code",        default_var="TCH-15")
    date_nodash    = ds.replace("-", "")

    s3_paths = {
        "input_bucket":  bucket_data,
        "input_key":     INPUT_S3_KEY,
        "output_bucket": bucket_results,
        "output_key":    f"predictions/{date_nodash}_predictions.csv",
        "target_date":   ds,
        "depot_code":    depot_code,
    }

    # Верификация через S3Hook: файл должен существовать
    hook = S3Hook(aws_conn_id=S3_CONN_ID)
    if not hook.check_for_key(key=INPUT_S3_KEY, bucket_name=bucket_data):
        raise FileNotFoundError(
            f"Входной файл не найден: s3://{bucket_data}/{INPUT_S3_KEY}"
        )

    log.info(f"S3 пути сформированы: {json.dumps(s3_paths, ensure_ascii=False)}")
    return s3_paths


# ── Функция 2: верификация результатов в S3 ────────────────────────────────────
def verify_results_in_s3(ds: str, **context) -> dict:
    """
    Проверяет, что контейнер записал файл прогнозов в S3.
    Читает файл через S3Hook, проверяет количество строк.
    """
    from io import StringIO
    import pandas as pd

    bucket_results = Variable.get("s3_bucket_results", default_var="rzd-airflow-results")
    date_nodash    = ds.replace("-", "")
    expected_key   = f"predictions/{date_nodash}_predictions.csv"

    hook = S3Hook(aws_conn_id=S3_CONN_ID)

    # Проверка наличия файла
    if not hook.check_for_key(key=expected_key, bucket_name=bucket_results):
        raise FileNotFoundError(
            f"Файл прогнозов не найден: s3://{bucket_results}/{expected_key}"
        )

    # Чтение и базовая верификация содержимого
    obj      = hook.get_key(key=expected_key, bucket_name=bucket_results)
    content  = obj.get()["Body"].read().decode("utf-8")
    df       = pd.read_csv(StringIO(content))

    required_cols = ["loco_number", "failure_prob", "risk_level", "predicted_temp"]
    missing_cols  = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"В файле прогнозов отсутствуют колонки: {missing_cols}")

    stats = df["risk_level"].value_counts().to_dict()
    size  = obj.get()["ContentLength"]

    log.info(
        f"Верификация пройдена: s3://{bucket_results}/{expected_key} | "
        f"{len(df)} строк | {size} байт | risk_level: {stats}"
    )

    return {
        "s3_key":    expected_key,
        "rows":      len(df),
        "size_bytes": size,
        "risk_stats": stats,
        "date":      ds,
    }


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
    dag_id="ml_ore_quality_pipeline",
    description=(
        "ML-пайплайн прогноза букс ТЧЭ-15 | "
        "S3 данные → DockerOperator → результаты в S3 | "
        "Деплой: rzd-airflow-dags/dags/"
    ),
    schedule="0 */4 * * *",
    start_date=datetime(2024, 3, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["rzd", "tceh15", "ml", "buxa", "docker", "s3"],
) as dag:

    # ── Задача 1: S3KeySensor — ожидание появления файла в S3 ─────────────────
    # Заменяет FileSensor. mode='reschedule' освобождает слот воркера.
    wait_for_data = S3KeySensor(
        task_id="wait_for_sensor_data",
        bucket_name="rzd-airflow-data",
        bucket_key=INPUT_S3_KEY,
        aws_conn_id=S3_CONN_ID,
        poke_interval=300,     # каждые 5 минут
        timeout=7200,          # таймаут 2 часа
        mode="reschedule",
    )

    # ── Задача 2: PythonOperator — формирование путей S3, передача в XCom ─────
    extract_s3_path_task = PythonOperator(
        task_id="extract_s3_path",
        python_callable=extract_s3_path,
    )

    # ── Задача 3: DockerOperator — контейнер читает S3, пишет S3 ──────────────
    # Данные передаются через переменные окружения (bucket + key), не volume.
    # Контейнер сам обращается к S3 через boto3.
    predict = DockerOperator(
        task_id="predict",
        image=f"{REGISTRY}/{IMAGE_NAME}:{IMAGE_TAG}",
        command="python predict_buxa_failure.py",
        environment={
            # Yandex Object Storage credentials
            "S3_ENDPOINT":       "https://storage.yandexcloud.net",
            "S3_REGION":         "ru-central1",
            "S3_ACCESS_KEY":     "{{ conn.yandex_s3.login }}",
            "S3_SECRET_KEY":     "{{ conn.yandex_s3.password }}",
            # Источник данных в S3
            "S3_BUCKET":         "rzd-airflow-data",
            "S3_KEY":            INPUT_S3_KEY,
            # Назначение результатов в S3
            "S3_BUCKET_RESULTS": "rzd-airflow-results",
            # Параметры прогноза
            "TARGET_DATE":       "{{ ds }}",
            "DEPOT_CODE":        "{{ var.value.depot_code }}",
            "THRESHOLD_TEMP":    "80",
        },
        # Локальные тома не нужны — данные в S3
        mounts=[],
        docker_url="unix:///var/run/docker.sock",
        network_mode="bridge",
        auto_remove=True,
        mem_limit="4g",
        cpus=2.0,
        # XCom: last line of stdout (JSON из print() в скрипте)
        retrieve_output=True,
        retrieve_output_path="/tmp/xcom_result.json",
    )

    # ── Задача 4: PythonOperator — верификация результатов в S3 ───────────────
    verify_results = PythonOperator(
        task_id="verify_results_in_s3",
        python_callable=verify_results_in_s3,
    )

    # ── Граф зависимостей ──────────────────────────────────────────────────────
    wait_for_data >> extract_s3_path_task >> predict >> verify_results
```

---

## 5. Деплой: загрузка в rzd-airflow-dags/ и проверка в UI

```bash
# 1. Загрузка DAG-файла в бакет Managed Airflow
yc storage cp ml_ore_quality_pipeline.py \
  s3://rzd-airflow-dags/dags/ml_ore_quality_pipeline.py

# Альтернатива через AWS CLI
aws s3 cp ml_ore_quality_pipeline.py \
  s3://rzd-airflow-dags/dags/ml_ore_quality_pipeline.py \
  --endpoint-url https://storage.yandexcloud.net

# 2. Проверка — файл в бакете
yc storage ls s3://rzd-airflow-dags/dags/
# Ожидаемый вывод:
# ... ml_ore_quality_pipeline.py

# 3. Через 1-2 минуты DAG появится в Airflow UI:
#    https://<managed-airflow-url>/dags/ml_ore_quality_pipeline

# 4. Проверка синтаксиса DAG перед загрузкой (локально)
python -c "import ml_ore_quality_pipeline; print('OK')"

# НЕ использовать для деплоя:
# - ssh / scp на воркер Airflow
# - airflow dags / airflow tasks CLI с локальным путём
# - прямое копирование в папку dags/ — доступа к ней нет
```

---

## 6. Ожидаемый результат

После успешного выполнения всех задач DAG:

**В Airflow UI (Graph View):**
```
wait_for_sensor_data  →  extract_s3_path  →  predict  →  verify_results_in_s3
     [success]              [success]        [success]        [success]
```

**В S3 бакете rzd-airflow-results:**
```
predictions/
  └── 20240315_predictions.csv   (≈ 15–50 KB, только failure_flag=1)
```

**XCom задачи predict (Admin → XCom в Airflow UI):**
```json
{
  "status":           "success",
  "date":             "2024-03-15",
  "depot":            "TCH-15",
  "total_records":    48320,
  "critical_count":   5,
  "high_count":       18,
  "predictions_rows": 23,
  "model_mae":        2.741,
  "s3_output":        "s3://rzd-airflow-results/predictions/20240315_predictions.csv"
}
```

**Первые строки файла predictions.csv:**

| loco_number | loco_series | buxa_position | predicted_temp | risk_level | failure_prob |
|---|---|---|---|---|---|
| ВЛ-80-0047 | VL80S | L2 | 87.4 | critical | 0.34 |
| 2ТЭ-116-0012 | 2TE116 | R1 | 84.1 | high | 0.18 |
| ЭП2К-0089 | EP2K | L3 | 83.7 | high | 0.16 |

---

## 7. Задания повышенной сложности

### Задание 1: Динамический выбор даты через S3

Измените задачу `extract_s3_path` так, чтобы она обрабатывала файл с динамическим
именем вида `sensor_readings_{{ ds_nodash }}.csv`. Если файл за текущую дату не найден
(S3KeySensor истёк по timeout) — использовать `sensor_readings.csv` как fallback.

**Требования:**
- Логика выбора файла — в `extract_s3_path` через `S3Hook.check_for_key`
- Приоритет: `sensor_readings_{{ ds_nodash }}.csv` > `sensor_readings.csv`
- Название итогового ключа передать в XCom под ключом `actual_input_key`
- `DockerOperator` должен читать `actual_input_key` из XCom через Jinja-шаблон:
  `"S3_KEY": "{{ ti.xcom_pull(task_ids='extract_s3_path')['actual_input_key'] }}"`

### Задание 2: Запись промежуточных результатов в S3 вместо XCom

В текущей реализации контейнер передаёт результаты через stdout (XCom).
Переделайте пайплайн: контейнер записывает результаты только в S3,
задача `verify_results_in_s3` читает их через `S3Hook` и формирует итоговый XCom.

**Требования:**
- Убрать `retrieve_output=True` из `DockerOperator`
- Скрипт `predict_buxa_failure.py` записывает JSON-метрики в S3:
  `s3://rzd-airflow-results/predictions/<date>_metrics.json`
- `verify_results_in_s3` читает этот JSON через `S3Hook.get_key` и проверяет `status == "success"`
- Если `model_mae > 5.0` — поднять исключение `AirflowException` с описанием проблемы

### Задание 3: Ежедневный отчёт в S3 с агрегатами по сериям

Добавьте задачу `generate_daily_report` (PythonOperator) после `verify_results_in_s3`.
Задача читает `predictions.csv` из S3, формирует агрегированный отчёт и записывает его в S3.

**Требования:**
- Использовать только `S3Hook` (без локальных файлов):
  ```python
  hook = S3Hook(aws_conn_id='yandex_s3')
  obj = hook.get_key(key=predictions_key, bucket_name=bucket_results)
  content = obj.get()['Body'].read().decode('utf-8')
  df = pd.read_csv(StringIO(content))
  ```
- Агрегация: GROUP BY `loco_series`, `risk_level` — COUNT, AVG(failure_prob), MAX(predicted_temp)
- Отчёт записать в `s3://rzd-airflow-results/reports/<date>_daily_report.csv`
- Если critical_count > 10 — добавить файл-флаг `s3://rzd-airflow-results/alerts/<date>_critical_alert.txt`
  с текстом `"CRITICAL: {critical_count} buxa failures predicted for {date}, depot {depot_code}"`
