# Лабораторная работа №07: Полный пайплайн на кастомных компонентах с Object Storage

**Модуль:** 07 — Разработка пользовательских компонент  
**Продолжительность:** 90 минут  
**Платформа:** Yandex Managed Service for Apache Airflow™  
**Организация:** РЖД, Западно-Сибирская дирекция тяги, ТЧЭ-15 Новосибирск-Главный  
**Уровень:** Продвинутый

---

## Цель

Разработать полноценный пайплайн `mes_custom_pipeline` на кастомных компонентах:

- `LatestFileS3Sensor` — ждёт появление нового файла телеметрии в бакете `rzd-airflow-data/incoming/`
- `YandexS3Hook` — все файловые операции только через Object Storage
- `LocomotiveTelemetryOperator` — читает CSV через `YandexS3Hook`, валидирует данные, пишет в PostgreSQL
- Деплой всех компонентов через бакет `rzd-airflow-dags`

Архитектура пайплайна:

```
Yandex Object Storage
  rzd-airflow-data/incoming/          rzd-airflow-results/
         │                                     ▲
         ▼                                     │
  LatestFileS3Sensor                           │
         │                                     │
         ▼                                     │
  LocomotiveTelemetryOperator ─── write ───────┘
         │
         ▼
  PostgreSQL (rzd_analytics.sensor_readings)
         │
         ▼
  fleet_summary (XCom → отчёт)
```

---

## Предварительные условия

### Managed Airflow

- Кластер Yandex Managed Service for Apache Airflow™ запущен.
- Бакет `rzd-airflow-dags` привязан к кластеру как источник DAG-файлов.
- Сервисный аккаунт кластера имеет роль `storage.editor` на все бакеты.

### Yandex Object Storage — бакеты и структура

```
rzd-airflow-dags/
└── dags/
    ├── yandex_s3_hook.py
    ├── loco_telemetry_operator.py
    ├── latest_file_s3_sensor.py
    └── mes_custom_pipeline.py

rzd-airflow-data/
├── sensor_readings.csv
├── locomotives.csv
├── trips.csv
├── maintenance.csv
└── incoming/                    ← сюда попадают новые файлы телеметрии
    └── telemetry_20240601.csv

rzd-airflow-results/
└── processed/                   ← результаты обработки
```

### Airflow Connections

Настроить в **Admin → Connections**:

**S3 (Yandex Object Storage):**

| Поле      | Значение                                                                          |
|-----------|-----------------------------------------------------------------------------------|
| Conn Id   | `yandex_s3`                                                                       |
| Conn Type | Amazon Web Services                                                                         |
| Login     | `<Access Key ID>`                                                                 |
| Password  | `<Secret Access Key>`                                                             |
| Extra     | `{"endpoint_url": "https://storage.yandexcloud.net", "region_name": "ru-central1"}` |

**PostgreSQL (Yandex Managed Service for PostgreSQL):**

| Поле      | Значение                                       |
|-----------|------------------------------------------------|
| Conn Id   | `rzd_postgres`                                 |
| Conn Type | Postgres                                       |
| Host      | `<FQDN кластера>.mdb.yandexcloud.net`          |
| Port      | `6432`                                         |
| Schema    | `rzd_analytics`                                |
| Login     | `<пользователь БД>`                            |
| Password  | `<пароль>`                                     |

### Airflow Variables

| Key                  | Value                  |
|----------------------|------------------------|
| `s3_bucket_data`     | `rzd-airflow-data`     |
| `s3_bucket_results`  | `rzd-airflow-results`  |
| `depot_code`         | `TCH-15`               |
| `delay_threshold_min` | `15`                  |

---

## Задание

### Шаг 1. Загрузка тестового файла телеметрии в incoming/

Создайте тестовый файл `telemetry_20240601.csv` и загрузите его в бакет:

```bash
yc storage cp telemetry_20240601.csv \
    s3://rzd-airflow-data/incoming/telemetry_20240601.csv
```

Формат файла:

```
loco_id,recorded_at,speed_kmh,buxa_temp_c,traction_current_a,engine_hours
ВЛ80С-1234,2024-06-01T06:00:00,72.5,45.2,650.0,1024.5
ВЛ80С-1234,2024-06-01T06:01:00,74.1,46.0,660.0,1024.52
ЭП2К-0412,2024-06-01T06:00:00,88.0,41.5,720.0,512.3
2ТЭ116-1876,2024-06-01T06:00:00,65.0,42.0,,2180.3
```

---

### Шаг 2. Создание `yandex_s3_hook.py`

```python
# yandex_s3_hook.py
"""
Кастомный хук для Yandex Object Storage (S3-совместимый).
Все файловые операции только через этот хук — без обращения
к локальной файловой системе.

Деплой:
    yc storage cp yandex_s3_hook.py \
        s3://rzd-airflow-dags/dags/yandex_s3_hook.py
"""
from __future__ import annotations

from io import StringIO
from typing import List, Optional

import boto3
import pandas as pd

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class YandexS3Hook(S3Hook):
    """
    Хук для Yandex Object Storage.
    Переопределяет get_conn() — явно прописывает endpoint_url.
    """

    conn_name_attr    = 'aws_conn_id'
    default_conn_name = 'yandex_s3'
    hook_name         = 'Yandex Object Storage'

    def __init__(self, aws_conn_id: str = 'yandex_s3', **kwargs) -> None:
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)

    def get_conn(self):
        """Создаёт boto3 S3-клиент с endpoint_url для Яндекса."""
        conn  = self.get_connection(self.aws_conn_id)
        extra = conn.extra_dejson if conn.extra else {}

        session = boto3.session.Session()
        return session.client(
            service_name='s3',
            endpoint_url=extra.get(
                'endpoint_url', 'https://storage.yandexcloud.net'
            ),
            region_name=extra.get('region_name', 'ru-central1'),
            aws_access_key_id=conn.login,
            aws_secret_access_key=conn.password,
        )

    def list_files(self, bucket: str, prefix: str = '') -> List[str]:
        """Возвращает список ключей объектов по префиксу."""
        client   = self.get_conn()
        response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        contents = response.get('Contents', [])
        keys = [obj['Key'] for obj in contents]
        self.log.info(
            "list_files: %s/%s → %d объектов", bucket, prefix, len(keys)
        )
        return keys

    def get_latest_file(
        self, bucket: str, prefix: str = ''
    ) -> Optional[str]:
        """Возвращает ключ самого нового объекта в бакете по префиксу."""
        client   = self.get_conn()
        response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        contents = response.get('Contents', [])
        if not contents:
            self.log.warning(
                "get_latest_file: нет файлов в %s/%s", bucket, prefix
            )
            return None
        latest = max(contents, key=lambda obj: obj['LastModified'])
        self.log.info(
            "get_latest_file: %s (изменён %s)",
            latest['Key'], latest['LastModified'].isoformat(),
        )
        return latest['Key']

    def read_csv(self, bucket: str, key: str) -> pd.DataFrame:
        """Читает CSV из Object Storage в DataFrame."""
        client   = self.get_conn()
        response = client.get_object(Bucket=bucket, Key=key)
        content  = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(content))
        self.log.info(
            "read_csv: %s/%s → %d строк", bucket, key, len(df)
        )
        return df

    def write_csv(
        self, df: pd.DataFrame, bucket: str, key: str
    ) -> None:
        """Записывает DataFrame в CSV в Object Storage."""
        client = self.get_conn()
        buffer = StringIO()
        df.to_csv(buffer, index=False)
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue().encode('utf-8'),
        )
        self.log.info(
            "write_csv: %s/%s (%d строк)", bucket, key, len(df)
        )
```

---

### Шаг 3. Создание `latest_file_s3_sensor.py`

```python
# latest_file_s3_sensor.py
"""
Сенсор ожидания появления нового файла телеметрии
в бакете rzd-airflow-data/incoming/.

В отличие от стандартного S3KeySensor, который ждёт конкретный ключ,
LatestFileS3Sensor ждёт ЛЮБОЙ новый файл по префиксу.

Деплой:
    yc storage cp latest_file_s3_sensor.py \
        s3://rzd-airflow-dags/dags/latest_file_s3_sensor.py
"""
from __future__ import annotations

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from yandex_s3_hook import YandexS3Hook


class LatestFileS3Sensor(BaseSensorOperator):
    """
    Ожидает появления нового файла в бакете Object Storage по префиксу.

    Сценарий: внешняя система кладёт файлы телеметрии в папку
    rzd-airflow-data/incoming/. Сенсор проверяет с заданным интервалом,
    появился ли новый файл. Когда файл найден — возвращает True,
    и ключ файла записывается в XCom для следующих задач.

    :param bucket:      имя бакета (напр. 'rzd-airflow-data')
    :param prefix:      префикс пути (напр. 'incoming/')
    :param aws_conn_id: ID соединения с Object Storage
    """

    template_fields = ('bucket', 'prefix')
    ui_color = '#70AD47'

    @apply_defaults
    def __init__(
        self,
        bucket: str,
        prefix: str = 'incoming/',
        aws_conn_id: str = 'yandex_s3',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket      = bucket
        self.prefix      = prefix
        self.aws_conn_id = aws_conn_id

    def poke(self, context: dict) -> bool:
        """
        Проверяет наличие файлов по префиксу.

        Возвращает True, если найден хотя бы один файл.
        Сохраняет ключ самого нового файла в XCom (ключ 'latest_file_key').
        """
        self.log.info(
            "[LatestFileS3Sensor] Проверяем %s/%s",
            self.bucket, self.prefix,
        )

        hook       = YandexS3Hook(aws_conn_id=self.aws_conn_id)
        latest_key = hook.get_latest_file(
            bucket=self.bucket, prefix=self.prefix
        )

        if latest_key is None:
            self.log.info("  → файлов нет, ждём следующей проверки")
            return False

        self.log.info("  → найден файл: %s", latest_key)

        # Сохраняем ключ файла в XCom для использования в следующих задачах
        context['task_instance'].xcom_push(
            key='latest_file_key', value=latest_key
        )
        return True
```

---

### Шаг 4. Создание `loco_telemetry_operator.py`

```python
# loco_telemetry_operator.py
"""
Оператор загрузки, валидации и записи телеметрии локомотивов.

Читает CSV из Yandex Object Storage через YandexS3Hook,
валидирует данные, пишет в PostgreSQL rzd_analytics.sensor_readings,
сохраняет отчёт обратно в Object Storage.

Деплой:
    yc storage cp loco_telemetry_operator.py \
        s3://rzd-airflow-dags/dags/loco_telemetry_operator.py
"""
from __future__ import annotations

from io import StringIO
from typing import Any, Dict, List, Optional

import pandas as pd

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.decorators import apply_defaults

from yandex_s3_hook import YandexS3Hook

# Критические пороги датчиков по регламенту ТЧЭ-15
THRESHOLDS = {
    'buxa_temp_c':        80.0,   # перегрев буксы, °C
    'traction_current_a': 900.0,  # перегрузка тягового тока, А
}


class LocomotiveTelemetryOperator(BaseOperator):
    """
    Загружает телеметрию из Object Storage, валидирует и пишет в PostgreSQL.

    Этапы выполнения:
      1. Читает ключ входного файла из XCom (от LatestFileS3Sensor)
      2. Читает CSV через YandexS3Hook
      3. Валидирует обязательные столбцы и типы данных
      4. Проверяет критические пороги датчиков
      5. Записывает строки в rzd_analytics.sensor_readings
      6. Сохраняет отчёт валидации в rzd-airflow-results
      7. Возвращает сводку через XCom

    :param src_bucket:       бакет с входными данными
    :param results_bucket:   бакет для результатов
    :param sensor_task_id:   task_id сенсора, из XCom которого берётся ключ файла
    :param aws_conn_id:      ID соединения с Object Storage
    :param pg_conn_id:       ID соединения с PostgreSQL
    """

    template_fields = ('src_bucket', 'results_bucket')
    ui_color  = '#4472C4'
    ui_fgcolor = '#ffffff'

    REQUIRED_COLUMNS = ['loco_id', 'recorded_at', 'speed_kmh', 'buxa_temp_c']

    @apply_defaults
    def __init__(
        self,
        src_bucket: str = 'rzd-airflow-data',
        results_bucket: str = 'rzd-airflow-results',
        sensor_task_id: str = 'wait_for_telemetry_file',
        aws_conn_id: str = 'yandex_s3',
        pg_conn_id: str = 'rzd_postgres',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.src_bucket      = src_bucket
        self.results_bucket  = results_bucket
        self.sensor_task_id  = sensor_task_id
        self.aws_conn_id     = aws_conn_id
        self.pg_conn_id      = pg_conn_id

    def execute(self, context: dict) -> Dict[str, Any]:
        ti  = context['task_instance']
        ds  = context['ds']

        # 1. Получаем ключ входного файла из XCom сенсора
        file_key = ti.xcom_pull(
            task_ids=self.sensor_task_id, key='latest_file_key'
        )
        if not file_key:
            raise AirflowException(
                f"Ключ файла не найден в XCom задачи '{self.sensor_task_id}'"
            )
        self.log.info("Входной файл: %s/%s", self.src_bucket, file_key)

        # 2. Читаем CSV через YandexS3Hook
        s3_hook = YandexS3Hook(aws_conn_id=self.aws_conn_id)
        df = s3_hook.read_csv(bucket=self.src_bucket, key=file_key)

        # 3. Валидация обязательных столбцов
        missing = [c for c in self.REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            raise AirflowException(
                f"Отсутствуют обязательные столбцы: {missing}"
            )

        df['recorded_at'] = pd.to_datetime(df['recorded_at'], errors='coerce')
        invalid_ts = df['recorded_at'].isna().sum()
        if invalid_ts:
            self.log.warning(
                "Отброшено %d строк с некорректным recorded_at", invalid_ts
            )
        df = df.dropna(subset=['recorded_at'])

        # 4. Проверка критических порогов
        alerts: List[Dict[str, Any]] = []
        for sensor, threshold in THRESHOLDS.items():
            if sensor in df.columns:
                over_limit = df[df[sensor] > threshold]
                if not over_limit.empty:
                    alerts.append({
                        'sensor':    sensor,
                        'max_value': float(df[sensor].max()),
                        'threshold': threshold,
                        'count':     len(over_limit),
                    })
                    self.log.warning(
                        "АЛЕРТ [%s]: %d превышений порога %.1f",
                        sensor, len(over_limit), threshold,
                    )

        # 5. Запись в PostgreSQL rzd_analytics.sensor_readings
        pg_hook = PostgresHook(postgres_conn_id=self.pg_conn_id)
        insert_sql = """
            INSERT INTO rzd_analytics.sensor_readings
                (loco_id, recorded_at, speed_kmh, buxa_temp_c,
                 traction_current_a, engine_hours, is_alert)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """
        rows_inserted = 0
        conn = pg_hook.get_conn()
        with conn.cursor() as cur:
            for _, row in df.iterrows():
                loco_id = row['loco_id']
                is_alert = any(
                    row.get(a['sensor'], 0) > a['threshold']
                    for a in alerts
                )
                cur.execute(insert_sql, (
                    loco_id,
                    row['recorded_at'],
                    row.get('speed_kmh'),
                    row.get('buxa_temp_c'),
                    row.get('traction_current_a'),
                    row.get('engine_hours'),
                    is_alert,
                ))
                rows_inserted += 1
        conn.commit()
        self.log.info(
            "Записано %d строк в rzd_analytics.sensor_readings", rows_inserted
        )

        # 6. Сохраняем отчёт в rzd-airflow-results
        report_df = pd.DataFrame([{
            'date':           ds,
            'source_key':     file_key,
            'total_rows':     len(df),
            'rows_inserted':  rows_inserted,
            'alerts_count':   len(alerts),
            'invalid_ts':     invalid_ts,
        }])
        report_key = f"processed/{ds}_validation_report.csv"
        s3_hook.write_csv(
            df=report_df,
            bucket=self.results_bucket,
            key=report_key,
        )
        self.log.info(
            "Отчёт сохранён: %s/%s", self.results_bucket, report_key
        )

        # 7. Возвращаем сводку через XCom
        return {
            'source_key':    file_key,
            'report_key':    report_key,
            'total_rows':    len(df),
            'rows_inserted': rows_inserted,
            'alerts_count':  len(alerts),
            'alerts':        alerts,
            'date':          ds,
        }
```

---

### Шаг 5. Создание DAG `mes_custom_pipeline.py`

```python
# mes_custom_pipeline.py
"""
Полный пайплайн MES на кастомных компонентах с Yandex Object Storage.

LatestFileS3Sensor   → LocomotiveTelemetryOperator → fleet_summary
(ждёт новый файл         (читает S3, валидирует,
 в incoming/)             пишет в PostgreSQL)

Деплой:
    yc storage cp mes_custom_pipeline.py \
        s3://rzd-airflow-dags/dags/mes_custom_pipeline.py

После загрузки DAG появится в Airflow UI через 1–2 минуты.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from latest_file_s3_sensor import LatestFileS3Sensor
from loco_telemetry_operator import LocomotiveTelemetryOperator

# ──────────────────────────────────────────────────────────────
# Конфигурация из Airflow Variables
# ──────────────────────────────────────────────────────────────

S3_BUCKET_DATA    = Variable.get('s3_bucket_data',    default_var='rzd-airflow-data')
S3_BUCKET_RESULTS = Variable.get('s3_bucket_results', default_var='rzd-airflow-results')

default_args = {
    'owner':            'tche15-analytics',
    'depends_on_past':  False,
    'retries':          2,
    'retry_delay':      timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# ──────────────────────────────────────────────────────────────
# DAG
# ──────────────────────────────────────────────────────────────

with DAG(
    dag_id='mes_custom_pipeline',
    description='MES-пайплайн ТЧЭ-15: S3Sensor → TelemetryOperator → PostgreSQL',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 7 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['rzd', 'tche15', 'mes', 's3', 'module-07'],
) as dag:

    # ── Задача 1: ждём новый файл телеметрии в incoming/ ──
    wait_for_telemetry_file = LatestFileS3Sensor(
        task_id='wait_for_telemetry_file',
        bucket=S3_BUCKET_DATA,
        prefix='incoming/',
        aws_conn_id='yandex_s3',
        poke_interval=300,    # проверка каждые 5 минут
        timeout=7200,         # максимальное ожидание — 2 часа
        mode='reschedule',    # освобождать worker-слот между проверками
    )

    # ── Задача 2: читаем файл, валидируем, пишем в PostgreSQL ──
    process_telemetry = LocomotiveTelemetryOperator(
        task_id='process_telemetry',
        src_bucket=S3_BUCKET_DATA,
        results_bucket=S3_BUCKET_RESULTS,
        sensor_task_id='wait_for_telemetry_file',
        aws_conn_id='yandex_s3',
        pg_conn_id='rzd_postgres',
    )

    # ── Задача 3: итоговый отчёт из XCom ──
    def fleet_summary(**context):
        """
        Читает результаты обработки из XCom и формирует итоговый отчёт.
        Дополнительно читает отчёт из Object Storage для верификации.
        """
        from yandex_s3_hook import YandexS3Hook

        ti     = context['task_instance']
        ds     = context['ds']
        result = ti.xcom_pull(task_ids='process_telemetry') or {}

        print("=" * 55)
        print(f" СУТОЧНЫЙ ОТЧЁТ MES ТЧЭ-15 — {ds}")
        print("=" * 55)
        print(f" Исходный файл:      {result.get('source_key', 'н/д')}")
        print(f" Всего строк:        {result.get('total_rows', 0)}")
        print(f" Записано в БД:      {result.get('rows_inserted', 0)}")
        print(f" Критических алертов: {result.get('alerts_count', 0)}")

        if result.get('alerts'):
            print(" Детали алертов:")
            for alert in result['alerts']:
                print(
                    f"   [{alert['sensor']}] макс. {alert['max_value']:.1f} "
                    f"(порог {alert['threshold']:.1f}), "
                    f"превышений: {alert['count']}"
                )

        # Верификация: читаем отчёт из Object Storage
        if result.get('report_key'):
            try:
                s3_hook    = YandexS3Hook(aws_conn_id='yandex_s3')
                report_df  = s3_hook.read_csv(
                    bucket=S3_BUCKET_RESULTS,
                    key=result['report_key'],
                )
                print(f" Отчёт в S3 ({result['report_key']}):")
                print(report_df.to_string(index=False))
            except Exception as exc:
                print(f" Не удалось прочитать отчёт из S3: {exc}")

        print("=" * 55)
        return result

    summary_task = PythonOperator(
        task_id='fleet_summary',
        python_callable=fleet_summary,
    )

    # ── Граф зависимостей ──
    wait_for_telemetry_file >> process_telemetry >> summary_task
```

---

### Шаг 6. Деплой всех компонентов в `rzd-airflow-dags`

Все файлы загружаются в бакет DAG-файлов. Прямой доступ к файловой системе
контейнера Managed Airflow отсутствует — только через Object Storage.

```bash
# Деплой хука
yc storage cp yandex_s3_hook.py \
    s3://rzd-airflow-dags/dags/yandex_s3_hook.py

# Деплой сенсора
yc storage cp latest_file_s3_sensor.py \
    s3://rzd-airflow-dags/dags/latest_file_s3_sensor.py

# Деплой оператора
yc storage cp loco_telemetry_operator.py \
    s3://rzd-airflow-dags/dags/loco_telemetry_operator.py

# Деплой DAG-файла
yc storage cp mes_custom_pipeline.py \
    s3://rzd-airflow-dags/dags/mes_custom_pipeline.py

# Проверка содержимого бакета
yc storage ls s3://rzd-airflow-dags/dags/
```

Ожидаемый вывод после деплоя:

```
2024-06-01 07:00:00    1234  dags/latest_file_s3_sensor.py
2024-06-01 07:00:00    2345  dags/loco_telemetry_operator.py
2024-06-01 07:00:00    3456  dags/mes_custom_pipeline.py
2024-06-01 07:00:00    2100  dags/yandex_s3_hook.py
```

---

### Шаг 7. Проверка в Airflow UI

1. Откройте **DAGs** — через 1–2 минуты появится `mes_custom_pipeline`.
2. Убедитесь, что DAG не содержит ошибок импорта (колонка **Last Parse Time**
   не должна содержать красных иконок).
3. Нажмите **Trigger DAG** для ручного запуска.
4. В **Graph View** дождитесь завершения задачи `wait_for_telemetry_file`
   (зелёный цвет появится после того, как файл обнаружен в `incoming/`).
5. Проследите выполнение `process_telemetry` и `fleet_summary`.

---

### Шаг 8. Проверка данных в PostgreSQL

Подключитесь к PostgreSQL через **Airflow UI → Admin → Connections → rzd_postgres**
или через psql:

```sql
-- Проверка загруженных строк
SELECT
    loco_id,
    COUNT(*)                          AS records,
    AVG(speed_kmh)::NUMERIC(5,1)      AS avg_speed,
    MAX(buxa_temp_c)                  AS max_buxa,
    SUM(CASE WHEN is_alert THEN 1 ELSE 0 END) AS alerts
FROM rzd_analytics.sensor_readings
WHERE recorded_at::date = '2024-06-01'
GROUP BY loco_id
ORDER BY max_buxa DESC;

-- Последние загруженные записи
SELECT loco_id, recorded_at, speed_kmh, buxa_temp_c, is_alert
FROM rzd_analytics.sensor_readings
ORDER BY loaded_at DESC
LIMIT 10;
```

---

### Шаг 9. Проверка результатов в Object Storage

```bash
# Список результирующих файлов
yc storage ls s3://rzd-airflow-results/processed/

# Скачать отчёт валидации для проверки
yc storage cp s3://rzd-airflow-results/processed/2024-06-01_validation_report.csv \
    ./2024-06-01_validation_report.csv
```

---

## Ожидаемый результат

После успешного выполнения лабораторной работы:

1. В бакете `rzd-airflow-dags/dags/` находятся 4 файла Python.
2. DAG `mes_custom_pipeline` появился в Airflow UI без ошибок импорта.
3. Задача `wait_for_telemetry_file` завершилась успешно — обнаружила файл
   в `rzd-airflow-data/incoming/` и сохранила его ключ в XCom.
4. Задача `process_telemetry` прочитала CSV через `YandexS3Hook`,
   записала строки в `rzd_analytics.sensor_readings`,
   сохранила отчёт в `rzd-airflow-results/processed/`.
5. XCom задачи `process_telemetry` содержит:
   ```json
   {
     "source_key": "incoming/telemetry_20240601.csv",
     "report_key": "processed/2024-06-01_validation_report.csv",
     "total_rows": 4,
     "rows_inserted": 4,
     "alerts_count": 0,
     "date": "2024-06-01"
   }
   ```
6. Запрос к PostgreSQL возвращает строки с корректными данными.

---

## Задания повышенной сложности

### Задание 1 (★★): Архивирование обработанных файлов

После успешной обработки добавьте задачу, которая перемещает файл из
`rzd-airflow-data/incoming/` в `rzd-airflow-data/archive/YYYY-MM-DD/`.

Используйте только `YandexS3Hook` (методы `get_conn()` → `copy_object`
и `delete_object`). Локальная файловая система не используется.

```python
def archive_processed_file(**context):
    from yandex_s3_hook import YandexS3Hook

    ti       = context['task_instance']
    ds       = context['ds']
    result   = ti.xcom_pull(task_ids='process_telemetry') or {}
    src_key  = result.get('source_key')

    if not src_key:
        return

    dst_key = f"archive/{ds}/{src_key.split('/')[-1]}"
    hook    = YandexS3Hook(aws_conn_id='yandex_s3')
    client  = hook.get_conn()

    # Копируем
    client.copy_object(
        Bucket='rzd-airflow-data',
        CopySource={'Bucket': 'rzd-airflow-data', 'Key': src_key},
        Key=dst_key,
    )
    # Удаляем оригинал
    client.delete_object(Bucket='rzd-airflow-data', Key=src_key)
    print(f"Файл перемещён: {src_key} → {dst_key}")
```

### Задание 2 (★★): S3KeySensor вместо LatestFileS3Sensor

Замените `LatestFileS3Sensor` на стандартный `S3KeySensor`, настроив его
на ожидание файла с именем вида `incoming/telemetry_{{ ds_nodash }}.csv`.

```python
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

wait_for_telemetry_file = S3KeySensor(
    task_id='wait_for_telemetry_file',
    bucket_name='rzd-airflow-data',
    bucket_key='incoming/telemetry_{{ ds_nodash }}.csv',
    aws_conn_id='yandex_s3',
    poke_interval=300,
    timeout=7200,
    mode='reschedule',
)
```

Сравните поведение двух подходов: что будет, если файл загружен
не точно по дате, а с опозданием на сутки?

### Задание 3 (★★★): Параллельная обработка нескольких файлов

Измените `LocomotiveTelemetryOperator` — вместо одного последнего файла
читайте все файлы из `incoming/`, появившиеся за последние 24 часа,
используя `list_files()` и фильтрацию по дате в имени файла.
Агрегируйте результаты в единый DataFrame перед записью в PostgreSQL.
