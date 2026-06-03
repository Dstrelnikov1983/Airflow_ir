# Практическая работа №07: Кастомный S3Hook для Yandex Object Storage

**Модуль:** 07 — Разработка пользовательских компонент  
**Продолжительность:** 45–60 минут  
**Платформа:** Yandex Managed Service for Apache Airflow™  
**Организация:** РЖД, Западно-Сибирская дирекция тяги, ТЧЭ-15 Новосибирск-Главный

---

## Цель и задачи

Разработать кастомный хук `YandexS3Hook`, расширяющий стандартный `S3Hook`
для работы с Yandex Object Storage, и оператор `LocomotiveTelemetryOperator`,
который читает файлы телеметрии через этот хук.

**Задачи:**

1. Создать `YandexS3Hook(S3Hook)` — переопределить `get_conn()` с `endpoint_url`
   для Яндекс и добавить вспомогательные методы для работы с бакетами.
2. Реализовать метод `list_files(bucket, prefix)` для перечисления файлов.
3. Реализовать метод `get_latest_file(bucket, prefix)` — возвращает самый новый файл.
4. Создать `LocomotiveTelemetryOperator`, использующий `YandexS3Hook` для чтения
   CSV с телеметрией из бакета `rzd-airflow-data`.
5. Задеплоить DAG-файл в бакет `rzd-airflow-dags` через Yandex Cloud CLI.

---

## Предварительные условия

### Managed Airflow

- Создан кластер Yandex Managed Service for Apache Airflow™.
- Кластер привязан к бакету DAG-файлов `rzd-airflow-dags`.
- Кластер имеет сервисный аккаунт с ролью `storage.editor`.

### Yandex Object Storage

Бакеты уже созданы и содержат данные:

```
rzd-airflow-dags/          — DAG-файлы (связан с Managed Airflow)
rzd-airflow-data/          — входные CSV-файлы
  ├── sensor_readings.csv
  ├── locomotives.csv
  ├── trips.csv
  └── maintenance.csv
rzd-airflow-results/       — результаты обработки
```

### Airflow Connection

Настроить в **Airflow UI → Admin → Connections → + Add**:

| Поле      | Значение                                                                          |
|-----------|-----------------------------------------------------------------------------------|
| Conn Id   | `yandex_s3`                                                                       |
| Conn Type | Amazon Web Services                                                                         |
| Login     | `<Access Key ID сервисного аккаунта>`                                             |
| Password  | `<Secret Access Key>`                                                             |
| Extra     | `{"endpoint_url": "https://storage.yandexcloud.net", "region_name": "ru-central1"}` |

### Airflow Variables

Добавить в **Admin → Variables**:

| Key                  | Value                  |
|----------------------|------------------------|
| `s3_bucket_data`     | `rzd-airflow-data`     |
| `s3_bucket_results`  | `rzd-airflow-results`  |
| `depot_code`         | `TCH-15`               |

### PostgreSQL

- Conn Id: `rzd_postgres`
- Conn Type: Postgres
- Host: `<FQDN кластера>.mdb.yandexcloud.net`
- Schema: `rzd_analytics`

---

## Шаги выполнения

### Шаг 1. Подготовка тестового CSV в Object Storage

Загрузите тестовый файл телеметрии в бакет `rzd-airflow-data`:

```bash
# Через Yandex Cloud CLI
yc storage cp sensor_readings.csv \
    s3://rzd-airflow-data/sensor_readings.csv

# Или через Yandex Cloud Console:
# Object Storage → rzd-airflow-data → Загрузить файлы
```

Структура ожидаемого CSV (`sensor_readings.csv`):

```
loco_id,recorded_at,speed_kmh,buxa_temp_c,traction_current_a,engine_hours
ВЛ80С-1234,2024-06-01T06:00:00,72.5,45.2,650.0,1024.5
ВЛ80С-1234,2024-06-01T06:01:00,74.1,46.0,660.0,1024.52
2ТЭ116-1876,2024-06-01T06:00:00,65.0,42.0,,2180.3
```

---

### Шаг 2. Создание файла `yandex_s3_hook.py`

Создайте файл локально, затем задеплойте его в бакет DAG-файлов.

Файл `yandex_s3_hook.py` содержит только хук — его нужно разместить
в бакете `rzd-airflow-dags` рядом с DAG-файлами, чтобы Managed Airflow
мог его импортировать.

```python
# yandex_s3_hook.py
"""
Кастомный хук для Yandex Object Storage.
Расширяет стандартный S3Hook — переопределяет endpoint_url
и добавляет методы для работы с файлами телеметрии.

Деплой:
    yc storage cp yandex_s3_hook.py s3://rzd-airflow-dags/dags/yandex_s3_hook.py

Connection (conn_id='yandex_s3'):
    Conn Type : Amazon Web Services
    Login     : <Access Key ID>
    Password  : <Secret Access Key>
    Extra     : {"endpoint_url": "https://storage.yandexcloud.net",
                 "region_name": "ru-central1"}
"""
from __future__ import annotations

from io import StringIO
from typing import List, Optional

import boto3
import pandas as pd

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class YandexS3Hook(S3Hook):
    """
    Хук для Yandex Object Storage (S3-совместимый).

    Переопределяет get_conn() — явно передаёт endpoint_url из Extra,
    чтобы boto3 использовал эндпоинт Яндекса вместо AWS.

    :param aws_conn_id: ID соединения (default: 'yandex_s3')
    """

    conn_name_attr    = 'aws_conn_id'
    default_conn_name = 'yandex_s3'
    hook_name         = 'Yandex Object Storage'

    def __init__(
        self,
        aws_conn_id: str = 'yandex_s3',
        **kwargs,
    ) -> None:
        super().__init__(aws_conn_id=aws_conn_id, **kwargs)

    # ──────────────────────────────────────────────
    # Переопределение подключения
    # ──────────────────────────────────────────────

    def get_conn(self):
        """
        Создаёт boto3 S3-клиент с endpoint_url для Yandex Object Storage.

        endpoint_url берётся из поля Extra соединения:
          {"endpoint_url": "https://storage.yandexcloud.net", ...}

        Без этого параметра boto3 будет обращаться к AWS S3,
        а не к Яндекс-совместимому эндпоинту.
        """
        conn  = self.get_connection(self.aws_conn_id)
        extra = conn.extra_dejson if conn.extra else {}

        endpoint_url = extra.get(
            'endpoint_url', 'https://storage.yandexcloud.net'
        )
        region_name = extra.get('region_name', 'ru-central1')

        session = boto3.session.Session()
        client = session.client(
            service_name='s3',
            endpoint_url=endpoint_url,
            region_name=region_name,
            aws_access_key_id=conn.login,
            aws_secret_access_key=conn.password,
        )
        return client

    # ──────────────────────────────────────────────
    # Вспомогательные методы
    # ──────────────────────────────────────────────

    def list_files(
        self,
        bucket: str,
        prefix: str = '',
    ) -> List[str]:
        """
        Возвращает список ключей объектов в бакете по префиксу.

        :param bucket: имя бакета (напр. 'rzd-airflow-data')
        :param prefix: префикс пути (напр. 'incoming/')
        :return: список ключей (str)
        """
        client   = self.get_conn()
        response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        contents = response.get('Contents', [])

        keys = [obj['Key'] for obj in contents]
        self.log.info(
            "list_files: bucket=%s prefix=%s → найдено %d объектов",
            bucket, prefix, len(keys),
        )
        return keys

    def get_latest_file(
        self,
        bucket: str,
        prefix: str = '',
    ) -> Optional[str]:
        """
        Возвращает ключ самого нового файла в бакете по префиксу.

        Сортировка по полю LastModified — берётся последний по времени.
        Возвращает None, если по префиксу нет объектов.

        :param bucket: имя бакета
        :param prefix: префикс пути
        :return: ключ самого нового объекта или None
        """
        client   = self.get_conn()
        response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        contents = response.get('Contents', [])

        if not contents:
            self.log.warning(
                "get_latest_file: в бакете '%s' по префиксу '%s' нет файлов",
                bucket, prefix,
            )
            return None

        latest = max(contents, key=lambda obj: obj['LastModified'])
        self.log.info(
            "get_latest_file: самый новый файл — %s (изменён %s)",
            latest['Key'],
            latest['LastModified'].isoformat(),
        )
        return latest['Key']

    def read_csv(
        self,
        bucket: str,
        key: str,
    ) -> pd.DataFrame:
        """
        Читает CSV-файл из Object Storage и возвращает DataFrame.

        :param bucket: имя бакета
        :param key:    ключ объекта (путь к файлу в бакете)
        :return: pd.DataFrame
        """
        client   = self.get_conn()
        response = client.get_object(Bucket=bucket, Key=key)
        content  = response['Body'].read().decode('utf-8')

        df = pd.read_csv(StringIO(content))
        self.log.info(
            "read_csv: %s/%s → прочитано %d строк, %d столбцов",
            bucket, key, len(df), len(df.columns),
        )
        return df

    def write_csv(
        self,
        df: pd.DataFrame,
        bucket: str,
        key: str,
    ) -> None:
        """
        Записывает DataFrame в CSV в Object Storage.

        :param df:     данные для записи
        :param bucket: имя бакета назначения
        :param key:    ключ объекта (путь к файлу в бакете)
        """
        client = self.get_conn()
        buffer = StringIO()
        df.to_csv(buffer, index=False)

        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue().encode('utf-8'),
        )
        self.log.info(
            "write_csv: записан %s/%s (%d строк)",
            bucket, key, len(df),
        )
```

---

### Шаг 3. Создание файла `loco_telemetry_operator.py`

```python
# loco_telemetry_operator.py
"""
Оператор загрузки и валидации телеметрии локомотива.

Читает CSV из Yandex Object Storage через YandexS3Hook,
выполняет базовую валидацию, записывает результаты
в бакет rzd-airflow-results.

Деплой:
    yc storage cp loco_telemetry_operator.py \
        s3://rzd-airflow-dags/dags/loco_telemetry_operator.py
"""
from __future__ import annotations

from io import StringIO
from typing import Any, Dict

import pandas as pd

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from yandex_s3_hook import YandexS3Hook


class LocomotiveTelemetryOperator(BaseOperator):
    """
    Читает файл телеметрии локомотива из Object Storage,
    проверяет критические показатели и сохраняет результат.

    Источник данных: бакет rzd-airflow-data
    Результат:       бакет rzd-airflow-results

    :param loco_id:        номер локомотива (напр. 'ВЛ80С-1234')
    :param src_bucket:     бакет с входными данными
    :param results_bucket: бакет для результатов
    :param src_prefix:     префикс пути к файлам телеметрии в бакете
    :param aws_conn_id:    ID соединения с Object Storage
    """

    template_fields = ('loco_id',)
    ui_color  = '#4472C4'
    ui_fgcolor = '#ffffff'

    # Критические пороги датчиков
    THRESHOLDS = {
        'buxa_temp_c':       80.0,
        'traction_current_a': 900.0,
    }

    @apply_defaults
    def __init__(
        self,
        loco_id: str,
        src_bucket: str = 'rzd-airflow-data',
        results_bucket: str = 'rzd-airflow-results',
        src_prefix: str = '',
        aws_conn_id: str = 'yandex_s3',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.loco_id        = loco_id
        self.src_bucket     = src_bucket
        self.results_bucket = results_bucket
        self.src_prefix     = src_prefix
        self.aws_conn_id    = aws_conn_id

    def execute(self, context: dict) -> Dict[str, Any]:
        """
        Основная логика оператора:
          1. Находит самый новый файл телеметрии через YandexS3Hook
          2. Читает CSV из Object Storage
          3. Фильтрует строки по loco_id
          4. Проверяет критические пороги датчиков
          5. Сохраняет результат в rzd-airflow-results
          6. Возвращает сводку через XCom
        """
        hook = YandexS3Hook(aws_conn_id=self.aws_conn_id)

        # 1. Находим самый свежий файл телеметрии
        latest_key = hook.get_latest_file(
            bucket=self.src_bucket,
            prefix=self.src_prefix,
        )

        if latest_key is None:
            self.log.warning(
                "Нет файлов в %s/%s — пропускаем",
                self.src_bucket, self.src_prefix,
            )
            return {'loco_id': self.loco_id, 'records_count': 0, 'has_data': False}

        self.log.info("Читаем файл: %s/%s", self.src_bucket, latest_key)

        # 2. Читаем CSV через YandexS3Hook
        df = hook.read_csv(bucket=self.src_bucket, key=latest_key)

        # 3. Фильтруем строки по loco_id
        loco_df = df[df['loco_id'] == self.loco_id].copy()
        self.log.info(
            "Локомотив %s: найдено %d записей", self.loco_id, len(loco_df)
        )

        if loco_df.empty:
            return {'loco_id': self.loco_id, 'records_count': 0, 'has_data': False}

        # 4. Проверяем критические пороги
        alerts = []
        for sensor, threshold in self.THRESHOLDS.items():
            if sensor in loco_df.columns:
                over_limit = loco_df[loco_df[sensor] > threshold]
                if not over_limit.empty:
                    alerts.append({
                        'sensor':    sensor,
                        'max_value': float(loco_df[sensor].max()),
                        'threshold': threshold,
                        'count':     len(over_limit),
                    })
                    self.log.warning(
                        "АЛЕРТ [%s]: %d превышений (макс. %.1f > %.1f)",
                        sensor, len(over_limit),
                        loco_df[sensor].max(), threshold,
                    )

        # 5. Сохраняем результат в rzd-airflow-results
        ds = context['ds']
        result_key = (
            f"telemetry/{self.loco_id.replace(' ', '_')}/"
            f"{ds}_processed.csv"
        )
        hook.write_csv(
            df=loco_df,
            bucket=self.results_bucket,
            key=result_key,
        )
        self.log.info(
            "Результат записан: %s/%s", self.results_bucket, result_key
        )

        # 6. Возвращаем сводку через XCom
        avg_speed     = float(loco_df['speed_kmh'].mean()) if 'speed_kmh' in loco_df else 0.0
        max_buxa_temp = float(loco_df['buxa_temp_c'].max()) if 'buxa_temp_c' in loco_df else 0.0

        return {
            'loco_id':         self.loco_id,
            'source_key':      latest_key,
            'result_key':      result_key,
            'records_count':   len(loco_df),
            'alerts_count':    len(alerts),
            'alerts':          alerts,
            'avg_speed_kmh':   round(avg_speed, 1),
            'max_buxa_temp_c': round(max_buxa_temp, 1),
            'has_data':        True,
            'date':            ds,
        }
```

---

### Шаг 4. Создание тестового DAG

```python
# dag_loco_telemetry_s3.py
"""
DAG тестирования LocomotiveTelemetryOperator с YandexS3Hook.

Деплой DAG-файла:
    yc storage cp dag_loco_telemetry_s3.py \
        s3://rzd-airflow-dags/dags/dag_loco_telemetry_s3.py

После загрузки DAG появится в Airflow UI через 1–2 минуты
(Managed Airflow синхронизирует бакет автоматически).
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from loco_telemetry_operator import LocomotiveTelemetryOperator

LOCO_FLEET = [
    'ВЛ80С-1234',
    'ЭП2К-0412',
    '2ТЭ116-1876',
]

default_args = {
    'owner':           'tche15-analytics',
    'depends_on_past': False,
    'retries':         2,
    'retry_delay':     timedelta(minutes=5),
}

with DAG(
    dag_id='loco_telemetry_s3_demo',
    description='Демо: LocomotiveTelemetryOperator + YandexS3Hook',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 7 * * *',
    catchup=False,
    tags=['rzd', 'tche15', 's3', 'module-07'],
) as dag:

    telemetry_tasks = []
    for loco_id in LOCO_FLEET:
        safe_id = loco_id.replace('-', '_').replace('«', '').replace('»', '').lower()
        task = LocomotiveTelemetryOperator(
            task_id=f'telemetry_{safe_id}',
            loco_id=loco_id,
            src_bucket='rzd-airflow-data',
            results_bucket='rzd-airflow-results',
            src_prefix='',
            aws_conn_id='yandex_s3',
        )
        telemetry_tasks.append(task)

    def fleet_summary(**context):
        """Печатает сводку по парку из XCom."""
        ti = context['task_instance']
        for loco_id in LOCO_FLEET:
            safe_id = loco_id.replace('-', '_').replace('«', '').replace('»', '').lower()
            result  = ti.xcom_pull(task_ids=f'telemetry_{safe_id}')
            if result:
                print(
                    f"  {result['loco_id']}: "
                    f"записей={result['records_count']}, "
                    f"алертов={result['alerts_count']}, "
                    f"avg_speed={result['avg_speed_kmh']} км/ч"
                )

    summary = PythonOperator(
        task_id='fleet_summary',
        python_callable=fleet_summary,
    )

    telemetry_tasks >> summary
```

---

### Шаг 5. Деплой всех файлов в Object Storage

Все файлы деплоятся через Yandex Cloud CLI в бакет, связанный с Managed Airflow.
Прямой доступ к файловой системе контейнера недоступен.

```bash
# Деплой хука
yc storage cp yandex_s3_hook.py \
    s3://rzd-airflow-dags/dags/yandex_s3_hook.py

# Деплой оператора
yc storage cp loco_telemetry_operator.py \
    s3://rzd-airflow-dags/dags/loco_telemetry_operator.py

# Деплой DAG-файла
yc storage cp dag_loco_telemetry_s3.py \
    s3://rzd-airflow-dags/dags/dag_loco_telemetry_s3.py

# Проверка содержимого бакета
yc storage ls s3://rzd-airflow-dags/dags/
```

Через 1–2 минуты DAG появится в Airflow UI.  
Путь в интерфейсе: **DAGs → loco_telemetry_s3_demo**.

---

### Шаг 6. Проверка работы в Airflow UI

1. Перейдите в **DAGs → loco_telemetry_s3_demo**.
2. Нажмите **Trigger DAG** для ручного запуска.
3. В **Graph View** дождитесь завершения задач (зелёный цвет).
4. Щёлкните на задачу `telemetry_вл80с_1234` → вкладка **XCom**.
5. Убедитесь, что `return_value` содержит:

```json
{
  "loco_id": "ВЛ80С-1234",
  "source_key": "sensor_readings.csv",
  "result_key": "telemetry/ВЛ80С-1234/2024-06-01_processed.csv",
  "records_count": 480,
  "alerts_count": 0,
  "avg_speed_kmh": 72.3,
  "max_buxa_temp_c": 54.2,
  "has_data": true,
  "date": "2024-06-01"
}
```

6. Проверьте результирующий файл в Object Storage:

```bash
yc storage ls s3://rzd-airflow-results/telemetry/
```

---

## Полный код (сводка)

Итоговая структура файлов в бакете `rzd-airflow-dags`:

```
rzd-airflow-dags/
└── dags/
    ├── yandex_s3_hook.py            ← кастомный хук
    ├── loco_telemetry_operator.py   ← кастомный оператор
    └── dag_loco_telemetry_s3.py     ← DAG-файл
```

Входные данные в `rzd-airflow-data`:

```
rzd-airflow-results/
└── telemetry/
    └── ВЛ80С-1234/
        └── 2024-06-01_processed.csv
```

---

## Контрольные вопросы

1. Почему `YandexS3Hook.get_conn()` явно передаёт `endpoint_url` в `boto3.session.Session().client()`?
   Что произойдёт, если этот параметр не указать?

2. Метод `get_latest_file()` сортирует объекты по полю `LastModified`.
   Какие альтернативные стратегии выбора файла можно использовать
   (например, в именах файлов зашита дата `{{ ds_nodash }}`)?

3. Почему `LocomotiveTelemetryOperator` возвращает словарь из `execute()`?
   Где этот словарь можно увидеть в Airflow UI и как получить его в другой задаче?

4. В чём разница между деплоем DAG-файлов через `rzd-airflow-dags` и подходом
   с `plugins.zip`? Когда каждый из вариантов предпочтительнее в Managed Airflow?

5. Как изменить оператор, чтобы при обнаружении критического алерта
   (`buxa_temp_c > 80°C`) задача завершалась с ошибкой (`raise AirflowException`),
   а не просто писала предупреждение в лог?
