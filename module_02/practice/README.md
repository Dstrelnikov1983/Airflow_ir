# Практическая работа №02: DAG для загрузки телеметрии из Object Storage в PostgreSQL

**Модуль:** 02 — Устройство DAG
**Организация:** Западно-Сибирская дирекция тяги, ТЧЭ-15 (депо Новосибирск-Главный)
**Продолжительность:** 45–60 минут
**Платформа:** Yandex Managed Service for Apache Airflow™

---

## Цель и задачи

**Цель:** научиться создавать DAG в Yandex Managed Airflow, который читает файлы из Yandex Object Storage, выполняет валидацию данных и загружает результаты в Yandex Managed PostgreSQL.

**Задачи:**

1. Подготовить бакеты Yandex Object Storage и загрузить CSV-файлы телеметрии
2. Настроить Connection `yandex_s3` и `rzd_postgres` в Airflow UI
3. Написать DAG `rzd_sensor_ingestion` с пятью задачами
4. Задеплоить DAG-файл в бакет, связанный с Managed Airflow
5. Запустить DAG через Airflow UI и проверить результат в Object Storage

> **Важно:** среда выполнения — Yandex Managed Service for Apache Airflow™.
> Прямого доступа к файловой системе нет. Все файлы хранятся в Yandex Object Storage.
> Команды `airflow dags test`, `airflow tasks test`, `ssh`, `scp` недоступны.

---

## Необходимые ресурсы

| Ресурс | Описание |
|---|---|
| Yandex Managed Airflow | Кластер с версией Airflow 2.7+ |
| Yandex Object Storage | Три бакета (dags, data, results) |
| Yandex Managed PostgreSQL | Кластер с базой `rzd_analytics` |
| Сервисный аккаунт YC | Роли `storage.viewer`, `storage.uploader` |
| Yandex Cloud CLI (`yc`) | Для загрузки файлов в Object Storage |

---

## Подготовка Object Storage

### Создание бакетов через Yandex Cloud Console

1. Откройте [console.yandex.cloud](https://console.yandex.cloud) → **Object Storage**
2. Нажмите **Создать бакет** и создайте три бакета:

| Бакет | Назначение |
|---|---|
| `rzd-airflow-dags` | DAG-файлы (связан с Managed Airflow) |
| `rzd-airflow-data` | Входные CSV-файлы телеметрии |
| `rzd-airflow-results` | Результаты обработки |

Параметры для каждого бакета:
- **Доступ:** Приватный
- **Класс хранилища:** Стандартный

### Загрузка CSV-файлов в `rzd-airflow-data`

Через Yandex Cloud Console:

1. Откройте бакет `rzd-airflow-data`
2. Нажмите **Загрузить объекты**
3. Загрузите файлы из набора данных курса:
   - `sensor_readings.csv`
   - `locomotives.csv`
   - `trips.csv`
   - `schedule_adherence.csv`
   - `maintenance.csv`

Через Yandex Cloud CLI:

```bash
yc storage cp sensor_readings.csv    s3://rzd-airflow-data/sensor_readings.csv
yc storage cp locomotives.csv        s3://rzd-airflow-data/locomotives.csv
yc storage cp trips.csv              s3://rzd-airflow-data/trips.csv
yc storage cp schedule_adherence.csv s3://rzd-airflow-data/schedule_adherence.csv
yc storage cp maintenance.csv        s3://rzd-airflow-data/maintenance.csv
```

Проверьте, что файлы загружены:

```bash
yc storage ls s3://rzd-airflow-data/
```

### Создание сервисного аккаунта и выдача роли

1. **Yandex Cloud Console → IAM → Сервисные аккаунты → Создать**
2. Имя: `rzd-airflow-sa`
3. Назначьте роли:
   - `storage.viewer` — чтение из Object Storage
   - `storage.uploader` — запись в Object Storage

4. Создайте статический ключ доступа:
   - **IAM → Сервисные аккаунты → rzd-airflow-sa → Создать новый ключ → Статический ключ**
   - Сохраните **Идентификатор ключа** (Access Key ID) и **Секретный ключ** (Secret Access Key)

> **Важно:** секретный ключ показывается один раз при создании. Сохраните его немедленно.

---

## Настройка Airflow Connections и Variables

### Connection yandex_s3

1. Откройте Airflow UI → **Admin → Connections → +**
2. Заполните форму:

| Поле | Значение |
|---|---|
| Conn Id | `yandex_s3` |
| Conn Type | `Amazon S3` |
| Login | `<Идентификатор ключа из шага выше>` |
| Password | `<Секретный ключ из шага выше>` |
| Extra | `{"endpoint_url": "https://storage.yandexcloud.net", "region_name": "ru-central1"}` |

3. Нажмите **Save**

### Connection rzd_postgres

1. **Admin → Connections → +**
2. Заполните форму:

| Поле | Значение |
|---|---|
| Conn Id | `rzd_postgres` |
| Conn Type | `Postgres` |
| Host | `<FQDN кластера>.mdb.yandexcloud.net` |
| Schema | `rzd_analytics` |
| Login | `<пользователь БД>` |
| Password | `<пароль БД>` |
| Port | `6432` |

> FQDN кластера PostgreSQL: **Managed Service for PostgreSQL → Кластер → Хосты → FQDN**

3. Нажмите **Save**

### Airflow Variables

**Admin → Variables → +** — добавьте переменные:

| Key | Value |
|---|---|
| `s3_bucket_data` | `rzd-airflow-data` |
| `s3_bucket_results` | `rzd-airflow-results` |
| `depot_code` | `TCH-15` |
| `delay_threshold_min` | `15` |

---

## Деплой DAG-файла в Managed Airflow

### Связывание бакета с Managed Airflow

1. **Managed Service for Apache Airflow → Кластер → Редактировать**
2. В поле **Бакет с DAG-файлами** укажите `rzd-airflow-dags`
3. Сохраните изменения

### Загрузка DAG-файла

После того как файл `rzd_sensor_ingestion.py` написан и сохранён на вашем компьютере:

Через Yandex Cloud Console:

1. Откройте бакет `rzd-airflow-dags`
2. Нажмите **Загрузить объекты**
3. Выберите файл `rzd_sensor_ingestion.py`

Через Yandex Cloud CLI:

```bash
yc storage cp rzd_sensor_ingestion.py s3://rzd-airflow-dags/dags/rzd_sensor_ingestion.py
```

### Проверка появления DAG в Airflow UI

1. Откройте Airflow UI
2. Подождите 1–3 минуты (планировщик периодически сканирует бакет)
3. Найдите DAG `rzd_sensor_ingestion` в списке
4. Если DAG не появился — проверьте раздел **Admin → Import Errors**

> Для быстрого обновления нажмите кнопку **Refresh** рядом с именем DAG.

---

## Шаги выполнения

### Шаг 1: Изучение структуры файла sensor_readings.csv

Откройте Yandex Cloud Console → Object Storage → `rzd-airflow-data` → `sensor_readings.csv` → **Просмотр**.

Структура файла:

```
reading_id,loco_id,series,timestamp,sensor_type,value,unit,quality_flag
SR00001,VL80S-1024,VL80S,2024-03-01 00:05:00,buxa_temp_c,42.3,C,1
SR00002,VL80S-1024,VL80S,2024-03-01 00:05:00,buxa_temp_c,41.8,C,1
SR00003,EP2K-0312,EP2K,2024-03-01 00:10:00,buxa_temp_c,38.5,C,1
SR00004,2TE116-1187,2TE116,2024-03-01 00:10:00,buxa_temp_c,85.2,C,1
```

**Описание колонок:**

| Колонка | Тип | Описание |
|---|---|---|
| `reading_id` | строка | Уникальный идентификатор показания |
| `loco_id` | строка | Бортовой номер локомотива |
| `series` | строка | Серия: `VL80S`, `EP2K`, `EP1M`, `2TE116`, `2TE25KM`, `CHME3`, `ES2G`, `ED4M` |
| `timestamp` | дата-время | Время снятия показания (UTC+7) |
| `sensor_type` | строка | Тип датчика: `buxa_temp_c`, `speed_kmh`, `fuel_consumption_lh` и др. |
| `value` | число | Числовое значение показания |
| `unit` | строка | Единица: `C`, `km/h`, `l/h`, `A`, `h` |
| `quality_flag` | целое | 1 = хорошее качество, 0 = ошибка датчика |

### Шаг 2: Написание вспомогательных функций S3

Все операции с файлами — только через `S3Hook`. Шаблон чтения CSV:

```python
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from io import StringIO
import pandas as pd

def read_csv_from_s3(bucket: str, key: str, conn_id: str = 'yandex_s3') -> pd.DataFrame:
    hook    = S3Hook(aws_conn_id=conn_id)
    obj     = hook.get_key(key=key, bucket_name=bucket)
    content = obj.get()['Body'].read().decode('utf-8')
    return pd.read_csv(StringIO(content))
```

Шаблон записи результатов:

```python
def write_csv_to_s3(df: pd.DataFrame, bucket: str, key: str, conn_id: str = 'yandex_s3'):
    hook       = S3Hook(aws_conn_id=conn_id)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=key,
        bucket_name=bucket,
        replace=True,
    )
```

### Шаг 3: Написание файла DAG

Создайте на своём компьютере файл `rzd_sensor_ingestion.py` и вставьте полный код из раздела ниже.

### Шаг 4: Деплой DAG в Object Storage

```bash
yc storage cp rzd_sensor_ingestion.py s3://rzd-airflow-dags/dags/rzd_sensor_ingestion.py
```

### Шаг 5: Активация и запуск DAG

1. Откройте Airflow UI, найдите `rzd_sensor_ingestion`
2. Включите тумблер (Enabled)
3. Нажмите **▷ Trigger DAG**

### Шаг 6: Проверка результатов в Object Storage

После успешного завершения DAG откройте бакет `rzd-airflow-results` и убедитесь, что появился файл отчёта:

```bash
yc storage ls s3://rzd-airflow-results/sensor_readings/
```

---

## Полный код DAG

```python
"""
DAG: rzd_sensor_ingestion
Организация: ТЧЭ-15, депо Новосибирск-Главный
Описание: Ежечасный пайплайн загрузки телеметрии буксовых датчиков
          из Yandex Object Storage в Managed PostgreSQL rzd_analytics.

Платформа: Yandex Managed Service for Apache Airflow™
Все операции с файлами — через S3Hook (Yandex Object Storage).

Задачи:
    1. wait_for_file      — S3KeySensor: ждём появления CSV в бакете
    2. read_from_s3       — читаем sensor_readings.csv через S3Hook
    3. validate_buxa_temp — валидация температур букс (порог 80°C)
    4. load_to_postgres   — загрузка чистых данных в rzd_analytics
    5. write_report_to_s3 — запись статистики в rzd-airflow-results

Расписание: каждый час (0 * * * *)
Теги: rzd, tche15, safety, buxa
"""

# ─────────────────────────────────────────────────────────────────
# Блок 1: Импорты
# ─────────────────────────────────────────────────────────────────
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
from io import StringIO
import pandas as pd
import logging

# ─────────────────────────────────────────────────────────────────
# Блок 2: Константы
# ─────────────────────────────────────────────────────────────────
S3_CONN_ID         = 'yandex_s3'
PG_CONN_ID         = 'rzd_postgres'
BUXA_CRITICAL_TEMP = 80.0      # °C — критический порог температуры буксы

# Допустимые диапазоны значений датчиков
SENSOR_RANGES = {
    'buxa_temp_c':         (0.0, 120.0),
    'traction_current_a':  (0.0, 3000.0),
    'fuel_consumption_lh': (0.0, 500.0),
    'speed_kmh':           (0.0, 160.0),
    'engine_hours':        (0.0, 1_000_000.0),
}

# ─────────────────────────────────────────────────────────────────
# Блок 3: Вспомогательные функции S3
# ─────────────────────────────────────────────────────────────────

def read_csv_from_s3(bucket: str, key: str, conn_id: str = S3_CONN_ID) -> pd.DataFrame:
    """Читает CSV из Object Storage и возвращает DataFrame."""
    hook    = S3Hook(aws_conn_id=conn_id)
    obj     = hook.get_key(key=key, bucket_name=bucket)
    content = obj.get()['Body'].read().decode('utf-8')
    return pd.read_csv(StringIO(content))


def write_csv_to_s3(
    df: pd.DataFrame,
    bucket: str,
    key: str,
    conn_id: str = S3_CONN_ID,
) -> None:
    """Записывает DataFrame как CSV в Object Storage."""
    hook       = S3Hook(aws_conn_id=conn_id)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=key,
        bucket_name=bucket,
        replace=True,
    )
    logging.info(f"[ТЧЭ-15] Записан файл: s3://{bucket}/{key}")


def write_text_to_s3(
    text: str,
    bucket: str,
    key: str,
    conn_id: str = S3_CONN_ID,
) -> None:
    """Записывает текст в Object Storage."""
    hook = S3Hook(aws_conn_id=conn_id)
    hook.load_string(
        string_data=text,
        key=key,
        bucket_name=bucket,
        replace=True,
    )
    logging.info(f"[ТЧЭ-15] Записан отчёт: s3://{bucket}/{key}")


# ─────────────────────────────────────────────────────────────────
# Блок 4: Аргументы по умолчанию
# ─────────────────────────────────────────────────────────────────
default_args = {
    'owner':             'rzd-tche15-analytics',
    'depends_on_past':   False,
    'retries':           2,
    'retry_delay':       timedelta(minutes=5),
    'email_on_failure':  True,
    'email':             ['duty_engineer@rzd.ru'],
    'email_on_retry':    False,
    'execution_timeout': timedelta(minutes=20),
}

# ─────────────────────────────────────────────────────────────────
# Блок 5: Python-функции для задач
# ─────────────────────────────────────────────────────────────────

def read_from_s3(**context) -> int:
    """
    Задача 2: прочитать sensor_readings.csv из Object Storage.
    Ключ S3 формируется по логической дате запуска DAG.
    Возвращает количество строк (сохраняется в XCom).
    """
    bucket = Variable.get('s3_bucket_data')
    ds     = context['ds_nodash']                   # напр. 20240301
    key    = f"sensor_readings/{ds}/data.csv"

    # Fallback: если файл за конкретную дату не найден, берём базовый
    hook = S3Hook(aws_conn_id=S3_CONN_ID)
    if not hook.check_for_key(key=key, bucket_name=bucket):
        key = 'sensor_readings.csv'
        logging.warning(
            f"[ТЧЭ-15] Файл за {ds} не найден, используем: s3://{bucket}/{key}"
        )

    df = read_csv_from_s3(bucket=bucket, key=key)

    logging.info(f"[ТЧЭ-15] Прочитано строк: {len(df)}")
    logging.info(f"[ТЧЭ-15] Локомотивов: {df['loco_id'].nunique()}")
    logging.info(f"[ТЧЭ-15] Серии: {df['series'].unique().tolist()}")
    logging.info(
        f"[ТЧЭ-15] Период: "
        f"{pd.to_datetime(df['timestamp']).min()} — "
        f"{pd.to_datetime(df['timestamp']).max()}"
    )

    # Статистика по типам датчиков
    for sensor_type, group in df.groupby('sensor_type'):
        logging.info(
            f"  {sensor_type}: {len(group)} записей, "
            f"среднее={group['value'].mean():.2f}, "
            f"мин={group['value'].min():.2f}, "
            f"макс={group['value'].max():.2f}"
        )

    # Сохраняем ключ в XCom для следующих задач
    context['ti'].xcom_push(key='s3_key', value=key)
    return len(df)


def validate_buxa_temp(**context) -> dict:
    """
    Задача 3: валидация температур букс локомотивов.
    - Проверяет структуру колонок
    - Проверяет quality_flag (предупреждение при >15% плохих записей)
    - Выявляет критические перегревы (> 80°C)
    Возвращает словарь со статистикой (сохраняется в XCom).
    """
    ti     = context['ti']
    bucket = Variable.get('s3_bucket_data')
    key    = ti.xcom_pull(task_ids='read_from_s3', key='s3_key') or 'sensor_readings.csv'

    df = read_csv_from_s3(bucket=bucket, key=key)

    # Проверка 1: обязательные колонки
    required_columns = {
        'reading_id', 'loco_id', 'series', 'timestamp',
        'sensor_type', 'value', 'unit', 'quality_flag',
    }
    missing_cols = required_columns - set(df.columns)
    if missing_cols:
        raise ValueError(
            f"[ТЧЭ-15] Отсутствуют колонки: {missing_cols}. "
            "Формат файла телеметрии изменился?"
        )
    logging.info("[ТЧЭ-15] Проверка 1/3: структура колонок — OK")

    # Проверка 2: quality_flag
    bad_quality  = int((df['quality_flag'] == 0).sum())
    good_quality = int((df['quality_flag'] == 1).sum())
    bad_pct      = bad_quality / len(df) * 100
    logging.info(
        f"[ТЧЭ-15] Проверка 2/3: quality_flag — "
        f"хорошие: {good_quality}, плохие: {bad_quality} ({bad_pct:.1f}%)"
    )
    if bad_pct > 15:
        logging.warning(
            f"[ТЧЭ-15] ПРЕДУПРЕЖДЕНИЕ: доля плохих записей {bad_pct:.1f}% > 15%. "
            "Возможна неисправность датчиков!"
        )

    # Проверка 3: диапазоны значений для каждого типа датчика
    out_of_range = 0
    for sensor_type, (min_val, max_val) in SENSOR_RANGES.items():
        sensor_df = df[df['sensor_type'] == sensor_type]
        bad_range = sensor_df[(sensor_df['value'] < min_val) | (sensor_df['value'] > max_val)]
        if len(bad_range) > 0:
            out_of_range += len(bad_range)
            logging.warning(
                f"[ТЧЭ-15] Датчик {sensor_type}: {len(bad_range)} записей "
                f"вне допустимого диапазона [{min_val}, {max_val}]"
            )

    # Проверка 4: критические перегревы букс
    buxa_df  = df[df['sensor_type'] == 'buxa_temp_c'].copy()
    critical = buxa_df[buxa_df['value'] > BUXA_CRITICAL_TEMP]

    if len(critical) > 0:
        logging.warning(
            f"[ТЧЭ-15] КРИТИЧНО: {len(critical)} случаев перегрева буксы "
            f"(>{BUXA_CRITICAL_TEMP}°C)!"
        )
        for _, row in critical.iterrows():
            logging.warning(
                f"  ПЕРЕГРЕВ: {row['loco_id']} ({row['series']}) — "
                f"{row['value']:.1f}°C в {row['timestamp']}"
            )
    else:
        logging.info("[ТЧЭ-15] Проверка 3/3: критических перегревов букс нет — OK")

    validation_stats = {
        'total':           len(df),
        'good_quality':    good_quality,
        'bad_quality':     bad_quality,
        'bad_pct':         round(bad_pct, 2),
        'out_of_range':    out_of_range,
        'buxa_critical':   len(critical),
        'locos_checked':   int(df['loco_id'].nunique()),
        'needs_alert':     len(critical) > 0,
    }
    logging.info(f"[ТЧЭ-15] Итоги валидации: {validation_stats}")
    return validation_stats


def load_to_postgres(**context) -> int:
    """
    Задача 4: загрузить данные с quality_flag=1 в Managed PostgreSQL.
    Читает CSV из Object Storage через S3Hook.
    Использует PostgresHook (Connection rzd_postgres).
    Возвращает количество загруженных строк.
    """
    ti     = context['ti']
    bucket = Variable.get('s3_bucket_data')
    key    = ti.xcom_pull(task_ids='read_from_s3', key='s3_key') or 'sensor_readings.csv'

    df       = read_csv_from_s3(bucket=bucket, key=key)
    df_clean = df[df['quality_flag'] == 1].copy()

    logging.info(
        f"[ТЧЭ-15] Подготовлено к загрузке: {len(df_clean)} строк "
        f"(пропущено {len(df) - len(df_clean)} записей с quality_flag=0)"
    )

    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn    = pg_hook.get_conn()
    cur     = conn.cursor()
    inserted = 0

    try:
        for _, row in df_clean.iterrows():
            cur.execute(
                """INSERT INTO sensor_readings
                   (reading_id, loco_id, series, timestamp,
                    sensor_type, value, unit, quality_flag)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (reading_id) DO NOTHING""",
                (
                    row['reading_id'],
                    row['loco_id'],
                    row['series'],
                    row['timestamp'],
                    row['sensor_type'],
                    float(row['value']),
                    row['unit'],
                    int(row['quality_flag']),
                ),
            )
            inserted += cur.rowcount

        conn.commit()
        logging.info(f"[ТЧЭ-15] Загружено строк в rzd_analytics: {inserted}")
        return inserted

    except Exception as e:
        conn.rollback()
        logging.error(f"[ТЧЭ-15] Ошибка загрузки в PostgreSQL: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def write_report_to_s3(**context) -> None:
    """
    Задача 5: записать итоговую статистику в rzd-airflow-results/.
    Формирует текстовый отчёт на основе XCom из предыдущих задач.
    Сохраняет результат через S3Hook — без обращения к файловой системе.
    """
    ti             = context['ti']
    ds             = context['ds']
    ds_nodash      = context['ds_nodash']
    bucket_results = Variable.get('s3_bucket_results')

    val_stats    = ti.xcom_pull(task_ids='validate_buxa_temp') or {}
    inserted_cnt = ti.xcom_pull(task_ids='load_to_postgres')   or 0

    report_lines = [
        "=" * 55,
        "[ТЧЭ-15] ОТЧЁТ: rzd_sensor_ingestion",
        f"Дата запуска DAG:        {ds}",
        f"Run ID:                  {context['run_id']}",
        "=" * 55,
        f"Локомотивов проверено:   {val_stats.get('locos_checked', '?')}",
        f"Всего записей в файле:   {val_stats.get('total', '?')}",
        f"Загружено в PostgreSQL:  {inserted_cnt}",
        f"Плохих записей (q=0):    {val_stats.get('bad_quality', '?')} "
        f"({val_stats.get('bad_pct', '?')}%)",
        f"Вне допустимых диапаз.:  {val_stats.get('out_of_range', '?')}",
        f"Критич. перегревов букс: {val_stats.get('buxa_critical', '?')}",
        "=" * 55,
    ]

    if val_stats.get('needs_alert'):
        report_lines.append("!!! АЛЕРТ: обнаружены критические перегревы букс !!!")
        report_lines.append("    Требуется проверка локомотивов дежурным инженером.")

    report_text = "\n".join(report_lines)

    # Ключ с датой логического запуска DAG
    report_key = f"sensor_readings/{ds_nodash}/report.txt"
    write_text_to_s3(text=report_text, bucket=bucket_results, key=report_key)
    logging.info(
        f"[ТЧЭ-15] Отчёт записан: s3://{bucket_results}/{report_key}"
    )


# ─────────────────────────────────────────────────────────────────
# Блок 6: Определение DAG
# ─────────────────────────────────────────────────────────────────
with DAG(
    dag_id='rzd_sensor_ingestion',
    description='ТЧЭ-15: загрузка телеметрии буксов из Object Storage в rzd_analytics',
    schedule='0 * * * *',
    start_date=datetime(2024, 3, 1),
    catchup=False,
    default_args=default_args,
    tags=['rzd', 'tche15', 'safety', 'buxa'],
    max_active_runs=1,
    doc_md="""
## rzd_sensor_ingestion DAG

Ежечасный пайплайн ТЧЭ-15 (депо Новосибирск-Главный).

**Платформа:** Yandex Managed Service for Apache Airflow™
**Хранилище:** Yandex Object Storage (все операции через S3Hook)
**БД:** Yandex Managed PostgreSQL, схема rzd_analytics

**Критический порог:** buxa_temp_c > 80°C → WARNING + отчёт в S3
**Схема:** rzd_analytics
**Таблица:** sensor_readings
**Парк:** ВЛ80С, ЭП2К, ЭП1М, 2ТЭ116, 2ТЭ25КМ, ЧМЭ3, ЭС2Г, ЭД4М
    """,
) as dag:

    # Точка входа
    pipeline_start = EmptyOperator(task_id='pipeline_start')

    # Задача 1: ожидание появления файла в Object Storage
    wait_for_file = S3KeySensor(
        task_id='wait_for_file',
        bucket_name='{{ var.value.s3_bucket_data }}',
        bucket_key='sensor_readings.csv',
        aws_conn_id=S3_CONN_ID,
        poke_interval=60,
        timeout=3600,
        mode='reschedule',
    )

    # Задача 2: чтение данных из Object Storage
    task_read = PythonOperator(
        task_id='read_from_s3',
        python_callable=read_from_s3,
    )

    # Задача 3: валидация температур букс
    task_validate = PythonOperator(
        task_id='validate_buxa_temp',
        python_callable=validate_buxa_temp,
    )

    # Задача 4: загрузка в PostgreSQL
    task_load = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        execution_timeout=timedelta(minutes=15),
    )

    # Задача 5: запись отчёта в Object Storage
    task_report = PythonOperator(
        task_id='write_report_to_s3',
        python_callable=write_report_to_s3,
        retries=1,
    )

    # Точка выхода
    pipeline_end = EmptyOperator(task_id='pipeline_end')

    # ─────────────────────────────────────────────────────────────
    # Блок 7: Граф зависимостей
    # pipeline_start → wait_for_file → read_from_s3
    #   → validate_buxa_temp → load_to_postgres
    #   → write_report_to_s3 → pipeline_end
    # ─────────────────────────────────────────────────────────────
    (
        pipeline_start
        >> wait_for_file
        >> task_read
        >> task_validate
        >> task_load
        >> task_report
        >> pipeline_end
    )
```

---

## Контрольные вопросы

1. **Почему нельзя использовать `pd.read_csv('/opt/airflow/data/sensor_readings.csv')` в Managed Airflow?**
   Объясните, где физически выполняется код задачи и почему локальная файловая система недоступна.

2. **Что произойдёт с задачами `validate_buxa_temp`, `load_to_postgres` и `write_report_to_s3`, если `wait_for_file` не дождётся файла в течение `timeout=3600` секунд?**
   Какой статус получит `wait_for_file` и downstream-задачи?

3. **Переменная `s3_bucket_data` читается через `Variable.get()` внутри функций задач, а не на уровне модуля. Почему это правильный подход?**
   Что случится, если вынести `Variable.get()` на уровень модуля (вне функций)?

4. **В задаче `load_to_postgres` используется `ON CONFLICT (reading_id) DO NOTHING`. Что произойдёт при повторном запуске DAG с тем же файлом `sensor_readings.csv`?**
   Как это обеспечивает идемпотентность пайплайна?

5. **Ключ S3 для отчёта формируется как `sensor_readings/{ds_nodash}/report.txt`. Какое значение примет `ds_nodash` для DAG Run с `execution_date=2024-03-01`?**
   Какой путь в Object Storage получится?
