# Практическая работа №01: Развёртывание Managed Airflow и подключение Object Storage

**Модуль:** 01 — Введение в Apache Airflow
**Организация:** Западно-Сибирская дирекция тяги, ТЧЭ-15 (депо Новосибирск-Главный)
**Расчётное время:** 45 минут
**Платформа:** Yandex Managed Service for Apache Airflow™
**Хранилище данных:** Yandex Object Storage (S3-совместимый)

---

## Цель и задачи

**Цель:** Создать инфраструктуру курса в Яндекс Облаке: развернуть кластер Managed Apache Airflow, настроить Object Storage как хранилище DAG-файлов и исходных данных, запустить первый тестовый DAG, который читает реестр локомотивов из Object Storage.

**Задачи:**

1. Создать бакеты Object Storage (`rzd-airflow-dags`, `rzd-airflow-data`, `rzd-airflow-results`)
2. Создать сервисный аккаунт с правами `storage.uploader` + `storage.viewer`
3. Загрузить CSV-файлы датасета в бакет `rzd-airflow-data`
4. Создать кластер Managed Apache Airflow и привязать бакет DAG-файлов
5. Настроить Connection `yandex_s3` и переменные в Airflow UI
6. Настроить кластер Managed PostgreSQL и Connection `rzd_postgres`
7. Задеплоить первый тестовый DAG через Object Storage и убедиться в его появлении в UI

**Контекст задачи:** Аналитический отдел депо ТЧЭ-15 (Новосибирск) собирает данные с 120+ локомотивов (ВЛ80С, ЭП2К, 2ТЭ116 и др.). Данные поступают из системы ГЛОНАСС, журналов ТО и системы ГВЦ РЖД. Требуется построить оркестрацию пайплайнов на Managed Airflow без доступа к локальной файловой системе — всё через Object Storage.

---

## Необходимые ресурсы

- Учётная запись Яндекс Облака с активным платёжным аккаунтом
- Роли на уровне каталога: `editor` или выше
- Установленный Яндекс CLI (`yc`) версии 0.120+ или доступ к веб-консоли
- CSV-файлы датасета курса (из репозитория): `locomotives.csv`, `sensor_readings.csv`, `trips.csv`, `schedule_adherence.csv`, `maintenance.csv`

---

## Подготовка Object Storage

### Создание бакетов через Yandex Cloud Console

1. Откройте [console.cloud.yandex.ru](https://console.cloud.yandex.ru)
2. Перейдите: **Object Storage → Создать бакет**
3. Создайте три бакета (повторите для каждого):

| Имя бакета | Назначение | Доступ |
|---|---|---|
| `rzd-airflow-dags` | DAG-файлы Managed Airflow | Приватный |
| `rzd-airflow-data` | Входные CSV-данные | Приватный |
| `rzd-airflow-results` | Результаты обработки | Приватный |

> Все бакеты должны находиться в одном каталоге и зоне доступности `ru-central1`.

Через Яндекс CLI (альтернатива):

```bash
yc storage bucket create --name rzd-airflow-dags
yc storage bucket create --name rzd-airflow-data
yc storage bucket create --name rzd-airflow-results
```

### Создание сервисного аккаунта и ключей доступа

```bash
# Создать сервисный аккаунт
yc iam service-account create --name rzd-airflow-sa

# Получить ID аккаунта
SA_ID=$(yc iam service-account get rzd-airflow-sa --format json | jq -r '.id')

# Назначить роли на бакеты
yc resource-manager folder add-access-binding \
    --name <ИМЯ_КАТАЛОГА> \
    --role storage.uploader \
    --subject serviceAccount:$SA_ID

yc resource-manager folder add-access-binding \
    --name <ИМЯ_КАТАЛОГА> \
    --role storage.viewer \
    --subject serviceAccount:$SA_ID

# Создать статический ключ доступа (для S3 API)
yc iam access-key create --service-account-name rzd-airflow-sa
```

Команда вернёт `key_id` (Access Key ID) и `secret` (Secret Access Key). Сохраните их — они понадобятся при настройке Connection в Airflow UI.

### Загрузка CSV-файлов датасета

```bash
# Загрузить CSV-файлы в бакет rzd-airflow-data
yc storage cp locomotives.csv        s3://rzd-airflow-data/locomotives.csv
yc storage cp sensor_readings.csv    s3://rzd-airflow-data/sensor_readings.csv
yc storage cp trips.csv              s3://rzd-airflow-data/trips.csv
yc storage cp schedule_adherence.csv s3://rzd-airflow-data/schedule_adherence.csv
yc storage cp maintenance.csv        s3://rzd-airflow-data/maintenance.csv

# Проверить загрузку
yc storage ls s3://rzd-airflow-data/
```

Ожидаемый вывод: список из 5 CSV-файлов.

---

## Создание кластера Managed Apache Airflow

1. В консоли перейдите: **Managed Service for Apache Airflow → Создать кластер**
2. Задайте параметры:

| Параметр | Значение |
|---|---|
| Имя | `rzd-airflow-tche15` |
| Зона доступности | `ru-central1-a` |
| Класс хоста (Webserver/Scheduler) | `m3-c2-m4` или выше |
| Количество воркеров | 1–2 (для курса достаточно) |
| Бакет DAG-файлов | `rzd-airflow-dags` |
| Сервисный аккаунт | `rzd-airflow-sa` |

3. В разделе **Сеть** выберите существующую VPC и подсеть в `ru-central1-a`.
4. Задайте пароль администратора Airflow (сохраните для входа в UI).
5. Нажмите **Создать кластер**. Готовность кластера — 10–15 минут.

После создания кластера:
- Скопируйте URL Airflow UI из карточки кластера
- Войдите с учётными данными администратора

---

## Настройка Airflow Connections и Variables

### Connection yandex_s3

В Airflow UI перейдите: **Admin → Connections → Add a new record (+)**

| Поле | Значение |
|---|---|
| Connection Id | `yandex_s3` |
| Connection Type | `Amazon S3` |
| Login | `<key_id из шага выше>` |
| Password | `<secret из шага выше>` |
| Extra | `{"endpoint_url": "https://storage.yandexcloud.net", "region_name": "ru-central1"}` |

Нажмите **Save**.

> **Важно:** Connection Type = `Amazon S3` (не `Google Cloud Storage`). Yandex Object Storage полностью совместим с S3 API.

### Connection rzd_postgres

1. Создайте кластер **Managed Service for PostgreSQL** в той же VPC:
   - Имя: `rzd-analytics-pg`
   - Версия PostgreSQL: 15
   - Класс хоста: `s3-c2-m8`
   - Имя БД: `rzd_analytics`
   - Имя пользователя: `rzd_analyst`

2. Получите FQDN кластера в консоли (формат: `rc1a-xxxxxxxxxxxxx.mdb.yandexcloud.net`)

3. В Airflow UI: **Admin → Connections → Add a new record (+)**

| Поле | Значение |
|---|---|
| Connection Id | `rzd_postgres` |
| Connection Type | `Postgres` |
| Host | `rc1a-xxxxxxxxxxxxx.mdb.yandexcloud.net` |
| Schema | `rzd_analytics` |
| Login | `rzd_analyst` |
| Password | `<пароль PostgreSQL>` |
| Port | `6432` |

Нажмите **Test**, затем **Save**.

> Порт `6432` — стандартный порт PgBouncer в Managed PostgreSQL Яндекс Облака.

### Переменные (Variables)

Перейдите: **Admin → Variables → Add a new record (+)**

| Key | Value |
|---|---|
| `s3_bucket_data` | `rzd-airflow-data` |
| `s3_bucket_results` | `rzd-airflow-results` |
| `depot_code` | `TCH-15` |
| `delay_threshold_min` | `15` |

---

## Деплой DAG-файла в Managed Airflow

В Managed Airflow **нет** прямого доступа к файловой системе. DAG-файлы передаются исключительно через бакет Object Storage, привязанный к кластеру.

### Загрузка DAG через Яндекс CLI

```bash
# Загрузить DAG-файл в бакет rzd-airflow-dags
yc storage cp locomotive_monitor_dag.py s3://rzd-airflow-dags/dags/locomotive_monitor_dag.py

# Проверить наличие файла в бакете
yc storage ls s3://rzd-airflow-dags/dags/
```

### Загрузка через Yandex Cloud Console

1. Перейдите: **Object Storage → rzd-airflow-dags → Загрузить объект**
2. Выберите файл `locomotive_monitor_dag.py`
3. Укажите путь объекта: `dags/locomotive_monitor_dag.py`
4. Нажмите **Загрузить**

### Проверка появления DAG в UI

После загрузки файла Managed Airflow автоматически подтягивает его из бакета. Это занимает **1–3 минуты**.

В Airflow UI:
1. Обновите страницу **DAGs**
2. Найдите `locomotive_monitor_dag` в списке
3. Убедитесь, что статус — `Active` (переключатель слева от DAG)
4. Нажмите на DAG → вкладка **Graph** — проверьте граф задач

---

## Шаги выполнения

### Шаг 1. Создать бакеты Object Storage

Создайте три бакета через консоль или CLI:
- `rzd-airflow-dags` — для DAG-файлов
- `rzd-airflow-data` — для входных данных
- `rzd-airflow-results` — для результатов

### Шаг 2. Создать сервисный аккаунт с правами S3

```bash
yc iam service-account create --name rzd-airflow-sa
# Назначить роли storage.uploader + storage.viewer (см. выше)
# Создать статический ключ доступа и сохранить key_id + secret
```

### Шаг 3. Загрузить CSV-файлы датасета в rzd-airflow-data

```bash
for f in locomotives.csv sensor_readings.csv trips.csv schedule_adherence.csv maintenance.csv; do
    yc storage cp $f s3://rzd-airflow-data/$f
done
```

### Шаг 4. Создать кластер Managed Apache Airflow

Через консоль: **Managed Service for Apache Airflow → Создать кластер**. Привязать бакет `rzd-airflow-dags` как источник DAG-файлов.

### Шаг 5. Настроить Connection yandex_s3 в Airflow UI

Тип `Amazon S3`, endpoint `https://storage.yandexcloud.net`, ключи из шага 2.

### Шаг 6. Настроить кластер Managed PostgreSQL и Connection rzd_postgres

Создать кластер PostgreSQL, получить FQDN, добавить Connection в Airflow UI (порт 6432).

### Шаг 7. Задеплоить тестовый DAG через Object Storage

```bash
yc storage cp locomotive_monitor_dag.py s3://rzd-airflow-dags/dags/locomotive_monitor_dag.py
```

Дождаться появления DAG в UI (1–3 мин), запустить вручную, убедиться в успешном завершении.

---

## Полный код DAG

Сохраните следующий код в файл `locomotive_monitor_dag.py` и загрузите его в бакет `rzd-airflow-dags/dags/`.

```python
"""
DAG: locomotive_monitor_dag
Назначение: Мониторинг состояния локомотивного парка ТЧЭ-15.
Читает реестр локомотивов из Object Storage (S3) и формирует
сводку по статусам. Записывает отчёт обратно в Object Storage.
Расписание: каждые 6 часов.
Платформа: Yandex Managed Service for Apache Airflow™
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logger = logging.getLogger(__name__)

# --- Константы ---
S3_CONN_ID = "yandex_s3"
LOCOS_KEY = "locomotives.csv"


def get_bucket_data() -> str:
    """Возвращает имя бакета с данными из Airflow Variable."""
    return Variable.get("s3_bucket_data", default_var="rzd-airflow-data")


def get_bucket_results() -> str:
    """Возвращает имя бакета для результатов из Airflow Variable."""
    return Variable.get("s3_bucket_results", default_var="rzd-airflow-results")


def read_csv_from_s3(bucket: str, key: str, conn_id: str = S3_CONN_ID) -> pd.DataFrame:
    """Универсальная функция чтения CSV из Object Storage через S3Hook."""
    hook = S3Hook(aws_conn_id=conn_id)
    obj = hook.get_key(key=key, bucket_name=bucket)
    content = obj.get()["Body"].read().decode("utf-8")
    return pd.read_csv(StringIO(content))


def write_csv_to_s3(
    df: pd.DataFrame,
    bucket: str,
    key: str,
    conn_id: str = S3_CONN_ID,
) -> None:
    """Универсальная функция записи DataFrame в Object Storage через S3Hook."""
    hook = S3Hook(aws_conn_id=conn_id)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=key,
        bucket_name=bucket,
        replace=True,
    )
    logger.info("Файл записан: s3://%s/%s", bucket, key)


default_args = {
    "owner": "rzd-de-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "depends_on_past": False,
}


# ------------------------------------------------------------------ #
#  ЗАДАЧА 1: Проверка доступности файла в Object Storage              #
# ------------------------------------------------------------------ #
def check_s3_file(**context) -> str:
    """
    Проверяет, что файл locomotives.csv доступен в Object Storage.
    Использует S3Hook.check_for_key() — не читает содержимое,
    только проверяет наличие объекта.
    """
    bucket = get_bucket_data()
    hook = S3Hook(aws_conn_id=S3_CONN_ID)

    if not hook.check_for_key(key=LOCOS_KEY, bucket_name=bucket):
        raise FileNotFoundError(
            f"Файл не найден в Object Storage: s3://{bucket}/{LOCOS_KEY}\n"
            "Убедитесь, что CSV-файлы загружены в бакет rzd-airflow-data."
        )

    logger.info("Файл доступен: s3://%s/%s", bucket, LOCOS_KEY)
    context["ti"].xcom_push(key="s3_bucket", value=bucket)
    context["ti"].xcom_push(key="s3_key", value=LOCOS_KEY)
    return f"s3://{bucket}/{LOCOS_KEY}"


# ------------------------------------------------------------------ #
#  ЗАДАЧА 2: Чтение и анализ реестра локомотивов                      #
# ------------------------------------------------------------------ #
def read_and_analyze_locomotives(**context) -> dict:
    """
    Читает locomotives.csv из Object Storage через S3Hook.
    Формирует сводку по статусам парка ТЧЭ-15.
    Передаёт результаты через XCom для следующей задачи.
    """
    bucket = context["ti"].xcom_pull(
        task_ids="check_s3_file", key="s3_bucket"
    )
    key = context["ti"].xcom_pull(
        task_ids="check_s3_file", key="s3_key"
    )

    df = read_csv_from_s3(bucket=bucket, key=key)
    logger.info("Прочитано локомотивов: %d", len(df))

    total = len(df)
    active = int((df["status"] == "active").sum())
    maintenance = int((df["status"] == "maintenance").sum())
    readiness_pct = round(active / total * 100, 1) if total > 0 else 0.0

    summary = {
        "total": total,
        "active": active,
        "maintenance": maintenance,
        "readiness_pct": readiness_pct,
        "depot": Variable.get("depot_code", default_var="TCH-15"),
        "run_date": context["ds"],
    }

    logger.info(
        "Парк ТЧЭ-15: всего=%d, активных=%d, на ТО=%d, готовность=%.1f%%",
        total, active, maintenance, readiness_pct,
    )

    context["ti"].xcom_push(key="summary", value=summary)
    return summary


# ------------------------------------------------------------------ #
#  ЗАДАЧА 3: Запись отчёта в Object Storage                           #
# ------------------------------------------------------------------ #
def write_report_to_s3(**context) -> str:
    """
    Формирует текстовый отчёт по парку ТЧЭ-15 и записывает его
    в бакет rzd-airflow-results через S3Hook.
    Ключ объекта содержит дату запуска DAG ({{ ds_nodash }}).
    """
    summary = context["ti"].xcom_pull(
        task_ids="read_and_analyze_locomotives", key="summary"
    )
    bucket_results = get_bucket_results()
    ds_nodash = context["ds_nodash"]
    result_key = f"reports/locomotive_monitor/{ds_nodash}_fleet_summary.txt"

    status_icon = "OK" if summary["readiness_pct"] >= 80 else "ВНИМАНИЕ"
    report_text = (
        f"{'='*55}\n"
        f"ОТЧЁТ ПАРКА {summary['depot']} | {summary['run_date']} | {status_icon}\n"
        f"{'='*55}\n"
        f"Парк локомотивов:  {summary['total']} ед.\n"
        f"  - Активных:      {summary['active']} ед.\n"
        f"  - На ТО/ремонте: {summary['maintenance']} ед.\n"
        f"  - Готовность:    {summary['readiness_pct']}%\n"
        f"{'='*55}\n"
    )

    hook = S3Hook(aws_conn_id=S3_CONN_ID)
    hook.load_string(
        string_data=report_text,
        key=result_key,
        bucket_name=bucket_results,
        replace=True,
    )

    logger.info("Отчёт записан: s3://%s/%s", bucket_results, result_key)
    return f"s3://{bucket_results}/{result_key}"


# ------------------------------------------------------------------ #
#  ОПРЕДЕЛЕНИЕ DAG                                                    #
# ------------------------------------------------------------------ #
with DAG(
    dag_id="locomotive_monitor_dag",
    description="Мониторинг парка локомотивов ТЧЭ-15 через Object Storage",
    schedule="0 */6 * * *",
    start_date=datetime(2024, 3, 1),
    catchup=False,
    default_args=default_args,
    tags=["rzd", "monitor", "tche15", "s3"],
    doc_md="""
    ## locomotive_monitor_dag

    Читает реестр локомотивов депо ТЧЭ-15 из Yandex Object Storage
    через S3Hook и формирует сводку по статусам парка.
    Отчёт записывается обратно в Object Storage.

    **Бакеты:**
    - Вход:  `rzd-airflow-data/locomotives.csv`
    - Выход: `rzd-airflow-results/reports/locomotive_monitor/<date>_fleet_summary.txt`

    **Connections:** `yandex_s3`
    **Variables:** `s3_bucket_data`, `s3_bucket_results`, `depot_code`
    """,
) as dag:

    t1_check = PythonOperator(
        task_id="check_s3_file",
        python_callable=check_s3_file,
    )

    t2_read = PythonOperator(
        task_id="read_and_analyze_locomotives",
        python_callable=read_and_analyze_locomotives,
    )

    t3_report = PythonOperator(
        task_id="write_report_to_s3",
        python_callable=write_report_to_s3,
    )

    t1_check >> t2_read >> t3_report
```

---

## Контрольные вопросы

1. Почему в Yandex Managed Service for Apache Airflow нельзя использовать `open("/path/to/file.csv")` или `pd.read_csv("/local/path")` для чтения данных? Какой компонент Managed Airflow отвечает за выполнение задач и где он выполняется физически?

2. В Connection `yandex_s3` в поле `Extra` задаётся `endpoint_url`. Зачем это нужно, если S3Hook по умолчанию работает с AWS S3? Что произойдёт, если не указать `endpoint_url` при работе с Yandex Object Storage?

3. DAG задеплоен в бакет `rzd-airflow-dags/dags/`, но через 10 минут он не появляется в Airflow UI. Перечислите не менее трёх возможных причин и способов их диагностики.

4. Зачем в коде DAG используется `Variable.get("s3_bucket_data")` вместо хардкоженной строки `"rzd-airflow-data"`? Приведите сценарий работы депо ТЧЭ-15, когда это принципиально важно.

5. S3KeySensor ожидает появления файла в Object Storage с `mode='reschedule'`. Чем этот режим отличается от `mode='poke'`? Какой режим предпочтительнее для Managed Airflow и почему?
