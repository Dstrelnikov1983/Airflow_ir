# Практическая работа №04: Параметризованные DAG с Variables и Jinja2 для Object Storage

**Модуль:** 04 — Шаблоны заданий и параметризация
**Продолжительность:** 60–80 минут
**Уровень:** средний
**Организация:** Западно-Сибирская дирекция тяги, ТЧЭ-15, депо Новосибирск-Главный
**Платформа:** Яндекс Облако — Managed Service for Apache Airflow™

---

## Цель и задачи

**Цель:** научиться создавать параметризованные DAG, в которых все файловые операции выполняются через Yandex Object Storage, пороговые значения хранятся в Airflow Variables, а произвольный период анализа задаётся через `dag_run.conf` без изменения кода.

**Задачи:**

- подготовить бакеты Object Storage и загрузить CSV-данные депо ТЧЭ-15;
- настроить Connection `yandex_s3` и Connection `rzd_postgres` в Airflow UI;
- создать Airflow Variables для хранения конфигурации;
- написать параметризованный DAG с Jinja2-шаблонами в ключах S3;
- передавать данные между задачами через XCom;
- задавать произвольный период запуска через `dag_run.conf`;
- задеплоить DAG-файл в Managed Airflow через Object Storage.

---

## Необходимые ресурсы

| Ресурс | Описание |
|---|---|
| Managed Service for Apache Airflow | Кластер Airflow в Яндекс Облаке |
| Managed Service for PostgreSQL | Кластер `rzd_analytics` |
| Object Storage | Бакеты для данных, DAG-файлов и результатов |
| Сервисный аккаунт | С ролями `storage.viewer` и `storage.uploader` |
| Yandex Cloud CLI (`yc`) | Для загрузки файлов в Object Storage |

---

## Подготовка Object Storage (ОБЯЗАТЕЛЬНЫЙ РАЗДЕЛ)

### Шаг 1.1 Создание бакетов через Yandex Cloud Console

Перейдите в **Yandex Cloud Console → Object Storage → Создать бакет** и создайте три бакета:

| Бакет | Назначение |
|---|---|
| `rzd-airflow-dags` | DAG-файлы (связывается с Managed Airflow) |
| `rzd-airflow-data` | Входные CSV-данные (locomotives, sensor_readings и т.д.) |
| `rzd-airflow-results` | Результаты обработки DAG-ов |

Параметры бакетов:

- Класс хранилища: **Стандартный**
- Доступ: **Приватный**
- Регион: **ru-central1**

### Шаг 1.2 Создание сервисного аккаунта

1. Перейдите в **IAM → Сервисные аккаунты → Создать сервисный аккаунт**.
2. Имя: `airflow-s3-sa`.
3. Назначьте роли:
   - `storage.viewer` — для чтения из бакетов с данными;
   - `storage.uploader` — для записи результатов и загрузки DAG-файлов.
4. Перейдите в созданный аккаунт → **Ключи доступа → Создать новый ключ**.
5. Сохраните **Access Key ID** и **Secret Access Key** — они понадобятся в Шаге 2.

### Шаг 1.3 Загрузка CSV-файлов в Object Storage

Загрузите файлы с данными депо ТЧЭ-15 в бакет `rzd-airflow-data`. Структура бакета:

```
rzd-airflow-data/
├── sensor_readings.csv
├── locomotives.csv
├── trips.csv
├── schedule_adherence.csv
└── maintenance.csv
```

Через Yandex Cloud CLI:

```bash
# Загрузка всех CSV-файлов
yc storage cp locomotives.csv        s3://rzd-airflow-data/locomotives.csv
yc storage cp sensor_readings.csv    s3://rzd-airflow-data/sensor_readings.csv
yc storage cp trips.csv              s3://rzd-airflow-data/trips.csv
yc storage cp schedule_adherence.csv s3://rzd-airflow-data/schedule_adherence.csv
yc storage cp maintenance.csv        s3://rzd-airflow-data/maintenance.csv

# Проверка загрузки
yc storage ls rzd-airflow-data/
```

Через Yandex Cloud Console: **Object Storage → rzd-airflow-data → Загрузить объекты**.

---

## Настройка Airflow Connections и Variables (в UI)

### Шаг 2.1 Connection для Object Storage

Перейдите в **Airflow UI → Admin → Connections → Add Connection**:

| Поле | Значение |
|---|---|
| Conn Id | `yandex_s3` |
| Conn Type | `Amazon Web Services` |
| Login | `<Access Key ID сервисного аккаунта>` |
| Password | `<Secret Access Key>` |
| Extra | `{"endpoint_url": "https://storage.yandexcloud.net", "region_name": "ru-central1"}` |

### Шаг 2.2 Connection для PostgreSQL

Перейдите в **Airflow UI → Admin → Connections → Add Connection**:

| Поле | Значение |
|---|---|
| Conn Id | `rzd_postgres` |
| Conn Type | `Postgres` |
| Host | `<FQDN кластера>.mdb.yandexcloud.net` |
| Database | `rzd_analytics` |
| Login | `<логин из Yandex Lockbox>` |
| Password | `<пароль из Yandex Lockbox>` |
| Port | `5432` |

> Значения FQDN, логина и пароля возьмите в **Yandex Cloud Console → Managed Service for PostgreSQL → Ваш кластер → Подключение**.

### Шаг 2.3 Airflow Variables

Перейдите в **Admin → Variables** и создайте следующие переменные:

| Key | Value | Описание |
|---|---|---|
| `s3_bucket_data` | `rzd-airflow-data` | Бакет с входными данными |
| `s3_bucket_results` | `rzd-airflow-results` | Бакет для результатов |
| `depot_code` | `TCH-15` | Код депо |
| `delay_threshold_min` | `15` | Порог опоздания для алерта, мин |
| `fuel_norm_lkm` | `0.28` | Норма расхода топлива, л/км |
| `energy_norm_kwhkm` | `14.5` | Норма расхода электроэнергии, кВт·ч/км |

---

## Деплой DAG-файла в Managed Airflow

### Шаг 3.1 Привязка бакета DAG-файлов к Managed Airflow

1. Перейдите в **Yandex Cloud Console → Managed Service for Apache Airflow → Ваш кластер → Редактировать**.
2. В разделе **DAG-файлы** укажите бакет `rzd-airflow-dags` и папку `dags/`.
3. Сохраните изменения.

### Шаг 3.2 Загрузка DAG-файла

После написания DAG-файла (Шаг 4) загрузите его в Object Storage:

```bash
# Через Yandex Cloud CLI
yc storage cp practice_04_shift_analysis.py \
    s3://rzd-airflow-dags/dags/practice_04_shift_analysis.py

# Проверка загрузки
yc storage ls rzd-airflow-dags/dags/
```

Через Yandex Cloud Console: **Object Storage → rzd-airflow-dags → dags/ → Загрузить объект**.

### Шаг 3.3 Проверка появления DAG в UI

После загрузки файла перейдите в **Airflow UI → DAGs**.
DAG появится автоматически в течение **1–3 минут** (интервал сканирования бакета).

Если DAG не появляется — проверьте:
- правильность имени файла (должен заканчиваться на `.py`);
- отсутствие синтаксических ошибок (раздел **Import Errors** в UI);
- корректность привязки бакета в настройках кластера.

---

## Шаги выполнения

### Шаг 4: Написание параметризованного DAG

Создайте файл `practice_04_shift_analysis.py` со следующим содержимым.

**Ключевые элементы DAG:**

- `Variable.get('s3_bucket_data')` — бакет с данными (не захардкоден в коде);
- `Variable.get('s3_bucket_results')` — бакет для результатов;
- Jinja2-шаблоны в ключах S3: `"reports/{{ ds }}/shift_{{ run_id }}.csv"`;
- `dag_run.conf` — для запуска за произвольный период или с другим бакетом;
- XCom — передача статистики между задачами через `return` в `@task`;
- `S3Hook` — для ВСЕХ операций чтения/записи файлов.

```python
"""
DAG: practice_04_shift_analysis
Модуль 04 — Практическая работа
Организация: ТЧЭ-15, депо Новосибирск-Главный

Задача: ежесменный анализ расхода топлива и соблюдения графика.
Все файловые операции — через Yandex Object Storage (S3Hook).
Пороговые значения хранятся в Airflow Variables.
Произвольный период задаётся через dag_run.conf.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# ─── Константы ──────────────────────────────────────────────────────────────

S3_CONN_ID = "yandex_s3"

DEFAULT_ARGS = {
    "owner":            "rzd-analytics",
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}


# ─── Вспомогательные функции для работы с S3 ────────────────────────────────

def read_csv_from_s3(bucket: str, key: str,
                     conn_id: str = S3_CONN_ID) -> pd.DataFrame:
    """Читает CSV-файл из Object Storage и возвращает DataFrame."""
    hook = S3Hook(aws_conn_id=conn_id)
    obj = hook.get_key(key=key, bucket_name=bucket)
    content = obj.get()['Body'].read().decode('utf-8')
    return pd.read_csv(StringIO(content))


def write_csv_to_s3(df: pd.DataFrame, bucket: str, key: str,
                    conn_id: str = S3_CONN_ID) -> None:
    """Записывает DataFrame в CSV-файл в Object Storage."""
    hook = S3Hook(aws_conn_id=conn_id)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=key,
        bucket_name=bucket,
        replace=True,
    )


# ─── DAG ────────────────────────────────────────────────────────────────────

@dag(
    dag_id="practice_04_shift_analysis",
    description=(
        "Анализ расхода топлива и соблюдения графика — ТЧЭ-15. "
        "Все данные в Yandex Object Storage."
    ),
    schedule="0 6,14,22 * * *",   # 6:00, 14:00, 22:00 — начало каждой смены
    start_date=datetime(2024, 3, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["rzd", "tch-15", "practice-04", "s3"],
)
def practice_04_shift_analysis():

    # ── Задача 1: Получение параметров запуска ───────────────────────────

    @task(task_id="get_run_params")
    def get_run_params(ds: str = None, **context) -> dict:
        """
        Определяет параметры анализа.
        Приоритет: dag_run.conf > Airflow Variables > значения по умолчанию.
        Возвращаемый dict автоматически попадает в XCom (TaskFlow API).
        """
        conf = context["dag_run"].conf or {}

        # Период анализа: из conf или текущий ds (дата планового запуска)
        start_date = conf.get("start_date", ds)
        end_date   = conf.get("end_date",   ds)

        # Бакеты: из conf или из Variables
        bucket_data    = conf.get(
            "bucket_data",
            Variable.get("s3_bucket_data", default_var="rzd-airflow-data")
        )
        bucket_results = conf.get(
            "bucket_results",
            Variable.get("s3_bucket_results", default_var="rzd-airflow-results")
        )

        # Прочие параметры
        depot       = conf.get(
            "depot_code",
            Variable.get("depot_code", default_var="TCH-15")
        )
        delay_thr   = int(
            conf.get(
                "delay_threshold_min",
                Variable.get("delay_threshold_min", default_var="15")
            )
        )
        fuel_norm   = float(
            Variable.get("fuel_norm_lkm", default_var="0.28")
        )

        params = {
            "start_date":     start_date,
            "end_date":       end_date,
            "bucket_data":    bucket_data,
            "bucket_results": bucket_results,
            "depot":          depot,
            "delay_thr":      delay_thr,
            "fuel_norm":      fuel_norm,
        }
        print(
            f"[get_run_params] Параметры: "
            f"{json.dumps(params, ensure_ascii=False, indent=2)}"
        )
        return params

    # ── Задача 2: Чтение данных из S3 ────────────────────────────────────

    @task(task_id="extract_from_s3")
    def extract_from_s3(params: dict) -> dict:
        """
        Читает sensor_readings.csv и schedule_adherence.csv из Object Storage.
        Ключи S3 содержат Jinja2-шаблоны — заменяются на конкретные даты
        через форматирование строки Python (аналог {{ ds_nodash }} в шаблонах).

        Возвращает агрегированные данные (не полный DataFrame —
        XCom хранит сериализуемые объекты).
        """
        bucket = params["bucket_data"]
        ds_nodash = params["start_date"].replace("-", "")

        # Ключи S3 с шаблонами дат (аналог {{ ds_nodash }})
        sensor_key    = f"sensor_readings/{ds_nodash}/data.csv"
        adherence_key = f"schedule_adherence/{ds_nodash}/data.csv"

        # Если партиционированных файлов нет — читаем полный файл и фильтруем
        try:
            df_sensor = read_csv_from_s3(bucket, sensor_key)
        except Exception:
            print(
                f"[extract_from_s3] Файл {sensor_key} не найден, "
                f"читаем полный sensor_readings.csv"
            )
            df_sensor = read_csv_from_s3(bucket, "sensor_readings.csv")
            if "reading_date" in df_sensor.columns:
                df_sensor = df_sensor[
                    df_sensor["reading_date"] == params["start_date"]
                ]

        try:
            df_adh = read_csv_from_s3(bucket, adherence_key)
        except Exception:
            print(
                f"[extract_from_s3] Файл {adherence_key} не найден, "
                f"читаем полный schedule_adherence.csv"
            )
            df_adh = read_csv_from_s3(bucket, "schedule_adherence.csv")
            if "date" in df_adh.columns:
                df_adh = df_adh[df_adh["date"] == params["start_date"]]

        print(
            f"[extract_from_s3] Прочитано: "
            f"sensor_readings={len(df_sensor)} строк, "
            f"schedule_adherence={len(df_adh)} строк"
        )

        # Возвращаем сводные данные (не DataFrame — XCom не сериализует их)
        return {
            "sensor_count":    len(df_sensor),
            "adherence_count": len(df_adh),
            "total_delays":    int(df_adh["delay_min"].sum())
                               if "delay_min" in df_adh.columns else 0,
            "max_delay":       int(df_adh["delay_min"].max())
                               if "delay_min" in df_adh.columns else 0,
        }

    # ── Задача 3: Расчёт статистики ───────────────────────────────────────

    @task(task_id="calculate_stats")
    def calculate_stats(params: dict, raw_data: dict) -> dict:
        """
        Рассчитывает KPI смены на основе данных из S3.
        Читает locomotives.csv для получения списка тепловозов.
        Возвращает словарь со статистикой (передаётся через XCom).
        """
        bucket = params["bucket_data"]

        df_locos = read_csv_from_s3(bucket, "locomotives.csv")
        diesel_count   = len(
            df_locos[df_locos["traction_type"] == "diesel"]
        ) if "traction_type" in df_locos.columns else 0
        electric_count = len(
            df_locos[df_locos["traction_type"] == "electric"]
        ) if "traction_type" in df_locos.columns else 0

        total   = raw_data["adherence_count"]
        delayed = int(
            raw_data["total_delays"] / params["delay_thr"]
        ) if params["delay_thr"] > 0 else 0
        on_time = max(total - delayed, 0)
        otd_pct = round(on_time / total * 100, 1) if total > 0 else 0.0

        stats = {
            "depot":          params["depot"],
            "shift_date":     params["start_date"],
            "diesel_locos":   diesel_count,
            "electric_locos": electric_count,
            "sensor_events":  raw_data["sensor_count"],
            "total_trips":    total,
            "on_time_trips":  on_time,
            "delayed_trips":  delayed,
            "otd_pct":        otd_pct,
            "max_delay_min":  raw_data["max_delay"],
            "fuel_norm_lkm":  params["fuel_norm"],
        }
        print(
            f"[calculate_stats] Статистика: "
            f"{json.dumps(stats, ensure_ascii=False, indent=2)}"
        )
        return stats

    # ── Задача 4: Запись отчёта в S3 ──────────────────────────────────────

    @task(task_id="save_report_to_s3")
    def save_report_to_s3(params: dict, stats: dict, **context) -> str:
        """
        Сохраняет CSV-отчёт в rzd-airflow-results/.
        Ключ S3 содержит Jinja2-шаблоны:
          reports/{{ ds }}/shift_{{ run_id }}.csv
        """
        run_id  = context["dag_run"].run_id
        ds      = params["start_date"]
        bucket  = params["bucket_results"]

        # Ключ с шаблоном даты и run_id (уникальность при повторных запусках)
        s3_key = f"reports/{ds}/shift_{run_id}.csv"

        df_report = pd.DataFrame([{
            "depot_code":     stats["depot"],
            "shift_date":     stats["shift_date"],
            "diesel_locos":   stats["diesel_locos"],
            "electric_locos": stats["electric_locos"],
            "total_trips":    stats["total_trips"],
            "on_time_trips":  stats["on_time_trips"],
            "otd_pct":        stats["otd_pct"],
            "max_delay_min":  stats["max_delay_min"],
            "run_id":         run_id,
        }])

        write_csv_to_s3(df_report, bucket, s3_key)
        print(f"[save_report_to_s3] Отчёт сохранён: s3://{bucket}/{s3_key}")
        return f"s3://{bucket}/{s3_key}"

    # ── Задача 5: Проверка результата ──────────────────────────────────────

    @task(task_id="verify_report_in_s3")
    def verify_report_in_s3(s3_path: str) -> None:
        """
        Проверяет, что файл отчёта существует в Object Storage.
        Демонстрирует использование S3Hook.check_for_key().
        """
        # Разбираем s3://bucket/key
        without_prefix = s3_path.replace("s3://", "")
        bucket, key = without_prefix.split("/", 1)

        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        exists = hook.check_for_key(key=key, bucket_name=bucket)

        if exists:
            print(f"[verify_report_in_s3] OK — файл найден: {s3_path}")
        else:
            raise FileNotFoundError(
                f"Файл отчёта не найден в Object Storage: {s3_path}"
            )

    # ── Граф задач ────────────────────────────────────────────────────────
    params   = get_run_params()
    raw_data = extract_from_s3(params)
    stats    = calculate_stats(params, raw_data)
    s3_path  = save_report_to_s3(params, stats)
    verify_report_in_s3(s3_path)


dag_instance = practice_04_shift_analysis()
```

---

### Шаг 5: Деплой DAG-файла в Managed Airflow

```bash
# Загрузить DAG-файл в бакет rzd-airflow-dags
yc storage cp practice_04_shift_analysis.py \
    s3://rzd-airflow-dags/dags/practice_04_shift_analysis.py

# Проверить, что файл загружен
yc storage ls rzd-airflow-dags/dags/
```

Перейдите в **Airflow UI → DAGs** и дождитесь появления `practice_04_shift_analysis` (1–3 мин).

---

### Шаг 6: Запуск DAG и проверка результатов

#### 6.1 Запуск через Airflow UI

1. Откройте **Airflow UI → DAGs → practice_04_shift_analysis**.
2. Включите DAG (переключатель слева).
3. Нажмите **Trigger DAG w/ config**.
4. В поле **Configuration JSON** введите:

```json
{
  "start_date":   "2024-03-15",
  "end_date":     "2024-03-15",
  "depot_code":   "TCH-15",
  "bucket_data":  "rzd-airflow-data",
  "bucket_results": "rzd-airflow-results"
}
```

5. Нажмите **Trigger**.

#### 6.2 Запуск за произвольный период

Через Yandex Cloud CLI (если доступен Airflow CLI):

```bash
# Произвольный период — квартальный отчёт
airflow dags trigger practice_04_shift_analysis \
    --conf '{
        "start_date": "2024-03-01",
        "end_date":   "2024-03-31",
        "depot_code": "TCH-15"
    }'
```

#### 6.3 Проверка XCom

1. В **Graph View** нажмите на задачу `calculate_stats`.
2. Перейдите на вкладку **XCom**.
3. Убедитесь, что `return_value` содержит словарь с KPI смены (otd_pct, total_trips и т.д.).

#### 6.4 Проверка файла отчёта в Object Storage

```bash
# Проверить, что отчёт появился в бакете результатов
yc storage ls rzd-airflow-results/reports/2024-03-15/
```

---

### Шаг 7: Изменение Variables без деплоя

Проверьте, что пороговые значения меняются без перезапуска или переписывания DAG:

1. В **Airflow UI → Admin → Variables** измените `delay_threshold_min` с `15` на `20`.
2. Запустите DAG повторно через **Trigger DAG**.
3. Откройте логи задачи `get_run_params` и убедитесь, что параметр `delay_thr` равен `20`.

---

## Полный код DAG

Полный рабочий код DAG приведён в Шаге 4 данной инструкции.

Краткое описание задач:

| Задача | Что делает |
|---|---|
| `get_run_params` | Читает Variables и `dag_run.conf`, формирует словарь параметров → XCom |
| `extract_from_s3` | Читает CSV из S3 через `S3Hook`, возвращает агрегаты → XCom |
| `calculate_stats` | Рассчитывает KPI (OTD, кол-во локомотивов) на основе данных из S3 → XCom |
| `save_report_to_s3` | Записывает CSV-отчёт в `rzd-airflow-results/reports/{{ ds }}/` → XCom |
| `verify_report_in_s3` | Проверяет наличие файла в S3 через `hook.check_for_key()` |

---

## Контрольные вопросы

1. Почему в Managed Airflow нельзя использовать `pd.read_csv("/local/path")` и как правильно читать файлы? Опишите, что происходит внутри функции `read_csv_from_s3()`.

2. В задаче `extract_from_s3` ключ S3 содержит шаблон `sensor_readings/{ds_nodash}/data.csv`. Что произойдёт, если файл с такой партицией не существует? Как в коде обработана эта ситуация?

3. Объясните разницу между `hook.load_string()` и `hook.load_bytes()` при записи файлов в Object Storage. Когда использовать каждый из вариантов?

4. Задача `get_run_params` использует `dag_run.conf` с приоритетом над Variables. Почему такой порядок приоритетов удобен при запуске квартального отчёта начальником депо из UI?

5. В ключе S3 используется `run_id`: `reports/{{ ds }}/shift_{run_id}.csv`. Зачем добавлять `run_id` в имя файла, если дата уже есть в пути? Что произойдёт без него при повторном запуске DAG за ту же дату?
