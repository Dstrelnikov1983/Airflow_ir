# Лабораторная работа №04: Анализ расхода топлива через XCom и S3

**Модуль:** 04 — Шаблоны заданий и параметризация
**Продолжительность:** 90–120 минут
**Уровень:** продвинутый
**Организация:** Западно-Сибирская дирекция тяги, ТЧЭ-15, депо Новосибирск-Главный
**Платформа:** Яндекс Облако — Managed Service for Apache Airflow™

---

## Цель

Разработать production-ready DAG на TaskFlow API, который:

- читает данные о расходе топлива из Yandex Object Storage через `S3Hook`;
- рассчитывает удельный расход топлива по каждому локомотиву депо ТЧЭ-15;
- классифицирует локомотивы по категориям: **норма / перерасход**;
- передаёт промежуточные результаты между задачами через **XCom** (механизм TaskFlow API);
- записывает итоговый CSV-отчёт в бакет `rzd-airflow-results` через `S3Hook`.

---

## Предварительные условия

- Managed Service for Apache Airflow запущен и доступен по веб-адресу.
- Бакеты и данные настроены согласно **Практической работе №04**:
  - `rzd-airflow-dags/` — бакет для DAG-файлов, привязан к кластеру Airflow;
  - `rzd-airflow-data/` — содержит `sensor_readings.csv`, `locomotives.csv`, `trips.csv`;
  - `rzd-airflow-results/` — бакет для записи результатов DAG.
- Connection **`yandex_s3`** настроен в Airflow UI:
  - Conn Type: `Amazon Web Services`
  - Extra: `{"endpoint_url": "https://storage.yandexcloud.net", "region_name": "ru-central1"}`
- Connection **`rzd_postgres`** настроен в Airflow UI (Managed PostgreSQL).
- Variables `s3_bucket_data`, `s3_bucket_results`, `depot_code` созданы (Практика №04, Шаг 2.3).

---

## Задание

Реализуйте DAG `fuel_analysis_dag` на TaskFlow API со следующей цепочкой задач:

**1.** Задача `@task extract` — читает `sensor_readings.csv` из бакета `rzd-airflow-data` через `S3Hook`, фильтрует строки с показаниями расходомера топлива (`sensor_type == 'fuel_flow'`), возвращает словарь с данными через XCom.

**2.** Задача `@task load_locomotives` — читает `locomotives.csv` из бакета `rzd-airflow-data` через `S3Hook`, возвращает словарь `{loco_id: {"series": ..., "number": ..., "traction_type": ...}}`.

**3.** Задача `@task calculate` — получает данные из XCom задач `extract` и `load_locomotives`, рассчитывает удельный расход топлива (л/км) по каждому дизельному локомотиву за период. Возвращает список словарей с расчётами через XCom.

**4.** Задача `@task classify` — получает результаты из XCom задачи `calculate`, сравнивает фактический расход с нормой из Variable `fuel_norm_lkm`, присваивает каждому локомотиву статус: `"норма"` / `"перерасход"` / `"критический перерасход"` (>20%). Возвращает классифицированный список через XCom.

**5.** Задача `@task enrich_with_postgres` — обогащает данные из XCom информацией о последнем ТО локомотива из таблицы `rzd_analytics.locomotives` (Managed PostgreSQL) через `PostgresHook`.

**6.** Задача `@task save_report` — формирует итоговый DataFrame, записывает CSV в `rzd-airflow-results/fuel_reports/{{ ds }}/depot_{{ depot_code }}.csv` через `S3Hook.load_string()`.

**7.** Задача `@task verify_and_log` — проверяет существование файла отчёта в S3 через `S3Hook.check_for_key()`, выводит итоговую сводку в лог, возвращает путь к файлу через XCom.

---

## Полный код DAG

Создайте файл `fuel_analysis_dag.py`:

```python
"""
DAG: fuel_analysis_dag
Лабораторная работа 04 — Анализ расхода топлива через XCom и S3
Организация: ТЧЭ-15, депо Новосибирск-Главный

Технологии:
- TaskFlow API (@dag, @task) — передача данных через XCom
- S3Hook — чтение CSV из Yandex Object Storage (НЕ локальная ФС)
- PostgresHook — обогащение данными из Managed PostgreSQL
- Airflow Variables — хранение нормативов без хардкода
- dag_run.conf — запуск за произвольный период из UI

ВАЖНО: все операции с файлами ТОЛЬКО через S3Hook.
pd.read_csv("/local/path") НЕ используется.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from io import StringIO
from typing import Any

import pandas as pd

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

# ─── Константы ──────────────────────────────────────────────────────────────

S3_CONN_ID       = "yandex_s3"
POSTGRES_CONN_ID = "rzd_postgres"

DEFAULT_ARGS = {
    "owner":            "rzd-analytics",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False,
}


# ─── Вспомогательные функции для работы с S3 ────────────────────────────────

def read_csv_from_s3(bucket: str, key: str,
                     conn_id: str = S3_CONN_ID) -> pd.DataFrame:
    """
    Читает CSV-файл из Yandex Object Storage и возвращает DataFrame.
    Использует S3Hook — единственный правильный способ работы с файлами
    в среде Managed Airflow (доступа к локальной ФС нет).
    """
    hook = S3Hook(aws_conn_id=conn_id)
    obj = hook.get_key(key=key, bucket_name=bucket)
    content = obj.get()['Body'].read().decode('utf-8')
    return pd.read_csv(StringIO(content))


def write_csv_to_s3(df: pd.DataFrame, bucket: str, key: str,
                    conn_id: str = S3_CONN_ID) -> None:
    """
    Записывает DataFrame в CSV-файл в Yandex Object Storage.
    Использует hook.load_string() — файл передаётся как строка,
    без промежуточного сохранения на диск.
    """
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
    dag_id="fuel_analysis_dag",
    description=(
        "Анализ расхода топлива локомотивов ТЧЭ-15. "
        "Данные из Yandex Object Storage. XCom + TaskFlow API."
    ),
    schedule="0 6,14,22 * * *",
    start_date=datetime(2024, 3, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["rzd", "tch-15", "lab-04", "fuel", "s3"],
    doc_md="""
## DAG: Анализ расхода топлива ТЧЭ-15

Запускается 3 раза в сутки. Принимает `dag_run.conf`:
- `start_date` / `end_date` — период (YYYY-MM-DD)
- `depot_code` — код депо (по умолчанию TCH-15)

Нормативы топлива — в Airflow Variable `fuel_norm_lkm`.
Все файлы читаются/пишутся через Yandex Object Storage (S3Hook).
    """,
)
def fuel_analysis_dag():

    # ── Задача 1: Чтение sensor_readings из S3 ───────────────────────────

    @task(task_id="extract")
    def extract(ds: str = None, **context) -> dict[str, Any]:
        """
        Читает sensor_readings.csv из бакета rzd-airflow-data.
        Фильтрует показания расходомера топлива (sensor_type='fuel_flow').
        Возвращает агрегированные данные по локомотивам через XCom.

        Файл НЕ сохраняется на диск — обрабатывается в памяти через StringIO.
        """
        conf         = context["dag_run"].conf or {}
        start_date   = conf.get("start_date", ds)
        end_date     = conf.get("end_date", ds)
        bucket       = Variable.get("s3_bucket_data",
                                    default_var="rzd-airflow-data")

        log.info(
            "Читаем sensor_readings.csv из s3://%s/sensor_readings.csv",
            bucket
        )
        df = read_csv_from_s3(bucket, "sensor_readings.csv")

        log.info("Прочитано строк: %d, колонки: %s",
                 len(df), list(df.columns))

        # Фильтрация по типу датчика и дате
        if "sensor_type" in df.columns:
            df = df[df["sensor_type"] == "fuel_flow"]
        if "reading_date" in df.columns:
            df = df[
                (df["reading_date"] >= start_date)
                & (df["reading_date"] <= end_date)
            ]

        if df.empty:
            log.warning(
                "Нет данных расходомера топлива за период %s – %s",
                start_date, end_date
            )
            return {
                "start_date":  start_date,
                "end_date":    end_date,
                "records":     [],
                "loco_ids":    [],
            }

        # Агрегация: суммарный расход и пробег по каждому локомотиву
        fuel_col     = "value"
        distance_col = "distance_km"
        loco_col     = "loco_id"

        agg_data = []
        for loco_id, group in df.groupby(loco_col):
            total_fuel = float(group[fuel_col].sum()) \
                         if fuel_col in group.columns else 0.0
            total_km   = float(group[distance_col].sum()) \
                         if distance_col in group.columns else 0.0
            agg_data.append({
                "loco_id":    int(loco_id),
                "total_fuel": round(total_fuel, 2),
                "total_km":   round(total_km, 2),
            })

        log.info(
            "Агрегированы данные по %d локомотивам за период %s – %s",
            len(agg_data), start_date, end_date
        )
        return {
            "start_date": start_date,
            "end_date":   end_date,
            "records":    agg_data,
            "loco_ids":   [r["loco_id"] for r in agg_data],
        }

    # ── Задача 2: Чтение реестра локомотивов из S3 ───────────────────────

    @task(task_id="load_locomotives")
    def load_locomotives() -> dict[str, Any]:
        """
        Читает locomotives.csv из бакета rzd-airflow-data через S3Hook.
        Возвращает словарь {loco_id: {...}} через XCom для обогащения
        результатов расчёта.
        """
        bucket = Variable.get("s3_bucket_data",
                              default_var="rzd-airflow-data")

        log.info("Читаем locomotives.csv из s3://%s/locomotives.csv", bucket)
        df = read_csv_from_s3(bucket, "locomotives.csv")

        log.info("Прочитано локомотивов: %d", len(df))

        locos = {}
        for _, row in df.iterrows():
            loco_id = int(row.get("loco_id", 0))
            locos[loco_id] = {
                "series":        str(row.get("series", "")),
                "number":        str(row.get("number", "")),
                "traction_type": str(row.get("traction_type", "")),
                "depot_code":    str(row.get("depot_code", "")),
            }

        diesel_count = sum(
            1 for v in locos.values() if v["traction_type"] == "diesel"
        )
        log.info(
            "Локомотивов всего: %d, из них тепловозов: %d",
            len(locos), diesel_count
        )
        return locos

    # ── Задача 3: Расчёт удельного расхода топлива ───────────────────────

    @task(task_id="calculate")
    def calculate(
        sensor_data: dict[str, Any],
        locos: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """
        Рассчитывает удельный расход топлива (л/км) по каждому тепловозу.
        Объединяет данные из двух XCom-значений:
        - sensor_data — агрегированные показания расходомеров
        - locos — реестр локомотивов (серия, номер, тип тяги)

        Возвращает список словарей с результатами расчёта → XCom.
        """
        records = sensor_data.get("records", [])

        if not records:
            log.warning("Нет данных для расчёта расхода топлива.")
            return []

        results = []
        for rec in records:
            loco_id   = rec["loco_id"]
            loco_info = locos.get(loco_id) or locos.get(str(loco_id), {})

            # Пропускаем электровозы
            if loco_info.get("traction_type") == "electric":
                continue

            total_fuel = rec["total_fuel"]
            total_km   = rec["total_km"]
            lkm        = round(total_fuel / total_km, 3) \
                         if total_km > 0 else 0.0

            result = {
                "loco_id":     loco_id,
                "series":      loco_info.get("series", "N/A"),
                "number":      loco_info.get("number", "N/A"),
                "total_fuel":  total_fuel,
                "total_km":    total_km,
                "lkm_actual":  lkm,
            }
            results.append(result)

        log.info(
            "Рассчитан расход топлива для %d тепловозов.", len(results)
        )
        return results

    # ── Задача 4: Классификация по норме ─────────────────────────────────

    @task(task_id="classify")
    def classify(
        fuel_results: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Классифицирует тепловозы по категориям расхода топлива:
        - "норма"                 — отклонение <= 0%
        - "перерасход"            — отклонение 0–20%
        - "критический перерасход" — отклонение > 20%

        Норма берётся из Airflow Variable 'fuel_norm_lkm'.
        Возвращает обогащённый список через XCom.
        """
        if not fuel_results:
            log.warning("Нет данных для классификации.")
            raise AirflowSkipException("Нет данных для классификации.")

        fuel_norm = float(
            Variable.get("fuel_norm_lkm", default_var="0.28")
        )
        log.info("Норма расхода топлива: %.3f л/км", fuel_norm)

        classified = []
        for item in fuel_results:
            lkm        = item["lkm_actual"]
            deviation  = round((lkm - fuel_norm) / fuel_norm * 100, 1) \
                         if fuel_norm > 0 else 0.0

            if deviation > 20:
                status = "критический перерасход"
            elif deviation > 0:
                status = "перерасход"
            else:
                status = "норма"

            classified_item = {
                **item,
                "lkm_norm":    fuel_norm,
                "deviation":   deviation,
                "status":      status,
            }
            classified.append(classified_item)

            if status != "норма":
                log.warning(
                    "[%s] %s-%s: %.3f л/км (норма %.3f, откл. %+.1f%%)",
                    status.upper(),
                    item["series"], item["number"],
                    lkm, fuel_norm, deviation,
                )

        norm_count  = sum(1 for x in classified if x["status"] == "норма")
        over_count  = sum(1 for x in classified if x["status"] == "перерасход")
        crit_count  = sum(
            1 for x in classified if x["status"] == "критический перерасход"
        )
        log.info(
            "Классификация: норма=%d, перерасход=%d, критический=%d",
            norm_count, over_count, crit_count,
        )
        return classified

    # ── Задача 5: Обогащение данными из PostgreSQL ────────────────────────

    @task(task_id="enrich_with_postgres")
    def enrich_with_postgres(
        classified: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """
        Обогащает результаты данными о последнем ТО из Managed PostgreSQL.
        Использует PostgresHook для запроса к rzd_analytics.locomotives.
        """
        if not classified:
            log.warning("Нет данных для обогащения из PostgreSQL.")
            return []

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        loco_ids = [item["loco_id"] for item in classified]
        placeholders = ",".join(["%s"] * len(loco_ids))

        sql = f"""
            SELECT
                loco_id,
                last_to_date,
                moto_hours,
                status
            FROM rzd_analytics.locomotives
            WHERE loco_id IN ({placeholders})
        """

        rows = hook.get_records(sql, parameters=loco_ids)
        pg_data = {
            int(r[0]): {
                "last_to_date": str(r[1]) if r[1] else None,
                "moto_hours":   int(r[2]) if r[2] else 0,
                "loco_status":  str(r[3]) if r[3] else "unknown",
            }
            for r in rows
        }

        enriched = []
        for item in classified:
            pg_info = pg_data.get(item["loco_id"], {})
            enriched.append({
                **item,
                "last_to_date": pg_info.get("last_to_date"),
                "moto_hours":   pg_info.get("moto_hours", 0),
                "loco_status":  pg_info.get("loco_status", "unknown"),
            })

        log.info(
            "Обогащены данными PostgreSQL: %d записей.", len(enriched)
        )
        return enriched

    # ── Задача 6: Запись отчёта в S3 ──────────────────────────────────────

    @task(task_id="save_report")
    def save_report(
        enriched: list[dict[str, Any]],
        sensor_data: dict[str, Any],
        **context,
    ) -> str:
        """
        Формирует итоговый DataFrame и записывает CSV-отчёт
        в rzd-airflow-results/fuel_reports/{{ ds }}/depot_{{ depot_code }}.csv

        Использует hook.load_string() — файл передаётся как строка,
        без записи на локальный диск.

        Возвращает путь к файлу в S3 через XCom.
        """
        if not enriched:
            log.warning("Нет данных для сохранения отчёта.")
            raise AirflowSkipException("Нет данных для отчёта.")

        bucket     = Variable.get("s3_bucket_results",
                                  default_var="rzd-airflow-results")
        depot_code = Variable.get("depot_code", default_var="TCH-15")
        ds         = sensor_data.get("start_date",
                                     context["ds"])
        run_id     = context["dag_run"].run_id

        # Ключ S3 с датой и кодом депо в пути
        s3_key = (
            f"fuel_reports/{ds}/depot_{depot_code}.csv"
        )

        df_report = pd.DataFrame(enriched)

        # Добавляем служебные поля
        df_report["report_date"] = ds
        df_report["depot_code"]  = depot_code
        df_report["dag_run_id"]  = run_id

        write_csv_to_s3(df_report, bucket, s3_key)

        s3_path = f"s3://{bucket}/{s3_key}"
        log.info(
            "Отчёт сохранён: %s (%d строк, %d колонок)",
            s3_path, len(df_report), len(df_report.columns)
        )
        return s3_path

    # ── Задача 7: Проверка и финальный лог ────────────────────────────────

    @task(task_id="verify_and_log")
    def verify_and_log(
        s3_path: str,
        classified: list[dict[str, Any]],
    ) -> None:
        """
        Проверяет наличие файла отчёта в Object Storage через
        S3Hook.check_for_key() и выводит итоговую сводку в лог.
        """
        without_prefix = s3_path.replace("s3://", "")
        bucket, key = without_prefix.split("/", 1)

        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        exists = hook.check_for_key(key=key, bucket_name=bucket)

        if not exists:
            raise FileNotFoundError(
                f"Файл отчёта не найден в Object Storage: {s3_path}"
            )

        sep = "=" * 60
        print(f"\n{sep}")
        print("  ИТОГОВЫЙ ОТЧЁТ — РАСХОД ТОПЛИВА ТЧЭ-15")
        print(f"  Файл: {s3_path}")
        print(sep)

        for item in classified:
            marker = ""
            if item["status"] == "критический перерасход":
                marker = "  <<< КРИТИЧНО"
            elif item["status"] == "перерасход":
                marker = "  <<< ВНИМАНИЕ"
            print(
                f"  {item['series']}-{item['number']}: "
                f"{item['lkm_actual']:.3f} л/км "
                f"(норма {item.get('lkm_norm', 0):.3f}, "
                f"откл. {item.get('deviation', 0):+.1f}%) "
                f"[{item['status']}]{marker}"
            )

        norm_count = sum(1 for x in classified if x["status"] == "норма")
        over_count = sum(1 for x in classified if x["status"] != "норма")
        print(f"\n  Итого тепловозов: {len(classified)}")
        print(f"  В норме:          {norm_count}")
        print(f"  Превышение нормы: {over_count}")
        print(f"{sep}\n")

        log.info(
            "Файл отчёта подтверждён в Object Storage: %s", s3_path
        )

    # ── Граф задач ────────────────────────────────────────────────────────
    sensor_data    = extract()
    locos          = load_locomotives()
    fuel_results   = calculate(sensor_data, locos)
    classified     = classify(fuel_results)
    enriched       = enrich_with_postgres(classified)
    s3_path        = save_report(enriched, sensor_data)
    verify_and_log(s3_path, classified)


dag_instance = fuel_analysis_dag()
```

---

## Деплой и тестирование

### Загрузка DAG в Object Storage

```bash
# Через Yandex Cloud CLI
yc storage cp fuel_analysis_dag.py \
    s3://rzd-airflow-dags/dags/fuel_analysis_dag.py

# Проверка загрузки
yc storage ls rzd-airflow-dags/dags/
```

Через Yandex Cloud Console: **Object Storage → rzd-airflow-dags → dags/ → Загрузить объект**.

### Проверка в Airflow UI

1. Перейдите в **Airflow UI → DAGs**.
2. Дождитесь появления `fuel_analysis_dag` (1–3 минуты).
3. Если DAG не появляется — проверьте раздел **Import Errors** в UI.
4. Включите DAG переключателем слева.
5. Нажмите **Trigger DAG w/ config** и введите JSON:

```json
{
  "start_date": "2024-03-15",
  "end_date":   "2024-03-15",
  "depot_code": "TCH-15"
}
```

6. Перейдите в **Graph View** — убедитесь, что все задачи завершились зелёным.
7. Нажмите на задачу `classify` → вкладка **XCom** → убедитесь, что `return_value` содержит список словарей с полями `status`, `lkm_actual`, `deviation`.
8. Проверьте файл отчёта в Object Storage:

```bash
yc storage ls rzd-airflow-results/fuel_reports/2024-03-15/
```

### Ожидаемый результат

После успешного выполнения DAG:

| Задача | Результат |
|---|---|
| `extract` | XCom содержит агрегированные данные по локомотивам |
| `load_locomotives` | XCom содержит словарь реестра локомотивов |
| `calculate` | XCom содержит список с полями `lkm_actual`, `total_fuel`, `total_km` |
| `classify` | XCom содержит классификацию: `"норма"` / `"перерасход"` / `"критический перерасход"` |
| `enrich_with_postgres` | XCom обогащён полями `last_to_date`, `moto_hours` из PostgreSQL |
| `save_report` | Файл `s3://rzd-airflow-results/fuel_reports/2024-03-15/depot_TCH-15.csv` создан |
| `verify_and_log` | Файл подтверждён, в логах — итоговая таблица по тепловозам |

---

## Задания повышенной сложности

### Задание 1. S3KeySensor вместо немедленного чтения

Добавьте перед задачей `extract` сенсор `S3KeySensor`, который ожидает появления файла `sensor_readings/{ds_nodash}/data.csv` в бакете (имитация ежесменной выгрузки из SCADA-системы):

```python
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

wait_for_sensor_data = S3KeySensor(
    task_id='wait_for_sensor_file',
    bucket_name='rzd-airflow-data',
    bucket_key='sensor_readings/{{ ds_nodash }}/data.csv',
    aws_conn_id='yandex_s3',
    poke_interval=300,   # проверять каждые 5 минут
    timeout=7200,        # таймаут 2 часа
    mode='reschedule',   # освобождает worker-слот между проверками
)
```

Реализуйте полную цепочку: `wait_for_sensor_file >> extract >> ...`

### Задание 2. Dynamic Task Mapping для нескольких депо

Измените DAG так, чтобы задачи `calculate` и `classify` запускались параллельно для каждого тепловоза через `expand()`:

```python
# Пример структуры (реализуйте самостоятельно)
from airflow.decorators import task_group

@task
def get_loco_ids(sensor_data: dict) -> list[dict]:
    """Возвращает список {loco_id, series, number} для expand()."""
    return sensor_data.get("records", [])

loco_params = get_loco_ids(sensor_data)
per_loco_results = calculate.expand(record=loco_params)
```

Убедитесь, что задача `classify` корректно принимает список результатов от всех параллельных инстансов `calculate`.

### Задание 3. Запись сводки в PostgreSQL

Добавьте задачу `save_summary_to_postgres` после `enrich_with_postgres`, которая записывает агрегированные метрики в таблицу `rzd_analytics.shift_reports`:

```python
@task(task_id="save_summary_to_postgres")
def save_summary_to_postgres(
    enriched: list[dict],
    sensor_data: dict,
    **context,
) -> None:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    run_id     = context["dag_run"].run_id
    depot_code = Variable.get("depot_code", default_var="TCH-15")

    overspend = sum(1 for x in enriched if x["status"] != "норма")
    avg_lkm   = (
        sum(x["lkm_actual"] for x in enriched) / len(enriched)
        if enriched else 0.0
    )

    hook.run(
        """
        INSERT INTO rzd_analytics.shift_reports
            (shift_date, depot_code, fuel_lkm_avg,
             overspend_count, dag_run_id)
        VALUES (%(date)s, %(depot)s, %(avg_lkm)s,
                %(overspend)s, %(run_id)s)
        ON CONFLICT DO NOTHING
        """,
        parameters={
            "date":      sensor_data.get("start_date"),
            "depot":     depot_code,
            "avg_lkm":   round(avg_lkm, 3),
            "overspend": overspend,
            "run_id":    run_id,
        },
    )
```

Добавьте задачу в граф и проверьте запись через SQL-клиент PostgreSQL.
