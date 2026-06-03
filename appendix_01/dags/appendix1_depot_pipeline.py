"""
Приложение 1. Capstone-DAG: суточный конвейер мониторинга локомотивов ТЧЭ-15.

Закрепляет навыки модулей 1–6 БЕЗ обращения к внешним источникам данных:
  • Модуль 3 — расписание: cron, часовой пояс Asia/Novosibirsk, start_date,
                catchup, max_active_runs, dagrun_timeout.
  • Модуль 1/4 — переменные: Variable.get() с default, {{ var.value.x }},
                {{ var.json.x }} в шаблонах.
  • Модуль 4 — шаблоны заданий: Jinja в BashOperator, params, templates_dict,
                user_defined_macros, встроенные макросы ({{ ds }}, {{ ts }}, ...).
  • Модуль 5 — зависимости: оператор >>, chain(), trigger_rule, ветвление.
  • Модуль 6 — потоки обработки: fan-out / fan-in, TaskGroup, branch + join.

Данные НЕ читаются из S3/PostgreSQL/файлов. Телеметрия генерируется
детерминированно из даты запуска ({{ ds_nodash }}), поэтому любой backfill
воспроизводим: один и тот же execution_date всегда даёт один и тот же результат.

Среда: Yandex Managed Service for Apache Airflow™.
Деплой: загрузить файл в бакет DAG-ов, привязанный к кластеру.
"""

from __future__ import annotations

import logging
import random
from datetime import timedelta

import pendulum

from airflow.decorators import task, task_group
from airflow.models.dag import DAG
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

log = logging.getLogger(__name__)

# Часовой пояс депо. Все cron-выражения трактуются в этом поясе (Модуль 3).
NSK_TZ = pendulum.timezone("Asia/Novosibirsk")

# Метрики, которые считаются параллельно (fan-out, Модуль 6).
METRICS = ["fuel", "axlebox_temp", "schedule"]


# --------------------------------------------------------------------------- #
#  Вспомогательная функция — пользовательский макрос (Модуль 4)               #
#  Доступна в шаблонах как {{ shift_by_hour(ts) }}.                           #
# --------------------------------------------------------------------------- #
def shift_by_hour(ts: str) -> int:
    """Возвращает номер смены (1/2/3) по часу из ISO-таймстампа execution_date."""
    hour = pendulum.parse(ts).in_timezone(NSK_TZ).hour
    if 0 <= hour < 8:
        return 1
    if 8 <= hour < 16:
        return 2
    return 3


default_args = {
    "owner": "tche15-analytics",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="appendix1_depot_pipeline",
    description="Приложение 1 — суточный конвейер мониторинга локомотивов ТЧЭ-15 (без внешних данных)",
    # Модуль 3: каждый день в 05:30 по времени депо (Asia/Novosibirsk).
    schedule="30 5 * * *",
    start_date=pendulum.datetime(2024, 3, 1, tz=NSK_TZ),
    catchup=True,                       # для упражнения с backfill
    max_active_runs=3,                  # не более 3 одновременных run-ов
    dagrun_timeout=timedelta(minutes=20),
    default_args=default_args,
    # Модуль 4: params — параметры запуска, доступные в шаблонах как {{ params.x }}.
    params={
        "fleet_size": 6,               # сколько локомотивов «опросить»
        "report_format": "csv",
    },
    # Модуль 4: пользовательские макросы для Jinja.
    user_defined_macros={"shift_by_hour": shift_by_hour},
    tags=["appendix1", "rzd", "tche15", "no-external-data", "capstone"],
) as dag:

    # --------------------------------------------------------------------- #
    #  ЗАДАЧА 0. Старт                                                       #
    # --------------------------------------------------------------------- #
    start = EmptyOperator(task_id="start")

    # --------------------------------------------------------------------- #
    #  ЗАДАЧА 1. Печать контекста запуска — демонстрация ШАБЛОНОВ (Модуль 4) #
    #  Ни одного «живого» значения в коде: всё подставляет Jinja.            #
    # --------------------------------------------------------------------- #
    print_context = BashOperator(
        task_id="print_run_context",
        bash_command=(
            'echo "=== Контекст запуска DAG ==="; '
            'echo "Депо       : {{ var.value.depot_code }} '
            '({{ var.value.depot_name }})"; '
            'echo "Дата (ds)  : {{ ds }}"; '
            'echo "Таймстамп  : {{ ts }}"; '
            'echo "Смена      : {{ shift_by_hour(ts) }}"; '
            'echo "run_id     : {{ run_id }}"; '
            'echo "Логич. дата: {{ logical_date.in_timezone(\'Asia/Novosibirsk\') }}"; '
            'echo "Парк (var) : {{ var.json.appendix1_fleet.electric | join(\', \') }}"; '
            'echo "Формат     : {{ params.report_format }}, парк {{ params.fleet_size }} ед."; '
            'echo "Вчера      : {{ macros.ds_add(ds, -1) }}"'
        ),
    )

    # --------------------------------------------------------------------- #
    #  ЗАДАЧА 2. Генерация телеметрии БЕЗ внешних источников                 #
    #  Детерминированно из ds_nodash — backfill воспроизводим.              #
    # --------------------------------------------------------------------- #
    @task(task_id="generate_telemetry")
    def generate_telemetry(**context) -> list[dict]:
        """Синтезирует суточную телеметрию парка локомотивов депо.

        Источник данных — НЕ файл и НЕ БД, а генератор со seed = дата запуска.
        Демонстрирует чтение Variable.get() с дефолтом (Модуль 1/4).
        """
        ds_nodash = context["ds_nodash"]            # YYYYMMDD
        seed = int(ds_nodash)
        rnd = random.Random(seed)                    # детерминированно

        fleet_cfg = Variable.get(
            "appendix1_fleet",
            default_var={"electric": [], "diesel": [], "emu": []},
            deserialize_json=True,
        )
        diesel = set(fleet_cfg.get("diesel", []))
        roster = (
            fleet_cfg.get("electric", [])
            + fleet_cfg.get("diesel", [])
            + fleet_cfg.get("emu", [])
        )

        rows: list[dict] = []
        for loco_id in roster:
            distance = rnd.randint(180, 520)         # пробег за сутки, км
            is_diesel = loco_id in diesel
            rows.append(
                {
                    "loco_id": loco_id,
                    "is_diesel": is_diesel,
                    "distance_km": distance,
                    # расход дизтоплива есть только у тепловозов
                    "fuel_l": round(distance * rnd.uniform(4.2, 6.1), 1)
                    if is_diesel
                    else 0.0,
                    "axlebox_temp_c": rnd.randint(45, 82),
                    "delay_min": rnd.choice([0, 0, 0, 5, 12, 18, 27, 41]),
                }
            )

        log.info("Сгенерирована телеметрия по %d локомотивам (seed=%d)", len(rows), seed)
        return rows

    telemetry = generate_telemetry()

    # --------------------------------------------------------------------- #
    #  ЗАДАЧИ 3a–3c. Параллельный анализ метрик — FAN-OUT (Модуль 6)        #
    #  Объединены в TaskGroup для наглядности графа.                        #
    # --------------------------------------------------------------------- #
    @task_group(group_id="analyze")
    def analyze_metrics(rows: list[dict]) -> dict:
        @task(task_id="fuel")
        def analyze_fuel(rows: list[dict]) -> dict:
            """Удельный расход топлива по тепловозам, л/км. Норма — из Variable."""
            norm = float(Variable.get("fuel_norm_lkm", default_var="5.0"))
            diesel = [r for r in rows if r["is_diesel"] and r["distance_km"] > 0]
            over = [
                r["loco_id"]
                for r in diesel
                if r["fuel_l"] / r["distance_km"] > norm
            ]
            avg = (
                round(sum(r["fuel_l"] / r["distance_km"] for r in diesel) / len(diesel), 2)
                if diesel
                else 0.0
            )
            log.info("Топливо: средний удельный расход %.2f л/км, норма %.1f", avg, norm)
            return {"metric": "fuel", "avg_lkm": avg, "over_norm": over}

        @task(task_id="axlebox_temp")
        def analyze_temp(rows: list[dict]) -> dict:
            """Перегрев букс: число локомотивов выше лимита из Variable."""
            limit = int(Variable.get("axlebox_temp_limit_c", default_var="70"))
            hot = [r["loco_id"] for r in rows if r["axlebox_temp_c"] > limit]
            max_t = max((r["axlebox_temp_c"] for r in rows), default=0)
            log.info("Буксы: %d перегретых (лимит %d°C), макс %d°C", len(hot), limit, max_t)
            return {"metric": "axlebox_temp", "limit_c": limit, "hot": hot, "max_c": max_t}

        @task(task_id="schedule")
        def analyze_schedule(rows: list[dict]) -> dict:
            """OTD депо: доля рейсов без опоздания сверх порога."""
            threshold = int(Variable.get("delay_threshold_min", default_var="15"))
            total = len(rows)
            on_time = sum(1 for r in rows if r["delay_min"] <= threshold)
            otd = round(on_time / total * 100, 1) if total else 100.0
            log.info("Расписание: OTD %.1f%% (порог %d мин)", otd, threshold)
            return {"metric": "schedule", "otd_pct": otd, "threshold_min": threshold}

        # fan-out внутри группы: три независимые задачи
        return {
            "fuel": analyze_fuel(rows),
            "axlebox_temp": analyze_temp(rows),
            "schedule": analyze_schedule(rows),
        }

    metrics = analyze_metrics(telemetry)

    # --------------------------------------------------------------------- #
    #  ЗАДАЧА 4. Сводный отчёт — FAN-IN (Модуль 6)                          #
    #  Зависит сразу от трёх задач анализа.                                 #
    # --------------------------------------------------------------------- #
    @task(task_id="build_report")
    def build_report(metrics: dict, **context) -> dict:
        fuel = metrics["fuel"]
        temp = metrics["axlebox_temp"]
        sched = metrics["schedule"]
        report = {
            "report_date": context["ds"],
            "depot": Variable.get("depot_code", default_var="TCH-15"),
            "avg_fuel_lkm": fuel["avg_lkm"],
            "fuel_over_norm": fuel["over_norm"],
            "axlebox_hot": temp["hot"],
            "axlebox_max_c": temp["max_c"],
            "otd_pct": sched["otd_pct"],
        }
        log.info("Сводный отчёт за %s: %s", report["report_date"], report)
        return report

    report = build_report(metrics)

    # --------------------------------------------------------------------- #
    #  ЗАДАЧА 5. Ветвление — BRANCH (Модуль 5/6)                            #
    #  Решает, нужно ли уведомлять диспетчера.                              #
    # --------------------------------------------------------------------- #
    @task.branch(task_id="route_by_alert")
    def route_by_alert(report: dict) -> str:
        otd_target = 85.0
        need_alert = (
            report["otd_pct"] < otd_target
            or bool(report["axlebox_hot"])
            or bool(report["fuel_over_norm"])
        )
        decision = "raise_alert" if need_alert else "no_alert"
        log.info("Ветвление → %s (OTD=%.1f%%)", decision, report["otd_pct"])
        return decision

    branch = route_by_alert(report)

    raise_alert = BashOperator(
        task_id="raise_alert",
        bash_command=(
            'echo "[ТЧЭ-15 ALERT] {{ ds }}: проверьте сводку — '
            'есть отклонения по OTD/буксам/топливу"'
        ),
    )
    no_alert = EmptyOperator(task_id="no_alert")

    # --------------------------------------------------------------------- #
    #  ЗАДАЧА 6. Финализация — TRIGGER RULE (Модуль 5)                      #
    #  Должна выполниться независимо от выбранной ветки.                    #
    # --------------------------------------------------------------------- #
    finalize = EmptyOperator(
        task_id="finalize",
        trigger_rule="none_failed_min_one_success",
    )

    end = EmptyOperator(task_id="end")

    # --------------------------------------------------------------------- #
    #  ПРОЕКТИРОВАНИЕ СВЯЗЕЙ МЕЖДУ ЗАДАЧАМИ (Модуль 5/6)                    #
    # --------------------------------------------------------------------- #
    # Линейный участок задаём явно через chain(): между print_context и
    # generate_telemetry нет передачи данных, поэтому связь нужна руками.
    chain(start, print_context, telemetry)

    # Дальше зависимости создаются автоматически передачей XCom:
    #   telemetry → analyze (fan-out) → build_report (fan-in) → branch
    # Остаётся развести ветвление и собрать поток обратно (join).
    branch >> [raise_alert, no_alert] >> finalize >> end
