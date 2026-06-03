"""
Приложение 1. Учебный DAG по РАСПИСАНИЯМ (Модуль 3) — без внешних данных.

Один лёгкий DAG, который ничего не считает, а только печатает контекст
интервала данных. На нём удобно увидеть разницу между понятиями:
  • logical_date / execution_date — «логическая» метка запуска;
  • data_interval_start / data_interval_end — границы обрабатываемого интервала;
  • prev_* / next_* — соседние интервалы;
  • влияние часового пояса Asia/Novosibirsk на cron;
  • поведение catchup=True при backfill.

Упражнение: студент по очереди подставляет варианты SCHEDULE из списка ниже,
передеплоивает DAG и наблюдает, как меняются интервалы и число пропущенных run-ов.

Среда: Yandex Managed Service for Apache Airflow™.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum

from airflow.decorators import task
from airflow.models.dag import DAG

log = logging.getLogger(__name__)

NSK_TZ = pendulum.timezone("Asia/Novosibirsk")

# --------------------------------------------------------------------------- #
#  ВАРИАНТЫ РАСПИСАНИЯ (Модуль 3). Раскомментируйте ОДИН и передеплойте DAG.   #
# --------------------------------------------------------------------------- #
SCHEDULE = "0 6 * * *"            # каждый день в 06:00 (время депо)
# SCHEDULE = "@hourly"            # пресет: каждый час
# SCHEDULE = "0 */4 * * *"        # каждые 4 часа
# SCHEDULE = "0 8 * * 1-5"        # будни в 08:00
# SCHEDULE = timedelta(hours=12)  # интервалом, а не cron-выражением
# SCHEDULE = "0 0,8,16 * * *"     # по сменам: 00:00 / 08:00 / 16:00
# SCHEDULE = None                 # только ручной запуск (Trigger DAG)


with DAG(
    dag_id="appendix1_schedule_lab",
    description="Приложение 1 — наблюдение интервалов данных и cron (Модуль 3)",
    schedule=SCHEDULE,
    start_date=pendulum.datetime(2024, 3, 1, tz=NSK_TZ),
    catchup=True,                  # поставьте False и сравните поведение
    max_active_runs=1,
    tags=["appendix1", "rzd", "schedule", "module3"],
) as dag:

    @task(task_id="print_interval")
    def print_interval(**context) -> dict:
        """Печатает все ключевые поля интервала данных текущего run-а."""
        info = {
            "ds": context["ds"],
            "logical_date_nsk": str(
                context["logical_date"].in_timezone(NSK_TZ)
            ),
            "data_interval_start_nsk": str(
                context["data_interval_start"].in_timezone(NSK_TZ)
            ),
            "data_interval_end_nsk": str(
                context["data_interval_end"].in_timezone(NSK_TZ)
            ),
            "prev_data_interval_start": str(
                context.get("prev_data_interval_start_success")
            ),
            "run_id": context["run_id"],
        }
        for key, value in info.items():
            log.info("%-26s = %s", key, value)
        return info

    print_interval()
