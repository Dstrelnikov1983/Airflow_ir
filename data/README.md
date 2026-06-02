# Учебный набор данных — РЖД, Западно-Сибирская дирекция тяги

Курс: **Apache Airflow — Практикум по оркестрации аналитических данных**
Организация: Западно-Сибирская дирекция тяги (ТЧЭ-15 Новосибирск, ТЧЭ-16 Барнаул, ТЧЭ-17 Омск)

---

## Контекст задачи

Западно-Сибирская железная дорога внедряет аналитическую платформу для мониторинга
локомотивного парка. Система должна обеспечивать:

- соблюдение графика движения грузовых и пассажирских поездов;
- анализ расхода топлива и электроэнергии на поездо-км;
- контроль технического состояния локомотивов по данным датчиков;
- расчёт OTD (on-time delivery) для грузовых перевозок;
- планирование технического обслуживания (ТО-1, ТО-2, ТР-1, ТР-2, КР).

Apache Airflow используется для оркестрации ETL-пайплайнов, которые загружают
телеметрию и транзакционные данные в PostgreSQL-аналитическую схему `rzd_analytics`.

---

## Описание файлов

### 1. `locomotives.csv` — Реестр локомотивного парка

**Строк:** 25  
**Период:** актуальные данные

| Колонка | Тип | Описание |
|---|---|---|
| `loco_id` | STRING | Уникальный идентификатор (L001–L025) |
| `series` | STRING | Серия локомотива (ВЛ80С, ЭП2К, ЭП1М, 2ТЭ116, ЧМЭ3, ЭС2Г) |
| `number` | STRING | Бортовой (заводской) номер |
| `depot` | STRING | Депо приписки (ТЧЭ-15, ТЧЭ-16, ТЧЭ-17) |
| `loco_type` | STRING | Тип: грузовой / пассажирский / маневровый / пригородный |
| `traction_type` | STRING | Вид тяги: электрическая / дизельная |
| `year_built` | INT | Год постройки |
| `status` | STRING | Статус: в_работе / ТО / ТР / резерв |
| `max_speed_kmh` | INT | Конструкционная скорость, км/ч |
| `power_kw` | INT | Мощность, кВт |
| `fuel_tank_l` | INT | Объём топливного бака, л (0 для электровозов) |

**Состав парка:** ВЛ80С × 8, ЭП2К × 4, ЭП1М × 3, 2ТЭ116 × 5, ЧМЭ3 × 3, ЭС2Г × 2.

---

### 2. `sensor_readings.csv` — Телеметрия с датчиков

**Строк:** 300  
**Период:** 2024-03-01 00:00:00 — 2024-03-07 23:00:00

| Колонка | Тип | Описание |
|---|---|---|
| `reading_id` | STRING | Идентификатор записи (SR001–SR300) |
| `loco_id` | STRING | FK → locomotives |
| `timestamp` | DATETIME | Метка времени снятия показания |
| `sensor_type` | STRING | Тип датчика |
| `value` | FLOAT | Числовое значение |
| `unit` | STRING | Единица измерения |
| `quality_flag` | INT | 1 = корректно, 0 = сбой (~5% записей) |

**Типы датчиков:**

| sensor_type | Диапазон | Единица | Применение |
|---|---|---|---|
| `speed_kmh` | 0–110 (грузовые), 0–160 (пасс.) | км/ч | Все локомотивы |
| `buxa_temp_c` | 20–95 (>80 = тревога) | °C | Все локомотивы |
| `traction_current_a` | 0–800 | А | Электровозы |
| `catenary_voltage_v` | 2400–4000 | В | Электровозы |
| `fuel_consumption_lh` | 0–180 | л/ч | Тепловозы |
| `engine_hours` | накопительный | ч | Тепловозы |

---

### 3. `trips.csv` — Журнал рейсов

**Строк:** 80  
**Период:** 2024-03-01 — 2024-03-07

| Колонка | Тип | Описание |
|---|---|---|
| `trip_id` | STRING | Идентификатор рейса (TR001–TR080) |
| `loco_id` | STRING | FK → locomotives |
| `train_number` | STRING | Номер поезда по расписанию |
| `route_from` | STRING | Станция отправления |
| `route_to` | STRING | Станция назначения |
| `departure_plan` | DATETIME | Плановое отправление |
| `departure_fact` | DATETIME | Фактическое отправление |
| `arrival_plan` | DATETIME | Плановое прибытие |
| `arrival_fact` | DATETIME | Фактическое прибытие |
| `delay_min` | INT | Опоздание прибытия, мин |
| `cargo_type` | STRING | Тип груза / пассажирский |
| `gross_weight_tons` | INT | Масса поезда брутто, т |
| `distance_km` | INT | Длина маршрута, км |
| `status` | STRING | выполнен / в_пути / отменён |

**Маршруты:**

| Маршрут | Расстояние |
|---|---|
| Новосибирск — Омск | 642 км |
| Новосибирск — Барнаул | 225 км |
| Новосибирск — Красноярск | 836 км |
| Новосибирск — Кемерово | 307 км |
| Омск — Тюмень | 643 км |
| Новосибирск — Бердск | 54 км |

**Распределение:** 70% рейсов с опозданием ≤ 10 мин; 5% — задержка > 60 мин.

---

### 4. `maintenance.csv` — Техническое обслуживание и ремонт

**Строк:** 40

| Колонка | Тип | Описание |
|---|---|---|
| `maint_id` | STRING | Идентификатор записи (MR001–MR040) |
| `loco_id` | STRING | FK → locomotives |
| `maint_type` | STRING | Вид ТО: ТО-1, ТО-2, ТО-3, ТР-1, ТР-2, КР |
| `start_datetime` | DATETIME | Начало обслуживания |
| `end_datetime` | DATETIME | Окончание (NULL — в процессе) |
| `depot` | STRING | Депо, выполнявшее ТО |
| `technician_id` | STRING | Идентификатор техника/бригадира |
| `cost_rub` | INT | Стоимость, руб. |
| `planned` | BOOLEAN | true — плановое, false — внеплановое |
| `description` | STRING | Описание выполненных работ |

**Нормативные стоимости и сроки:**

| Вид ТО | Длительность | Стоимость |
|---|---|---|
| ТО-1 | 1–2 ч | 5 000 руб. |
| ТО-2 | 4–8 ч | 15 000 руб. |
| ТО-3 | 12–24 ч | 35 000 руб. |
| ТР-1 | 2–3 дня | 150 000 руб. |
| ТР-2 | 5–7 дней | 800 000 руб. |
| КР | 30–45 дней | 5 000 000 руб. |

---

### 5. `schedule_adherence.csv` — Соблюдение графика движения

**Строк:** 100  
**Период:** 2024-03-01 — 2024-03-07

| Колонка | Тип | Описание |
|---|---|---|
| `record_id` | STRING | Идентификатор записи (SA001–SA100) |
| `date` | DATE | Дата выполнения |
| `train_number` | STRING | Номер поезда |
| `section` | STRING | Участок следования |
| `planned_time_min` | INT | Плановое время, мин |
| `actual_time_min` | INT | Фактическое время, мин |
| `delay_min` | INT | Опоздание на участке, мин |
| `cause_code` | STRING | Код причины (01–06) |
| `cause_description` | STRING | Описание причины |
| `shift_number` | INT | Номер смены (1, 2, 3) |

**Коды причин опоздания:**

| Код | Описание |
|---|---|
| 01 | Нет причин (в графике) |
| 02 | Неисправность локомотива |
| 03 | Технологические работы |
| 04 | Перегруженность участка |
| 05 | Неблагоприятные погодные условия |
| 06 | Неисправность инфраструктуры |

**Участки:** Новосибирск–Барабинск, Барабинск–Омск, Омск–Тюмень, Новосибирск–Бердск, Новосибирск–Инская.

---

### 6. `create_tables.sql` — DDL-скрипт PostgreSQL

Создаёт схему `rzd_analytics` и 5 таблиц с полными ограничениями целостности.

**Содержимое:**
- `CREATE SCHEMA IF NOT EXISTS rzd_analytics`
- Таблицы: `locomotives`, `sensor_readings`, `trips`, `maintenance`, `schedule_adherence`
- PRIMARY KEY на всех таблицах
- FOREIGN KEY: `loco_id → locomotives` в четырёх зависимых таблицах
- CHECK constraints: скорость 0–350, температура буксы, статусы перечислением
- INDEX на `timestamp`, `loco_id`, `trip_id`, `section`, `cause_code`
- COMMENT ON TABLE и COMMENT ON COLUMN на русском языке
- VIEW `rzd_analytics.locomotive_kpi` — расчёт KPI: пробег, OTD%, доступность, расход топлива, перегрев буксы

---

## Связи между таблицами

```
locomotives (loco_id)
    │
    ├──< sensor_readings (loco_id) — телеметрия датчиков
    │
    ├──< trips (loco_id) — журнал рейсов
    │
    └──< maintenance (loco_id) — журнал ТО и ремонтов

trips (train_number) ──< schedule_adherence (train_number)
```

**ER-связи:**
- `locomotives` 1:N `sensor_readings` — один локомотив, много показаний датчиков
- `locomotives` 1:N `trips` — один локомотив, много рейсов
- `locomotives` 1:N `maintenance` — один локомотив, много записей ТО
- `trips.train_number` — `schedule_adherence.train_number` (логическая связь, без FK)

---

## Использование в практических работах

### Модуль 1. Введение в Airflow. Первый DAG

Задача: загрузить `locomotives.csv` в PostgreSQL-таблицу `rzd_analytics.locomotives`.

```python
# Пример оператора в DAG
load_locomotives = PythonOperator(
    task_id='load_locomotives',
    python_callable=load_csv_to_postgres,
    op_kwargs={
        'csv_path': '/data/locomotives.csv',
        'table': 'rzd_analytics.locomotives'
    }
)
```

### Модуль 2. Зависимости и сенсоры

Задача: дождаться появления нового файла `sensor_readings_<date>.csv`
(FileSensor), затем загрузить его в `sensor_readings`.

### Модуль 3. Работа с PostgreSQL

Задача: загрузить все CSV, выполнить агрегации через `PostgresOperator`,
построить ежедневный отчёт по OTD.

### Модуль 4. Параметризация и XCom

Задача: передавать дату отчётного периода через `dag_run.conf`,
вычислять количество рейсов с опозданием > 30 мин и передавать
результат через XCom в следующую задачу (отправка алерта).

### Модуль 5. Обработка ошибок и качество данных

Задача: фильтровать записи `sensor_readings` с `quality_flag = 0`,
логировать аномалии (`buxa_temp_c > 80`) и направлять их в отдельную
таблицу-карантин.

### Модуль 6. Планирование и мониторинг

Задача: настроить расписание DAG на ежедневный запуск в 03:00 МСК,
отслеживать среднее опоздание по участкам за скользящие 7 дней,
визуализировать KPI через VIEW `locomotive_kpi`.

---

## Быстрый старт — загрузка данных в PostgreSQL

```sql
-- 1. Создать схему и таблицы
\i create_tables.sql

-- 2. Загрузить данные (пример для psql)
\copy rzd_analytics.locomotives
    FROM 'locomotives.csv' CSV HEADER;

\copy rzd_analytics.sensor_readings (reading_id, loco_id, ts, sensor_type, value, unit, quality_flag)
    FROM 'sensor_readings.csv' CSV HEADER;

\copy rzd_analytics.trips
    FROM 'trips.csv' CSV HEADER;

\copy rzd_analytics.maintenance
    FROM 'maintenance.csv' CSV HEADER;

\copy rzd_analytics.schedule_adherence
    FROM 'schedule_adherence.csv' CSV HEADER;

-- 3. Проверить KPI
SELECT loco_id, series, otd_pct, avg_delay_min, max_buxa_temp_c
FROM rzd_analytics.locomotive_kpi
ORDER BY otd_pct NULLS LAST;
```

---

## Аналитические запросы для практики

```sql
-- Топ-5 локомотивов по количеству опозданий > 30 мин
SELECT
    t.loco_id,
    l.series,
    COUNT(*) AS delays_over_30min
FROM rzd_analytics.trips t
JOIN rzd_analytics.locomotives l USING (loco_id)
WHERE t.delay_min > 30
GROUP BY t.loco_id, l.series
ORDER BY delays_over_30min DESC
LIMIT 5;

-- Суточный OTD (On-Time Delivery) по дням недели
SELECT
    DATE_TRUNC('day', departure_plan)           AS day,
    COUNT(*)                                    AS total_trips,
    SUM(CASE WHEN delay_min <= 10 THEN 1 END)   AS on_time,
    ROUND(
        SUM(CASE WHEN delay_min <= 10 THEN 1 END)::NUMERIC
        / COUNT(*) * 100, 1
    )                                           AS otd_pct
FROM rzd_analytics.trips
WHERE status = 'выполнен'
GROUP BY 1
ORDER BY 1;

-- Перегревы буксы (>80°C) с привязкой к типу локомотива
SELECT
    sr.loco_id,
    l.series,
    l.depot,
    sr.ts,
    sr.value AS buxa_temp_c
FROM rzd_analytics.sensor_readings sr
JOIN rzd_analytics.locomotives l USING (loco_id)
WHERE sr.sensor_type = 'buxa_temp_c'
  AND sr.value > 80
  AND sr.quality_flag = 1
ORDER BY sr.value DESC;

-- Средний расход топлива тепловозов по депо
SELECT
    l.depot,
    l.series,
    ROUND(AVG(sr.value), 1) AS avg_fuel_lh
FROM rzd_analytics.sensor_readings sr
JOIN rzd_analytics.locomotives l USING (loco_id)
WHERE sr.sensor_type = 'fuel_consumption_lh'
  AND sr.quality_flag = 1
  AND sr.value > 0
GROUP BY l.depot, l.series
ORDER BY l.depot, avg_fuel_lh DESC;
```

---

*Набор данных разработан для учебного курса Apache Airflow.*
*Все данные являются синтетическими и созданы в образовательных целях.*
