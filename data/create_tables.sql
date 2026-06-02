-- =============================================================================
-- DDL-скрипт для аналитической платформы РЖД
-- Западно-Сибирская дирекция тяги (ТЧЭ-15, ТЧЭ-16, ТЧЭ-17)
-- База данных: PostgreSQL 15+
-- Схема: rzd_analytics
-- Описание: Таблицы для хранения данных о локомотивном парке,
--           показаниях датчиков, рейсах, техническом обслуживании
--           и соблюдении графика движения.
-- Версия: 1.0  Дата: 2024-03-01
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS rzd_analytics;

COMMENT ON SCHEMA rzd_analytics IS
    'Аналитическая схема данных Западно-Сибирской дирекции тяги РЖД';

-- =============================================================================
-- ТАБЛИЦА: locomotives — реестр локомотивного парка
-- =============================================================================
CREATE TABLE IF NOT EXISTS rzd_analytics.locomotives (
    loco_id         VARCHAR(10)     NOT NULL,
    series          VARCHAR(20)     NOT NULL,
    number          VARCHAR(10)     NOT NULL,
    depot           VARCHAR(30)     NOT NULL,
    loco_type       VARCHAR(20)     NOT NULL,
    traction_type   VARCHAR(20)     NOT NULL,
    year_built      SMALLINT        NOT NULL,
    status          VARCHAR(20)     NOT NULL,
    max_speed_kmh   SMALLINT        NOT NULL,
    power_kw        INTEGER         NOT NULL,
    fuel_tank_l     INTEGER         NOT NULL DEFAULT 0,

    CONSTRAINT pk_locomotives
        PRIMARY KEY (loco_id),

    CONSTRAINT chk_loco_max_speed
        CHECK (max_speed_kmh BETWEEN 0 AND 350),

    CONSTRAINT chk_loco_year
        CHECK (year_built BETWEEN 1950 AND 2030),

    CONSTRAINT chk_loco_power
        CHECK (power_kw > 0),

    CONSTRAINT chk_loco_status
        CHECK (status IN ('в_работе', 'ТО', 'ТР', 'резерв', 'КР', 'списан')),

    CONSTRAINT chk_loco_type
        CHECK (loco_type IN ('грузовой', 'пассажирский', 'маневровый', 'пригородный')),

    CONSTRAINT chk_loco_traction
        CHECK (traction_type IN ('электрическая', 'дизельная'))
);

COMMENT ON TABLE rzd_analytics.locomotives IS
    'Реестр локомотивного парка Западно-Сибирской дирекции тяги';
COMMENT ON COLUMN rzd_analytics.locomotives.loco_id IS
    'Уникальный идентификатор локомотива (например, L001)';
COMMENT ON COLUMN rzd_analytics.locomotives.series IS
    'Серия локомотива (ВЛ80С, ЭП2К, 2ТЭ116, ЧМЭ3 и т.д.)';
COMMENT ON COLUMN rzd_analytics.locomotives.number IS
    'Заводской (бортовой) номер локомотива';
COMMENT ON COLUMN rzd_analytics.locomotives.depot IS
    'Депо приписки (ТЧЭ-15, ТЧЭ-16, ТЧЭ-17)';
COMMENT ON COLUMN rzd_analytics.locomotives.loco_type IS
    'Тип локомотива: грузовой, пассажирский, маневровый, пригородный';
COMMENT ON COLUMN rzd_analytics.locomotives.traction_type IS
    'Вид тяги: электрическая или дизельная';
COMMENT ON COLUMN rzd_analytics.locomotives.year_built IS
    'Год постройки локомотива';
COMMENT ON COLUMN rzd_analytics.locomotives.status IS
    'Текущий статус: в_работе, ТО, ТР, резерв, КР, списан';
COMMENT ON COLUMN rzd_analytics.locomotives.max_speed_kmh IS
    'Максимально допустимая конструкционная скорость, км/ч';
COMMENT ON COLUMN rzd_analytics.locomotives.power_kw IS
    'Мощность локомотива, кВт';
COMMENT ON COLUMN rzd_analytics.locomotives.fuel_tank_l IS
    'Объём топливного бака, л (0 для электровозов)';

CREATE INDEX IF NOT EXISTS idx_locomotives_depot
    ON rzd_analytics.locomotives (depot);

CREATE INDEX IF NOT EXISTS idx_locomotives_status
    ON rzd_analytics.locomotives (status);

CREATE INDEX IF NOT EXISTS idx_locomotives_series
    ON rzd_analytics.locomotives (series);


-- =============================================================================
-- ТАБЛИЦА: sensor_readings — телеметрия с датчиков локомотивов
-- =============================================================================
CREATE TABLE IF NOT EXISTS rzd_analytics.sensor_readings (
    reading_id      VARCHAR(10)     NOT NULL,
    loco_id         VARCHAR(10)     NOT NULL,
    ts              TIMESTAMP       NOT NULL,
    sensor_type     VARCHAR(30)     NOT NULL,
    value           NUMERIC(10, 2)  NOT NULL,
    unit            VARCHAR(15)     NOT NULL,
    quality_flag    SMALLINT        NOT NULL DEFAULT 1,

    CONSTRAINT pk_sensor_readings
        PRIMARY KEY (reading_id),

    CONSTRAINT fk_sensor_loco
        FOREIGN KEY (loco_id)
        REFERENCES rzd_analytics.locomotives (loco_id)
        ON DELETE RESTRICT ON UPDATE CASCADE,

    CONSTRAINT chk_sensor_quality
        CHECK (quality_flag IN (0, 1)),

    CONSTRAINT chk_sensor_type
        CHECK (sensor_type IN (
            'speed_kmh',
            'buxa_temp_c',
            'traction_current_a',
            'catenary_voltage_v',
            'fuel_consumption_lh',
            'engine_hours'
        ))
);

COMMENT ON TABLE rzd_analytics.sensor_readings IS
    'Показания телеметрических датчиков локомотивов';
COMMENT ON COLUMN rzd_analytics.sensor_readings.reading_id IS
    'Уникальный идентификатор записи показания';
COMMENT ON COLUMN rzd_analytics.sensor_readings.loco_id IS
    'Идентификатор локомотива (FK → locomotives)';
COMMENT ON COLUMN rzd_analytics.sensor_readings.ts IS
    'Метка времени снятия показания (UTC+7 Новосибирск)';
COMMENT ON COLUMN rzd_analytics.sensor_readings.sensor_type IS
    'Тип датчика: speed_kmh, buxa_temp_c, traction_current_a и т.д.';
COMMENT ON COLUMN rzd_analytics.sensor_readings.value IS
    'Числовое значение показания датчика';
COMMENT ON COLUMN rzd_analytics.sensor_readings.unit IS
    'Единица измерения (км/ч, °C, А, В, л/ч, ч)';
COMMENT ON COLUMN rzd_analytics.sensor_readings.quality_flag IS
    'Флаг качества: 1 = данные корректны, 0 = сбой / некорректные данные';

CREATE INDEX IF NOT EXISTS idx_sensor_loco_ts
    ON rzd_analytics.sensor_readings (loco_id, ts);

CREATE INDEX IF NOT EXISTS idx_sensor_ts
    ON rzd_analytics.sensor_readings (ts);

CREATE INDEX IF NOT EXISTS idx_sensor_type
    ON rzd_analytics.sensor_readings (sensor_type);


-- =============================================================================
-- ТАБЛИЦА: trips — рейсы (поездки) локомотивов
-- =============================================================================
CREATE TABLE IF NOT EXISTS rzd_analytics.trips (
    trip_id             VARCHAR(10)     NOT NULL,
    loco_id             VARCHAR(10)     NOT NULL,
    train_number        VARCHAR(10)     NOT NULL,
    route_from          VARCHAR(50)     NOT NULL,
    route_to            VARCHAR(50)     NOT NULL,
    departure_plan      TIMESTAMP       NOT NULL,
    departure_fact      TIMESTAMP,
    arrival_plan        TIMESTAMP       NOT NULL,
    arrival_fact        TIMESTAMP,
    delay_min           SMALLINT        NOT NULL DEFAULT 0,
    cargo_type          VARCHAR(30),
    gross_weight_tons   INTEGER,
    distance_km         SMALLINT        NOT NULL,
    status              VARCHAR(20)     NOT NULL,

    CONSTRAINT pk_trips
        PRIMARY KEY (trip_id),

    CONSTRAINT fk_trips_loco
        FOREIGN KEY (loco_id)
        REFERENCES rzd_analytics.locomotives (loco_id)
        ON DELETE RESTRICT ON UPDATE CASCADE,

    CONSTRAINT chk_trip_delay
        CHECK (delay_min >= 0),

    CONSTRAINT chk_trip_distance
        CHECK (distance_km > 0),

    CONSTRAINT chk_trip_weight
        CHECK (gross_weight_tons IS NULL OR gross_weight_tons >= 0),

    CONSTRAINT chk_trip_status
        CHECK (status IN ('выполнен', 'в_пути', 'отменён', 'задержан')),

    CONSTRAINT chk_trip_cargo
        CHECK (cargo_type IN (
            'уголь', 'нефтепродукты', 'металлы', 'зерно',
            'контейнеры', 'пассажирский', NULL
        ))
);

COMMENT ON TABLE rzd_analytics.trips IS
    'Журнал рейсов (поездок) локомотивов по маршрутам дирекции';
COMMENT ON COLUMN rzd_analytics.trips.trip_id IS
    'Уникальный идентификатор рейса';
COMMENT ON COLUMN rzd_analytics.trips.loco_id IS
    'Идентификатор локомотива (FK → locomotives)';
COMMENT ON COLUMN rzd_analytics.trips.train_number IS
    'Номер поезда по расписанию РЖД';
COMMENT ON COLUMN rzd_analytics.trips.route_from IS
    'Станция отправления';
COMMENT ON COLUMN rzd_analytics.trips.route_to IS
    'Станция назначения';
COMMENT ON COLUMN rzd_analytics.trips.departure_plan IS
    'Плановое время отправления';
COMMENT ON COLUMN rzd_analytics.trips.departure_fact IS
    'Фактическое время отправления';
COMMENT ON COLUMN rzd_analytics.trips.arrival_plan IS
    'Плановое время прибытия';
COMMENT ON COLUMN rzd_analytics.trips.arrival_fact IS
    'Фактическое время прибытия';
COMMENT ON COLUMN rzd_analytics.trips.delay_min IS
    'Опоздание прибытия в минутах (0 = без опоздания)';
COMMENT ON COLUMN rzd_analytics.trips.cargo_type IS
    'Тип перевозимого груза или пассажирский рейс';
COMMENT ON COLUMN rzd_analytics.trips.gross_weight_tons IS
    'Масса поезда брутто, тонн (0 для пассажирских)';
COMMENT ON COLUMN rzd_analytics.trips.distance_km IS
    'Протяжённость маршрута, км';
COMMENT ON COLUMN rzd_analytics.trips.status IS
    'Статус рейса: выполнен, в_пути, отменён, задержан';

CREATE INDEX IF NOT EXISTS idx_trips_loco_id
    ON rzd_analytics.trips (loco_id);

CREATE INDEX IF NOT EXISTS idx_trips_departure_plan
    ON rzd_analytics.trips (departure_plan);

CREATE INDEX IF NOT EXISTS idx_trips_status
    ON rzd_analytics.trips (status);

CREATE INDEX IF NOT EXISTS idx_trips_train_number
    ON rzd_analytics.trips (train_number);


-- =============================================================================
-- ТАБЛИЦА: maintenance — техническое обслуживание и ремонт
-- =============================================================================
CREATE TABLE IF NOT EXISTS rzd_analytics.maintenance (
    maint_id        VARCHAR(10)     NOT NULL,
    loco_id         VARCHAR(10)     NOT NULL,
    maint_type      VARCHAR(10)     NOT NULL,
    start_datetime  TIMESTAMP       NOT NULL,
    end_datetime    TIMESTAMP,
    depot           VARCHAR(30)     NOT NULL,
    technician_id   VARCHAR(10)     NOT NULL,
    cost_rub        INTEGER         NOT NULL,
    planned         BOOLEAN         NOT NULL DEFAULT TRUE,
    description     TEXT,

    CONSTRAINT pk_maintenance
        PRIMARY KEY (maint_id),

    CONSTRAINT fk_maint_loco
        FOREIGN KEY (loco_id)
        REFERENCES rzd_analytics.locomotives (loco_id)
        ON DELETE RESTRICT ON UPDATE CASCADE,

    CONSTRAINT chk_maint_type
        CHECK (maint_type IN ('ТО-1', 'ТО-2', 'ТО-3', 'ТР-1', 'ТР-2', 'КР')),

    CONSTRAINT chk_maint_cost
        CHECK (cost_rub >= 0),

    CONSTRAINT chk_maint_dates
        CHECK (end_datetime IS NULL OR end_datetime >= start_datetime)
);

COMMENT ON TABLE rzd_analytics.maintenance IS
    'Журнал технического обслуживания и ремонта локомотивов';
COMMENT ON COLUMN rzd_analytics.maintenance.maint_id IS
    'Уникальный идентификатор записи ТО/ТР';
COMMENT ON COLUMN rzd_analytics.maintenance.loco_id IS
    'Идентификатор локомотива (FK → locomotives)';
COMMENT ON COLUMN rzd_analytics.maintenance.maint_type IS
    'Вид ТО/ТР: ТО-1, ТО-2, ТО-3, ТР-1, ТР-2, КР';
COMMENT ON COLUMN rzd_analytics.maintenance.start_datetime IS
    'Дата и время начала технического обслуживания';
COMMENT ON COLUMN rzd_analytics.maintenance.end_datetime IS
    'Дата и время окончания (NULL если ещё в процессе)';
COMMENT ON COLUMN rzd_analytics.maintenance.depot IS
    'Депо, выполнявшее техническое обслуживание';
COMMENT ON COLUMN rzd_analytics.maintenance.technician_id IS
    'Идентификатор бригадира / техника';
COMMENT ON COLUMN rzd_analytics.maintenance.cost_rub IS
    'Стоимость технического обслуживания, рублей';
COMMENT ON COLUMN rzd_analytics.maintenance.planned IS
    'true — плановое ТО, false — внеплановое (аварийное)';
COMMENT ON COLUMN rzd_analytics.maintenance.description IS
    'Описание выполненных работ';

CREATE INDEX IF NOT EXISTS idx_maint_loco_id
    ON rzd_analytics.maintenance (loco_id);

CREATE INDEX IF NOT EXISTS idx_maint_start
    ON rzd_analytics.maintenance (start_datetime);

CREATE INDEX IF NOT EXISTS idx_maint_type
    ON rzd_analytics.maintenance (maint_type);


-- =============================================================================
-- ТАБЛИЦА: schedule_adherence — соблюдение графика движения
-- =============================================================================
CREATE TABLE IF NOT EXISTS rzd_analytics.schedule_adherence (
    record_id           VARCHAR(10)     NOT NULL,
    date                DATE            NOT NULL,
    train_number        VARCHAR(10)     NOT NULL,
    section             VARCHAR(60)     NOT NULL,
    planned_time_min    SMALLINT        NOT NULL,
    actual_time_min     SMALLINT        NOT NULL,
    delay_min           SMALLINT        NOT NULL DEFAULT 0,
    cause_code          CHAR(2)         NOT NULL,
    cause_description   VARCHAR(100)    NOT NULL,
    shift_number        SMALLINT        NOT NULL,

    CONSTRAINT pk_schedule_adherence
        PRIMARY KEY (record_id),

    CONSTRAINT chk_sa_planned_time
        CHECK (planned_time_min > 0),

    CONSTRAINT chk_sa_actual_time
        CHECK (actual_time_min > 0),

    CONSTRAINT chk_sa_delay
        CHECK (delay_min >= 0),

    CONSTRAINT chk_sa_cause_code
        CHECK (cause_code IN ('01', '02', '03', '04', '05', '06')),

    CONSTRAINT chk_sa_shift
        CHECK (shift_number BETWEEN 1 AND 3)
);

COMMENT ON TABLE rzd_analytics.schedule_adherence IS
    'Данные о соблюдении графика движения поездов по участкам';
COMMENT ON COLUMN rzd_analytics.schedule_adherence.record_id IS
    'Уникальный идентификатор записи';
COMMENT ON COLUMN rzd_analytics.schedule_adherence.date IS
    'Дата выполнения поездки';
COMMENT ON COLUMN rzd_analytics.schedule_adherence.train_number IS
    'Номер поезда';
COMMENT ON COLUMN rzd_analytics.schedule_adherence.section IS
    'Участок следования (например, Новосибирск-Барабинск)';
COMMENT ON COLUMN rzd_analytics.schedule_adherence.planned_time_min IS
    'Плановое время хода по участку, минут';
COMMENT ON COLUMN rzd_analytics.schedule_adherence.actual_time_min IS
    'Фактическое время хода по участку, минут';
COMMENT ON COLUMN rzd_analytics.schedule_adherence.delay_min IS
    'Опоздание на данном участке, минут';
COMMENT ON COLUMN rzd_analytics.schedule_adherence.cause_code IS
    'Код причины опоздания: 01–нет, 02–локомотив, 03–работы, 04–перегруз, 05–погода, 06–инфраструктура';
COMMENT ON COLUMN rzd_analytics.schedule_adherence.cause_description IS
    'Текстовое описание причины опоздания';
COMMENT ON COLUMN rzd_analytics.schedule_adherence.shift_number IS
    'Номер смены: 1, 2 или 3';

CREATE INDEX IF NOT EXISTS idx_sa_date
    ON rzd_analytics.schedule_adherence (date);

CREATE INDEX IF NOT EXISTS idx_sa_train_number
    ON rzd_analytics.schedule_adherence (train_number);

CREATE INDEX IF NOT EXISTS idx_sa_section
    ON rzd_analytics.schedule_adherence (section);

CREATE INDEX IF NOT EXISTS idx_sa_cause_code
    ON rzd_analytics.schedule_adherence (cause_code);


-- =============================================================================
-- VIEW: locomotive_kpi — ключевые показатели эффективности локомотивов
-- =============================================================================
CREATE OR REPLACE VIEW rzd_analytics.locomotive_kpi AS
WITH

-- Суммарный пробег (км) за все рейсы в статусе "выполнен"
mileage AS (
    SELECT
        loco_id,
        SUM(distance_km)                        AS total_distance_km,
        COUNT(*)                                AS total_trips,
        SUM(CASE WHEN delay_min <= 10 THEN 1 ELSE 0 END) AS on_time_trips,
        AVG(delay_min)                          AS avg_delay_min,
        SUM(gross_weight_tons)                  AS total_cargo_tons
    FROM rzd_analytics.trips
    WHERE status = 'выполнен'
    GROUP BY loco_id
),

-- Время в плановом ТО/ТР (часов)
maint_hours AS (
    SELECT
        loco_id,
        SUM(
            EXTRACT(EPOCH FROM (
                COALESCE(end_datetime, NOW()) - start_datetime
            )) / 3600.0
        )                                       AS total_maint_hours,
        COUNT(*)                                AS total_maint_events,
        SUM(cost_rub)                           AS total_maint_cost_rub,
        SUM(CASE WHEN planned = false THEN 1 ELSE 0 END) AS unplanned_events
    FROM rzd_analytics.maintenance
    GROUP BY loco_id
),

-- Средний расход топлива (только для тепловозов, датчик fuel_consumption_lh)
fuel_avg AS (
    SELECT
        loco_id,
        AVG(value)                              AS avg_fuel_consumption_lh,
        MAX(value)                              AS max_fuel_consumption_lh
    FROM rzd_analytics.sensor_readings
    WHERE sensor_type = 'fuel_consumption_lh'
      AND quality_flag = 1
      AND value > 0
    GROUP BY loco_id
),

-- Максимальная зафиксированная температура буксы
buxa_max AS (
    SELECT
        loco_id,
        MAX(value)                              AS max_buxa_temp_c,
        COUNT(CASE WHEN value > 80 THEN 1 END)  AS buxa_overheat_events
    FROM rzd_analytics.sensor_readings
    WHERE sensor_type = 'buxa_temp_c'
      AND quality_flag = 1
    GROUP BY loco_id
)

SELECT
    l.loco_id,
    l.series,
    l.number,
    l.depot,
    l.loco_type,
    l.traction_type,
    l.status,

    -- Пробег и рейсы
    COALESCE(m.total_distance_km, 0)            AS total_distance_km,
    COALESCE(m.total_trips, 0)                  AS total_trips,

    -- OTD (On-Time Delivery) — доля рейсов с опозданием ≤ 10 мин
    CASE
        WHEN COALESCE(m.total_trips, 0) = 0 THEN NULL
        ELSE ROUND(
            m.on_time_trips::NUMERIC / m.total_trips * 100, 1
        )
    END                                         AS otd_pct,

    ROUND(COALESCE(m.avg_delay_min, 0), 1)      AS avg_delay_min,
    COALESCE(m.total_cargo_tons, 0)             AS total_cargo_tons,

    -- Техническое обслуживание
    ROUND(COALESCE(mh.total_maint_hours, 0)::NUMERIC, 1) AS total_maint_hours,
    COALESCE(mh.total_maint_events, 0)          AS total_maint_events,
    COALESCE(mh.unplanned_events, 0)            AS unplanned_maint_events,
    COALESCE(mh.total_maint_cost_rub, 0)        AS total_maint_cost_rub,

    -- Процент времени в работе (приблизительно, за 7 суток = 168 ч)
    CASE
        WHEN COALESCE(mh.total_maint_hours, 0) = 0 THEN NULL
        ELSE ROUND(
            (1 - mh.total_maint_hours / 168.0) * 100, 1
        )
    END                                         AS availability_pct,

    -- Топливо (только тепловозы)
    ROUND(COALESCE(f.avg_fuel_consumption_lh, 0)::NUMERIC, 1) AS avg_fuel_lh,
    ROUND(COALESCE(f.max_fuel_consumption_lh, 0)::NUMERIC, 1) AS max_fuel_lh,

    -- Температура буксы
    COALESCE(b.max_buxa_temp_c, 0)              AS max_buxa_temp_c,
    COALESCE(b.buxa_overheat_events, 0)         AS buxa_overheat_events

FROM rzd_analytics.locomotives l
LEFT JOIN mileage        m  ON m.loco_id  = l.loco_id
LEFT JOIN maint_hours    mh ON mh.loco_id = l.loco_id
LEFT JOIN fuel_avg       f  ON f.loco_id  = l.loco_id
LEFT JOIN buxa_max       b  ON b.loco_id  = l.loco_id
ORDER BY l.depot, l.series, l.number;

COMMENT ON VIEW rzd_analytics.locomotive_kpi IS
    'KPI-сводка по локомотивному парку: пробег, OTD, доступность, ТО и состояние датчиков';
