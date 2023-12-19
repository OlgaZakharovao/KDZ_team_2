-----------------------------------------------------------------------
-----------------------------------------------------------------------
-- Таблица flights
-----------------------------------------------------------------------
-----------------------------------------------------------------------

-- Создание таблицы src_flights

CREATE TABLE kdz_2_src.src_flights (
	"year" int4 NOT NULL,
	quarter int4 NOT NULL,
	"month" int4 NOT NULL,
	flight_date varchar(50) NOT NULL,
	reporting_airline varchar(10) NOT NULL,
	tail_number varchar(15) NULL,
	flight_number int4 NOT NULL,
	origin varchar(3) NOT NULL,
	dest varchar(3) NOT NULL,
	dep_delay_minutes numeric(6, 2) NULL,
	cancelled numeric(3, 2) NOT NULL,
	cancellation_code varchar(1) NULL,
	dep_time varchar(10) NULL,
	air_time numeric(5, 2) NULL,
	crs_dep_time varchar(10) NOT NULL,
	distance numeric(6, 2) NOT NULL,
	weather_delay numeric(6, 2) NULL,
	loaded_ts timestamp NOT NULL DEFAULT now()
);

-- Создание таблицы staging_flights

CREATE TABLE kdz_2_staging.staging_flights (
	"year" int4 NOT NULL,
	quarter int4 NOT NULL,
	"month" int4 NOT NULL,
	flight_date varchar(50) NOT NULL,
	reporting_airline varchar(10) NOT NULL,
	tail_number varchar(15) NULL,
	flight_number int4 NOT NULL,
	origin varchar(3) NOT NULL,
	dest varchar(3) NOT NULL,
	dep_delay_minutes numeric(6, 2) NULL,
	cancelled numeric(3, 2) NOT NULL,
	cancellation_code varchar(1) NULL,
	dep_time varchar(10) NULL,
	air_time numeric(5, 2) NULL,
	crs_dep_time varchar(10) NOT NULL,
	distance numeric(6, 2) NOT NULL,
	weather_delay numeric(6, 2) NULL,
	loaded_ts timestamp NOT NULL DEFAULT now(),
	CONSTRAINT src_flights_cancellation_code_check CHECK (((cancellation_code)::text = ANY ((ARRAY['A'::character varying, 'B'::character varying, 'C'::character varying, 'D'::character varying])::text[]))),
	CONSTRAINT src_flights_month_check CHECK (((month >= 1) AND (month <= 12))),
	CONSTRAINT src_flights_quarter_check CHECK ((quarter = ANY (ARRAY[1, 2, 3, 4]))),
	CONSTRAINT staging_flights_pkey PRIMARY KEY (flight_date, flight_number, origin, dest, crs_dep_time)
);

-- Загрузка файла csv в таблицу src_flights

--\copy kdz_2_src.src_flights(year,quarter,month,flight_date,reporting_airline,tail_number,flight_number,origin,dest,crs_dep_time,dep_time,dep_delay_minutes,cancelled,cancellation_code,air_time,distance,weather_delay) from 'C:\TasksWH\KDZ\T_ONTIME_REPORTING.csv' with delimiter ',’ CSV HEADER;


-- Создание вспомогательных таблиц с временными границами и маркером загрузки

create table if not exists kdz_2_etl.etl_staging_flights_timestamps(
	ts1 timestamp,
	ts2 timestamp
);

-- Таблица с маркером загрузки

create table if not exists kdz_2_etl.etl_staging_flights_timestamp(
	loaded_ts timestamp not null primary key -- маркер последнего обработанного значения
);

-- Добавление временных меток в таблицу etl_staging_flights_timestamps

insert into kdz_2_etl.etl_staging_flights_timestamps(ts1, ts2) 
select
min(loaded_ts) as ts1,
max(loaded_ts) as ts2
from kdz_2_src.src_flights
where 
loaded_ts >= coalesce((select max(loaded_ts) from kdz_2_etl.etl_staging_flights_timestamp), '1970-01-01') and 
(select max(loaded_ts) from kdz_2_etl.etl_staging_flights_timestamp)  is not null 
or 
(select max(loaded_ts) from kdz_2_etl.etl_staging_flights_timestamp)  is null 
;

-- Чтение сырых данных и их загрузка в таблицу etl_staging_reading_raw_data_flights

drop table if exists kdz_2_etl.etl_staging_reading_raw_data_flights;
create table kdz_2_etl.etl_staging_reading_raw_data_flights as
select distinct on (flight_date, flight_number, origin, dest, crs_dep_time)
	"year",
	quarter,
	"month",
	flight_date,
	reporting_airline,
	tail_number,
	flight_number,
	origin,
	dest,
	crs_dep_time,
	dep_time,
	dep_delay_minutes,
	cancelled,
	cancellation_code,
	air_time,
	distance,
	weather_delay
from kdz_2_src.src_flights, kdz_2_etl.etl_staging_flights_timestamps
where loaded_ts between ts1 and ts2
order by flight_date, flight_number, origin, dest, crs_dep_time, loaded_ts desc;


-- Запись в целевую таблицу staging_flights в режиме upsert

insert into kdz_2_staging.staging_flights("year",
	quarter,
	"month",
	flight_date,
	reporting_airline,
	tail_number,
	flight_number,
	origin,
	dest,
	crs_dep_time,
	dep_time,
	dep_delay_minutes,
	cancelled,
	cancellation_code,
	air_time,
	distance,
	weather_delay)
select "year",
	quarter,
	"month",
	flight_date,
	reporting_airline,
	tail_number,
	flight_number,
	origin,
	dest,
	crs_dep_time,
	dep_time,
	dep_delay_minutes,
	cancelled,
	cancellation_code,
	air_time,
	distance,
	weather_delay
from kdz_2_etl.etl_staging_reading_raw_data_flights
on conflict(flight_date, flight_number, origin, dest, crs_dep_time) do update
set
	"year" = excluded."year",
	quarter = excluded.quarter,
	"month" = excluded."month",
	reporting_airline = excluded.reporting_airline,
	tail_number = excluded.tail_number,
	dep_time = excluded.dep_time,
	dep_delay_minutes = excluded.dep_delay_minutes,
	cancelled = excluded.cancelled,
	cancellation_code = excluded.cancellation_code,
	air_time = excluded.air_time,
	distance = excluded.distance,
	weather_delay = excluded.weather_delay,
loaded_ts = now();

-- Сохранение метки последней загрузки на будущее

delete from kdz_2_etl.etl_staging_flights_timestamp
where exists (select 1 from kdz_2_etl.etl_staging_flights_timestamp);

insert into kdz_2_etl.etl_staging_flights_timestamp(loaded_ts)
select ts2
from kdz_2_etl.etl_staging_flights_timestamps
where exists (select 1 from kdz_2_etl.etl_staging_flights_timestamps);






