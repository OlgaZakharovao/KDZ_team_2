-----------------------------------------------------------------------
-----------------------------------------------------------------------
-- Таблица flights
-----------------------------------------------------------------------
-----------------------------------------------------------------------

-- Создание вспомогательных таблиц с временными границами и маркером загрузки

create table if not exists kdz_2_etl.etl_dds_flights_timestamps(
	ts1 timestamp,
	ts2 timestamp
);

-- Таблица с маркером загрузки

create table if not exists kdz_2_etl.etl_dds_flights_timestamp(
	loaded_ts timestamp not null primary key -- маркер последнего обработанного значения
);

-- Добавление временных меток в таблицу etl_dds_flights_timestamps
delete from kdz_2_etl.etl_dds_flights_timestamps;

insert into kdz_2_etl.etl_dds_flights_timestamps(ts1, ts2) 
select
min(loaded_ts) as ts1,
max(loaded_ts) as ts2
from kdz_2_staging.staging_flights
where 
loaded_ts >= coalesce((select max(loaded_ts) from kdz_2_etl.etl_dds_flights_timestamp), '1970-01-01') and 
(select max(loaded_ts) from kdz_2_etl.etl_dds_flights_timestamp)  is not null 
or 
(select max(loaded_ts) from kdz_2_etl.etl_dds_flights_timestamp)  is null 
;

-- Чтение данных с учётом инкремента и их загрузка в таблицу etl_dds_reading_data_flights

drop table if exists kdz_2_etl.etl_dds_reading_data_flights;

create table kdz_2_etl.etl_dds_reading_data_flights as
select distinct on (flight_date, flight_number, origin, dest, crs_dep_time)
	"year",
	"quarter",
	"month",
	flight_date,
	reporting_airline,
	tail_number,
	flight_number,
	origin,
	dest,
	dep_delay_minutes,
	cancelled,
	cancellation_code,
	dep_time,
	air_time,
	crs_dep_time,
	distance,
	weather_delay
from kdz_2_staging.staging_flights, 
     kdz_2_etl.etl_dds_flights_timestamps
where loaded_ts between ts1 and ts2
order by flight_date, flight_number, origin, dest, crs_dep_time, loaded_ts desc;

-- Определение новых ключей для системы flights

drop table if exists kdz_2_dwh.id_flights_keys;

create table kdz_2_dwh.id_flights_keys as
select distinct
	flight_date,
	flight_number,
	origin,
	dest,
	crs_dep_time,
	'flights' system_n
from 
kdz_2_staging.staging_flights
where (flight_date, flight_number, origin, dest, crs_dep_time) 
      not in (select flight_date, flight_number, origin, dest, crs_dep_time
              from kdz_2_dwh.id_flights
			  where system_n = 'flights');

-- Добавление новых ключей из системы flights в таблицу перекодировки
										 
insert into kdz_2_dwh.id_flights
(
	flight_date,
	flight_number,
	origin,
	dest,
	crs_dep_time,
	system_n
-- ,dwh_id -- это поле автоматически заполняется
-- ,loaded_ts -- это поле автоматически заполняется
)
select distinct
	flight_date,
	flight_number,
	origin,
	dest,
	crs_dep_time,
	'flights' system_n
from kdz_2_dwh.id_flights_keys;

-- Запись в целевую таблицу flights в режиме upsert

insert into kdz_2_dds.flights(
	"year",
	"quarter",
	"month",
	flight_scheduled_date,
	flight_actual_date,
	flight_dep_scheduled_ts,
	flight_dep_actual_ts,
	report_airline,
	tail_number,
	flight_number_reporting_airline,
	airport_origin_dk,
	origin_code,
	airport_dest_dk,
	dest_code,
	dep_delay_minutes,
	cancelled,
	cancellation_code,
	weather_delay,
	air_time,
	distance)
select 
	"year",
	"quarter",
	"month",
	flight_scheduled_date,
	flight_actual_date,
	flight_dep_scheduled_ts,
	flight_dep_actual_ts,
	report_airline,
	tail_number,
	flight_number_reporting_airline,
	airport_origin_dk,
	origin_code,
	airport_dest_dk,
	dest_code,
	dep_delay_minutes,
	cancelled,
	cancellation_code,
	weather_delay,
	air_time,
	distance
from 
(
     	select
		"year",
		"quarter",
		"month",
		flight_scheduled_date,
		to_date(to_char(flight_dep_actual_ts,'yyyymmdd'),'yyyymmdd') as flight_actual_date,
		flight_dep_scheduled_ts,
		flight_dep_actual_ts,
		report_airline,
		tail_number,
		flight_number_reporting_airline,
		airport_origin_dk,
		origin_code,
		airport_dest_dk,
		dest_code,
		dep_delay_minutes,
		cancelled,
		cancellation_code,
		weather_delay,
		air_time,
		distance,
		loaded_ts
		from 
			(
			select 
			fff1.*,
			flight_dep_scheduled_ts + (dep_delay_minutes::int4)*interval '1 minute'  as flight_dep_actual_ts
			from
			(
				select 
				"year",
				"quarter",
				"month",
				to_date(flight_date,'MM/DD/YYYY') flight_scheduled_date,
				to_timestamp(flight_date,'MM/DD/YYYY HH24:MI:SS') + cast ((substring(crs_dep_time from 1 for 2)||':'||substring(crs_dep_time from 3 for 4)) as time) as flight_dep_scheduled_ts,
				reporting_airline as report_airline,
				tail_number,
				flight_number as flight_number_reporting_airline,
				(select max(airport_dk) from dds.airport a where iata_code = ttt.origin) airport_origin_dk,
				origin as origin_code,
				(select max(airport_dk) from dds.airport a where iata_code = ttt.dest) airport_dest_dk,
				dest dest_code,
				dep_delay_minutes,
				cancelled,
				cancellation_code,
				weather_delay,
				air_time,
				distance,
				loaded_ts
				from kdz_2_staging.staging_flights ttt
				where tail_number is not null 
			) as fff1
		) as fff2
) as etl_dds_reading_data_flights
on conflict (flight_dep_scheduled_ts, flight_number_reporting_airline, origin_code, dest_code) do update
set
	"year" = excluded."year",
	quarter = excluded.quarter,
	"month" = excluded."month",
	flight_scheduled_date = excluded.flight_scheduled_date,
	flight_actual_date = excluded.flight_actual_date,
	flight_dep_actual_ts = excluded.flight_dep_actual_ts,
	report_airline = excluded.report_airline,
	tail_number = excluded.tail_number,
	airport_origin_dk = excluded.airport_origin_dk,
	airport_dest_dk = excluded.airport_dest_dk,
	dep_delay_minutes = excluded.dep_delay_minutes,
	cancelled = excluded.cancelled,
	cancellation_code = excluded.cancellation_code,
	weather_delay = excluded.weather_delay,
	air_time = excluded.air_time,
	distance = excluded.distance,
    loaded_ts = now();



-- Сохранение метки последней загрузки на будущее

delete from kdz_2_etl.etl_dds_flights_timestamp
where exists (select 1 from kdz_2_etl.etl_dds_flights_timestamp);

insert into kdz_2_etl.etl_dds_flights_timestamp(loaded_ts)
select ts2
from kdz_2_etl.etl_dds_flights_timestamps
where exists (select 1 from kdz_2_etl.etl_dds_flights_timestamps);



