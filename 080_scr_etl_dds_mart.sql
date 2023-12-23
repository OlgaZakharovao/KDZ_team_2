--Скрипт etl из kdz_2_dds в mart


-- Создание вспомогательных таблиц с временными границами и маркером загрузки

create table if not exists kdz_2_etl.etl_mart_fact_departure_timestamps(
	ts1 timestamp,
	ts2 timestamp
);

-- Таблица с маркером загрузки

create table if not exists kdz_2_etl.etl_mart_fact_departure_timestamp(
	loaded_ts timestamp not null primary key -- маркер последнего обработанного значения
);

-- Добавление временных меток в таблицу etl_mart_fact_departure_timestamps 
-- В случае первоначальной загрузки в таблицу вставляется минимум и максимум времени загрузки из объединения таблиц flights и airport_weather, в случае инкрементальной загрузки берутся минимум и максимум времени загрузки из объедиенения таблиц flights и airport_weather с учётом маркера загрузки.

delete from kdz_2_etl.etl_mart_fact_departure_timestamps;

insert into kdz_2_etl.etl_mart_fact_departure_timestamps(ts1, ts2) 
select
min(loaded_ts) as ts1,
max(loaded_ts) as ts2
from 
	(
	select loaded_ts
	from kdz_2_dds.airport_weather 
	where
		loaded_ts >= coalesce((select max(loaded_ts) from kdz_2_etl.etl_mart_fact_departure_timestamp), '1970-01-01') and 
		(select max(loaded_ts) from kdz_2_etl.etl_mart_fact_departure_timestamp)  is not null 
		or 
		(select max(loaded_ts) from kdz_2_etl.etl_mart_fact_departure_timestamp)  is null 
	union all
	select loaded_ts
	from kdz_2_dds.flights 
	where
		loaded_ts >= coalesce((select max(loaded_ts) from kdz_2_etl.etl_mart_fact_departure_timestamp), '1970-01-01') and 
		(select max(loaded_ts) from kdz_2_etl.etl_mart_fact_departure_timestamp)  is not null 
		or 
		(select max(loaded_ts) from kdz_2_etl.etl_mart_fact_departure_timestamp)  is null 
	) as fff
;

drop table if exists kdz_2_etl.etl_mart_reading_data_fact_departure;

create table kdz_2_etl.etl_mart_reading_data_fact_departure as
select distinct on (flight_scheduled_ts, flight_number, airport_origin_dk, airport_destination_dk)
	airport_origin_dk,
	airport_destination_dk,
	weather_type_dk,
	flight_scheduled_ts,
	flight_actual_time,
	flight_number,
	distance,
	tail_number,
	airline,
	dep_delay_min,
	cancelled,
	cancellation_code,
	t,
	max_gws,
	w_speed,
	air_time,
	author,
	loaded_ts
from (
		select distinct on (flight_scheduled_ts, flight_number, airport_origin_dk, airport_destination_dk)
		fff.airport_origin_dk,
		fff.airport_dest_dk as airport_destination_dk,
		www.weather_type_dk,
		fff.flight_dep_scheduled_ts as flight_scheduled_ts,
		fff.flight_dep_actual_ts as flight_actual_time,
		fff.flight_number_reporting_airline as flight_number,
		fff.distance,
		fff.tail_number,
		fff.report_airline as airline,
		fff.dep_delay_minutes as dep_delay_min,
		fff.cancelled,
		fff.cancellation_code,
		www.t,
		www.max_gws,
		www.w_speed,
		air_time,
		'2' author,
		fff.loaded_ts 
		from kdz_2_dds.flights fff
		join kdz_2_dds.airport_weather www
		on (fff.airport_origin_dk=www.airport_dk and 
			fff.flight_dep_scheduled_ts >= www.date_start and fff.flight_dep_scheduled_ts < date_end)
     ) as vw_etl_mart_reading_data_fact_departure, 
     kdz_2_etl.etl_mart_fact_departure_timestamps
where loaded_ts between ts1 and ts2
order by flight_scheduled_ts, flight_number, airport_origin_dk, airport_destination_dk, loaded_ts desc;


-- Запись в целевую таблицу mart.fact_departure в режиме upsert

insert into mart.fact_departure
(	airport_origin_dk,
	airport_destination_dk,
	weather_type_dk,
	flight_scheduled_ts,
	flight_actual_time,
	flight_number,
	distance,
	tail_number,
	airline,
	dep_delay_min,
	cancelled,
	cancellation_code,
	t,
	max_gws,
	w_speed,
	air_time,
	author,
	loaded_ts)
select 
	airport_origin_dk,
	airport_destination_dk,
	weather_type_dk,
	flight_scheduled_ts,
	flight_actual_time,
	flight_number,
	distance,
	tail_number,
	airline,
	dep_delay_min,
	cancelled,
	cancellation_code,
	t,
	max_gws,
	w_speed,
	air_time,
	author,
	loaded_ts
from kdz_2_etl.etl_mart_reading_data_fact_departure
on conflict(flight_scheduled_ts, flight_number, airport_origin_dk, airport_destination_dk) do update
set
	weather_type_dk = excluded.weather_type_dk,
	flight_actual_time = excluded.flight_actual_time,
	distance = excluded.distance,
	tail_number = excluded.tail_number,
	airline = excluded.airline,
	dep_delay_min = excluded.dep_delay_min,
	cancelled = excluded.cancelled,
	cancellation_code = excluded.cancellation_code,
	t = excluded.t,
	max_gws = excluded.max_gws,
	w_speed = excluded.w_speed,
	air_time = excluded.air_time,
	author = excluded.author,
	loaded_ts = now();

-- Сохранение метки последней загрузки на будущее

delete from kdz_2_etl.etl_mart_fact_departure_timestamp
where exists (select 1 from kdz_2_etl.etl_mart_fact_departure_timestamp);

insert into kdz_2_etl.etl_mart_fact_departure_timestamp(loaded_ts)
select ts2
from kdz_2_etl.etl_mart_fact_departure_timestamps
where exists (select 1 from kdz_2_etl.etl_mart_fact_departure_timestamps);

