-----------------------------------------------------------------------
-----------------------------------------------------------------------
-- Таблица dds.kdz_2_airport_weather
-----------------------------------------------------------------------
-----------------------------------------------------------------------

-- Создание вспомогательных таблиц с временными границами и маркером загрузки

create table if not exists kdz_2_etl.etl_dds_weather_timestamps(
	ts1 timestamp,
	ts2 timestamp
);

-- Таблица с маркером загрузки

create table if not exists kdz_2_etl.etl_dds_weather_timestamp(
	loaded_ts timestamp not null primary key -- маркер последнего обработанного значения
);

-- Добавление временных меток в таблицу etl_dds_weather_timestamps
delete from kdz_2_etl.etl_dds_weather_timestamps;

insert into kdz_2_etl.etl_dds_weather_timestamps(ts1, ts2) 
select
min(loaded_ts) as ts1,
max(loaded_ts) as ts2
from kdz_2_staging.staging_weather
where 
loaded_ts >= coalesce((select max(loaded_ts) from kdz_2_etl.etl_dds_weather_timestamp), '1970-01-01') and 
(select max(loaded_ts) from kdz_2_etl.etl_dds_weather_timestamp)  is not null 
or 
(select max(loaded_ts) from kdz_2_etl.etl_dds_weather_timestamp)  is null 
;

-- Чтение данных с учётом инкремента и их загрузка в таблицу etl_dds_reading_data_weather

drop table if exists kdz_2_etl.etl_dds_reading_data_weather;

create table kdz_2_etl.etl_dds_reading_data_weather as
select distinct on (icao_code, local_datetime)
	icao_code,
	local_datetime,
	t_air_temperature, 
	p0_sea_lvl,
	p_station_lvl, 
	u_humidity, 
	dd_wind_direction, 
	ff_wind_speed, 
	ff10_max_gust_value,
	ww_present, 
	ww_recent, 
	c_total_clouds, 
	vv_horizontal_visibility,
	td_temperature_dewpoint
from kdz_2_staging.staging_weather, kdz_2_etl.etl_dds_weather_timestamps
where loaded_ts between ts1 and ts2
order by icao_code, local_datetime, loaded_ts desc;


-- Определение новых ключей для системы weather

drop table if exists kdz_2_dwh.id_weather_keys;

create table kdz_2_dwh.id_weather_keys as
select distinct
icao_code,
local_datetime,  
'weather' system_n
from 
kdz_2_staging.staging_weather
where (icao_code, local_datetime) not in (select icao_code, local_datetime
										  from kdz_2_dwh.id_weather
										  where system_n = 'weather');

-- Добавление новых ключей из системы weather в таблицу перекодировки
										 
insert into kdz_2_dwh.id_weather
(
icao_code,
local_time,
system_n
-- ,dwh_id -- это поле автоматически заполняется
-- ,loaded_ts -- это поле автоматически заполняется
)
select  
distinct
icao_code,
local_datetime,  
'weather' system_n
from kdz_2_dwh.id_weather_keys;


-- Создание представления для mapping

create or replace view kdz_2_etl.kdz_2_view as
select 
	airport_dk,
	cast(tb.cold as char)||cast(tb.rain as char)||cast(tb.snow as char)||cast(tb.thunderstorm as char)||cast(tb.drizzle as char)||cast(tb.fog_mist as char) as weather_type_dk,
	cold,
	rain,
	snow,
	thunderstorm,
	drizzle,
	fog_mist,
	t,
	max_gws,
	w_speed,
	date_start,
	date_end,
	now() as loaded_ts
from
(
	select 
		(select max(airport_dk) 
		from dds.airport a 
		where icao_code = 'KLNK') as airport_dk,
		(case 
			when t_air_temperature < 0 then 1
			else 0
		end) cold,
		(case 
			when 	lower (ww_present) like '%rain%' or lower (ww_recent) like '%rain%' then 1
			else 0
		end) rain,
		(case 
			when 	lower (ww_present) like '%snow%' or lower (ww_recent) like '%snow%' then 1
			else 0
		end) snow,
		(case 
			when 	lower (ww_present) like '%thunderstorm%' or lower (ww_recent) like '%thunderstorm%' then 1
			else 0
		end) thunderstorm,
		(case 
			when 	lower (ww_present) like '%drizzle%' or lower (ww_recent) like '%drizzle%' then 1
			else 0
		end)  drizzle ,
		(case 
			when 	lower (ww_present) like '%fog%' or lower (ww_recent) like '%fog%' or 
					lower (ww_present) like '%mist%' or lower (ww_recent) like '%mist%' then 1
			else 0
		end) fog_mist,
		t_air_temperature as t,
		ff10_max_gust_value as max_gws,
		ff_wind_speed as w_speed, 
		local_datetime as date_start,
		--'3000-01-01 00:00:00' as date_end
		now() as date_end
	from kdz_2_etl.etl_dds_reading_weather) as tb;
 
-- Запись в целевую таблицу airport_weather в режиме upsert

insert into kdz_2_dds.airport_weather
(
	airport_dk,
	weather_type_dk, 
	cold,
	rain,
	snow,
	thunderstorm ,
	drizzle,
	fog_mist,
	t,
	max_gws,
	w_speed,
	date_start,
	date_end
)
select 
	airport_dk,
	weather_type_dk, 
	cold,
	rain,
	snow,
	thunderstorm ,
	drizzle,
	fog_mist,
	t,
	max_gws,
	w_speed,
	date_start,
	date_end
from 
( 
	select 
		airport_dk,
		weather_type_dk,
		cold,
		rain,
		snow,
		thunderstorm,
		drizzle,
		fog_mist,
		t,
		max_gws,
		w_speed,
		date_start,
		(case when ww.dt is null then '3000-01-01' else ww.dt end) date_end
	--select ww.*, (case when dt is null then '3000-01-01' else dt end) dt_update
	from (
		select ww.*, 
		(lead(ww.date_start) over ( order by date_start)) dt
		from kdz_2_etl.kdz_2_view ww
		) ww
) kdz_2_ww
on conflict(airport_dk, date_start) do update
set 
	weather_type_dk = excluded.weather_type_dk,
	cold = excluded.cold,
	rain = excluded.rain,
	snow = excluded.snow,
	thunderstorm = excluded.thunderstorm,
	drizzle = excluded.drizzle,
	fog_mist = excluded.fog_mist,
	t = excluded.t,
	max_gws = excluded.max_gws,
	w_speed = excluded.w_speed,
	date_end = excluded.date_end,
	loaded_ts = now();

-- Update date_end для старых записей в целевой таблице airport_weather, которые необходимо закрыть

with c as
(
	select *
	from 
		(
		select ww.*, 
		       (lead(ww.date_start) over ( order by date_start)) dt
		from kdz_2_dds.airport_weather ww
		) ww
)
update kdz_2_dds.airport_weather aaa
set date_end = (select max(c.dt) from c where c.date_start = aaa.date_start)
where date_end <> (select max(c.dt) from c where c.date_start = aaa.date_start);


-- Сохранение метки последней загрузки на будущее

delete from kdz_2_etl.etl_dds_weather_timestamp
where exists (select 1 from kdz_2_etl.etl_dds_weather_timestamp);

insert into kdz_2_etl.etl_dds_weather_timestamp(loaded_ts)
select ts2
from kdz_2_etl.etl_dds_weather_timestamps
where exists (select 1 from kdz_2_etl.etl_dds_weather_timestamps);



