-----------------------------------------------------------------------
-----------------------------------------------------------------------
-- Таблица weather
-----------------------------------------------------------------------
-----------------------------------------------------------------------

-- Создание вспомогательных таблиц с временными границами и маркером загрузки

create table if not exists kdz_2_etl.etl_staging_weather_timestamps(
	ts1 timestamp,
	ts2 timestamp
);

-- Таблица с маркером загрузки

create table if not exists kdz_2_etl.etl_staging_weather_timestamp(
	loaded_ts timestamp not null primary key -- маркер последнего обработанного значения
);

-- Добавление временных меток в таблицу etl_staging_weather_timestamps
delete from kdz_2_etl.etl_staging_weather_timestamps;

insert into kdz_2_etl.etl_staging_weather_timestamps(ts1, ts2) 
select
min(loaded_ts) as ts1,
max(loaded_ts) as ts2
from kdz_2_src.src_weather
where 
loaded_ts >= coalesce((select max(loaded_ts) from kdz_2_etl.etl_staging_weather_timestamp), '1970-01-01') and 
(select max(loaded_ts) from kdz_2_etl.etl_staging_weather_timestamp)  is not null 
or 
(select max(loaded_ts) from kdz_2_etl.etl_staging_weather_timestamp)  is null 
;

-- Чтение сырых данных и их загрузка в таблицу etl_staging_reading_raw_data_weather

drop table if exists kdz_2_etl.etl_staging_reading_raw_data_weather;

create table kdz_2_etl.etl_staging_reading_raw_data_weather as
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
from kdz_2_src.src_weather, kdz_2_etl.etl_staging_weather_timestamps
where loaded_ts between ts1 and ts2
order by icao_code, local_datetime, loaded_ts desc;


-- Запись в целевую таблицу staging_weather в режиме upsert

insert into kdz_2_staging.staging_weather
(icao_code, local_datetime, t_air_temperature, p0_sea_lvl, p_station_lvl, u_humidity, dd_wind_direction, ff_wind_speed, ff10_max_gust_value, ww_present, ww_recent, c_total_clouds, vv_horizontal_visibility, td_temperature_dewpoint)
select icao_code, local_datetime, t_air_temperature, p0_sea_lvl, p_station_lvl, u_humidity, dd_wind_direction, ff_wind_speed, ff10_max_gust_value, ww_present, ww_recent, c_total_clouds, vv_horizontal_visibility, td_temperature_dewpoint 
from kdz_2_etl.etl_staging_reading_raw_data_weather
on conflict(icao_code, local_datetime) do update
set
	t_air_temperature = excluded.t_air_temperature,
	p0_sea_lvl = excluded.p0_sea_lvl,
	p_station_lvl = excluded.p_station_lvl,
	u_humidity = excluded.u_humidity,
	dd_wind_direction = excluded.dd_wind_direction,
	ff_wind_speed = excluded.ff_wind_speed,
	ff10_max_gust_value = excluded.ff10_max_gust_value,
	ww_present = excluded.ww_present,
	ww_recent = excluded.ww_recent,
	c_total_clouds = excluded.c_total_clouds,
	vv_horizontal_visibility = excluded.vv_horizontal_visibility,
	td_temperature_dewpoint = excluded.td_temperature_dewpoint,
loaded_ts = now();

-- Сохранение метки последней загрузки на будущее

delete from kdz_2_etl.etl_staging_weather_timestamp
where exists (select 1 from kdz_2_etl.etl_staging_weather_timestamp);

insert into kdz_2_etl.etl_staging_weather_timestamp(loaded_ts)
select ts2
from kdz_2_etl.etl_staging_weather_timestamps
where exists (select 1 from kdz_2_etl.etl_staging_weather_timestamps);







