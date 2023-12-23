-----------------------------------------------------------------------
-----------------------------------------------------------------------
-- Таблица src_flights
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

-----------------------------------------------------------------------
-----------------------------------------------------------------------
-- Таблица src_weather
-----------------------------------------------------------------------
-----------------------------------------------------------------------

-- Создание таблицы src_weather

CREATE TABLE kdz_2_src.src_weather (
	icao_code varchar(10) NOT NULL DEFAULT 'LNK'::character varying,
	local_datetime timestamp NOT NULL,
	t_air_temperature numeric(3, 1) NOT NULL,
	p0_sea_lvl numeric(4, 1) NULL,
	p_station_lvl numeric(4, 1) NULL,
	u_humidity int4 NULL,
	dd_wind_direction varchar(100) NULL,
	ff_wind_speed int4 NULL,
	ff10_max_gust_value int4 NULL,
	ww_present varchar(100) NULL,
	ww_recent varchar(50) NULL,
	c_total_clouds varchar(200) NULL,
	vv_horizontal_visibility numeric(3, 1) NULL,
	td_temperature_dewpoint numeric(3, 1) NULL,
	loaded_ts timestamp NOT NULL DEFAULT now()
);



