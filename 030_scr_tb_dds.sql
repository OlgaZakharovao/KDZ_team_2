-----------------------------------------------------------------------
-----------------------------------------------------------------------
-- Таблица flights
-----------------------------------------------------------------------
-----------------------------------------------------------------------


-- Создание таблицы flights

DROP TABLE kdz_2_dds.flights Cascade;

CREATE TABLE kdz_2_dds.flights (
	year int NULL,
	quarter int NULL,
	month int NULL,
	flight_scheduled_date date NULL,
	flight_actual_date date NULL,
	flight_dep_scheduled_ts timestamp NOT NULL,
	flight_dep_actual_ts timestamp NULL,
	report_airline varchar(10) NOT NULL,
	tail_number varchar(10) NOT NULL,
	flight_number_reporting_airline varchar(15) NOT NULL,
	airport_origin_dk int NULL, 
	origin_code varchar(5) null,
	airport_dest_dk int NULL, 
	dest_code varchar(5) null,
	dep_delay_minutes float NULL,
	cancelled int NOT NULL,
	cancellation_code char(1) NULL,
	weather_delay float NULL,
	air_time float NULL,
	distance float NULL,
	loaded_ts timestamp default(now()),
	CONSTRAINT flights_pk PRIMARY KEY (flight_dep_scheduled_ts, flight_number_reporting_airline, origin_code, dest_code)
);


-----------------------------------------------------------------------
-----------------------------------------------------------------------
-- Таблица airport_weather
-----------------------------------------------------------------------
-----------------------------------------------------------------------

-- Создание таблицы airport_weather


DROP TABLE kdz_2_dds.airport_weather Cascade;


CREATE TABLE kdz_2_dds.airport_weather (
	airport_dk int NOT NULL,
	weather_type_dk char(6) NOT NULL,
	cold smallint default(0),
	rain smallint default(0),
	snow smallint default(0),
	thunderstorm smallint default(0),
	drizzle smallint default(0),
	fog_mist smallint default(0),
	t int NULL,
	max_gws int NULL,
	w_speed int NULL,
	date_start timestamp NOT NULL,
	date_end timestamp NOT NULL default('3000-01-01'::timestamp),
	loaded_ts timestamp default(now()),
	PRIMARY KEY (airport_dk, date_start)
);





