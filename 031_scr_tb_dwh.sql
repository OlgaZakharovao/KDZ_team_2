-----------------------------------------------------------------------
-----------------------------------------------------------------------
-- Таблица id_flights
-----------------------------------------------------------------------
-----------------------------------------------------------------------

-- DROP TABLE kdz_2_dwh.id_flights;

CREATE TABLE kdz_2_dwh.id_flights (
	flight_date varchar(50) NOT NULL,
	flight_number int4 NOT NULL,
	origin varchar(50) NOT NULL,
	dest varchar(50) NOT NULL,
	crs_dep_time varchar(50) NOT NULL,
	dwh_id int8 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE),
	system_n varchar(50) NOT NULL,
	loaded_ts timestamptz NOT NULL DEFAULT now(),
	CONSTRAINT flights_id_pkey PRIMARY KEY (dwh_id)
);


-----------------------------------------------------------------------
-----------------------------------------------------------------------
-- Таблица id_weather
-----------------------------------------------------------------------
-----------------------------------------------------------------------
--DROP TABLE kdz_2_dwh.id_weather;

CREATE TABLE kdz_2_dwh.id_weather (
	icao_code varchar(10) NOT NULL,
	local_time timestamp NOT NULL,
	dwh_id int8 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE),
	system_n varchar(50) NOT NULL,
	loaded_ts timestamptz NOT NULL DEFAULT now(),
	CONSTRAINT weather_id_pkey PRIMARY KEY (dwh_id)
);

