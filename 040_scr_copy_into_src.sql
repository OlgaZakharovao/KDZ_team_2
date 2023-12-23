-----------------------------------------------------------------------
-----------------------------------------------------------------------
-- Таблица src_flights
-----------------------------------------------------------------------
-----------------------------------------------------------------------

-- Загрузка файла csv в таблицу src_flights

--\copy kdz_2_src.src_flights(year,quarter,month,flight_date,reporting_airline,tail_number,flight_number,origin,dest,crs_dep_time,dep_time,dep_delay_minutes,cancelled,cancellation_code,air_time,distance,weather_delay) from 'C:\TasksWH\KDZ\T_ONTIME_REPORTING01.csv' with delimiter ',’ CSV HEADER;
--\copy kdz_2_src.src_flights(year,quarter,month,flight_date,reporting_airline,tail_number,flight_number,origin,dest,crs_dep_time,dep_time,dep_delay_minutes,cancelled,cancellation_code,air_time,distance,weather_delay) from 'C:\TasksWH\KDZ\T_ONTIME_REPORTING02.csv' with delimiter ',’ CSV HEADER;
--\copy kdz_2_src.src_flights(year,quarter,month,flight_date,reporting_airline,tail_number,flight_number,origin,dest,crs_dep_time,dep_time,dep_delay_minutes,cancelled,cancellation_code,air_time,distance,weather_delay) from 'C:\TasksWH\KDZ\T_ONTIME_REPORTING03.csv' with delimiter ',’ CSV HEADER;
--\copy kdz_2_src.src_flights(year,quarter,month,flight_date,reporting_airline,tail_number,flight_number,origin,dest,crs_dep_time,dep_time,dep_delay_minutes,cancelled,cancellation_code,air_time,distance,weather_delay) from 'C:\TasksWH\KDZ\T_ONTIME_REPORTING04.csv' with delimiter ',’ CSV HEADER;

-----------------------------------------------------------------------
-----------------------------------------------------------------------
-- Таблица src_weather
-----------------------------------------------------------------------
-----------------------------------------------------------------------

-- Загрузка файла csv в таблицу src_weather

--\copy kdz_2_src.src_weather(local_datetime,t_air_temperature,p0_sea_lvl,p_station_lvl,u_humidity,dd_wind_direction,ff_wind_speed,ff10_max_gust_value,ww_present,ww_recent,c_total_clouds,vv_horizontal_visibility,td_temperature_dewpoint) from 'C:\TasksWH\KDZ\Weather.csv' with delimiter ';’ CSV HEADER;

