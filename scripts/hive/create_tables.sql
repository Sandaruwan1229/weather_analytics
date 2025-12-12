create database weather_analytics;
use weather_analytics;

CREATE TABLE weather_data (location_id INT, weather_date STRING,weather_code INT,temperature_2m_max DOUBLE,temperature_2m_min DOUBLE,temperature_2m_mean DOUBLE,apparent_temperature_max DOUBLE,apparent_temperature_min DOUBLE,apparent_temperature_mean DOUBLE,daylight_duration DOUBLE,sunshine_duration DOUBLE,precipitation_sum DOUBLE,rain_sum DOUBLE,precipitation_hours DOUBLE,wind_speed_10m_max DOUBLE,wind_gusts_10m_max DOUBLE,wind_direction_10m_dominant DOUBLE,shortwave_radiation_sum DOUBLE,et0_fao_evapotranspiration DOUBLE,sunrise STRING,sunset STRING) ROW format delimited fields terminated by ',' STORED AS TEXTFILE TBLPROPERTIES ("skip.header.line.count"="1");


ALTER TABLE weather_data SET TBLPROPERTIES ("skip.header.line.count"="1"); 

LOAD DATA INPATH 'hdfs://namenode:8020/user/hive/warehouse/weather_analytics.db/weather_data/weatherData.csv'
INTO TABLE weather_data;

CREATE TABLE location_data (
    location_id INT,
    latitude DOUBLE,
    longitude DOUBLE,
    elevation DOUBLE,
    utc_offset_seconds INT,
    timezone STRING,
    timezone_abbreviation STRING,
    city_name STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH 'hdfs://namenode:8020/user/hive/warehouse/weather_analytics.db/location_data/locationData.csv'
INTO TABLE location_data;



create view combined_weather_data as 
select 
    w.location_id,
    l.city_name,
    CAST(SPLIT( w.weather_date, '/')[2] as INT) as year_val,
    CAST(SPLIT( w.weather_date, '/')[0] as INT) as month_val,
    CAST(SPLIT( w.weather_date, '/')[1] as INT) as day_val,
    w.precipitation_hours,
    w.temperature_2m_mean,
    w.temperature_2m_max,
    w.temperature_2m_min,
    w.et0_fao_evapotranspiration
from weather_data w 
join location_data l on w.location_id = l.location_id 
where w.location_id is not null and l.location_id is not null;

with city_temperature_avg as (
    select 
        city_name,
        AVG(temperature_2m_max) as avg_max_temperature,
        count(*) as record_count
    from combined_weather_data
    where temperature_2m_max is not null 
        and temperature_2m_max > -50
        and temperature_2m_max < 60
    group by city_name
    having count(*) >= 365 
),
ranked_cities as (
    select
        city_name,
        avg_max_temperature,
        record_count,
        ROW_NUMBER() over (order by avg_max_temperature asc) as temperature_rank
    from city_temperature_avg
)
select 
    temperature_rank,
    city_name,
    ROUND(avg_max_temperature, 2) as avg_max_temperature_celsius,
    record_count
from ranked_cities
where temperature_rank <= 10
order by temperature_rank desc;