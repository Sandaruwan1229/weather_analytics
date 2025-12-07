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