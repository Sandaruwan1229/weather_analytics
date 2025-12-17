with seasonal_data as (
    select  
        city_name,
        year_val,
        month_val,
        et0_fao_evapotranspiration,
        case
            when month_val >= 9 or month_val <= 3 then 'Sep-Mar'
            when month_val >= 4 or month_val <= 8 then 'Apr-Aug'
            else 'Unknown'
        end as agricultural_season
    from combined_weather_data
    where et0_fao_evapotranspiration is not null
        and et0_fao_evapotranspiration >= 0
        and et0_fao_evapotranspiration <= 20
),
seasonal_averages as (
    select
        city_name,
        agricultural_season,
        AVG(et0_fao_evapotranspiration) as avg_evapotranspiration,
        count(*) as record_count
    from seasonal_data
    where agricultural_season != 'Unknown'
    group by city_name, agricultural_season
    having count(*) >= 30
)
select 
    city_name,
    agricultural_season,
    ROUND(avg_evapotranspiration, 3) as avg_evapotranspiration_mm,
    record_count
from seasonal_averages
order by city_name,agricultural_season;
