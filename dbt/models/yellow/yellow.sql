{{ config(
    materialized='external',
    location='output/yellow_trips.csv',
    format='csv',
    header=true
) }}

{{ log("Building yellow_trips", info=True) }}

with yellow_trips as (
    select
        tpep_pickup_datetime as pickup_datetime,
        tpep_dropoff_datetime as dropoff_datetime,
        trip_distance
    from {{ source('main', 'yellow') }}
),

final_calculations as (
    select
        yt.pickup_datetime,
        yt.dropoff_datetime,
        yt.trip_distance,

        -- Total CO2 output (kg)
        (yt.trip_distance * em.co2_grams_per_mile) / 1000 as trip_co2_kgs,

        -- Trip duration (minutes)
        date_diff('minute', yt.pickup_datetime, yt.dropoff_datetime) as duration_minutes,

        -- Average speed (mph)
        case
            when date_diff('minute', yt.pickup_datetime, yt.dropoff_datetime) > 0
            then yt.trip_distance / (date_diff('minute', yt.pickup_datetime, yt.dropoff_datetime) / 60.0)
            else 0
        end as avg_mph,

        -- Date parts
        extract(hour from yt.pickup_datetime) as hour_of_day,
        extract(dayofweek from yt.pickup_datetime) as day_of_week,
        extract(week from yt.pickup_datetime) as week_of_year,
        extract(month from yt.pickup_datetime) as month_of_year

    from yellow_trips yt
    left join {{ source('main', 'emissions') }} em
        on yt.vehicle_type = em.vehicle_type
        and yt.fuel_type = em.fuel_Type
)

select *
from final_calculations
where trip_distance > 0
  and duration_minutes > 0;
