{{ config(
    materialized='external',
    location='output/green_trips.csv',
    format='csv',
    header=true
) }}

{{ log("Building green_trips", info=True) }}

with green_trips as (
    select
        lpep_pickup_datetime as pickup_datetime,
        lpep_dropoff_datetime as dropoff_datetime,
        trip_distance
    from {{ source('main', 'green') }}
),

final_calculations as (
    select
        gt.pickup_datetime,
        gt.dropoff_datetime,
        gt.trip_distance,

        -- Total CO2 output (kg)
        (gt.trip_distance * em.co2_grams_per_mile) / 1000 as trip_co2_kgs,

        -- Trip duration (minutes)
        date_diff('minute', gt.pickup_datetime, gt.dropoff_datetime) as duration_minutes,

        -- Average speed (mph)
        case
            when date_diff('minute', gt.pickup_datetime, gt.dropoff_datetime) > 0
            then gt.trip_distance / (date_diff('minute', gt.pickup_datetime, gt.dropoff_datetime) / 60.0)
            else 0
        end as avg_mph,

        -- Date parts
        extract(hour from gt.pickup_datetime) as hour_of_day,
        extract(dayofweek from gt.pickup_datetime) as day_of_week,
        extract(week from gt.pickup_datetime) as week_of_year,
        extract(month from gt.pickup_datetime) as month_of_year

    from green_trips gt
    left join {{ source('main', 'emissions') }} em
        on gt.vehicle_type = em.vehicle_type
        and gt.fuel_type = em.fuel_Type
)

select *
from final_calculations
where trip_distance > 0
  and duration_minutes > 0;
