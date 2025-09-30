{{ config(
    materialized='external',
    location='output/green_trips.csv',
    format='csv',
    header=true
) }}

WITH green_trips AS (
    SELECT
        'green' AS service_type,
        lpep_pickup_datetime AS pickup_datetime,
        lpep_dropoff_datetime AS dropoff_datetime,
        trip_distance
    FROM {{ source('main', 'green') }}
),

final_calculations AS (
    SELECT
        gt.service_type,
        gt.pickup_datetime,
        gt.dropoff_datetime,
        gt.trip_distance,

        (gt.trip_distance * em.co2_grams_per_mile) / 1000 AS trip_co2_kgs,

        DATE_DIFF('minute', gt.pickup_datetime, gt.dropoff_datetime) as duration_minutes,

        CASE
            WHEN duration_minutes > 0
            THEN gt.trip_distance / (duration_minutes / 60.0)
            ELSE 0
        END AS avg_mph,

        EXTRACT(hour FROM gt.pickup_datetime) AS hour_of_day,
        EXTRACT(dayofweek FROM gt.pickup_datetime) AS day_of_week,
        EXTRACT(week FROM gt.pickup_datetime) AS week_of_year,
        EXTRACT(month FROM gt.pickup_datetime) AS month_of_year
    FROM green_trips gt
    LEFT JOIN {{ source('main', 'emissions') }} em
        ON gt.service_type = em.vehicle_category -- adjust join logic!
)

SELECT *
FROM final_calculations
WHERE trip_distance > 0
  AND duration_minutes > 0;
