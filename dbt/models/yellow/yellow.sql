{{ config(
    materialized='external',
    location='output/yellow_trips.csv',
    format='csv',
    header=true
) }}

WITH yellow_trips AS (
    SELECT
        'yellow' AS service_type,
        tpep_pickup_datetime AS pickup_datetime,
        tpep_dropoff_datetime AS dropoff_datetime,
        trip_distance
    FROM {{ source('main', 'yellow') }}
),

final_calculations AS (
    SELECT
        yt.service_type,
        yt.pickup_datetime,
        yt.dropoff_datetime,
        yt.trip_distance,

        (yt.trip_distance * em.co2_grams_per_mile) / 1000 AS trip_co2_kgs,

        DATE_DIFF('minute', yt.pickup_datetime, yt.dropoff_datetime) as duration_minutes,

        CASE
            WHEN duration_minutes > 0
            THEN yt.trip_distance / (duration_minutes / 60.0)
            ELSE 0
        END AS avg_mph,

        EXTRACT(hour FROM yt.pickup_datetime) AS hour_of_day,
        EXTRACT(dayofweek FROM yt.pickup_datetime) AS day_of_week,
        EXTRACT(week FROM yt.pickup_datetime) AS week_of_year,
        EXTRACT(month FROM yt.pickup_datetime) AS month_of_year
    FROM yellow_trips yt
    LEFT JOIN {{ source('main', 'emissions') }} em
        ON yt.service_type = em.vehicle_category -- adjust join logic!
)

SELECT *
FROM final_calculations
WHERE trip_distance > 0
  AND duration_minutes > 0;
