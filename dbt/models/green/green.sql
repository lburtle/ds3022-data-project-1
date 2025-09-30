{{ config(
    materialized='external',
    location='output/green_trips.csv',
    format='csv',
    header=true
) }}

WITH green_trips AS (
    SELECT
        lpep_pickup_datetime AS pickup_datetime,
        lpep_dropoff_datetime AS dropoff_datetime,
        trip_distance,
        VendorID,
        RatecodeID,
        passenger_count,
        PULocationID,
        DOLocationID,
        payment_type
    FROM {{ source('main', 'green') }}
),

green_with_emissions AS (
    SELECT
        yt.*,
        em.co2_grams_per_mile
    FROM green_trips yt
    LEFT JOIN {{ source('main', 'emissions') }} em
        ON em.vehicle_type = 'green'  -- or a column mapping if you have one
),

final_calculations AS (
    SELECT
        *,
        -- 1. Trip CO2 in kilograms
        (trip_distance * co2_grams_per_mile) / 1000.0 AS trip_co2_kgs,

        -- 2. Trip duration in minutes
        DATE_DIFF('minute', pickup_datetime, dropoff_datetime) AS duration_minutes,

        -- 3. Average speed in mph
        CASE
            WHEN DATE_DIFF('minute', pickup_datetime, dropoff_datetime) > 0
            THEN trip_distance / (DATE_DIFF('minute', pickup_datetime, dropoff_datetime)/60.0)
            ELSE 0
        END AS avg_mph,

        -- 4. Time-based features
        EXTRACT(hour FROM pickup_datetime) AS hour_of_day,
        EXTRACT(dayofweek FROM pickup_datetime) AS day_of_week,
        EXTRACT(week FROM pickup_datetime) AS week_of_year,
        EXTRACT(month FROM pickup_datetime) AS month_of_year
    FROM green_with_emissions
)

SELECT *
FROM final_calculations
WHERE trip_distance > 0
  AND DATE_DIFF('minute', pickup_datetime, dropoff_datetime) > 0
