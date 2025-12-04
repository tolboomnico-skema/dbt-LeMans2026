WITH fill_val AS
(
    SELECT
    car_nb,
    driver_nb,
    driver_name,
    lap_nb,
    in_pit,
    in_pit_lag,
    s1_seconds,
    s2_seconds,
    s3_seconds,
    lap_time_total,
    avg_kph,
    cum_time,
    local_time,
    top_speed,
    class,
    team,
    manufacturer,
    flag,
    pit_time,
    year_race,
    circuit,
    time_hhmm,
    day_night,

    pressure AS pressure_old,


    -- Fill-forward columns
    LAST_VALUE(datetime_utc IGNORE NULLS)
        OVER (PARTITION BY year_race, circuit ORDER BY lap_nb
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS datetime_utc,

    LAST_VALUE(air_temp IGNORE NULLS)
        OVER (PARTITION BY year_race, circuit ORDER BY lap_nb
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS air_temp,

    LAST_VALUE(track_temp IGNORE NULLS)
        OVER (PARTITION BY year_race, circuit ORDER BY lap_nb
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS track_temp,

    LAST_VALUE(humidity IGNORE NULLS)
        OVER (PARTITION BY year_race, circuit ORDER BY lap_nb
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS humidity,

    LAST_VALUE(pressure IGNORE NULLS)
        OVER (PARTITION BY year_race, circuit ORDER BY lap_nb
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS pressure,

    LAST_VALUE(wind_speed IGNORE NULLS)
        OVER (PARTITION BY year_race, circuit ORDER BY lap_nb
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS wind_speed,

    LAST_VALUE(wind_direction IGNORE NULLS)
        OVER (PARTITION BY year_race, circuit ORDER BY lap_nb
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS wind_direction,

    LAST_VALUE(rain IGNORE NULLS)
        OVER (PARTITION BY year_race, circuit ORDER BY lap_nb
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rain

FROM {{ ref('all_races_weather_24_25_clean_new') }}
)

,

median_values AS (
    SELECT*,

        -- Median lap time by circuit and by year
        PERCENTILE_CONT(lap_time_total, 0.5)
            OVER (PARTITION BY year_race, circuit) AS median_by_circuit,

        -- Median lap time at Le Mans for the same year
        PERCENTILE_CONT(
            CASE WHEN circuit = 'LeMans' THEN lap_time_total END,
            0.5
        ) OVER (PARTITION BY year_race) AS median_lemans_by_year

    FROM fill_val
)

SELECT
    *,
    (median_lemans_by_year / NULLIF(median_by_circuit, 0)) * lap_time_total
        AS ponderated_lap_time,
    CASE
    WHEN rain = 0 THEN "no_rain"
    WHEN rain <3 THEN "light_rain"
    WHEN rain >=3 THEN "heavy_rain"
    ELSE "" END AS rain_intensity    
FROM median_values
WHERE median_lemans_by_year IS NOT NULL