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
    datetime_utc AS datetime_utc_old,
    air_temp AS air_temp_old,
    track_temp AS track_temp_old,
    humidity AS humidity_old,
    pressure AS pressure_old,
    wind_speed AS wind_speed_old,
    wind_direction AS wind_direction_old,
    rain AS rain_old,

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