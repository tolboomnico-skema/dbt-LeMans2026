WITH join_w AS(
    SELECT
races.*,
weather.datetime_utc,
weather.air_temp,
weather.track_temp,
weather.humidity, 
weather.pressure,
weather.wind_speed,
weather.wind_direction,
weather.rain,
FROM {{ ref('transfo_all_race') }} AS races
LEFT JOIN {{ ref('weathernewtime') }} AS weather
ON races.year_race=weather.year_race AND races.circuit=weather.circuit AND races.time_hhmm=weather.time_hhmm_transfo
)

,

add_local_rise AS (
SELECT
races.*,
CAST(CONCAT(daynight.sunrise_local, ':00') AS TIME) AS sunrise_local,
CAST(CONCAT(daynight.sunset_local, ':00') AS TIME) AS sunset_local
FROM join_w AS races
LEFT JOIN {{ source('day_night', 'day_night') }} AS daynight
USING (circuit)
)

SELECT*,
CASE 
WHEN local_time BETWEEN sunrise_local AND sunset_local THEN 'DAY'
ELSE 'NIGHT'
END AS day_night
FROM add_local_rise