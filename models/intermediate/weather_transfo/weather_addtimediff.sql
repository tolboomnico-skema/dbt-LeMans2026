
SELECT
weather.*,
td.timediff
FROM {{ ref('weather_master_transfo') }} AS weather
LEFT JOIN {{ source('timediff', 'timediff_weather') }} AS td
ON weather.circuit=td.circuit AND weather.year_race=td.year_race