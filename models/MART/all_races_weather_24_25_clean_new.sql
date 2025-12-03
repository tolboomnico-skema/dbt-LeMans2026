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