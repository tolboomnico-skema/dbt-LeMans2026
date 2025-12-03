SELECT 

CIRCUIT AS circuit,
YEAR AS year_race,
AIR_TEMP AS air_temp,
TRACK_TEMP AS track_temp,
HUMIDITY AS humidity, 
PRESSURE AS pressure,
WIND_SPEED AS wind_speed,
WIND_DIRECTION AS wind_direction,
RAIN AS rain,
TIME_HHMM AS time_hhmm

FROM {{ source('raw_all_weather', 'weather_master') }}