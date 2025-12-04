SELECT *,
       FORMAT_DATETIME('%H:%M',
           DATETIME_ADD(datetime_utc, INTERVAL timediff HOUR)
       ) AS time_hhmm_transfo

FROM {{ ref('weather_addtimediff') }}