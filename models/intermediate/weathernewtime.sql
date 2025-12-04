SELECT *,
CASE 
  WHEN CAST(LEFT(time_hhmm, 2) AS INT64) < 22 
    THEN CONCAT(
           CAST(LEFT(time_hhmm, 2) AS INT64) + timediff,
           RIGHT(time_hhmm, 3)
         )

  WHEN CAST(LEFT(time_hhmm, 2) AS INT64) = 22 
    THEN CONCAT("00", RIGHT(time_hhmm, 3))

  WHEN CAST(LEFT(time_hhmm, 2) AS INT64) = 23 
    THEN CONCAT("01", RIGHT(time_hhmm, 3))

  ELSE "error"
END AS time_hhmm_transfo
FROM {{ ref('weather_addtimediff') }}