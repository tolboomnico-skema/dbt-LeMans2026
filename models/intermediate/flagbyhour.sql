SELECT
year_race,
hour_range,
CASE
WHEN FLAG_AT_FL="GF" THEN "green_flag"
WHEN FLAG_AT_FL="SF" THEN "safety_car"
WHEN FLAG_AT_FL="FF" THEN "green_flag"
WHEN FLAG_AT_FL="FCY" THEN "yellow_flag"
WHEN FLAG_AT_FL="RF" THEN "red_flag"
ELSE "other" END AS flag,
COUNT(*) as nb_tour  
FROM {{ ref('winerbyhouryear') }}
GROUP BY year_race,hour_range,flag