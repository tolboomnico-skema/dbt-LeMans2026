SELECT
        NUMBER AS car_nb,
        ` DRIVER_NUMBER` AS driver_nb,
        DRIVER_NAME AS driver_name,
        ` LAP_NUMBER` AS lap_nb,
        ` CROSSING_FINISH_LINE_IN_PIT` AS in_pit,
        CASE
       WHEN LAG(` CROSSING_FINISH_LINE_IN_PIT`)
       OVER (ORDER BY ` LAP_NUMBER`) = 'B' THEN 1
       ELSE 0
   END AS in_pit_lag,
        S1_SECONDS AS s1_seconds,
        S2_SECONDS AS s2_seconds,
        S3_SECONDS AS s3_seconds,
        ROUND((S1_SECONDS + S2_SECONDS + S3_SECONDS), 3) AS lap_time_total,
        ` KPH` AS avg_kph,
        ` ELAPSED` AS cum_time,
        ` HOUR` AS local_time,
        TOP_SPEED AS top_speed,
        CLASS AS class,
        TEAM AS team,
        MANUFACTURER AS manufacturer,
        CASE
WHEN FLAG_AT_FL="GF" THEN "green_flag"
WHEN FLAG_AT_FL="SF" THEN "safety_car"
WHEN FLAG_AT_FL="FF" THEN "green_flag"
WHEN FLAG_AT_FL="FCY" THEN "yellow_flag"
WHEN FLAG_AT_FL="RF" THEN "red_flag"
ELSE "other" END AS flag,
PIT_TIME AS pit_time,
year_race,
circuit,
LEFT(CAST(` HOUR`AS STRING),5)AS time_hhmm
FROM  {{ ref('unionall_race_24_25') }}      