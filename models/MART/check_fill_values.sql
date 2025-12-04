WITH fill_values AS (
SELECT
circuit,
year_race,
COUNT(*)AS nb_fill_values
FROM {{ ref('races_weather_clean_to_use') }}
WHERE pressure_old IS NULL
GROUP BY 1,2
)

,

tot_val AS (
    SELECT
circuit,
year_race,
COUNT(*)AS total_values
FROM {{ ref('races_weather_clean_to_use') }}
GROUP BY 1,2
)

SELECT*,
ROUND(SAFE_DIVIDE(nb_fill_values,total_values)*100,1) AS percent_null_values
FROM fill_values
JOIN tot_val
USING(circuit,year_race)
ORDER BY percent_null_values DESC


