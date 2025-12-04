
SELECT
year_race,
hour_range,
  SAFE_DIVIDE(SUM(CASE WHEN flag = 'green_flag' THEN nb_tour ELSE 0 END), MAX(nb_tour_heure)) * 100 AS pct_green,
  SAFE_DIVIDE(SUM(CASE WHEN flag = 'yellow_flag' THEN nb_tour ELSE 0 END), MAX(nb_tour_heure)) * 100 AS pct_yellow,
  SAFE_DIVIDE(SUM(CASE WHEN flag = 'safety_car' THEN nb_tour ELSE 0 END), MAX(nb_tour_heure)) * 100 AS pct_sc

FROM {{ ref('flag_percent_by_hour_year') }}
GROUP BY year_race, hour_range
ORDER BY year_race, hour_range