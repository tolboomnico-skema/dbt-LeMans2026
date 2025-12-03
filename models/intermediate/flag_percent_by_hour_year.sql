SELECT
year_race,
hour_range,
flag,
nb_tour,
SUM(nb_tour) OVER(PARTITION BY year_race,hour_range) AS nb_tour_heure

FROM {{ ref('flagbyhour') }}
