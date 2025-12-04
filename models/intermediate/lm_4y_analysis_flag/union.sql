SELECT *,
2022 as year_race
 FROM {{ source('lm_histo' , 'lm_22_raw') }}
WHERE NUMBER = 7

UNION ALL
SELECT *,
2023 as year_race
 FROM {{ source('lm_histo' , 'lm_23_raw') }} 
WHERE NUMBER = 50

UNION ALL
SELECT *,
2024 as year_race
 FROM {{ source('lm_histo' , 'lm_24_raw') }}
WHERE NUMBER = 50

UNION ALL
SELECT *,
2025 as year_race
 FROM {{ source('lm_histo' , 'lm_25_raw') }}
WHERE NUMBER = 83