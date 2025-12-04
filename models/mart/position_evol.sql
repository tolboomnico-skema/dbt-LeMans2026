
WITH RawDataFix AS (
  SELECT
    *,
    CAST(cum_time AS STRING) as cum_time_str
  FROM {{ ref('races_weather_clean_to_use') }}
  WHERE circuit ='LeMans' AND year_race = 2025
),
CleanedData AS (
  SELECT
    *,
    -- ROBUST TIME PARSING LOGIC using ARRAY_LENGTH
    (
      CASE ARRAY_LENGTH(SPLIT(cum_time_str, ':'))
        -- If 3 parts (e.g. 14:02:10.555 -> Hours:Mins:Secs)
        WHEN 3 THEN
          SAFE_CAST(SPLIT(cum_time_str, ':')[OFFSET(0)] AS INT64) * 3600 + -- Hours
          SAFE_CAST(SPLIT(cum_time_str, ':')[OFFSET(1)] AS INT64) * 60 +   -- Minutes
          SAFE_CAST(SPLIT(cum_time_str, ':')[OFFSET(2)] AS FLOAT64)        -- Seconds
        -- If 2 parts (e.g. 02:10.555 -> Mins:Secs)
        WHEN 2 THEN
          SAFE_CAST(SPLIT(cum_time_str, ':')[OFFSET(0)] AS INT64) * 60 +   -- Minutes
          SAFE_CAST(SPLIT(cum_time_str, ':')[OFFSET(1)] AS FLOAT64)        -- Seconds
        ELSE NULL
      END
    ) as cum_seconds
  FROM RawDataFix
),
RankedData AS (
  SELECT
    *,
    -- Calculate Position per Class per Lap
    RANK() OVER (PARTITION BY class, lap_nb ORDER BY cum_seconds ASC) as class_position,
    -- Get the cumulative time of the LEADER of that class for that lap
    MIN(cum_seconds) OVER (PARTITION BY class, lap_nb) as leader_cum_seconds
  FROM CleanedData
  WHERE cum_seconds IS NOT NULL
)
SELECT
  *,
  -- Calculate Gap to Leader (Class)
  ROUND(cum_seconds - leader_cum_seconds, 3) as gap_to_class_leader,
  -- Create a "Lap Label" for filters
  CONCAT("Lap ", CAST(lap_nb AS STRING)) as lap_label,
  -- Coordinates for Sector Visualization (Approximated for Le Mans)
  CASE
    -- Sector 1 (Dunlop Curve area)
    WHEN s1_seconds IS NOT NULL THEN ST_GEOGPOINT(0.2086, 47.9554)
    -- Sector 2 (Mulsanne Straight)
    WHEN s2_seconds IS NOT NULL THEN ST_GEOGPOINT(0.2311, 47.9378)
    -- Sector 3 (Porsche Curves)
    ELSE ST_GEOGPOINT(0.2087, 47.9255)
  END as sector_geo
FROM RankedData
ORDER BY lap_nb, class_position