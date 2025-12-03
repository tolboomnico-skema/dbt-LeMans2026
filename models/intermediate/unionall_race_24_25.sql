    SELECT 
        NUMBER,
        ` DRIVER_NUMBER`,
        DRIVER_NAME,
        ` LAP_NUMBER`,
        ` CROSSING_FINISH_LINE_IN_PIT`,
        S1_SECONDS,
        S2_SECONDS,
        S3_SECONDS,
        ` KPH`,
        ` ELAPSED`,
        ` HOUR`,
        TOP_SPEED,
        CLASS,
        TEAM,
        MANUFACTURER,
        FLAG_AT_FL,
        PIT_TIME,
        2025 AS year_race,
        'LeMans' AS circuit
    FROM {{ source('raw_LeMans_2025', 'LeMans_2025') }}

    UNION ALL
    SELECT 
        NUMBER,
        ` DRIVER_NUMBER`,
        DRIVER_NAME,
        ` LAP_NUMBER`,
        ` CROSSING_FINISH_LINE_IN_PIT`,
        S1_SECONDS,
        S2_SECONDS,
        S3_SECONDS,
        ` KPH`,
        ` ELAPSED`,
        ` HOUR`,
        TOP_SPEED,
        CLASS,
        TEAM,
        MANUFACTURER,
        FLAG_AT_FL,
        PIT_TIME,
        2024 AS year_race,
        'LeMans' AS circuit
    FROM {{ source('raw_LeMans_2024', 'LeMans_2024') }}

    UNION ALL
    SELECT 
        NUMBER,
        ` DRIVER_NUMBER`,
        DRIVER_NAME,
        ` LAP_NUMBER`,
        ` CROSSING_FINISH_LINE_IN_PIT`,
        S1_SECONDS,
        S2_SECONDS,
        S3_SECONDS,
        ` KPH`,
        ` ELAPSED`,
        ` HOUR`,
        TOP_SPEED,
        CLASS,
        TEAM,
        MANUFACTURER,
        FLAG_AT_FL,
        PIT_TIME,
        2024 AS year_race,
        'Imola' AS circuit
    FROM {{ source('raw_Imola_2024', 'Imola_2024') }}

    UNION ALL
    SELECT 
        NUMBER,
        ` DRIVER_NUMBER`,
        DRIVER_NAME,
        ` LAP_NUMBER`,
        ` CROSSING_FINISH_LINE_IN_PIT`,
        S1_SECONDS,
        S2_SECONDS,
        S3_SECONDS,
        ` KPH`,
        ` ELAPSED`,
        ` HOUR`,
        TOP_SPEED,
        CLASS,
        TEAM,
        MANUFACTURER,
        FLAG_AT_FL,
        PIT_TIME,
        2025 AS year_race,
        'Imola' AS circuit
    FROM {{ source('raw_Imola_2025', 'Imola_2025') }}

    UNION ALL
    SELECT 
        NUMBER,
        ` DRIVER_NUMBER`,
        DRIVER_NAME,
        ` LAP_NUMBER`,
        ` CROSSING_FINISH_LINE_IN_PIT`,
        S1_SECONDS,
        S2_SECONDS,
        S3_SECONDS,
        ` KPH`,
        ` ELAPSED`,
        ` HOUR`,
        TOP_SPEED,
        CLASS,
        TEAM,
        MANUFACTURER,
        FLAG_AT_FL,
        PIT_TIME,
        2025 AS year_race,
        'Losail' AS circuit   -- Qatar = Losail
    FROM {{ source('raw_Qatar_2025', 'Qatar_2025') }}

    UNION ALL
    SELECT 
        NUMBER,
        ` DRIVER_NUMBER`,
        DRIVER_NAME,
        ` LAP_NUMBER`,
        ` CROSSING_FINISH_LINE_IN_PIT`,
        S1_SECONDS,
        S2_SECONDS,
        S3_SECONDS,
        ` KPH`,
        ` ELAPSED`,
        ` HOUR`,
        TOP_SPEED,
        CLASS,
        TEAM,
        MANUFACTURER,
        FLAG_AT_FL,
        PIT_TIME,
        2024 AS year_race,
        'Losail' AS circuit
    FROM {{ source('raw_Qatar_2024', 'Qatar_2024') }}

    UNION ALL
    SELECT 
        NUMBER,
        ` DRIVER_NUMBER`,
        DRIVER_NAME,
        ` LAP_NUMBER`,
        ` CROSSING_FINISH_LINE_IN_PIT`,
        S1_SECONDS,
        S2_SECONDS,
        S3_SECONDS,
        ` KPH`,
        ` ELAPSED`,
        ` HOUR`,
        TOP_SPEED,
        CLASS,
        TEAM,
        MANUFACTURER,
        FLAG_AT_FL,
        PIT_TIME,
        2025 AS year_race,
        'Spa' AS circuit
    FROM {{ source('raw_Spa_2025', 'Spa_2025') }}

    UNION ALL
    SELECT 
        NUMBER,
        ` DRIVER_NUMBER`,
        DRIVER_NAME,
        ` LAP_NUMBER`,
        ` CROSSING_FINISH_LINE_IN_PIT`,
        S1_SECONDS,
        S2_SECONDS,
        S3_SECONDS,
        ` KPH`,
        ` ELAPSED`,
        ` HOUR`,
        TOP_SPEED,
        CLASS,
        TEAM,
        MANUFACTURER,
        FLAG_AT_FL,
        PIT_TIME,
        2024 AS year_race,
        'Spa' AS circuit
    FROM {{ source('raw_Spa_2024', 'Spa_2024') }}

    UNION ALL
    SELECT 
        NUMBER,
        ` DRIVER_NUMBER`,
        DRIVER_NAME,
        ` LAP_NUMBER`,
        ` CROSSING_FINISH_LINE_IN_PIT`,
        S1_SECONDS,
        S2_SECONDS,
        S3_SECONDS,
        ` KPH`,
        ` ELAPSED`,
        ` HOUR`,
        TOP_SPEED,
        CLASS,
        TEAM,
        MANUFACTURER,
        FLAG_AT_FL,
        PIT_TIME,
        2024 AS year_race,
        'Americas' AS circuit
    FROM {{ source('raw_Americas_2024', 'Americas_2024') }}

    UNION ALL
    SELECT 
        NUMBER,
        ` DRIVER_NUMBER`,
        DRIVER_NAME,
        ` LAP_NUMBER`,
        ` CROSSING_FINISH_LINE_IN_PIT`,
        S1_SECONDS,
        S2_SECONDS,
        S3_SECONDS,
        ` KPH`,
        ` ELAPSED`,
        ` HOUR`,
        TOP_SPEED,
        CLASS,
        TEAM,
        MANUFACTURER,
        FLAG_AT_FL,
        PIT_TIME,
        2025 AS year_race,
        'Americas' AS circuit
    FROM {{ source('raw_Americas_2025', 'Americas_2025') }}

    UNION ALL
    SELECT 
        NUMBER,
        ` DRIVER_NUMBER`,
        DRIVER_NAME,
        ` LAP_NUMBER`,
        ` CROSSING_FINISH_LINE_IN_PIT`,
        S1_SECONDS,
        S2_SECONDS,
        S3_SECONDS,
        ` KPH`,
        ` ELAPSED`,
        ` HOUR`,
        TOP_SPEED,
        CLASS,
        TEAM,
        MANUFACTURER,
        FLAG_AT_FL,
        PIT_TIME,
        2025 AS year_race,
        'SaoPaulo' AS circuit
    FROM {{ source('raw_saopaulo_2025', 'saopaulo_2025') }}

    UNION ALL
    SELECT 
        NUMBER,
        ` DRIVER_NUMBER`,
        DRIVER_NAME,
        ` LAP_NUMBER`,
        ` CROSSING_FINISH_LINE_IN_PIT`,
        S1_SECONDS,
        S2_SECONDS,
        S3_SECONDS,
        ` KPH`,
        ` ELAPSED`,
        ` HOUR`,
        TOP_SPEED,
        CLASS,
        TEAM,
        MANUFACTURER,
        FLAG_AT_FL,
        PIT_TIME,
        2024 AS year_race,
        'SaoPaulo' AS circuit
    FROM {{ source('raw_saopaulo_2024', 'saopaulo_2024') }}

    UNION ALL
    SELECT 
        NUMBER,
        ` DRIVER_NUMBER`,
        DRIVER_NAME,
        ` LAP_NUMBER`,
        ` CROSSING_FINISH_LINE_IN_PIT`,
        S1_SECONDS,
        S2_SECONDS,
        S3_SECONDS,
        ` KPH`,
        ` ELAPSED`,
        ` HOUR`,
        TOP_SPEED,
        CLASS,
        TEAM,
        MANUFACTURER,
        FLAG_AT_FL,
        PIT_TIME,
        2025 AS year_race,
        'Fuji' AS circuit
    FROM {{ source('raw_fuji_2025', 'fuji_2025') }}

    UNION ALL
    SELECT 
        NUMBER,
        ` DRIVER_NUMBER`,
        DRIVER_NAME,
        ` LAP_NUMBER`,
        ` CROSSING_FINISH_LINE_IN_PIT`,
        S1_SECONDS,
        S2_SECONDS,
        S3_SECONDS,
        ` KPH`,
        ` ELAPSED`,
        ` HOUR`,
        TOP_SPEED,
        CLASS,
        TEAM,
        MANUFACTURER,
        FLAG_AT_FL,
        PIT_TIME,
        2024 AS year_race,
        'Fuji' AS circuit
    FROM {{ source('raw_fuji_2024', 'fuji_2024') }}

    UNION ALL
    SELECT 
        NUMBER,
        ` DRIVER_NUMBER`,
        DRIVER_NAME,
        ` LAP_NUMBER`,
        ` CROSSING_FINISH_LINE_IN_PIT`,
        S1_SECONDS,
        S2_SECONDS,
        S3_SECONDS,
        ` KPH`,
        ` ELAPSED`,
        ` HOUR`,
        TOP_SPEED,
        CLASS,
        TEAM,
        MANUFACTURER,
        FLAG_AT_FL,
        PIT_TIME,
        2025 AS year_race,
        'Bahrain' AS circuit
    FROM {{ source('raw_bahrein_2025', 'bahrein_2025') }}

    UNION ALL
    SELECT 
        NUMBER,
        ` DRIVER_NUMBER`,
        DRIVER_NAME,
        ` LAP_NUMBER`,
        ` CROSSING_FINISH_LINE_IN_PIT`,
        S1_SECONDS,
        S2_SECONDS,
        S3_SECONDS,
        ` KPH`,
        ` ELAPSED`,
        ` HOUR`,
        TOP_SPEED,
        CLASS,
        TEAM,
        MANUFACTURER,
        FLAG_AT_FL,
        PIT_TIME,
        2024 AS year_race,
        'Bahrain' AS circuit
    FROM {{ source('raw_bahrein_2024', 'bahrein_2024') }}





