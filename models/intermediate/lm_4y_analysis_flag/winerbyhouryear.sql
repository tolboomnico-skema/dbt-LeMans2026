SELECT*,
LEFT(` HOUR`, 2) as hour_range 
FROM {{ ref('union') }} 