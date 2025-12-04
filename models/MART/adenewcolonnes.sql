select
    *,
    case
        when RAIN = 0 then 'No Rain'
        when RAIN > 0 and RAIN < 3 then 'Small Rain'
        else 'Heavy Rain'
    end as rain_intensity
from {{ ref('races_weather_clean_to_use') }}