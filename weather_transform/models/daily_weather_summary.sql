{{ config(
    materialized='incremental',
    unique_key='weather_date'
) }}

WITH raw_data AS (
    SELECT * FROM public.weather_data
    
    {% if is_incremental() %}
    -- O dbt só vai ler os dados da tabela original que sejam mais recentes
    -- do que a última data que já temos na nossa tabela final
    WHERE DATE(extracted_at) >= (SELECT MAX(weather_date) FROM {{ this }})
    {% endif %}
)

SELECT
    city,
    DATE(extracted_at) as weather_date,
    ROUND(AVG(temperature)::numeric, 2) as avg_temperature,
    MAX(temperature) as max_temperature,
    MIN(temperature) as min_temperature,
    ROUND(AVG(humidity)::numeric, 2) as avg_humidity,
    MAX(wind_speed) as max_wind_speed,
    COUNT(*) as total_readings
FROM raw_data
GROUP BY 
    city, 
    DATE(extracted_at)
