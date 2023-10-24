
{{ config(materialized='table') }}

SELECT
    timestamp::date as date_key,
    EXTRACT(DOW FROM timestamp::date) AS day_of_week,
    EXTRACT(WEEK FROM timestamp::date) AS week,
    EXTRACT(MONTH FROM timestamp::date) AS month,
    EXTRACT(QUARTER FROM timestamp::date) AS quarter,
    EXTRACT(YEAR FROM timestamp::date) AS year
FROM public.user_events