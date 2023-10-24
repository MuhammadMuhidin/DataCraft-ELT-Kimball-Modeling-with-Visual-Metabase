{{ config(materialized='table') }}

SELECT
    *
FROM public.user_events