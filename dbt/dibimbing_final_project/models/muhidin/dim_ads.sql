{{ config(materialized='table') }}

SELECT
    'Facebook' AS ads_source,
    id,
    ads_id,
    device_type,
    device_id,
    timestamp
FROM public.facebook_ads
UNION ALL
SELECT
    'Instagram' AS ads_source,
    id,
    ads_id,
    device_type,
    device_id,
    timestamp
FROM public.instagram_ads