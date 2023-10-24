{{ config(materialized='table') }}

SELECT
    u.id AS user_id,
    u.client_id,
    u.first_name || ' ' || u.last_name AS full_name,
    u.email,
    u.dob,
    u.gender,
    u.register_date::DATE AS register_date,
    CASE
        WHEN fa.id IS NOT NULL THEN 'Facebook'
        WHEN ia.id IS NOT NULL THEN 'Instagram'
        ELSE 'Other'
    END AS ads_source,
    EXTRACT(YEAR FROM AGE(now(), CAST(u.dob AS DATE))) AS age
FROM public.users u
LEFT JOIN public.facebook_ads fa ON u.client_id = fa.id
LEFT JOIN public.instagram_ads ia ON u.client_id = ia.id
