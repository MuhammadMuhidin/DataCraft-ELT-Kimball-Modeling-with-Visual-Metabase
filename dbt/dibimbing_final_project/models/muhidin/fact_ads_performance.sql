{{ config(materialized='table') }}
-- Use the `ref` function to select from other models

SELECT
    a.ads_id,
    COUNT(DISTINCT a.id) AS total_clicks,
    COUNT(DISTINCT CASE WHEN a.ads_source = 'Facebook' THEN a.id END) AS total_facebook_clicks,
    COUNT(DISTINCT CASE WHEN a.ads_source = 'Instagram' THEN a.id END) AS total_instagram_clicks,
    SUM(CASE WHEN t.transaction_type = 'purchase' THEN 1 ELSE 0 END) AS total_purchases,
    SUM(CASE WHEN t.transaction_type = 'purchase' THEN t.amount ELSE 0 END) AS total_purchase_amount
FROM {{ ref('dim_ads') }} a
LEFT JOIN {{ ref('dim_user') }} u ON a.id = u.client_id
LEFT JOIN {{ ref('dim_transactions') }} t ON u.user_id = t.user_id
GROUP BY a.ads_id