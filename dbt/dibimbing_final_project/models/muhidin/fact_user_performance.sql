{{ config(materialized='table') }}
-- Use the `ref` function to select from other models

SELECT
    u.user_id,
    u.full_name,
    MAX(e.timestamp)::date AS last_login,
    MAX(e.timestamp)::date AS last_activity,
    COUNT(DISTINCT e.id) AS total_events,
    COUNT(DISTINCT CASE WHEN e.event_type = 'login' THEN e.id END) AS total_logins,
    COUNT(DISTINCT CASE WHEN e.event_type = 'search' THEN e.id END) AS total_searches,
    COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN e.id END) AS total_purchases,
    SUM(CASE WHEN t.transaction_type = 'purchase' THEN t.amount ELSE 0 END) AS total_purchase_amount
FROM {{ ref('dim_user') }}  u
LEFT JOIN {{ ref('dim_events') }} e ON u.user_id = e.user_id
LEFT JOIN {{ ref('dim_transactions') }} t ON u.user_id = t.user_id
WHERE e.timestamp IS NOT NULL
GROUP BY u.user_id, full_name