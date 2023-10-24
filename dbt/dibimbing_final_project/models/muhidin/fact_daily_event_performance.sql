{{ config(materialized='table') }}
-- Use the `ref` function to select from other models

SELECT
    dd.date_key AS event_date,
    COUNT(DISTINCT e.id) AS total_events,
    COUNT(DISTINCT CASE WHEN e.event_type = 'login' THEN e.id END) AS total_logins,
    COUNT(DISTINCT CASE WHEN e.event_type = 'logout' THEN e.id END) AS total_logouts,
    COUNT(DISTINCT CASE WHEN e.event_type = 'search' THEN e.id END) AS total_searches,
    COUNT(DISTINCT u.user_id) AS total_users,
    COUNT(DISTINCT CASE WHEN t.transaction_type = 'purchase' THEN u.user_id END) AS total_purchasing_users,
    SUM(CASE WHEN t.transaction_type = 'purchase' THEN t.amount ELSE 0 END) AS total_purchase_amount
FROM {{ ref('dim_events') }} e
LEFT JOIN {{ ref('dim_user') }} u ON e.user_id = u.user_id
LEFT JOIN {{ ref('dim_transactions') }} t ON u.user_id = t.user_id
left join {{ ref('dim_date') }} dd on cast(e.timestamp as date) = dd.date_key 
GROUP BY event_date
ORDER BY event_date