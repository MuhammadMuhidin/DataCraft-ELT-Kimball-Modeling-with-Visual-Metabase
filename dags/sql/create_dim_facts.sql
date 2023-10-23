-- Create warehouse schema if not exists
CREATE SCHEMA IF NOT EXISTS warehouse;

-- Drop table if exists and create table dim_user;
DROP TABLE IF EXISTS warehouse.dim_user;
CREATE TABLE warehouse.dim_user AS
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
FROM users u
LEFT JOIN facebook_ads fa ON u.client_id = fa.id
LEFT JOIN instagram_ads ia ON u.client_id = ia.id;
/*
this query is used to create a dimension table (dim_user) that consolidates user information from the users table,
creates a full name, determines the registration source, and calculates the age of the user.
The resulting dim_user table is likely intended for use in analytics or reporting,
providing a denormalized and more convenient structure for analysis.
*/

-- Drop table if exists and create table dim_ads;
DROP TABLE IF EXISTS warehouse.dim_ads;
CREATE TABLE warehouse.dim_ads AS
SELECT
    'Facebook' AS ads_source,
    id,
    ads_id,
    device_type,
    device_id,
    timestamp
FROM facebook_ads
UNION ALL
SELECT
    'Instagram' AS ads_source,
    id,
    ads_id,
    device_type,
    device_id,
    timestamp
FROM instagram_ads;
/*
this query creates a dimension table (dim_ads) that consolidates advertising data from both Facebook and Instagram.
It's likely designed to provide a unified view of advertising data for analytics or reporting purposes.
*/

-- Drop table if exists and create table dim_transactions;
DROP TABLE IF EXISTS warehouse.dim_transactions;
CREATE TABLE warehouse.dim_transactions AS
select
	t.user_id,
	t.transaction_date,
	t.amount,
    t.transaction_type
FROM user_transactions t;
/*
this query is used to create a dimension table (dim_transactions) that consolidates transaction data from the user_transactions table.
*/

-- Drop table if exists and create table dim_events;
DROP TABLE IF EXISTS warehouse.dim_events;
CREATE TABLE warehouse.dim_events AS
SELECT
    *
FROM user_events;
/*
this query is used to create a dimension table (dim_events) that consolidates event data from the user_events table.
*/

-- Drop table if exists and create table dim_date;
DROP TABLE IF EXISTS warehouse.dim_date;
CREATE TABLE warehouse.dim_date AS
SELECT
    timestamp::date as date_key,
    EXTRACT(DOW FROM timestamp::date) AS day_of_week,
    EXTRACT(WEEK FROM timestamp::date) AS week,
    EXTRACT(MONTH FROM timestamp::date) AS month,
    EXTRACT(QUARTER FROM timestamp::date) AS quarter,
    EXTRACT(YEAR FROM timestamp::date) AS year
FROM user_events;
/*
this query is used to create a dimension table (dim_date) that consolidates date data from the user_events table.
*/

-- Drop table if exists and create table fact_ads_performance;
DROP TABLE IF EXISTS warehouse.fact_ads_performance;
CREATE TABLE warehouse.fact_ads_performance AS
SELECT
    a.ads_id,
    COUNT(DISTINCT a.id) AS total_clicks,
    COUNT(DISTINCT CASE WHEN a.ads_source = 'Facebook' THEN a.id END) AS total_facebook_clicks,
    COUNT(DISTINCT CASE WHEN a.ads_source = 'Instagram' THEN a.id END) AS total_instagram_clicks,
    SUM(CASE WHEN t.transaction_type = 'purchase' THEN 1 ELSE 0 END) AS total_purchases,
    SUM(CASE WHEN t.transaction_type = 'purchase' THEN t.amount ELSE 0 END) AS total_purchase_amount
FROM warehouse.dim_ads a
LEFT JOIN warehouse.dim_user u ON a.id = u.client_id
LEFT JOIN warehouse.dim_transactions t ON u.user_id = t.user_id
GROUP BY a.ads_id;
/*
this query is designed to create a fact table that summarizes the performance of ads by aggregating various metrics such as total clicks,
clicks by source (Facebook/Instagram), total conversions, total purchases, and total purchase amount.
The resulting fact_ads_performance table is likely intended for use in analytics and reporting to analyze the effectiveness
of advertising campaigns.
*/

-- Drop table if exists and create table fact_user_performance;
DROP TABLE IF EXISTS warehouse.fact_user_performance;
CREATE TABLE warehouse.fact_user_performance AS
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
FROM warehouse.dim_user  u
LEFT JOIN warehouse.dim_events e ON u.user_id = e.user_id
LEFT JOIN warehouse.dim_transactions t ON u.user_id = t.user_id
WHERE e.timestamp IS NOT NULL
GROUP BY u.user_id, full_name;
/*
this query is designed to create a fact table that provides a consolidated view of user performance metrics,
including last login and activity dates, total events, count of specific event types (login, search, purchase), and total purchase amount.
The resulting fact_user_performance table is likely intended for use in analytics and reporting to analyze user engagement and behavior.
*/


-- Drop table if exists and create table fact_daily_event_performance;
DROP TABLE IF EXISTS warehouse.fact_daily_event_performance;
CREATE TABLE warehouse.fact_daily_event_performance AS
SELECT
    dd.date_key AS event_date,
    COUNT(DISTINCT e.id) AS total_events,
    COUNT(DISTINCT CASE WHEN e.event_type = 'login' THEN e.id END) AS total_logins,
    COUNT(DISTINCT CASE WHEN e.event_type = 'logout' THEN e.id END) AS total_logouts,
    COUNT(DISTINCT CASE WHEN e.event_type = 'search' THEN e.id END) AS total_searches,
    COUNT(DISTINCT u.user_id) AS total_users,
    COUNT(DISTINCT CASE WHEN t.transaction_type = 'purchase' THEN u.user_id END) AS total_purchasing_users,
    SUM(CASE WHEN t.transaction_type = 'purchase' THEN t.amount ELSE 0 END) AS total_purchase_amount
FROM warehouse.dim_events e
LEFT JOIN warehouse.dim_user u ON e.user_id = u.user_id
LEFT JOIN warehouse.dim_transactions t ON u.user_id = t.user_id
left join warehouse.dim_date dd on cast(e.timestamp as date) = dd.date_key 
GROUP BY event_date
ORDER BY event_date;
/*
this query is designed to create a fact table that provides a consolidated view of event daily performance metrics, including total events,
total logins, total logouts, total searches, total users, total purchasing users, and total purchase amount.
The resulting fact_daily_event_performance table is likely intended for use in analytics
and reporting to analyze event engagement and behavior.
*/

-- Drop table if exists and create table fact_weekly_ads_performance;
DROP TABLE IF EXISTS warehouse.fact_weekly_ads_performance;
CREATE TABLE warehouse.fact_weekly_ads_performance AS
SELECT
    dd.week,
    a.ads_id,
    COUNT(DISTINCT a.id) AS total_clicks,
    COUNT(DISTINCT CASE WHEN u.user_id IS NOT NULL THEN a.id END) AS total_converted_users,
    COUNT(DISTINCT CASE WHEN a.ads_source = 'Facebook' THEN a.id END) AS total_facebook_clicks,
    COUNT(DISTINCT CASE WHEN a.ads_source = 'Instagram' THEN a.id END) AS total_instagram_clicks,
    SUM(CASE WHEN t.transaction_type = 'purchase' THEN 1 ELSE 0 END) AS total_purchases,
    SUM(CASE WHEN t.transaction_type = 'purchase' THEN t.amount ELSE 0 END) AS total_purchase_amount
FROM warehouse.dim_ads a
LEFT JOIN warehouse.dim_user u ON a.id = u.client_id
LEFT JOIN warehouse.dim_transactions t ON u.user_id = t.user_id
inner join warehouse.dim_date dd on t.transaction_date = dd.date_key 
GROUP BY dd.week, a.ads_id;
/*
this query is designed to create a fact table that provides a weekly summary of advertising performance metrics,
including total clicks, converted users, clicks by source (Facebook/Instagram), total purchases, and total purchase amount.
The resulting fact_weekly_ads_performance table is likely intended for analytical purposes, allowing insights into
the weekly effectiveness of advertising campaigns.
*/