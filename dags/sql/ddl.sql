--users
DROP TABLE IF EXISTS users;
CREATE TABLE users (id INT, first_name VARCHAR, last_name VARCHAR, email VARCHAR, dob DATE, gender VARCHAR, register_date DATE, client_id INT);
COPY users FROM '/data/extracted/users.csv' DELIMITER AS ',' CSV HEADER;

-- user_events
DROP TABLE IF EXISTS user_events;
CREATE TABLE user_events (id INT, user_id INT, event_type TEXT, timestamp TEXT, device_type TEXT, location TEXT, event_data TEXT);
COPY user_events FROM '/data/extracted/user_events.csv' DELIMITER AS ',' CSV HEADER;

-- user_transactions
DROP TABLE IF EXISTS user_transactions;
CREATE TABLE user_transactions (id INT, user_id INT, transaction_date DATE, amount INT, transaction_type TEXT);
COPY user_transactions FROM '/data/extracted/user_transactions.csv' DELIMITER AS ',' CSV HEADER;

-- facebook_ads
DROP TABLE IF EXISTS facebook_ads;
CREATE TABLE facebook_ads (id INT, ads_id VARCHAR, device_type VARCHAR, device_id VARCHAR, timestamp TIMESTAMP);
COPY facebook_ads FROM '/data/extracted/facebook_ads.csv' DELIMITER AS ',' CSV HEADER;

-- instagram_ads
DROP TABLE IF EXISTS instagram_ads;
CREATE TABLE instagram_ads (id INT, ads_id TEXT, device_type TEXT, device_id TEXT, timestamp TIMESTAMP);
COPY instagram_ads FROM '/data/extracted/instagram_ads.csv' DELIMITER AS ',' CSV HEADER;

-- schema warehouse
DROP SCHEMA IF EXISTS warehouse CASCADE;
CREATE SCHEMA warehouse;
