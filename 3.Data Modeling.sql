-- Set role
USE ROLE sysadmin;

-- Create separate warehouses for loading/querying data
CREATE WAREHOUSE IF NOT EXISTS ghosts_loading_wh
  WAREHOUSE_SIZE = 'SMALL'
  AUTO_RESUME = TRUE
  AUTO_SUSPEND = 180;

-- Create new database
CREATE DATABASE IF NOT EXISTS investigation;

-- Set context for investigation database
USE DATABASE investigation;
USE SCHEMA public;

-- Create tables
CREATE TABLE IF NOT EXISTS phone_directory (
  directory_id INTEGER,
  phone_number STRING,
  name STRING,
  address STRING,
  district STRING
);

CREATE TABLE IF NOT EXISTS call_log (
  call_id INTEGER,
  caller_id INTEGER,
  receiver_id INTEGER,
  call_duration INTEGER,
  call_start_date DATE,
  call_status STRING
);

-- Create stage to access initial crime data
CREATE STAGE IF NOT EXISTS investigation3651
  URL = 's3://investigation-3651/';

-- List files and properties of files in staging
LIST @investigation2134;

-- Copy data from staging into tables
COPY INTO phone_directory
  FROM '@investigation3651/phone_directory'
  FILE_FORMAT = (FORMAT_NAME = ghosts_csv);

COPY INTO call_log
  FROM '@investigation3651/call_log'
  FILE_FORMAT = (null_if = ('N/A'), skip_header = 1);

-- Switch to warehouse for querying data
USE WAREHOUSE ghosts_query_wh;

-- Count no. of calls
SELECT COUNT(DISTINCT call_id) AS call_num
FROM call_log;

-- Determine last call
SELECT MAX(call_start_date) AS last_call
FROM call_log;

-- % of answered calls
SELECT COUNT(call_id) / 3983.90 AS ans_calls
FROM call_log 
WHERE call_status = 'answered';

-- Avg. call durations in min
SELECT AVG(call_duration) / 60 AS call_duration_min
FROM call_log 
WHERE call_status = 'answered';

-- Call volume by month
SELECT
    EXTRACT(YEAR FROM call_start_date) AS year,
    EXTRACT(MONTH FROM call_start_date) AS month,
    COUNT(call_id) AS monthly_total,
    SUM(monthly_total) OVER (ORDER BY year, month) AS running_total
FROM
    call_log
GROUP BY
    year, month;

-- Outbound call volume by district
SELECT
    pd.district,
    COUNT(cl.call_id) AS outbound_call_volume
FROM
    call_log cl
JOIN
    phone_directory pd ON cl.caller_id = pd.directory_id
GROUP BY
    pd.district;

-- Create view
CREATE VIEW victim_detail_views AS
WITH homicide_victims AS (
    SELECT vp.victim_id,
           vp.victim_name,
           vp.address,
           cd.date AS homicide_date,
           pd.directory_id
    FROM victim_profiles vp
    JOIN crime_details cd ON vp.victim_id = cd.victim_id
    JOIN phone_directory pd ON vp.address = pd.address
    WHERE cd.type = 'Homicide'
)

SELECT *
FROM homicide_victims; 

SELECT *
FROM victim_detail_views;