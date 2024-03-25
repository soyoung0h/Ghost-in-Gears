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

-- Clear existing call_log table
TRUNCATE TABLE call_log;

-- Create stage to access initial data
CREATE STAGE IF NOT EXISTS investigation4275
  URL = 's3://investigation-4275/';

-- List files and properties of files in staging
LIST @investigation4275;

-- Create file format ghosts_pipe
CREATE OR REPLACE FILE FORMAT ghosts_pipe
    FIELD_DELIMITER = '|'
    SKIP_HEADER = 1
    NULL_IF = ('N/A')
    TRIM_SPACE = TRUE;

-- Copy updated call log
COPY INTO call_log
  FROM @investigation4275/call_log.txt
  FILE_FORMAT = (FORMAT_NAME = ghosts_pipe)
  ON_ERROR = 'CONTINUE';

-- Validate load
SELECT COUNT(*) AS records
FROM @investigation4275;

-- Verify file was created and data is in the file
LIST @investigation4275;

-- Switch to warehouse for querying data
USE WAREHOUSE ghosts_query_wh;

-- Explore contents of "victim_detail_views"
SELECT *
FROM victim_detail_views;

SELECT * 
FROM call_log;

-- Inbound call # received by homicide victims
SELECT vd.victim_name AS victim_name,
       COUNT(*) AS num_inbound_calls
FROM call_log c
JOIN victim_detail_views vd ON c.receiver_id = vd.directory_id
GROUP BY vd.victim_name
ORDER BY vd.victim_name ASC;

-- Inbound calls received by certain victims
SELECT vd.victim_name AS victim_name,
       c.caller_id AS caller_id,
       c.call_id AS unique_id,
       MAX(c.call_start_date) AS call_date,
       c.call_status AS call_status
FROM call_log c
JOIN victim_detail_views vd ON c.receiver_id = vd.directory_id
GROUP BY vd.victim_name, c.caller_id, c.call_id, c.call_status
ORDER BY vd.victim_name ASC, call_date DESC;

-- Copy and adjust previous query
SELECT vd.victim_name AS victim_name,
       c.caller_id AS caller_id,
       c.call_id AS unique_id,
       c.call_start_date AS call_date,
       c.call_status AS call_status
FROM call_log c
JOIN victim_detail_views vd ON c.receiver_id = vd.directory_id
WHERE c.call_start_date <= vd.homicide_date
AND DATEDIFF(day, c.call_start_date, vd.homicide_date) <= 14
ORDER BY vd.victim_name ASC, c.call_start_date DESC;

-- Add caller names
SELECT vd.victim_name AS victim_name,
       pd.name AS caller_name,
       c.caller_id AS caller_id,
       c.call_id AS unique_id,
       c.call_start_date AS call_date,
       c.call_status AS call_status
FROM call_log c
JOIN victim_detail_views vd ON c.receiver_id = vd.directory_id
JOIN phone_directory pd ON pd.directory_id = c.caller_id
WHERE c.call_start_date <= vd.homicide_date
AND DATEDIFF(day, c.call_start_date, vd.homicide_date) <= 14
ORDER BY vd.victim_name ASC, c.call_start_date DESC;

-- Create table for security cameras
CREATE TABLE IF NOT EXISTS video_activity_json (
    activity_data VARIANT
);

-- Copy JSON data from file into table
COPY INTO video_activity_json
FROM @investigation4275/video_activity.json
FILE_FORMAT = (TYPE = 'JSON');

-- Create view
CREATE VIEW video_activity_view AS
SELECT
    f.value:camera_location::string AS camera_location,
    f.value:camera_status::string AS camera_status,
    f.value:observed_activity::string AS observed_activity,
    f.value:footage_id::int AS footage_id,
    TO_TIMESTAMP(f.value:timestamp::string, 'MM/DD/YYYY HH24:MI') AS timestamp
FROM video_activity_json,
LATERAL FLATTEN(input => activity_data) f;

-- Timespan of video activities
SELECT 
    TO_CHAR(MIN(timestamp), 'MM/DD/YY') AS earliest_activity_date,
    TO_CHAR(MAX(timestamp), 'MM/DD/YY') AS latest_activity_date
FROM video_activity_view;

-- List video activities on days when homicides occurred
SELECT v.* FROM video_activity_view v
JOIN
    victim_detail_views vd ON TO_DATE(v.timestamp) = TO_DATE(vd.homicide_date)
ORDER BY
    v.timestamp DESC;

-- Count video activities collected for 12/28/23
SELECT COUNT(*) AS activity_count
FROM video_activity_view
WHERE TO_CHAR(timestamp, 'MM/DD/YY') = '12/28/23';