-- Set role
USE ROLE sysadmin;

-- Create separate warehouses for loading/querying data
CREATE WAREHOUSE IF NOT EXISTS ghosts_loading_wh
  WAREHOUSE_SIZE = 'SMALL'
  AUTO_RESUME = TRUE
  AUTO_SUSPEND = 180;

CREATE WAREHOUSE IF NOT EXISTS ghosts_query_wh
  WAREHOUSE_SIZE = 'SMALL'
  AUTO_RESUME = TRUE
  AUTO_SUSPEND = 180;

-- Create new database
CREATE DATABASE IF NOT EXISTS investigation;

-- Set context for investigation database
USE DATABASE investigation;
USE SCHEMA public;

-- Create tables
CREATE TABLE IF NOT EXISTS crime_details (
  crime_id INTEGER,
  victim_id INTEGER,
  date DATE,
  time TIME,
  location STRING,
  type STRING,
  notes STRING
);

CREATE TABLE IF NOT EXISTS victim_profiles (
  victim_id INTEGER,
  victim_name STRING,
  age INTEGER,
  occupation STRING,
  address STRING,
  last_known_location STRING,
  notes STRING
);

-- Create stage to access initial crime data
CREATE STAGE IF NOT EXISTS investigation1026
  URL = 's3://investigation-1026/';

-- Create file format
CREATE FILE FORMAT IF NOT EXISTS ghosts_csv
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  FIELD_DELIMITER = ','
  TRIM_SPACE = TRUE
  NULL_IF = '-';

-- Copy data from staging into tables
COPY INTO crime_details
  FROM '@investigation1026/crime_details'

COPY INTO victim_profiles
  FROM '@investigation1026/victim_profiles'

-- Switch to warehouse for querying data
USE WAREHOUSE ghosts_query_wh;

-- Determine date range of crimes
SELECT MIN(date) AS first_crime_date FROM crime_details;
SELECT MAX(date) AS last_crime_date FROM crime_details;


-- Count no. of crimes by type
SELECT type, COUNT(*) AS crime_count
FROM crime_details
GROUP BY type;

-- Count no. of homicides
SELECT COUNT(*) AS homicide_count
FROM crime_details
WHERE type = 'Homicide';

-- Count no. of crimes by location
SELECT location, COUNT(*) AS crime_count
FROM crime_details
GROUP BY location
ORDER BY crime_count DESC
LIMIT 1;

-- Categorize/count crimes by day
SELECT
  CASE
    WHEN TIME >= '06:00:00' AND TIME < '12:00:00' THEN 'Morning'
    WHEN TIME >= '12:00:00' AND TIME < '18:00:00' THEN 'Afternoon'
    WHEN TIME >= '18:00:00' AND TIME < '21:00:00' THEN 'Evening'
    ELSE 'Night'
  END AS time_category,
  COUNT(*) AS crime_count
FROM crime_details
GROUP BY time_category;

-- Calculate avg age of homicide victims
SELECT AVG(age) AS average_age
FROM victim_profiles
WHERE victim_id IN (
  SELECT victim_id
  FROM crime_details
  WHERE type = 'Homicide'
);

-- List homicide victims, occupation, available notes
SELECT vp.victim_name, vp.occupation, vp.notes
FROM victim_profiles vp
JOIN crime_details cd ON vp.victim_id = cd.victim_id
WHERE cd.type = 'Homicide';

-- Select victims wth notes that refer specific topics
SELECT vp.victim_name, vp.notes
FROM victim_profiles vp
JOIN crime_details cd ON vp.victim_id = cd.victim_id
WHERE cd.type = 'homicide'
AND (
  vp.notes ILIKE '%AI research or regulations%'
  OR vp.notes ILIKE '%Investigative reporting%'
  OR vp.notes ILIKE '%investigative journalist%'
  OR vp.notes ILIKE '%Community organizing%'
  OR vp.notes ILIKE '%Social causes%'
  OR vp.notes ILIKE '%Civil rights%'
);

-- Switch to warehouse for querying data
USE WAREHOUSE ghosts_query_wh;

SELECT CURRENT_WAREHOUSE();

-- Determine date range of crimes
SELECT MIN(date) AS first_crime_date, MAX(date) AS last_crime_date
FROM crime_details;

-- Count no. of crimes by type
SELECT type, COUNT(*) AS crime_count
FROM crime_details
GROUP BY type;

-- Categorize/count crimes by time of day
SELECT
  CASE
    WHEN TIME >= '06:00:00' AND TIME < '12:00:00' THEN 'Morning'
    WHEN TIME >= '12:00:00' AND TIME < '18:00:00' THEN 'Afternoon'
    WHEN TIME >= '18:00:00' AND TIME < '21:00:00' THEN 'Evening'
    ELSE 'Night'
  END AS time_category,
  COUNT(*) AS crime_count
FROM crime_details
GROUP BY time_category;
