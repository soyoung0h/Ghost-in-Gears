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
CREATE TABLE IF NOT EXISTS forum_activities (
  post_id INTEGER,
  user_id INTEGER,
  user_ip_address STRING,
  post_title STRING,
  post_category STRING,
  post_date DATE
);

CREATE TABLE IF NOT EXISTS city_officials (
  official_id INTEGER,
  name STRING,
  position STRING,
  department STRING,
  office_location STRING,
  tenture_start DATE,
  public_initiatives STRING,
  ip_address STRING
);

-- Create stage to access initial crime data
CREATE STAGE IF NOT EXISTS investigation2134
  URL = 's3://investigation-2134/';

-- List files and properties of files in staging
LIST @investigation2134;

-- Copy data from staging into tables
COPY INTO forum_activities
  FROM '@investigation2134/forum_activities'
  FILE_FORMAT = (FORMAT_NAME = ghosts_csv);

COPY INTO city_officials
  FROM '@investigation2134/city_officials'
  FILE_FORMAT = (FORMAT_NAME = ghosts_csv);

-- Switch to warehouse for querying data
USE WAREHOUSE ghosts_query_wh;

-- Count no. of forum posts
SELECT COUNT(DISTINCT post_id) AS forum_posts
FROM forum_activities;

-- Determine date range of crimes
SELECT MIN(post_date) AS first_post_date,
       MAX(post_date) AS last_post_date
FROM forum_activities;

-- Count posts and cumulative posts by year and month
SELECT
    YEAR(post_date) AS year,
    MONTH(post_date) AS month,
    COUNT(*) AS monthly_posts,
    SUM(COUNT(*)) OVER (ORDER BY YEAR(post_date), MONTH(post_date)) AS cumulative_posts
FROM
    forum_activities
GROUP BY
    YEAR(post_date),
    MONTH(post_date)
ORDER BY
    YEAR(post_date),
    MONTH(post_date);

-- Top 10 most active users
SELECT
    user_id,
    COUNT(*) AS num_posts,
    RANK() OVER (ORDER BY COUNT(*) DESC) AS user_rank
FROM
    forum_activities
GROUP BY
    user_id
ORDER BY
    user_rank
LIMIT 10;

-- Count no. of city officials
SELECT COUNT(DISTINCT official_id) AS num_officials
FROM city_officials;

-- Count officials by department
SELECT department, COUNT(*) AS num_department
FROM city_officials
GROUP BY department
ORDER BY COUNT(*) desc;

-- Calculate tenture in months
SELECT 
    name,
    tenture_start,
    DATEDIFF('month', tenture_start, '2024-01-01') AS tenture_months
FROM 
    city_officials
WHERE
    department = 'Urban Planning'
ORDER BY 
    tenture_months DESC;

-- Officials whose IP appears on forum
SELECT DISTINCT
    co.name,
    co.department,
    fo.user_ip_address
FROM 
    city_officials co
JOIN 
    forum_activities fo ON co.ip_address = fo.user_ip_address;

-- Officials whose IP appears on forum rank by posts
SELECT DISTINCT
    co.name,
    co.department,
    fo.user_ip_address
FROM 
    city_officials co
JOIN 
    forum_activities fo ON co.ip_address = fo.user_ip_address
LEFT JOIN 
    (SELECT user_ip_address, COUNT(*) AS post_count FROM forum_activities GROUP BY user_ip_address) AS pc
    ON fo.user_ip_address = pc.user_ip_address;

-- List details for each posts
SELECT
    co.name AS official_name,
    co.department,
    fo.post_category,
    fo.post_title,
    fo.post_date
FROM
    city_officials co
JOIN
    forum_activities fo ON co.ip_address = fo.user_ip_address
ORDER BY
    co.name,
    fo.post_date;

--Oops! MISTAKE!
UPDATE city_officials SET position = 'AI Specialist';
SELECT * FROM city_officials LIMIT 10;

-- Previous 10 quesries
SELECT * FROM table(information_schema.query_history_by_session (result_limit=>10));

-- Restore Table
CREATE OR REPLACE TABLE city_officials AS
(SELECT * FROM city_officials before (statement => '01b2dd30-0001-b985-0005-64a2000160d6'));

-- Switch to SECURITYADMIN role to create new role
USE ROLE securityadmin;

-- Create new role and assign user to it
CREATE ROLE junior_detective;
GRANT ROLE junior_detective TO USER SOYOUNGOH;

__ Provide access
USE ROLE securityadmin;
GRANT USAGE ON DATABASE investigation TO ROLE junior_detective;
GRANT USAGE ON SCHEMA investigation.public TO ROLE junior_detective;
GRANT SELECT ON ALL TABLES IN SCHEMA investigation.public to ROLE junior_detective;
GRANT OPERATE ON WAREHOUSE ghosts_loading_wh TO ROLE junior_detective;

-- Switch roles to see access
USE ROLE junior_detective;
SELECT * FROM city_officials LIMIT 10;
USE ROLE sysadmin;
SELECT * FROM city_officials LIMIT 10;

-- Validate role
SHOW ROLES LIKE 'junior_detective';