
-- create a db with name MYSQL_PROJECT
CREATE DATABASE MYSQL_PROJECT;

-- Load the db MYSQL_PROJECT
USE MYSQL_PROJECT;

-- Create a table structure to put our required data 
CREATE TABLE calls (
	ID VARCHAR(50),
	cust_name VARCHAR (50),
	sentiment VARCHAR (20),
	csat_score VARCHAR (20),
	call_timestamp VARCHAR (10),
	reason VARCHAR (20),
	city VARCHAR (20),
	state VARCHAR (20),
	channel VARCHAR (20),
	response_time VARCHAR (20),
	call_duration_minutes INT,
	call_center VARCHAR (20)
);


-- Used 'table data import wizard' to load our data in. 

-- -------------------------HANDLING ERRORS---------------------------------------
-- While  trying to import csat_score(INT) is showing errors so I changed it to csat_score VARCHAR (20)

-- checking for imported data
SELECT * FROM calls limit 10;


-- ----------------------------------DATA CLEANING----------------------------------------------
-- The 'call_timestamp' is a VARCHAR/string, so converting it to a DATE format.
SET SQL_SAFE_UPDATES = 0;  
-- SQL_SAFE_UPDATES setting is a safety feature and is set to off,while updating a column without specifying a WHERE clause 
UPDATE calls SET call_timestamp = str_to_date(call_timestamp, "%m/%d/%Y");
UPDATE calls SET csat_score = NULL WHERE csat_score = '';
SET SQL_SAFE_UPDATES = 1;

-- altering table to change 'csat_score' column from VARCHAR back to INT
ALTER TABLE calls
MODIFY COLUMN csat_score INT;

-- checking for the modified data
DESCRIBE calls;
SELECT distinct csat_score FROM calls;


-- ---------------------------------------DATA ANALYSIS-------------------------------------------
-- checking for the number of columns and rows in the data
SELECT COUNT(*) AS rows_num FROM calls;
SELECT COUNT(*) AS cols_num FROM information_schema.columns WHERE table_name = 'calls' ;


-- Checking for the distinct values of some columns:
SELECT DISTINCT sentiment FROM calls;
SELECT DISTINCT reason FROM calls;
SELECT DISTINCT channel FROM calls; -- channels for reaching 
SELECT DISTINCT response_time FROM calls; -- SLA:Service-Level -Agreement time
SELECT DISTINCT call_center FROM calls; -- data has 4 call centers


-- The count and precentage from total of each of the distinct values. (rounded to 2 decimals)
SELECT sentiment, count(*), ROUND((COUNT(*) / (SELECT COUNT(*) FROM calls)) * 100, 2) AS percount
FROM calls GROUP BY 1 ORDER BY 3 DESC;
SELECT reason, count(*), ROUND((COUNT(*) / (SELECT COUNT(*) FROM calls)) * 100, 2) AS percount
FROM calls GROUP BY 1 ORDER BY 3 DESC;
SELECT channel, count(*), ROUND((COUNT(*) / (SELECT COUNT(*) FROM calls)) * 100, 2) AS percount
FROM calls GROUP BY 1 ORDER BY 3 DESC;
SELECT response_time, count(*), ROUND((COUNT(*) / (SELECT COUNT(*) FROM calls)) * 100, 2) AS percount
FROM calls GROUP BY 1 ORDER BY 3 DESC;
SELECT call_center, count(*), ROUND((COUNT(*) / (SELECT COUNT(*) FROM calls)) * 100, 2) AS percount
FROM calls GROUP BY 1 ORDER BY 3 DESC;

-- state-wise count
SELECT state, COUNT(*) FROM calls GROUP BY 1 ORDER BY 2 DESC;

-- day-wise counts in descending order
SELECT DAYNAME(call_timestamp) as Day_of_call, COUNT(*) num_of_calls 
FROM calls 
GROUP BY 1 ORDER BY 2 DESC;


-- Data analysis using aggregations 
SELECT MIN(csat_score) AS min_score, MAX(csat_score) AS max_score, ROUND(AVG(csat_score),2) AS avg_score
FROM calls WHERE csat_score != 0; -- min is 1, max is 10 

SELECT MIN(call_timestamp) AS earliest_date, MAX(call_timestamp) AS most_recent 
FROM calls;

SELECT MIN(call_duration_minutes) AS min_call_duration, MAX(call_duration_minutes) AS max_call_duration, AVG(call_duration_minutes) AS avg_call_duration 
FROM calls;

SELECT call_center, response_time, COUNT(*) AS count
FROM calls 
GROUP BY 1,2 
ORDER BY 1,3 DESC;

SELECT call_center, AVG(call_duration_minutes) 
FROM calls 
GROUP BY 1 
ORDER BY 2 DESC;

SELECT channel, AVG(call_duration_minutes) 
FROM calls 
GROUP BY 1 
ORDER BY 2 DESC;

SELECT state, COUNT(*) 
FROM calls 
GROUP BY 1 
ORDER BY 2 DESC;

SELECT state, reason, COUNT(*) 
FROM calls 
GROUP BY 1,2
-- Having count(*)>1 
ORDER BY 1,2,3 DESC;

SELECT state, sentiment , COUNT(*) FROM calls GROUP BY 1,2 ORDER BY 1,3 DESC;

SELECT state, AVG(csat_score) as avg_csat_score FROM calls WHERE csat_score != 0 GROUP BY 1 ORDER BY 2 DESC;

SELECT sentiment, AVG(call_duration_minutes) FROM calls GROUP BY 1 ORDER BY 2 DESC;



-- --------------------------------WINDOW FUNCTION----------------------------------------
-- MAX call duration of each state in descending order
SELECT state , MAX(call_duration_minutes) OVER(PARTITION BY state) AS state_max_call_duration
FROM calls 
GROUP BY 1
ORDER BY 2 DESC;





