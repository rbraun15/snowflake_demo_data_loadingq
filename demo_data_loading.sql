---------------------------------------
---------------------------------------
/*
Simple demo to show loading from:
Internal Stage
External Stage

Data Types:
CSV - comma delmited, pipe delimited
JSON

Includes - Create tables, file formats, simple queries

*/
---------------------------------------
---------------------------------------
-- Create DB if needed
-- create database <DBNAME>
-- create schema <SCHEMANAME>

use database SNOWDEV;
use schema RAW;

--------------------------------------------------------------------------------------------
--  Create an internal stage, alternatively can create external stage in AWS, Azure, GCP
--------------------------------------------------------------------------------------------
CREATE OR REPLACE STAGE DEMO_STAGE 
	DIRECTORY = ( ENABLE = true ) 
	COMMENT = 'simple data load from internal stage';


--------------------------------------------------------------------------------------------
-- Our stage is empty
--------------------------------------------------------------------------------------------
List @demo_stage;


--------------------------------------------------------------------------------------------
-- Add CSV files to stage 
-- In GUI navigate to the Stage, click on +Files (in upper right corner) 
-- navigate to csvs on our computer, select the files,  specify path as csv (bottom of box)
-- click upload
-- refresh browswer and see the csv folder with the files in it
--------------------------------------------------------------------------------------------


--------------------------------------------------------------------------------------------
-- Add Pipe delimited files to stage 
-- In GUI navigate to the Stage, click on +Files (in upper right corner) 
-- navigate to file  on our computer, select the file,  specify path as pipe_delim (bottom of box)
-- click upload
-- refresh browswer and see the csv folder with the files in it
-- Files can be added via command line
-- https://docs.snowflake.com/en/user-guide/data-load-local-file-system-stage-ui
--------------------------------------------------------------------------------------------
List @demo_stage;
List @demo_stage/csv;
List @demo_stage/pipe_delim;

 
--------------------------------------------------------------------------------------------
-- Create simple table
-- https://docs.snowflake.com/en/sql-reference-data-types
--------------------------------------------------------------------------------------------
create or replace TABLE MEMBER (
	SOURCE_MEMBER_ID INTEGER,
	FIRST_NAME VARCHAR(16777216),
	LAST_NAME VARCHAR(16777216),
	PHONE_NUMBER VARCHAR(16777216),
	MEMBER_AGE INTEGER,
	MEMBER_LANGUAGE VARCHAR(16777216),
	MEMBER_SEX VARCHAR(16777216),
	RACE_DESC VARCHAR(16777216)
);


--------------------------------------------------------------------------------------------
-- Create file formats
-- https://docs.snowflake.com/en/sql-reference/sql/create-file-format
--------------------------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1;

CREATE OR REPLACE FILE FORMAT my_pipe_delimited_format
  TYPE = 'CSV'
  FIELD_DELIMITER = '|'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1;

  
--------------------------------------------------------------------------------------------
-- Load the data from csv directory
-- https://docs.snowflake.com/en/sql-reference/sql/copy-into-table
--------------------------------------------------------------------------------------------

------------------
-- CSV
------------------
COPY INTO MEMBER
FROM @DEMO_STAGE/csv/
FILE_FORMAT = (FORMAT_NAME = 'my_csv_format');

select count(*) from MEMBER;
select * from MEMBER limit 5;

------------------
-- PIPE DELIMITED
------------------
COPY INTO MEMBER
FROM @DEMO_STAGE/pipe_delim/
FILE_FORMAT = (FORMAT_NAME = 'my_pipe_delimited_format');

select count(*) from MEMBER;
select * from MEMBER ;


----------------------------------------------------
----------------------------------------------------
----------------------------------------------------



-------------------------------------------------------------------
-- S3 Bucket Example with Json data from Getting Started Quickstart
-------------------------------------------------------------------
 

------------------
-- Create Stage
------------------
CREATE or REPLACE STAGE cybersyn_sec_filings
url = 's3://sfquickstarts/zero_to_snowflake/cybersyn_cpg_sec_filings/';



------------------
-- List Stage
------------------
LIST @cybersyn_sec_filings;


------------------------------------
-- Create Tables with Variant Column
------------------------------------
CREATE or replace TABLE sec_filings_index (v variant);
CREATE or replace TABLE sec_filings_attributes (v variant);


------------------------------------
-- Load Data
------------------------------------
COPY INTO sec_filings_index
FROM @cybersyn_sec_filings/cybersyn_sec_report_index.json.gz
    file_format = (type = json strip_outer_array = true);

COPY INTO sec_filings_attributes
FROM @cybersyn_sec_filings/cybersyn_sec_report_attributes.json.gz
    file_format = (type = json strip_outer_array = true);


------------------------------------
-- Simple Selects
------------------------------------
SELECT * FROM sec_filings_index LIMIT 10;
SELECT * FROM sec_filings_attributes LIMIT 10;


------------------------------------
-- Create Views for Easy Consumption
------------------------------------
CREATE OR REPLACE VIEW sec_filings_index_view AS
SELECT
    v:CIK::string                   AS cik,
    v:COMPANY_NAME::string          AS company_name,
    v:EIN::int                      AS ein,
    v:ADSH::string                  AS adsh,
    v:TIMESTAMP_ACCEPTED::timestamp AS timestamp_accepted,
    v:FILED_DATE::date              AS filed_date,
    v:FORM_TYPE::string             AS form_type,
    v:FISCAL_PERIOD::string         AS fiscal_period,
    v:FISCAL_YEAR::string           AS fiscal_year
FROM sec_filings_index;

CREATE OR REPLACE VIEW sec_filings_attributes_view AS
SELECT
    v:VARIABLE::string            AS variable,
    v:CIK::string                 AS cik,
    v:ADSH::string                AS adsh,
    v:MEASURE_DESCRIPTION::string AS measure_description,
    v:TAG::string                 AS tag,
    v:TAG_VERSION::string         AS tag_version,
    v:UNIT_OF_MEASURE::string     AS unit_of_measure,
    v:VALUE::string               AS value,
    v:REPORT::int                 AS report,
    v:STATEMENT::string           AS statement,
    v:PERIOD_START_DATE::date     AS period_start_date,
    v:PERIOD_END_DATE::date       AS period_end_date,
    v:COVERED_QTRS::int           AS covered_qtrs,
    TRY_PARSE_JSON(v:METADATA)    AS metadata
FROM sec_filings_attributes;


------------------------------------
-- Simple Select from  View
------------------------------------
SELECT *
FROM sec_filings_index_view
LIMIT 20;
 




