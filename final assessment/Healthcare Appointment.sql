CREATE OR REPLACE WAREHOUSE HEALTHCARE_WH 
WITH WAREHOUSE_SIZE = 'XSMALL' 
AUTO_SUSPEND = 300 
AUTO_RESUME = TRUE;


ALTER WAREHOUSE HEALTHCARE_WH SET WAREHOUSE_SIZE = 'SMALL';


ALTER WAREHOUSE HEALTHCARE_WH SET WAREHOUSE_SIZE = 'XSMALL';


-- ALTER WAREHOUSE HEALTHCARE_WH SUSPEND;

-- ALTER WAREHOUSE HEALTHCARE_WH RESUME;

-- Create the Database and Schema
CREATE OR REPLACE DATABASE HEALTHCARE_DB;
CREATE OR REPLACE SCHEMA HEALTHCARE_DB.APPOINTMENT_SCHEMA;


USE WAREHOUSE HEALTHCARE_WH;
USE DATABASE HEALTHCARE_DB;
USE SCHEMA APPOINTMENT_SCHEMA;

-- Create File Format for CSV loading
CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('NULL', 'null', '');

-- Create Internal Stage
CREATE OR REPLACE STAGE APPOINTMENT_STAGE
  FILE_FORMAT = my_csv_format;



  -- step 5
  -- Create Raw Table 
CREATE OR REPLACE TABLE APPOINTMENT_RAW (
    PatientId NUMBER(20,2),
    AppointmentID NUMBER(20,0),
    Gender VARCHAR(10),
    ScheduledDay VARCHAR(50),
    AppointmentDay VARCHAR(50),
    Age NUMBER(3,0),
    Neighbourhood VARCHAR(255),
    Scholarship NUMBER(1,0),
    Hipertension NUMBER(1,0),
    Diabetes NUMBER(1,0),
    Alcoholism NUMBER(1,0),
    Handcap NUMBER(1,0),
    SMS_received NUMBER(1,0),
    No_show VARCHAR(10)
);

-- Copy data from  internal stage into  raw table
COPY INTO APPOINTMENT_RAW
FROM @APPOINTMENT_STAGE/appointment_no_show.csv
FILE_FORMAT = (FORMAT_NAME = my_csv_format)
ON_ERROR = 'CONTINUE';


SELECT * FROM APPOINTMENT_RAW LIMIT 10;

-- Step 6
CREATE OR REPLACE TABLE APPOINTMENT_FINAL (
    Patient_ID NUMBER(20,2),
    Appointment_ID NUMBER(20,0),
    Gender VARCHAR(10),
    Age NUMBER(3,0),
    Age_Group VARCHAR(20),
    Scheduled_Date TIMESTAMP_NTZ,
    Appointment_Date TIMESTAMP_NTZ,
    Waiting_Days NUMBER(5,0),
    Neighborhood VARCHAR(255),
    Scholarship NUMBER(1,0),
    Hypertension NUMBER(1,0),
    Diabetes NUMBER(1,0),
    Alcoholism NUMBER(1,0),
    Handicap NUMBER(1,0),
    SMS_received NUMBER(1,0),
    No_Show_Status VARCHAR(10),
    Attendance_Status VARCHAR(20)
);


-- Step 7 snowpark python
CREATE OR REPLACE PROCEDURE sp_transform_healthcare_data()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9' 
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, when, to_timestamp, datediff, abs

def main(session: snowpark.Session):
    # 1. Read from raw table
    df_raw = session.table("HEALTHCARE_DB.APPOINTMENT_SCHEMA.APPOINTMENT_RAW")
    
    # 2. Transform the columns
    df_transformed = df_raw.select(
        col("PatientId").alias("Patient_ID"),
        col("AppointmentID").alias("Appointment_ID"),
        col("Gender"),
        col("Age"),
        # Age Group conditional clustering
        when(col("Age") < 12, "Child")
        .when((col("Age") >= 12) & (col("Age") < 20), "Teen")
        .when((col("Age") >= 20) & (col("Age") < 65), "Adult")
        .otherwise("Senior").alias("Age_Group"),
        # Convert strings to proper Timestamps
        to_timestamp(col("ScheduledDay")).alias("Scheduled_Date"),
        to_timestamp(col("AppointmentDay")).alias("Appointment_Date"),
        # Calculate Waiting Days (absolute difference)
        abs(datediff("day", to_timestamp(col("ScheduledDay")), to_timestamp(col("AppointmentDay")))).alias("Waiting_Days"),
        col("Neighbourhood").alias("Neighborhood"),
        col("Scholarship"),
        col("Hipertension").alias("Hypertension"),
        col("Diabetes"),
        col("Alcoholism"),
        col("Handcap").alias("Handicap"),
        col("SMS_received"),
        col("No_show").alias("No_Show_Status"),
        # Attendance Status translation
        when(col("No_show") == "Yes", "Missed").otherwise("Visited").alias("Attendance_Status")
    )
    
    # 3. Write transformed results into the Final table
    df_transformed.write.mode("append").save_as_table("HEALTHCARE_DB.APPOINTMENT_SCHEMA.APPOINTMENT_FINAL")
    
    return "Data Transformation Completed Successfully via Snowpark Python!"
$$;

CALL sp_transform_healthcare_data();

SELECT * FROM APPOINTMENT_FINAL LIMIT 10;


-- Step 8

-- Create a stream to monitor new inserts on the raw table
CREATE OR REPLACE STREAM STR_APPOINTMENT_RAW ON TABLE APPOINTMENT_RAW;

INSERT INTO APPOINTMENT_RAW VALUES 
(99999999, 8888888, 'F', '2026-05-20T08:00:00Z', '2026-05-25T10:00:00Z', 34, 'JARDIM CAMBURI', 0, 0, 0, 0, 0, 1, 'No');


SELECT * FROM STR_APPOINTMENT_RAW;

--  incremental load using the Stream 
INSERT INTO APPOINTMENT_FINAL (
    Patient_ID, Appointment_ID, Gender, Age, Age_Group, 
    Scheduled_Date, Appointment_Date, Waiting_Days, Neighborhood, 
    Scholarship, Hypertension, Diabetes, Alcoholism, Handicap, 
    SMS_received, No_Show_Status, Attendance_Status
)
SELECT 
    PatientId, AppointmentID, Gender, Age,
    CASE 
        WHEN Age < 12 THEN 'Child'
        WHEN Age >= 12 AND Age < 20 THEN 'Teen'
        WHEN Age >= 20 AND Age < 65 THEN 'Adult'
        ELSE 'Senior'
    END,
    TO_TIMESTAMP(ScheduledDay), TO_TIMESTAMP(AppointmentDay),
    ABS(DATEDIFF('day', TO_TIMESTAMP(ScheduledDay), TO_TIMESTAMP(AppointmentDay))),
    Neighbourhood, Scholarship, Hipertension, Diabetes, Alcoholism, Handcap, SMS_received,
    No_show,
    CASE WHEN No_show = 'Yes' THEN 'Missed' ELSE 'Visited' END
FROM STR_APPOINTMENT_RAW 
WHERE METADATA$ACTION = 'INSERT';


SELECT * FROM STR_APPOINTMENT_RAW;


-- step 9
-- Create the automated ingestion pipe wrapper
CREATE OR REPLACE PIPE PIPE_APPOINTMENT_INGEST
AUTO_INGEST = FALSE
AS
COPY INTO HEALTHCARE_DB.APPOINTMENT_SCHEMA.APPOINTMENT_RAW
FROM @HEALTHCARE_DB.APPOINTMENT_SCHEMA.APPOINTMENT_STAGE
FILE_FORMAT = (FORMAT_NAME = my_csv_format);


CREATE OR REPLACE VIEW VW_APPOINTMENT_DASHBOARD AS
SELECT 
    Patient_ID,
    Appointment_ID,
    Gender,
    Age,
    Age_Group,
    Scheduled_Date,
    Appointment_Date,
    Waiting_Days,
    Neighborhood,
    Scholarship AS Has_Scholarship,
    Hypertension AS Has_Hypertension,
    Diabetes AS Has_Diabetes,
    Alcoholism AS Has_Alcoholism,
    Handicap AS Is_Handicapped,
    SMS_received AS SMS_Reminders_Received,
    No_Show_Status,
    Attendance_Status
FROM APPOINTMENT_FINAL;



TRUNCATE TABLE HEALTHCARE_DB.APPOINTMENT_SCHEMA.APPOINTMENT_FINAL;
TRUNCATE TABLE HEALTHCARE_DB.APPOINTMENT_SCHEMA.APPOINTMENT_RAW;