"""
This module contains the strings which are nothing but the SQL statements for various database operations.
The SQLs include creating tables, inserting data from source files, insert into stage tables and ETL steps to load to final LKP/DIM/FACT tables
This module needs to be imported in the main ETL script.
"""

# Section for Create table SQLs for Source tables
# The src and stg tables are DROPPED and RECREATED as they will be loaded from scratch everytime
# The core tables are not dropped everytime. They are created only if they are not present already.
# This ensures that the data in the warehouse is never purged/deleted

CREATE_TABLE_SRC_AIRPORT_CODES_SQL = '''
DROP TABLE IF EXISTS "SRC_DB".stg_src_airport_codes;
CREATE TABLE "SRC_DB".stg_src_airport_codes (
ID VARCHAR(10),
City VARCHAR(100),
Name VARCHAR(500),
PRIMARY KEY(ID));
'''

CREATE_TABLE_SRC_US_ACCIDENTS_SQL = '''
DROP TABLE IF EXISTS "SRC_DB".stg_src_us_accidents;
CREATE TABLE "SRC_DB".stg_src_us_accidents (
ID VARCHAR(10),
Source VARCHAR(15),
TMC DECIMAL(5,1),
Severity INTEGER,
Start_Time TIMESTAMP,
End_Time TIMESTAMP,
Start_Lat DECIMAL(10,2),
Start_Lng DECIMAL(10,2),
End_Lat DECIMAL(10,2),
End_Lng DECIMAL(10,2),
Distance_mi DECIMAL(5,2),
Description VARCHAR(2000),
Number DECIMAL(12,2),
Street VARCHAR(100),
Side VARCHAR(100),
City VARCHAR(100),
County VARCHAR(100),
State VARCHAR(2),
Zipcode VARCHAR(10),
Country VARCHAR(2),
Timezone VARCHAR(20),
Airport_Code VARCHAR(10),
Weather_Timestamp TIMESTAMP,
Temperature_f DECIMAL(5,2),
Wind_Chill_f DECIMAL(5,2),
Humidity_pct DECIMAL(5,2),
Pressure_in DECIMAL(5,2),
Visibility_mi DECIMAL(5,2),
Wind_Direction VARCHAR(10),
Wind_Speed_mph DECIMAL(5,2),
Precipitation_in DECIMAL(5,2),
Weather_Condition VARCHAR(100),
Amenity BOOLEAN,
Bump BOOLEAN,
Crossing BOOLEAN,
Give_Way BOOLEAN,
Junction BOOLEAN,
No_Exit BOOLEAN,
Railway BOOLEAN,
Roundabout BOOLEAN,
Station BOOLEAN,
Stop BOOLEAN,
Traffic_Calming BOOLEAN,
Traffic_Signal BOOLEAN,
Turning_Loop BOOLEAN,
Sunrise_Sunset VARCHAR(10),
Civil_Twilight VARCHAR(10),
Nautical_Twilight VARCHAR(10),
Astronomical_Twilight VARCHAR(10),
PRIMARY KEY(ID));
'''

CREATE_TABLE_SRC_DC_TAXI_TRIPS_SQL = '''
DROP TABLE IF EXISTS "SRC_DB".stg_src_dc_taxi_trips;
CREATE TABLE "SRC_DB".stg_src_dc_taxi_trips (
Type VARCHAR(100),
ProviderName VARCHAR(100),
StartDateTime TIMESTAMP,
DateCreated TIMESTAMP,
ID VARCHAR(100),
ExternalID VARCHAR(100),
FareAmount DECIMAL(10,2),
GratuityAmount DECIMAL(10,2),
SurchargeAmount DECIMAL(10,2),
ExtraFareAmount DECIMAL(10,2),
TollAmount DECIMAL(10,2),
TotalAmount DECIMAL(10,2),
PaymentType INTEGER,
StartDateTime1 TIMESTAMP,
EndDateTime TIMESTAMP,
OriginStreetNumber VARCHAR(200),
OriginStreetName VARCHAR(200),
OriginCity VARCHAR(100),
OriginState VARCHAR(2),
OriginZip VARCHAR(10),
OriginLatitude DECIMAL(10,6),
OriginLongitude DECIMAL(10,6),
DestinationStreetNumber VARCHAR(100),
DestinationStreetName VARCHAR(200),
DestinationCity VARCHAR(100),
DestinationState VARCHAR(2),
DestinationZip VARCHAR(10),
DestinationLatitude DECIMAL(10,6),
DestinationLongitude DECIMAL(10,6),
Milage DECIMAL(10,2),
Duration DECIMAL(20,4),
Misc VARCHAR(1000),
PRIMARY KEY(ID));
'''


# Section for Create table SQLs for Stage tables

CREATE_TABLE_STG_ADDRESS_SQL = '''
DROP TABLE IF EXISTS "STG_DB".stg_address;
CREATE TABLE "STG_DB".stg_address (
City VARCHAR(100),
State VARCHAR(2),
Zipcode VARCHAR(10),
Create_TS TIMESTAMP,
Create_User VARCHAR(10),
Last_Updt_TS TIMESTAMP,
Last_Updt_User VARCHAR(10),
PRIMARY KEY(City, State, Zipcode));
'''

CREATE_TABLE_STG_ACCIDENT_CONDITION_SQL = '''
DROP TABLE IF EXISTS "STG_DB".stg_accident_condition;
CREATE TABLE "STG_DB".stg_accident_condition (
Amenity BOOLEAN,
Bump BOOLEAN,
Crossing BOOLEAN,
Give_Way BOOLEAN,
Junction BOOLEAN,
No_Exit BOOLEAN,
Railway BOOLEAN,
Roundabout BOOLEAN,
Station BOOLEAN,
Stop BOOLEAN,
Traffic_Calming BOOLEAN,
Traffic_Signal BOOLEAN,
Turning_Loop BOOLEAN,
Sunrise_Sunset VARCHAR(10),
Civil_Twilight VARCHAR(10),
Nautical_Twilight VARCHAR(10),
Astronomical_Twilight VARCHAR(10),
Create_TS TIMESTAMP,
Create_User VARCHAR(10),
Last_Updt_TS TIMESTAMP,
Last_Updt_User VARCHAR(10),
PRIMARY KEY(Amenity, Bump, Crossing, Give_Way, Junction, No_Exit, Railway, Roundabout, Station, Stop, Traffic_Calming, Traffic_Signal, Turning_Loop, Sunrise_Sunset, Civil_Twilight, Nautical_Twilight, Astronomical_Twilight));
'''

CREATE_TABLE_STG_AIRPORT_SQL = '''
DROP TABLE IF EXISTS "STG_DB".stg_airport;
CREATE TABLE "STG_DB".stg_airport (
ID VARCHAR(10),
City VARCHAR(100),
Name VARCHAR(500),
Create_TS TIMESTAMP,
Create_User VARCHAR(10),
Last_Updt_TS TIMESTAMP,
Last_Updt_User VARCHAR(10),
PRIMARY KEY(ID));
'''


CREATE_TABLE_STG_WEATHER_CONDITION_SQL = '''
DROP TABLE IF EXISTS "STG_DB".stg_weather_condition;
CREATE TABLE "STG_DB".stg_weather_condition (
Wind_Direction VARCHAR(10),
Weather_Condition VARCHAR(100),
Create_TS TIMESTAMP,
Create_User VARCHAR(10),
Last_Updt_TS TIMESTAMP,
Last_Updt_User VARCHAR(10),
PRIMARY KEY(Wind_Direction, Weather_Condition));
'''

CREATE_TABLE_STG_PROVIDER_SQL = '''
DROP TABLE IF EXISTS "STG_DB".stg_provider;
CREATE TABLE "STG_DB".stg_provider (
Provider_Name VARCHAR(100),
Create_TS TIMESTAMP,
Create_User VARCHAR(10),
Last_Updt_TS TIMESTAMP,
Last_Updt_User VARCHAR(10),
PRIMARY KEY(Provider_Name));
'''

CREATE_TABLE_STG_SOURCE_SQL = '''
DROP TABLE IF EXISTS "STG_DB".stg_source;
CREATE TABLE "STG_DB".stg_source (
Source_Name VARCHAR(100),
Create_TS TIMESTAMP,
Create_User VARCHAR(10),
Last_Updt_TS TIMESTAMP,
Last_Updt_User VARCHAR(10),
PRIMARY KEY(Source_Name));
'''

CREATE_TABLE_STG_ACCIDENT_SQL = '''
DROP TABLE IF EXISTS "STG_DB".stg_accident;
CREATE TABLE "STG_DB".stg_accident (
ID VARCHAR(10),
Source VARCHAR(15),
TMC DECIMAL(5,1),
Severity INTEGER,
Start_Time TIMESTAMP,
End_Time TIMESTAMP,
Start_Lat DECIMAL(10,2),
Start_Lng DECIMAL(10,2),
End_Lat DECIMAL(10,2),
End_Lng DECIMAL(10,2),
Distance_mi DECIMAL(5,2),
Description VARCHAR(2000),
Number DECIMAL(12,2),
Street VARCHAR(100),
Side VARCHAR(100),
City VARCHAR(100),
County VARCHAR(100),
State VARCHAR(2),
Zipcode VARCHAR(10),
Country VARCHAR(2),
Timezone VARCHAR(20),
Airport_Code VARCHAR(10),
Weather_Timestamp TIMESTAMP,
Temperature_f DECIMAL(5,2),
Wind_Chill_f DECIMAL(5,2),
Humidity_pct DECIMAL(5,2),
Pressure_in DECIMAL(5,2),
Visibility_mi DECIMAL(5,2),
Wind_Direction VARCHAR(10),
Wind_Speed_mph DECIMAL(5,2),
Precipitation_in DECIMAL(5,2),
Weather_Condition VARCHAR(100),
Amenity BOOLEAN,
Bump BOOLEAN,
Crossing BOOLEAN,
Give_Way BOOLEAN,
Junction BOOLEAN,
No_Exit BOOLEAN,
Railway BOOLEAN,
Roundabout BOOLEAN,
Station BOOLEAN,
Stop BOOLEAN,
Traffic_Calming BOOLEAN,
Traffic_Signal BOOLEAN,
Turning_Loop BOOLEAN,
Sunrise_Sunset VARCHAR(10),
Civil_Twilight VARCHAR(10),
Nautical_Twilight VARCHAR(10),
Astronomical_Twilight VARCHAR(10),
Create_TS TIMESTAMP,
Create_User VARCHAR(10),
Last_Updt_TS TIMESTAMP,
Last_Updt_User VARCHAR(10),
PRIMARY KEY(ID));
'''

CREATE_TABLE_STG_TRIP_SQL = '''
DROP TABLE IF EXISTS "STG_DB".stg_trip;
CREATE TABLE "STG_DB".stg_trip (
Type VARCHAR(100),
ProviderName VARCHAR(100),
StartDateTime TIMESTAMP,
DateCreated TIMESTAMP,
ID VARCHAR(100),
ExternalID VARCHAR(100),
FareAmount DECIMAL(10,2),
GratuityAmount DECIMAL(10,2),
SurchargeAmount DECIMAL(10,2),
ExtraFareAmount DECIMAL(10,2),
TollAmount DECIMAL(10,2),
TotalAmount DECIMAL(10,2),
PaymentType INTEGER,
StartDateTime1 TIMESTAMP,
EndDateTime TIMESTAMP,
OriginStreetNumber VARCHAR(200),
OriginStreetName VARCHAR(200),
OriginCity VARCHAR(100),
OriginState VARCHAR(2),
OriginZip VARCHAR(10),
OriginLatitude DECIMAL(10,6),
OriginLongitude DECIMAL(10,6),
DestinationStreetNumber VARCHAR(100),
DestinationStreetName VARCHAR(200),
DestinationCity VARCHAR(100),
DestinationState VARCHAR(2),
DestinationZip VARCHAR(10),
DestinationLatitude DECIMAL(10,6),
DestinationLongitude DECIMAL(10,6),
Milage DECIMAL(10,2),
Duration DECIMAL(20,4),
Misc VARCHAR(1000),
Create_TS TIMESTAMP,
Create_User VARCHAR(10),
Last_Updt_TS TIMESTAMP,
Last_Updt_User VARCHAR(10),
PRIMARY KEY(ID));
'''


# Section for Create table SQLs for LKP/DIM/FACT tables

CREATE_TABLE_DIM_DATE_SQL = '''
CREATE TABLE IF NOT EXISTS "CORE_DB".dim_date (
Date_SK INTEGER,
Date_DT DATE,
DOW_No INTEGER,
Day_Name VARCHAR(100),
Week_Day_Ind BOOLEAN,
Week_No INTEGER,
Month_No INTEGER,
Month_Name VARCHAR(100),
Quarter_No INTEGER,
Quarter_Desc VARCHAR(100),
Year_No INTEGER,
Leap_Year_Ind BOOLEAN,
Create_TS TIMESTAMP,
Create_User VARCHAR(10),
Last_Updt_TS TIMESTAMP,
Last_Updt_User VARCHAR(10),
PRIMARY KEY(Date_SK));
'''

CREATE_TABLE_DIM_TIME_SQL = '''
CREATE TABLE IF NOT EXISTS "CORE_DB".dim_time (
Time_SK INTEGER,
Hour INTEGER,
Minute INTEGER,
Second INTEGER,
Time_Desc VARCHAR(100),
Day_Phase_Desc VARCHAR(100),
Create_TS TIMESTAMP,
Create_User VARCHAR(10),
Last_Updt_TS TIMESTAMP,
Last_Updt_User VARCHAR(10),
PRIMARY KEY(Time_SK));
'''


CREATE_TABLE_DIM_ADDRESS_SQL = '''
CREATE TABLE IF NOT EXISTS "CORE_DB".dim_address (
Address_SK INTEGER,
City VARCHAR(100),
State VARCHAR(2),
Zipcode VARCHAR(10),
Create_TS TIMESTAMP,
Create_User VARCHAR(10),
Last_Updt_TS TIMESTAMP,
Last_Updt_User VARCHAR(10),
PRIMARY KEY(Address_SK));
'''


CREATE_TABLE_DIM_ACC_COND_SQL = '''
CREATE TABLE IF NOT EXISTS "CORE_DB".dim_acc_cond (
Acc_Cond_SK INTEGER,
Amenity BOOLEAN,
Bump BOOLEAN,
Crossing BOOLEAN,
Give_Way BOOLEAN,
Junction BOOLEAN,
No_Exit BOOLEAN,
Railway BOOLEAN,
Roundabout BOOLEAN,
Station BOOLEAN,
Stop BOOLEAN,
Traffic_Calming BOOLEAN,
Traffic_Signal BOOLEAN,
Turning_Loop BOOLEAN,
Sunrise_Sunset VARCHAR(10),
Civil_Twilight VARCHAR(10),
Nautical_Twilight VARCHAR(10),
Astronomical_Twilight VARCHAR(10),
Create_TS TIMESTAMP,
Create_User VARCHAR(10),
Last_Updt_TS TIMESTAMP,
Last_Updt_User VARCHAR(10),
PRIMARY KEY(Acc_Cond_SK));
'''


CREATE_TABLE_DIM_AIRPORT_SQL = '''
CREATE TABLE IF NOT EXISTS "CORE_DB".dim_airport (
Airport_SK INTEGER,
ID VARCHAR(10),
City VARCHAR(100),
Name VARCHAR(500),
Create_TS TIMESTAMP,
Create_User VARCHAR(10),
Last_Updt_TS TIMESTAMP,
Last_Updt_User VARCHAR(10),
PRIMARY KEY(Airport_SK));
'''

CREATE_TABLE_DIM_WTHR_COND_SQL = '''
CREATE TABLE IF NOT EXISTS "CORE_DB".dim_wthr_cond (
Weather_Cond_SK INTEGER,
Wind_Direction VARCHAR(10),
Weather_Condition VARCHAR(100),
Create_TS TIMESTAMP,
Create_User VARCHAR(10),
Last_Updt_TS TIMESTAMP,
Last_Updt_User VARCHAR(10),
PRIMARY KEY(Weather_Cond_SK));
'''

CREATE_TABLE_LKP_PROVIDER_SQL = '''
CREATE TABLE IF NOT EXISTS "CORE_DB".lkp_provider (
Provider_SK INTEGER,
Provider_Name VARCHAR(100),
Create_TS TIMESTAMP,
Create_User VARCHAR(10),
Last_Updt_TS TIMESTAMP,
Last_Updt_User VARCHAR(10),
PRIMARY KEY(Provider_SK));
'''

CREATE_TABLE_LKP_SOURCE_SQL = '''
CREATE TABLE IF NOT EXISTS "CORE_DB".lkp_source (
Source_SK INTEGER,
Source_Name VARCHAR(100),
Create_TS TIMESTAMP,
Create_User VARCHAR(10),
Last_Updt_TS TIMESTAMP,
Last_Updt_User VARCHAR(10),
PRIMARY KEY(Source_SK));
'''


CREATE_TABLE_FACT_ACCIDENT_SQL = '''
CREATE TABLE IF NOT EXISTS "CORE_DB".fact_accident (
Accident_SK INTEGER,
Source_FK INTEGER,
Address_FK INTEGER,
Airport_FK INTEGER,
Weather_Cond_FK INTEGER,
Acc_Cond_FK INTEGER,
Start_Date_FK INTEGER,
Start_Time_FK INTEGER,
End_Date_FK INTEGER,
End_Time_FK INTEGER,
Weather_Date_FK INTEGER,
Weather_Time_FK INTEGER,
Accident_ID VARCHAR(10),
TMC DECIMAL(5,1),
Severity INTEGER,
Start_TS TIMESTAMP,
End_TS TIMESTAMP,
Start_Lat DECIMAL(10,2),
Start_Lng DECIMAL(10,2),
End_Lat DECIMAL(10,2),
End_Lng DECIMAL(10,2),
Distance_mi DECIMAL(5,2),
Description VARCHAR(2000),
Timezone VARCHAR(20),
Weather_Timestamp TIMESTAMP,
Temperature_f DECIMAL(5,2),
Wind_Chill_f DECIMAL(5,2),
Humidity_pct DECIMAL(5,2),
Pressure_in DECIMAL(5,2),
Visibility_mi DECIMAL(5,2),
Wind_Speed_mph DECIMAL(5,2),
Precipitation_in DECIMAL(5,2),
Create_TS TIMESTAMP,
Create_User VARCHAR(10),
Last_Updt_TS TIMESTAMP,
Last_Updt_User VARCHAR(10),
PRIMARY KEY(Accident_SK));
'''


CREATE_TABLE_FACT_TRIP_SQL = '''
CREATE TABLE IF NOT EXISTS "CORE_DB".fact_trip (
Trip_SK INTEGER,
Provider_FK INTEGER,
Origin_Address_FK INTEGER,
Destination_Address_FK INTEGER,
Start_Date_FK INTEGER,
Start_Time_FK INTEGER,
DateCreated_Date_FK INTEGER,
DateCreated_Time_FK INTEGER,
Start1_Date_FK INTEGER,
Start1_Time_FK INTEGER,
End_Date_FK INTEGER,
End_Time_FK INTEGER,
Trip_ID VARCHAR(100),
Trip_Type VARCHAR(100),
External_ID VARCHAR(100),
Fare_Amt DECIMAL(10,2),
Gratuity_Amt DECIMAL(10,2),
Srchrg_Amt DECIMAL(10,2),
Extra_Fare_Amt DECIMAL(10,2),
Toll_Amt DECIMAL(10,2),
Total_Amt DECIMAL(10,2),
Pymt_Type INTEGER,
Origin_Lat DECIMAL(10,6),
Origin_Lon DECIMAL(10,6),
Dest_Lat DECIMAL(10,6),
Dest_Lon DECIMAL(10,6),
Milage DECIMAL(10,2),
Trip_Duration DECIMAL(20,4),
Misc VARCHAR(1000),
Create_TS TIMESTAMP,
Create_User VARCHAR(10),
Last_Updt_TS TIMESTAMP,
Last_Updt_User VARCHAR(10),
PRIMARY KEY(Trip_SK));
'''


# Section for COPY statements to load Source tables from source .csv files (tablename and file name parameterized)

COPY_SQL = """
COPY {}
FROM '{}'
CSV HEADER
DELIMITER ','
"""


# Section for INSERT/UPDATE statements to load Stage/Core tables from Source tables (ETL statements)

# Section for INSERT statements to load Stage tables from Source tables

INSERT_STG_ADDRESS_SQL = '''
INSERT INTO "STG_DB".stg_address
SELECT 
COALESCE(City,'') City,
COALESCE(State,'') State,
COALESCE(Zipcode,'') Zipcode,
CURRENT_TIMESTAMP,
'ETL_USR',
CURRENT_TIMESTAMP,
'ETL_USR'
FROM (
SELECT 
City,
State,
SUBSTR(Zipcode,1,5) Zipcode
FROM "SRC_DB".stg_src_us_accidents

UNION

SELECT
OriginCity,
OriginState,
SUBSTR(OriginZip,1,5)
FROM "SRC_DB".stg_src_dc_taxi_trips
WHERE LENGTH(OriginState) = 2
AND LENGTH(OriginCity) > 2
AND OriginZip <> '0'

UNION

SELECT
DestinationCity,
DestinationState,
SUBSTR(DestinationZip,1,5)
FROM "SRC_DB".stg_src_dc_taxi_trips
WHERE LENGTH(DestinationState) = 2
AND LENGTH(DestinationCity) > 2
AND DestinationZip <> '0'
) A
where zipcode ~ '^[0-9]+$';
'''

INSERT_STG_ACCIDENT_CONDITION_SQL = '''
INSERT INTO "STG_DB".stg_accident_condition
SELECT DISTINCT
COALESCE(Amenity,'False'),
COALESCE(Bump,'False'),
COALESCE(Crossing,'False'),
COALESCE(Give_Way,'False'),
COALESCE(Junction,'False'),
COALESCE(No_Exit,'False'),
COALESCE(Railway,'False'),
COALESCE(Roundabout,'False'),
COALESCE(Station,'False'),
COALESCE(Stop,'False'),
COALESCE(Traffic_Calming,'False'),
COALESCE(Traffic_Signal,'False'),
COALESCE(Turning_Loop,'False'),
COALESCE(Sunrise_Sunset,''),
COALESCE(Civil_Twilight,''),
COALESCE(Nautical_Twilight,''),
COALESCE(Astronomical_Twilight,''),
CURRENT_TIMESTAMP,
'ETL_USR',
CURRENT_TIMESTAMP,
'ETL_USR'
FROM "SRC_DB".stg_src_us_accidents;
'''

INSERT_STG_AIRPORT_SQL = '''
INSERT INTO "STG_DB".stg_airport
SELECT
ID,
City,
Name,
CURRENT_TIMESTAMP,
'ETL_USR',
CURRENT_TIMESTAMP,
'ETL_USR'
FROM "SRC_DB".stg_src_airport_codes;
'''

INSERT_STG_WEATHER_CONDITION_SQL = '''
INSERT INTO "STG_DB".stg_weather_condition
SELECT DISTINCT 
COALESCE(Wind_Direction,''),
COALESCE(Weather_Condition,''),
CURRENT_TIMESTAMP,
'ETL_USR',
CURRENT_TIMESTAMP,
'ETL_USR'
FROM "SRC_DB".stg_src_us_accidents;
'''

INSERT_STG_PROVIDER_SQL = '''
INSERT INTO "STG_DB".stg_provider
SELECT DISTINCT 
COALESCE(ProviderName,''),
CURRENT_TIMESTAMP,
'ETL_USR',
CURRENT_TIMESTAMP,
'ETL_USR'
FROM "SRC_DB".stg_src_dc_taxi_trips;
'''

INSERT_STG_SOURCE_SQL = '''
INSERT INTO "STG_DB".stg_source
SELECT DISTINCT 
Source,
CURRENT_TIMESTAMP,
'ETL_USR',
CURRENT_TIMESTAMP,
'ETL_USR'
FROM "SRC_DB".stg_src_us_accidents;
'''

INSERT_STG_ACCIDENT_SQL = '''
INSERT INTO "STG_DB".stg_accident
SELECT 
id,
source,
tmc,
severity,
start_time,
end_time,
start_lat,
start_lng,
end_lat,
end_lng,
distance_mi,
description,
number,
street,
side,
city,
county,
state,
zipcode,
country,
timezone,
airport_code,
weather_timestamp,
temperature_f,
wind_chill_f,
humidity_pct,
pressure_in,
visibility_mi,
wind_direction,
wind_speed_mph,
precipitation_in,
weather_condition,
amenity,
bump,
crossing,
give_way,
junction,
no_exit,
railway,
roundabout,
station,
stop,
traffic_calming,
traffic_signal,
turning_loop,
sunrise_sunset,
civil_twilight,
nautical_twilight,
astronomical_twilight,
CURRENT_TIMESTAMP,
'ETL_USR',
CURRENT_TIMESTAMP,
'ETL_USR'
FROM "SRC_DB".stg_src_us_accidents;
'''

INSERT_STG_TRIP_SQL = '''
INSERT INTO "STG_DB".stg_trip
SELECT 
type,
providername,
startdatetime,
datecreated,
id,
externalid,
fareamount,
gratuityamount,
surchargeamount,
extrafareamount,
tollamount,
totalamount,
paymenttype,
startdatetime1,
enddatetime,
originstreetnumber,
originstreetname,
origincity,
originstate,
originzip,
originlatitude,
originlongitude,
destinationstreetnumber,
destinationstreetname,
destinationcity,
destinationstate,
destinationzip,
destinationlatitude,
destinationlongitude,
milage,
duration,
misc,
CURRENT_TIMESTAMP,
'ETL_USR',
CURRENT_TIMESTAMP,
'ETL_USR'
FROM "SRC_DB".stg_src_dc_taxi_trips;
'''


# Section for INSERT/UPDATE statements to load Core tables from Stage tables

# Generate and load dates from 1/1/2010 till 12/31/2030
INSERT_UPDATE_DIM_DATE_SQL = '''
WITH RECURSIVE GEN_DIM_DATE AS (

SELECT
CAST(TO_CHAR(TO_DATE('01/01/2010','MM/DD/YYYY'),'YYYYMMDD') AS INTEGER) AS Date_SK,
TO_DATE('01/01/2010','MM/DD/YYYY') AS Date_DT,
DATE_PART('ISODOW',TO_DATE('01/01/2010','MM/DD/YYYY')) AS DOW_No,
INITCAP(TO_CHAR(TO_DATE('01/01/2010','MM/DD/YYYY'),'DAY')) AS Day_Name,
CASE WHEN DATE_PART('ISODOW',TO_DATE('01/01/2010','MM/DD/YYYY')) BETWEEN 6 AND 7 THEN False
ELSE True 
END AS Week_Day_Ind,
DATE_PART('WEEK',TO_DATE('01/01/2010','MM/DD/YYYY')) AS Week_No,
DATE_PART('MONTH',TO_DATE('01/01/2010','MM/DD/YYYY')) AS Month_No,
INITCAP(TO_CHAR(TO_DATE('01/01/2010','MM/DD/YYYY'),'MONTH')) AS Month_Name,
DATE_PART('QUARTER',TO_DATE('01/01/2010','MM/DD/YYYY')) AS Quarter_No,
'Quarter - ' || DATE_PART('QUARTER',TO_DATE('01/01/2010','MM/DD/YYYY')) AS Quarter_Desc,
DATE_PART('YEAR',TO_DATE('01/01/2010','MM/DD/YYYY')) AS Year_No,
CASE WHEN (CAST(DATE_PART('YEAR',TO_DATE('01/01/2010','MM/DD/YYYY')) AS INTEGER) % 4 = 0) 
AND ((CAST(DATE_PART('YEAR',TO_DATE('01/01/2010','MM/DD/YYYY')) AS INTEGER) % 100 <> 0) 
	 OR (CAST(DATE_PART('YEAR',TO_DATE('01/01/2010','MM/DD/YYYY')) AS INTEGER) % 400 = 0)) THEN True
ELSE 'False'
END AS Leap_Year_Ind,
CURRENT_TIMESTAMP AS Create_TS,
'ETL_USR' AS Create_User,
CURRENT_TIMESTAMP AS Last_Updt_TS,
'ETL_USR' AS Last_Updt_User

UNION ALL

SELECT
CAST(TO_CHAR(Date_DT+1,'YYYYMMDD') AS INTEGER) AS Date_SK,
Date_DT+1 AS Date_DT,
DATE_PART('ISODOW',Date_DT+1) AS DOW_No,
INITCAP(TO_CHAR(Date_DT+1,'DAY')) AS Day_Name,
CASE WHEN DATE_PART('ISODOW',Date_DT+1) BETWEEN 6 AND 7 THEN False
ELSE True 
END AS Week_Day_Ind,
DATE_PART('WEEK',Date_DT+1) AS Week_No,
DATE_PART('MONTH',Date_DT+1) AS Month_No,
INITCAP(TO_CHAR(Date_DT+1,'MONTH')) AS Month_Name,
DATE_PART('QUARTER',Date_DT+1) AS Quarter_No,
'Quarter - ' || DATE_PART('QUARTER',Date_DT+1) AS Quarter_Desc,
DATE_PART('YEAR',Date_DT+1) AS Year_No,
CASE WHEN (CAST(DATE_PART('YEAR',Date_DT+1) AS INTEGER) % 4 = 0) 
AND ((CAST(DATE_PART('YEAR',Date_DT+1) AS INTEGER) % 100 <> 0) 
	 OR (CAST(DATE_PART('YEAR',Date_DT+1) AS INTEGER) % 400 = 0)) THEN True
ELSE 'False'
END AS Leap_Year_Ind,
CURRENT_TIMESTAMP AS Create_TS,
'ETL_USR' AS Create_User,
CURRENT_TIMESTAMP AS Last_Updt_TS,
'ETL_USR' AS Last_Updt_User
FROM GEN_DIM_DATE
WHERE Date_DT < TO_DATE('12/31/2030','MM/DD/YYYY')
)

INSERT INTO "CORE_DB".dim_date
SELECT * FROM GEN_DIM_DATE
WHERE Date_SK NOT IN (
SELECT Date_SK FROM "CORE_DB".dim_date
);
'''

# Generate and load seconds data from 12 AM to 12 PM (86400 in total)
INSERT_UPDATE_DIM_TIME_SQL = '''
WITH RECURSIVE GEN_DIM_TIME AS (

SELECT 
CURRENT_DATE + INTERVAL '0 SECOND' AS MidNight,
0 Time_SK,
0 AS Hour,
0 AS Minute,
0 AS Second,
'00:00:00' Time_Desc,
'Mid Night' Day_Phase_Desc,
CURRENT_TIMESTAMP AS Create_TS,
'ETL_USR' AS Create_User,
CURRENT_TIMESTAMP AS Last_Updt_TS,
'ETL_USR' AS Last_Updt_User

UNION ALL

SELECT 
MidNight + INTERVAL '1 SECOND' MidNight,
Time_SK+1 AS Time_SK,
CAST(DATE_PART('HOUR',MidNight + INTERVAL '1 SECOND') AS INTEGER) AS Hour,
CAST(DATE_PART('MINUTE',MidNight + INTERVAL '1 SECOND') AS INTEGER) AS Minute,
CAST(DATE_PART('SECOND',MidNight + INTERVAL '1 SECOND') AS INTEGER) AS Second,
CASE WHEN CAST(DATE_PART('HOUR',MidNight + INTERVAL '1 SECOND') AS INTEGER) < 10 THEN '0' ELSE '' END ||
CAST(CAST(DATE_PART('HOUR',MidNight + INTERVAL '1 SECOND') AS INTEGER) AS VARCHAR(2)) || ':' || 
CASE WHEN CAST(DATE_PART('MINUTE',MidNight + INTERVAL '1 SECOND') AS INTEGER) < 10 THEN '0' ELSE '' END ||
CAST(CAST(DATE_PART('MINUTE',MidNight + INTERVAL '1 SECOND') AS INTEGER) AS VARCHAR(2)) || ':' || 
CASE WHEN CAST(DATE_PART('SECOND',MidNight + INTERVAL '1 SECOND') AS INTEGER) < 10 THEN '0' ELSE '' END ||
CAST(CAST(DATE_PART('SECOND',MidNight + INTERVAL '1 SECOND') AS INTEGER) AS VARCHAR(2)) Time_Desc,
CASE WHEN CAST(DATE_PART('HOUR',MidNight + INTERVAL '1 SECOND') AS INTEGER) BETWEEN 0 AND 4 THEN 'Mid Night'
	 WHEN CAST(DATE_PART('HOUR',MidNight + INTERVAL '1 SECOND') AS INTEGER) BETWEEN 5 AND 7 THEN 'Early Morning'
	 WHEN CAST(DATE_PART('HOUR',MidNight + INTERVAL '1 SECOND') AS INTEGER) BETWEEN 8 AND 11 THEN 'Morning'
	 WHEN CAST(DATE_PART('HOUR',MidNight + INTERVAL '1 SECOND') AS INTEGER) BETWEEN 12 AND 16 THEN 'Afternoon'
	 WHEN CAST(DATE_PART('HOUR',MidNight + INTERVAL '1 SECOND') AS INTEGER) BETWEEN 17 AND 19 THEN 'Evening'
	 WHEN CAST(DATE_PART('HOUR',MidNight + INTERVAL '1 SECOND') AS INTEGER) BETWEEN 20 AND 23 THEN 'Night'
END AS Day_Phase_Desc,
CURRENT_TIMESTAMP AS Create_TS,
'ETL_USR' AS Create_User,
CURRENT_TIMESTAMP AS Last_Updt_TS,
'ETL_USR' AS Last_Updt_User
FROM GEN_DIM_TIME
WHERE Time_SK < 86399
)

INSERT INTO "CORE_DB".dim_time
SELECT
Time_SK,
Hour,
Minute,
Second,
Time_Desc,
Day_Phase_Desc,
Create_TS,
Create_User,
Last_Updt_TS,
Last_Updt_User
FROM GEN_DIM_TIME
WHERE Time_SK NOT IN (
SELECT Time_SK FROM "CORE_DB".dim_time
);
'''

INSERT_UPDATE_DIM_ADDRESS_SQL = '''
INSERT INTO "CORE_DB".dim_address
SELECT 
(SELECT COALESCE(MAX(Address_SK),0) MAX_Address_SK FROM "CORE_DB".dim_address) + 
ROW_NUMBER() OVER(ORDER BY stg.City, stg.State, stg.ZipCode) Address_SK,
stg.City,
stg.State,
stg.Zipcode,
CURRENT_TIMESTAMP,
'ETL_USR',
CURRENT_TIMESTAMP,
'ETL_USR'
FROM "STG_DB".stg_address stg
LEFT OUTER JOIN "CORE_DB".dim_address dim_add
ON COALESCE(stg.City,'') = COALESCE(dim_add.City,'')
AND COALESCE(stg.State,'') = COALESCE(dim_add.State,'')
AND COALESCE(stg.Zipcode,'') = COALESCE(dim_add.Zipcode,'')
WHERE dim_add.Address_SK IS NULL;
'''

INSERT_UPDATE_DIM_ACC_COND_SQL = '''
INSERT INTO "CORE_DB".dim_acc_cond
SELECT 
(SELECT COALESCE(MAX(Acc_Cond_SK),0) MAX_Acc_Cond_SK FROM "CORE_DB".dim_acc_cond) + 
ROW_NUMBER() OVER(ORDER BY stg.amenity) Acc_Cond_SK,
stg.amenity,
stg.bump,
stg.crossing,
stg.give_way,
stg.junction,
stg.no_exit,
stg.railway,
stg.roundabout,
stg.station,
stg.stop,
stg.traffic_calming,
stg.traffic_signal,
stg.turning_loop,
stg.sunrise_sunset,
stg.civil_twilight,
stg.nautical_twilight,
stg.astronomical_twilight,
CURRENT_TIMESTAMP,
'ETL_USR',
CURRENT_TIMESTAMP,
'ETL_USR'
FROM "STG_DB".stg_accident_condition stg
LEFT OUTER JOIN "CORE_DB".dim_acc_cond dim
ON COALESCE(stg.amenity,'False') = COALESCE(dim.amenity,'False')
AND COALESCE(stg.bump,'False') = COALESCE(dim.bump,'False')
AND COALESCE(stg.crossing,'False') = COALESCE(dim.crossing,'False')
AND COALESCE(stg.give_way,'False') = COALESCE(dim.give_way,'False')
AND COALESCE(stg.junction,'False') = COALESCE(dim.junction,'False')
AND COALESCE(stg.no_exit,'False') = COALESCE(dim.no_exit,'False')
AND COALESCE(stg.railway,'False') = COALESCE(dim.railway,'False')
AND COALESCE(stg.roundabout,'False') = COALESCE(dim.roundabout,'False')
AND COALESCE(stg.station,'False') = COALESCE(dim.station,'False')
AND COALESCE(stg.stop,'False') = COALESCE(dim.stop,'False')
AND COALESCE(stg.traffic_calming,'False') = COALESCE(dim.traffic_calming,'False')
AND COALESCE(stg.traffic_signal,'False') = COALESCE(dim.traffic_signal,'False')
AND COALESCE(stg.turning_loop,'False') = COALESCE(dim.turning_loop,'False')
AND COALESCE(stg.sunrise_sunset,'False') = COALESCE(dim.sunrise_sunset,'False')
AND COALESCE(stg.civil_twilight,'False') = COALESCE(dim.civil_twilight,'False')
AND COALESCE(stg.nautical_twilight,'False') = COALESCE(dim.nautical_twilight,'False')
AND COALESCE(stg.astronomical_twilight,'False') = COALESCE(dim.astronomical_twilight,'False')
WHERE dim.Acc_Cond_SK IS NULL;
'''

INSERT_UPDATE_DIM_AIRPORT_SQL = '''
INSERT INTO "CORE_DB".dim_airport
SELECT 
(SELECT COALESCE(MAX(Airport_SK),0) MAX_Airport_SK  FROM "CORE_DB".dim_airport) + 
ROW_NUMBER() OVER(ORDER BY stg.ID) Airport_SK,
stg.ID,
stg.City,
stg.Name,
CURRENT_TIMESTAMP,
'ETL_USR',
CURRENT_TIMESTAMP,
'ETL_USR'
FROM "STG_DB".stg_airport stg
LEFT OUTER JOIN "CORE_DB".dim_airport dim
ON COALESCE(stg.ID,'') = COALESCE(dim.ID,'')
WHERE dim.Airport_SK IS NULL;
'''

INSERT_UPDATE_DIM_WTHR_COND_SQL = '''
INSERT INTO "CORE_DB".dim_wthr_cond
SELECT 
(SELECT COALESCE(MAX(Weather_Cond_SK),0) MAX_Weather_Cond_SK FROM "CORE_DB".dim_wthr_cond) + 
ROW_NUMBER() OVER(ORDER BY stg.Wind_Direction) Weather_Cond_SK,
stg.Wind_Direction,
stg.Weather_Condition,
CURRENT_TIMESTAMP,
'ETL_USR',
CURRENT_TIMESTAMP,
'ETL_USR'
FROM "STG_DB".stg_weather_condition stg
LEFT OUTER JOIN "CORE_DB".dim_wthr_cond dim
ON COALESCE(stg.Wind_Direction,'') = COALESCE(dim.Wind_Direction,'')
AND COALESCE(stg.Weather_Condition,'') = COALESCE(dim.Weather_Condition,'')
WHERE dim.Weather_Cond_SK IS NULL;
'''

INSERT_UPDATE_LKP_PROVIDER_SQL = '''
INSERT INTO "CORE_DB".lkp_provider
SELECT 
(SELECT COALESCE(MAX(Provider_SK),0) MAX_Provider_SK FROM "CORE_DB".lkp_provider) + 
ROW_NUMBER() OVER(ORDER BY stg.Provider_Name) Provider_SK,
stg.Provider_Name,
CURRENT_TIMESTAMP,
'ETL_USR',
CURRENT_TIMESTAMP,
'ETL_USR'
FROM "STG_DB".stg_provider stg
LEFT OUTER JOIN "CORE_DB".lkp_provider dim
ON COALESCE(stg.Provider_Name,'') = COALESCE(dim.Provider_Name,'')
WHERE dim.Provider_SK IS NULL;
'''

INSERT_UPDATE_LKP_SOURCE_SQL = '''
INSERT INTO "CORE_DB".lkp_source
SELECT 
(SELECT COALESCE(MAX(Source_SK),0) MAX_Source_SK FROM "CORE_DB".lkp_source) + 
ROW_NUMBER() OVER(ORDER BY stg.Source_Name) Source_SK,
stg.Source_Name,
CURRENT_TIMESTAMP,
'ETL_USR',
CURRENT_TIMESTAMP,
'ETL_USR'
FROM "STG_DB".stg_source stg
LEFT OUTER JOIN "CORE_DB".lkp_source dim
ON COALESCE(stg.Source_Name,'') = COALESCE(dim.Source_Name,'')
WHERE dim.Source_SK IS NULL;
'''

INSERT_UPDATE_FACT_ACCIDENT_SQL = '''
INSERT INTO "CORE_DB".fact_accident
SELECT 
(SELECT COALESCE(MAX(Accident_SK),0) MAX_Accident_SK  FROM "CORE_DB".fact_accident) + 
ROW_NUMBER() OVER(ORDER BY stg.ID) Accident_SK,
COALESCE(dim_src.Source_SK,-1) Source_FK,
COALESCE(dim_add.Address_SK-1) Address_FK,
COALESCE(dim_ap.Airport_SK,-1) Airport_FK,
COALESCE(dim_wc.Weather_Cond_SK,-1) Weather_Cond_FK,
COALESCE(dim_ac.Acc_Cond_SK,-1) Acc_Cond_FK,
CAST(TO_CHAR(CAST(stg.Start_Time AS DATE),'YYYYMMDD') AS INTEGER) Start_Date_FK,
dim_st.Time_SK Start_Time_FK,
CAST(TO_CHAR(CAST(stg.End_Time AS DATE),'YYYYMMDD') AS INTEGER) End_Date_FK,
dim_et.Time_SK End_Time_FK,
CAST(TO_CHAR(CAST(stg.Weather_TImestamp AS DATE),'YYYYMMDD') AS INTEGER) Weather_Date_FK,
dim_wt.Time_SK Weather_Time_FK,
stg.ID,
stg.TMC,
stg.Severity,
stg.Start_Time,
stg.End_Time,
stg.Start_Lat,
stg.Start_Lng,
stg.End_Lat,
stg.End_Lng,
stg.Distance_mi,
stg.Description,
stg.Timezone,
stg.Weather_Timestamp,
stg.Temperature_f,
stg.Wind_Chill_f,
stg.Humidity_pct,
stg.Pressure_in,
stg.Visibility_mi,
stg.Wind_Speed_mph,
stg.Precipitation_in,
CURRENT_TIMESTAMP,
'ETL_USR',
CURRENT_TIMESTAMP,
'ETL_USR'
FROM "STG_DB".stg_accident stg
LEFT OUTER JOIN "CORE_DB".fact_accident fact
ON COALESCE(stg.ID,'') = COALESCE(fact.Accident_ID,'')
LEFT OUTER JOIN "CORE_DB".lkp_source dim_src
ON COALESCE(stg.Source,'') = COALESCE(dim_src.Source_Name,'')
LEFT OUTER JOIN "CORE_DB".dim_address dim_add
ON COALESCE(stg.City,'') = COALESCE(dim_add.City,'')
AND COALESCE(stg.State,'') = COALESCE(dim_add.State,'')
AND COALESCE(stg.Zipcode,'') = COALESCE(dim_add.Zipcode,'')
LEFT OUTER JOIN "CORE_DB".dim_airport dim_ap
ON COALESCE(stg.airport_code,'') = COALESCE(dim_ap.ID,'')
LEFT OUTER JOIN "CORE_DB".dim_wthr_cond dim_wc
ON COALESCE(stg.Wind_Direction,'') = COALESCE(dim_wc.Wind_Direction,'')
AND COALESCE(stg.Weather_Condition,'') = COALESCE(dim_wc.Weather_Condition,'')
LEFT OUTER JOIN "CORE_DB".dim_acc_cond dim_ac
ON COALESCE(stg.amenity,'False') = COALESCE(dim_ac.amenity,'False')
AND COALESCE(stg.bump,'False') = COALESCE(dim_ac.bump,'False')
AND COALESCE(stg.crossing,'False') = COALESCE(dim_ac.crossing,'False')
AND COALESCE(stg.give_way,'False') = COALESCE(dim_ac.give_way,'False')
AND COALESCE(stg.junction,'False') = COALESCE(dim_ac.junction,'False')
AND COALESCE(stg.no_exit,'False') = COALESCE(dim_ac.no_exit,'False')
AND COALESCE(stg.railway,'False') = COALESCE(dim_ac.railway,'False')
AND COALESCE(stg.roundabout,'False') = COALESCE(dim_ac.roundabout,'False')
AND COALESCE(stg.station,'False') = COALESCE(dim_ac.station,'False')
AND COALESCE(stg.stop,'False') = COALESCE(dim_ac.stop,'False')
AND COALESCE(stg.traffic_calming,'False') = COALESCE(dim_ac.traffic_calming,'False')
AND COALESCE(stg.traffic_signal,'False') = COALESCE(dim_ac.traffic_signal,'False')
AND COALESCE(stg.turning_loop,'False') = COALESCE(dim_ac.turning_loop,'False')
AND COALESCE(stg.sunrise_sunset,'') = COALESCE(dim_ac.sunrise_sunset,'')
AND COALESCE(stg.civil_twilight,'') = COALESCE(dim_ac.civil_twilight,'')
AND COALESCE(stg.nautical_twilight,'') = COALESCE(dim_ac.nautical_twilight,'')
AND COALESCE(stg.astronomical_twilight,'') = COALESCE(dim_ac.astronomical_twilight,'')
LEFT OUTER JOIN "CORE_DB".dim_time dim_st
ON TO_CHAR(stg.Start_Time,'HH24:MI:SS') = dim_st.time_desc
LEFT OUTER JOIN "CORE_DB".dim_time dim_et
ON TO_CHAR(stg.End_Time,'HH24:MI:SS') = dim_et.time_desc
LEFT OUTER JOIN "CORE_DB".dim_time dim_wt
ON TO_CHAR(stg.Weather_TImestamp,'HH24:MI:SS') = dim_wt.time_desc
WHERE fact.Accident_SK IS NULL;
'''

INSERT_UPDATE_FACT_TRIP_SQL = '''
INSERT INTO "CORE_DB".fact_trip
SELECT 
(SELECT COALESCE(MAX(Trip_SK),0) MAX_Trip_SK  FROM "CORE_DB".fact_trip) + 
ROW_NUMBER() OVER(ORDER BY stg.ID) Trip_SK,
COALESCE(dim_prv.Provider_SK,-1) Provider_FK,
COALESCE(dim_add_o.Address_SK,-1) Origin_Address_FK,
COALESCE(dim_add_d.Address_SK,-1) Destination_Address_FK,
CAST(TO_CHAR(CAST(stg.StartDateTime AS DATE),'YYYYMMDD') AS INTEGER) Start_Date_FK,
dim_st.Time_SK Start_Time_FK,
CAST(TO_CHAR(CAST(stg.DateCreated AS DATE),'YYYYMMDD') AS INTEGER) DateCreated_Date_FK,
dim_dct.Time_SK DateCreated_Time_FK,
CAST(TO_CHAR(CAST(stg.StartDateTime1 AS DATE),'YYYYMMDD') AS INTEGER) Start1_Date_FK,
dim_s1t.Time_SK Start1_Time_FK,
CAST(TO_CHAR(CAST(stg.EndDateTime AS DATE),'YYYYMMDD') AS INTEGER) End_Date_FK,
dim_et.Time_SK End_Time_FK,
stg.ID,
stg.Type,
stg.ExternalID,
stg.FareAmount,
stg.GratuityAmount,
stg.SurchargeAmount,
stg.ExtraFareAmount,
stg.TollAmount,
stg.TotalAmount,
stg.PaymentType,
stg.OriginLatitude,
stg.OriginLongitude,
stg.DestinationLatitude,
stg.DestinationLongitude,
stg.Milage,
stg.Duration,
stg.Misc,
CURRENT_TIMESTAMP,
'ETL_USR',
CURRENT_TIMESTAMP,
'ETL_USR'
FROM "STG_DB".stg_trip stg
LEFT OUTER JOIN "CORE_DB".fact_trip fact
ON COALESCE(stg.ID,'') = COALESCE(fact.Trip_ID,'')
LEFT OUTER JOIN "CORE_DB".lkp_provider dim_prv
ON COALESCE(ProviderName,'') = COALESCE(dim_prv.Provider_Name,'')
LEFT OUTER JOIN "CORE_DB".dim_address dim_add_o
ON COALESCE(stg.OriginCity,'') = COALESCE(dim_add_o.City,'')
AND COALESCE(stg.OriginState,'') = COALESCE(dim_add_o.State,'')
AND COALESCE(stg.OriginZip,'') = COALESCE(dim_add_o.Zipcode,'')
LEFT OUTER JOIN "CORE_DB".dim_address dim_add_d
ON COALESCE(stg.DestinationCity,'') = COALESCE(dim_add_d.City,'')
AND COALESCE(stg.DestinationState,'') = COALESCE(dim_add_d.State,'')
AND COALESCE(stg.DestinationZip,'') = COALESCE(dim_add_d.Zipcode,'')
LEFT OUTER JOIN "CORE_DB".dim_time dim_st
ON TO_CHAR(stg.StartDateTime,'HH24:MI:SS') = dim_st.time_desc
LEFT OUTER JOIN "CORE_DB".dim_time dim_dct
ON TO_CHAR(stg.DateCreated,'HH24:MI:SS') = dim_dct.time_desc
LEFT OUTER JOIN "CORE_DB".dim_time dim_s1t
ON TO_CHAR(stg.StartDateTime1,'HH24:MI:SS') = dim_s1t.time_desc
LEFT OUTER JOIN "CORE_DB".dim_time dim_et
ON TO_CHAR(stg.EndDateTime,'HH24:MI:SS') = dim_et.time_desc
WHERE fact.Trip_SK IS NULL;
'''

CREATE_INDEXES = '''
CREATE INDEX IF NOT EXISTS idx_Address ON "STG_DB".stg_accident (City, State, Zipcode);
CREATE INDEX IF NOT EXISTS idx_AccCond ON "STG_DB".stg_accident (Amenity, Bump, Crossing, Give_Way, Junction, No_Exit, Railway, Roundabout, Station, Stop, Traffic_Calming, Traffic_Signal, Turning_Loop, Sunrise_Sunset, Civil_Twilight, Nautical_Twilight, Astronomical_Twilight);
CREATE INDEX IF NOT EXISTS idx_AccidentID ON "CORE_DB".fact_accident (Accident_ID);

CREATE INDEX IF NOT EXISTS idx_OriginAddress ON "STG_DB".stg_trip (OriginCity, OriginState, OriginZip);
CREATE INDEX IF NOT EXISTS idx_DestinationAddress ON "STG_DB".stg_trip (DestinationCity, DestinationState, DestinationZip);
CREATE INDEX IF NOT EXISTS idx_TripID ON "CORE_DB".fact_trip (Trip_ID);
'''

# Section with strings related to validation after tables are loaded

VALIDATE_ROW_CNT_SQL = '''
SELECT COUNT(*) CNT FROM {}
'''

VALIDATE_NAT_KEYS_DUP_SQL = '''
SELECT COUNT(*) CNT FROM {}
GROUP BY {}
HAVING COUNT(*) > 1
LIMIT 1
'''

# Dictionary which has tablename and it's corresponding minimum rows expected in the table
DICT_ROW_CNT_VALDTN = [
{
'table' : '"SRC_DB".STG_SRC_AIRPORT_CODES',
'min_row_cnt' : 4760
},
{
'table' : '"SRC_DB".STG_SRC_DC_TAXI_TRIPS',
'min_row_cnt' : 2099990
},
{
'table' : '"SRC_DB".STG_SRC_US_ACCIDENTS',
'min_row_cnt' : 2974330
},
{
'table' : '"STG_DB".STG_ADDRESS',
'min_row_cnt' : 26100
},
{
'table' : '"STG_DB".STG_ACCIDENT_CONDITION',
'min_row_cnt' : 860
},
{
'table' : '"STG_DB".STG_AIRPORT',
'min_row_cnt' : 4700
},
{
'table' : '"STG_DB".STG_WEATHER_CONDITION',
'min_row_cnt' : 1360
},
{
'table' : '"STG_DB".STG_PROVIDER',
'min_row_cnt' : 10
},
{
'table' : '"STG_DB".STG_SOURCE',
'min_row_cnt' : 3
},
{
'table' : '"STG_DB".STG_ACCIDENT',
'min_row_cnt' : 2974330
},
{
'table' : '"STG_DB".STG_TRIP',
'min_row_cnt' : 2099990
},

{
'table' : '"CORE_DB".DIM_DATE',
'min_row_cnt' : 7670
},
{
'table' : '"CORE_DB".DIM_TIME',
'min_row_cnt' : 86400
},
{
'table' : '"CORE_DB".DIM_ADDRESS',
'min_row_cnt' : 26100
},
{
'table' : '"CORE_DB".DIM_ACC_COND',
'min_row_cnt' : 860
},
{
'table' : '"CORE_DB".DIM_AIRPORT',
'min_row_cnt' : 4760
},
{
'table' : '"CORE_DB".DIM_WTHR_COND',
'min_row_cnt' : 1360
},
{
'table' : '"CORE_DB".LKP_PROVIDER',
'min_row_cnt' : 10
},
{
'table' : '"CORE_DB".LKP_SOURCE',
'min_row_cnt' : 3
},
{
'table' : '"CORE_DB".FACT_ACCIDENT',
'min_row_cnt' : 86400
},
{
'table' : '"CORE_DB".FACT_TRIP',
'min_row_cnt' : 2099990
}
]

# Dictionary which has tablename and it's corresponding natural key columns (columns on which the rows will be unique as per source)
DICT_NAT_KEYS_DUP_VALDTN = [
{
'table' : '"CORE_DB".DIM_DATE',
'natural_key' : 'Date_DT'
},
{
'table' : '"CORE_DB".DIM_TIME',
'natural_key' : 'Time_Desc'
},
{
'table' : '"CORE_DB".DIM_ADDRESS',
'natural_key' : 'City, State, Zipcode'
},
{
'table' : '"CORE_DB".DIM_ACC_COND',
'natural_key' : 'Amenity, Bump, Crossing, Give_Way, Junction, No_Exit, Railway, Roundabout, Station, Stop, Traffic_Calming, Traffic_Signal, Turning_Loop, Sunrise_Sunset, Civil_Twilight, Nautical_Twilight, Astronomical_Twilight'
},
{
'table' : '"CORE_DB".DIM_AIRPORT',
'natural_key' : 'ID'
},
{
'table' : '"CORE_DB".DIM_WTHR_COND',
'natural_key' : 'Wind_Direction, Weather_Condition'
},
{
'table' : '"CORE_DB".LKP_PROVIDER',
'natural_key' : 'Provider_Name'
},
{
'table' : '"CORE_DB".LKP_SOURCE',
'natural_key' : 'Source_Name'
},
{
'table' : '"CORE_DB".FACT_ACCIDENT',
'natural_key' : 'Accident_ID'
},
{
'table' : '"CORE_DB".FACT_TRIP',
'natural_key' : 'Trip_ID'
}
]
