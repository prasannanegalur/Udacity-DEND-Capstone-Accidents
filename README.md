## Udacity-DEND-Capstone-Accidents
Code for final capstone project for Udacity Data Engineering Nano Degree

## Objective:
The objective of this project is to create a data model and ETL flow to be able to provide analytical capability for analysing traffic accidents. The outcome of the analysis will provide insights to what conditions may cause maximum accidents and what steps can be taken to minimize such incidents. The data spans across multiple years and has close to 3 million rows.

## Datasource Details:
The data is gathered from Kaggle and below are the details.
* US Accidents from Feb 2016 to Dec 2019. [US_Accidents_Dec19.csv](https://www.kaggle.com/sobhanmoosavi/us-accidents)
* Washington DC taxi cab trips for 2017. [taxi_final.csv](https://www.kaggle.com/bvc5283/dc-taxi-trips)

## Supporting Datasets:
List of US airport codes is are got through 2 websites as below-
* List of US State Codes [US State Codes](https://developers.google.com/public-data/docs/canonical/states_csv)
* List of airport codes for each US State [Airport Codes](https://www.airnav.com/airports/us/)

Below are snapshots of these datasets-
Accidents
![Accidents](https://github.com/prasannanegalur/Udacity-DEND-Capstone-Accidents/tree/master/images/Accidents.jpg)

Trips
![Trips](https://github.com/prasannanegalur/Udacity-DEND-Capstone-Accidents/tree/master/images/Trips.jpg)

Airport Codes
![Airports](https://github.com/prasannanegalur/Udacity-DEND-Capstone-Accidents/tree/master/images/Airports.jpg)

## Project Scope:
The scope of the project is to create a data pipeline which accepts the source files, process and clean them, transform as per the the data model and load them in dimension and fact tables. The source files will be read from local machine, use apache airflow and python to create a data pipeline and eventually load the processed and transformed data into the data model created in local postgresql database.

## Technologies used:
- Apache Airflow
- Python
- PostgreSQL

## Data Model
The data model consists of 8 dimension and 2 fact tables. The Data Model is as below:
![Data-Model](https://github.com/prasannanegalur/Udacity-DEND-Capstone-Accidents/tree/master/images/Data_Model.jpg)

## Design of Data Pipelines
The data pipeline is created in Apache Airflow. Below are the various stages - 
* Generate airport codes file
* Create tables in the local postgres database (src, stg and core tables)
* Load the src tables using the source datasets (.csv files)
* Insert data into stg tables using the data from src tables
* Insert data into lkp/dim tables
* Insert data into fact tables
* Include data quality checks (row counts and duplicate rows validations)

Below is the snapshot of the pipeline -
![Airflow-DAG](https://github.com/prasannanegalur/Udacity-DEND-Capstone-Accidents/tree/master/images/ETL_Pipeline.jpg)


## Handling Special Scenarios

### If the data was increased by 100x.

As the size increases, handling data from local csv files may not be feasible. In that case, moving the files to Amazon S3 and loading them using Spark would be a better option.

### If the pipelines would be run on a daily basis by 7 am every day.

This can be handled using the existing Airflow DAG using the scheduling feature of Airflow.

### If the database needed to be accessed by 100+ people.

Local postgresql database may not be able to handle the load of 100+ people. In that case instead of using local postgresql, using Amazon redshift would be able to handle this as redshift has clustering abilities and is highly scalable.
