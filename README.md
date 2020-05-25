## Udacity-DEND-Capstone-Accidents
Code for final capstone project for Udacity Data Engineering Nano Degree

## Objective:
The objective of this project is to create a data model and ETL flow which will be able to provide analytical capability using traffic accidents data. The outcome of the analysis will provide insights to what conditions may cause maximum accidents and what steps can be taken to minimize such incidents and make the roads much safer. The data spans across multiple years and has close to 3 million rows.

## Dataset Details:
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
The scope of the project is to create a data pipeline which accepts the source files, processes, cleans and transforms them as per the the data model and loads them in dimension and fact tables. The source files will be read from local machine, using apache airflow and python to create a data pipeline and eventually load the data into the tables created in local postgresql database.

## Technologies used:
- Apache Airflow
- Python
- PostgreSQL

## Data Model
The data model consists of 8 dimension and 2 fact tables. The Data Model is as below:
![Data-Model](https://github.com/prasannanegalur/Udacity-DEND-Capstone-Accidents/tree/master/images/Data_Model.jpg)

## Design of Data Pipeline
The data pipeline is created in Apache Airflow. Below are the various steps. 
* Generate airport codes file
* Create tables in the local postgres database (src, stg and core tables)
* Load the src tables using the source datasets (.csv files)
* Insert data into stg tables using the data from src tables
* Insert data into lkp/dim tables
* Insert data into fact tables
* Create necessary index on the tables to improve query performance
* Include data quality checks at appropriate stages (row counts and duplicate rows validation)

Below is the snapshot of the pipeline -
![Airflow-DAG](https://github.com/prasannanegalur/Udacity-DEND-Capstone-Accidents/tree/master/images/ETL_Pipeline.jpg)


## Handling of Special Scenarios

### If the data was increased by 100x.

Loading of data from local csv files may not be feasible. In that case, moving the source files to Amazon S3 and loading them using Spark would be a better option.

### If the pipelines were run on a daily basis by 7AM.

This can be handled using the existing Airflow DAG using the scheduling features of the Airflow.

### If the database needed to be accessed by 100+ people.

Local postgresql database may not be able to handle the load of 100+ concurrent users. In that case, using Amazon Redshift would resolve this scenario as Redshift has clustering abilities and is highly scalable.
