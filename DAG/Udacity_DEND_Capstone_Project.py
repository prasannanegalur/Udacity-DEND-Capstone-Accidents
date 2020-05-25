'''
This module performs ETL operations using apache airflow modules.
Below are the steps that the module performs:
1. Generate airport codes file (using 2 websites as the source)
2. Create necessary tables in the local postgres database (src, stg and core tables)
3. Load the src tables using the source datasets (.csv files)
4. Insert data into stg tables using the data from src tables
5. Insert data into lkp/dim tables
6. Insert data into fact tables
7. Include data quality checks (row counts validation at each stage)
'''

# Import necessary modules
import datetime
import logging
import pandas as pd
from pandas import DataFrame
import time
import sys

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

# import the SQLs module, which contains necessary SQL statements to perform ETL operations
import SQLs
from SQLs import *

# create dataframe out of a dictionary, which has necessary information to perform row count validation
DF_ROW_CNT_VALDTN = DataFrame(DICT_ROW_CNT_VALDTN)

# create dataframe out of a dictionary, which has necessary information to perform duplicate row count validation
DF_NAT_KEYS_DUP_VALDTN = DataFrame(DICT_NAT_KEYS_DUP_VALDTN)

# Instantiate the DAG with a name and start_date
dag = DAG(
'udacity-dend-capstone-project',
start_date=datetime.datetime.now()
)

# function to create airports file
def create_airports_file():
    '''
    The function creates .csv files with list of all US airport codes.
    It utilizes 2 websites to arrive at the final list.
    First website provides list of all US state codes.
    Second website provides list of all airport codes for each state code
    '''

    us_state_codes_df = pd.read_html('https://developers.google.com/public-data/docs/canonical/states_csv')
    us_state_codes = list(us_state_codes_df[0]['state'])
    airport_codes = DataFrame(columns=['ID', 'City', 'Name'])
    for state_code in us_state_codes:
        try:
            # wait for 5 seconds before the next call; the code will fail with service unavailable if run continuously without any wait
            time.sleep(5)
            df = pd.read_html('https://www.airnav.com/airports/us/' + state_code)
            airport_codes = airport_codes.append(df[3][['ID', 'City', 'Name']])
            print('https://www.airnav.com/airports/us/' + state_code)
        except:
            continue
    airport_codes.sort_values(by=['ID'], inplace=True)
    airport_codes.to_csv('/mnt/c/Program Files/PostgreSQL/12/data/airnav_airport_codes.csv',index=False)

# function to create table in local postgres database
def create_table(SQL):
    '''Create tables in the local postgres database'''
    pghook = PostgresHook('postgres_local')
    pghook.run(SQL)

# function to copy the source file into src_db schema table in local postgres database
def copy_table(tablename, filename):
    '''Copy data from source files (.csv files) into src stage tables in the local postgres database'''
    pghook = PostgresHook('postgres_local')
    pghook.run(COPY_SQL.format(tablename, filename))

# function to INSERT/UPDATE data into the table in local postgres database
def insert_update_table(SQL):
    '''INSERT/UPDATE data into the table in local postgres database'''
    pghook = PostgresHook('postgres_local')
    pghook.run(SQL)

# function to crate indexes on the tables for faster query performance
def create_indexes(SQL):
    '''Create indexes on the tables to improve query performance'''
    pghook = PostgresHook('postgres_local')
    pghook.run(SQL)

# funcation to validate the row count in the tables
def validate_row_count(schema):
    '''
    Validate row count for each table.
    If the row count for a table is less than the minimum defined for that table, then log an error and fail the task
    If the row count for a table is greater than the minimum defined for that table, then log the info and succeed the task
    '''

    # extract only the required tables from the dataframe (src/stg/core tables list)
    DF_STG_SRC_TABLES = DF_ROW_CNT_VALDTN[DF_ROW_CNT_VALDTN['table'].str.contains(schema+'.')]
    pghook = PostgresHook('postgres_local')
    conn = pghook.get_conn()
    cursor = conn.cursor()

    # validate the row count for each table
    for ix, row in DF_STG_SRC_TABLES.iterrows():
        table = row[0]
        min_rows = int(row[1])
        cursor.execute(VALIDATE_ROW_CNT_SQL.format(table))
        result = cursor.fetchall()
        row_cnt = int(result[0][0])
        if row_cnt < min_rows:
            logging.error('Row count validation FAILED for : '+table+'. Number of rows in the table = '+str(row_cnt)+', Minimum rows expected = '+str(min_rows))
            sys.exit(200)
        else:
            logging.info('Row count validation PASSED for : '+table+'. Number of rows in the table = '+str(row_cnt))


# funcation to validate for duplicates in core tables based on natural keys
def validate_nat_keys_dup():
    '''
    Validate duplicates for each core table based on natural key
    For a natural key, there should be only one row in the table.
    If there are more than one row, then that indicates an issue. So the validation task will be marked as FAIL
    '''

    pghook = PostgresHook('postgres_local')
    conn = pghook.get_conn()
    cursor = conn.cursor()

    # validate the row count for each table based on natural key
    for ix, row in DF_NAT_KEYS_DUP_VALDTN.iterrows():
        table = row[0]
        columns = row[1]
        cursor.execute(VALIDATE_NAT_KEYS_DUP_SQL.format(table, columns))
        result = cursor.fetchall()
        if len(result) > 0:
            logging.error('Duplicate row count validation FAILED for : '+table+'. There are duplicates for natural keys = {'+columns+'}, Please check the downstream pipeline/source data for any data issues.')
            sys.exit(300)
        else:
            logging.info('Duplicate row count validation PASSED for : '+table)


# Define various airflow tasks

# start task
start_task = DummyOperator(
    task_id = 'start',
    dag = dag
)

# task to create airports file from couple of websites
create_airports_file_task = PythonOperator (
    task_id = 'create_airports_file',
    dag = dag,
    python_callable = create_airports_file
)


# Define tasks to create src/stg/core tables

create_stg_src_airport_codes_table_task = PythonOperator(
    task_id = 'create_stg_src_airport_codes_table',
    dag = dag,
    op_kwargs = {'SQL' : CREATE_TABLE_SRC_AIRPORT_CODES_SQL},
    python_callable=create_table
)

create_stg_src_us_accidents_table_task = PythonOperator(
    task_id = 'create_stg_src_us_accidents_table',
    dag = dag,
    op_kwargs = {'SQL' : CREATE_TABLE_SRC_US_ACCIDENTS_SQL},
    python_callable=create_table
)

create_stg_src_dc_taxi_trips_table_task = PythonOperator(
    task_id = 'create_stg_src_dc_trips_table',
    dag = dag,
    op_kwargs = {'SQL' : CREATE_TABLE_SRC_DC_TAXI_TRIPS_SQL},
    python_callable=create_table
)

create_stg_address_table_task = PythonOperator(
    task_id = 'create_stg_address_table',
    dag = dag,
    op_kwargs = {'SQL' : CREATE_TABLE_STG_ADDRESS_SQL},
    python_callable=create_table
)

create_stg_accident_condition_table_task = PythonOperator(
    task_id = 'create_stg_accident_condition_table',
    dag = dag,
    op_kwargs = {'SQL' : CREATE_TABLE_STG_ACCIDENT_CONDITION_SQL},
    python_callable=create_table
)

create_stg_airport_table_task = PythonOperator(
    task_id = 'create_stg_airport_table',
    dag = dag,
    op_kwargs = {'SQL' : CREATE_TABLE_STG_AIRPORT_SQL},
    python_callable=create_table
)

create_stg_weather_condition_table_task = PythonOperator(
    task_id = 'create_stg_weather_condition_table',
    dag = dag,
    op_kwargs = {'SQL' : CREATE_TABLE_STG_WEATHER_CONDITION_SQL},
    python_callable=create_table
)

create_stg_provider_table_task = PythonOperator(
    task_id = 'create_stg_provider_table',
    dag = dag,
    op_kwargs = {'SQL' : CREATE_TABLE_STG_PROVIDER_SQL},
    python_callable=create_table
)

create_stg_source_table_task = PythonOperator(
    task_id = 'create_stg_source_table',
    dag = dag,
    op_kwargs = {'SQL' : CREATE_TABLE_STG_SOURCE_SQL},
    python_callable=create_table
)

create_stg_accident_table_task = PythonOperator(
    task_id = 'create_stg_accident_table',
    dag = dag,
    op_kwargs = {'SQL' : CREATE_TABLE_STG_ACCIDENT_SQL},
    python_callable=create_table
)

create_stg_trip_table_task = PythonOperator(
    task_id = 'create_stg_trip_table',
    dag = dag,
    op_kwargs = {'SQL' : CREATE_TABLE_STG_TRIP_SQL},
    python_callable=create_table
)

create_dim_date_table_task = PythonOperator(
    task_id = 'create_dim_date_table',
    dag = dag,
    op_kwargs = {'SQL' : CREATE_TABLE_DIM_DATE_SQL},
    python_callable=create_table
)

create_dim_time_table_task = PythonOperator(
    task_id = 'create_dim_time_table',
    dag = dag,
    op_kwargs = {'SQL' : CREATE_TABLE_DIM_TIME_SQL},
    python_callable=create_table
)

create_dim_address_table_task = PythonOperator(
    task_id = 'create_dim_address_table',
    dag = dag,
    op_kwargs = {'SQL' : CREATE_TABLE_DIM_ADDRESS_SQL},
    python_callable=create_table
)

create_dim_acc_cond_task = PythonOperator(
    task_id = 'create_dim_acc_cond_table',
    dag = dag,
    op_kwargs = {'SQL' : CREATE_TABLE_DIM_ACC_COND_SQL},
    python_callable=create_table
)

create_dim_airport_task = PythonOperator(
    task_id = 'create_dim_airport_table',
    dag = dag,
    op_kwargs = {'SQL' : CREATE_TABLE_DIM_AIRPORT_SQL},
    python_callable=create_table
)

create_dim_wthr_cond_task = PythonOperator(
    task_id = 'create_dim_wthr_cond_table',
    dag = dag,
    op_kwargs = {'SQL' : CREATE_TABLE_DIM_WTHR_COND_SQL},
    python_callable=create_table
)

create_lkp_provider_task = PythonOperator(
    task_id = 'create_lkp_provider_table',
    dag = dag,
    op_kwargs = {'SQL' : CREATE_TABLE_LKP_PROVIDER_SQL},
    python_callable=create_table
)

create_lkp_source_task = PythonOperator(
    task_id = 'create_lkp_source_table',
    dag = dag,
    op_kwargs = {'SQL' : CREATE_TABLE_LKP_SOURCE_SQL},
    python_callable=create_table
)

create_fact_accident_task = PythonOperator(
    task_id = 'create_fact_accident_table',
    dag = dag,
    op_kwargs = {'SQL' : CREATE_TABLE_FACT_ACCIDENT_SQL},
    python_callable=create_table
)

create_fact_trip_task = PythonOperator(
    task_id = 'create_fact_trip_table',
    dag = dag,
    op_kwargs = {'SQL' : CREATE_TABLE_FACT_TRIP_SQL},
    python_callable=create_table
)

copy_stg_src_airport_codes_task = PythonOperator(
    task_id = 'copy_stg_src_airport_codes_table',
    dag = dag,
    op_kwargs = {'tablename' : '"SRC_DB".stg_src_airport_codes',
                 'filename' : 'airnav_airport_codes.csv'},
    python_callable=copy_table
)

# Define tasks to copy data from source file to landing tables (src tables)

copy_stg_src_us_accidents_task = PythonOperator(
    task_id = 'copy_stg_src_us_accidents_table',
    dag = dag,
    op_kwargs = {'tablename' : '"SRC_DB".stg_src_us_accidents',
                 'filename' : 'US_Accidents_Dec19.csv'},
    python_callable=copy_table
)

copy_stg_src_dc_taxi_trips_task = PythonOperator(
    task_id = 'copy_stg_src_dc_taxi_trips_table',
    dag = dag,
    op_kwargs = {'tablename' : '"SRC_DB".stg_src_dc_taxi_trips',
                 'filename' : 'taxi_final.csv'},
    python_callable=copy_table
)

# Define tasks to insert data into stg/core tables

insert_stg_address_task = PythonOperator(
    task_id = 'insert_stg_address_table',
    dag = dag,
    op_kwargs = {'SQL' : INSERT_STG_ADDRESS_SQL},
    python_callable=insert_update_table
)

insert_stg_accident_condition_task = PythonOperator(
    task_id = 'insert_stg_accident_condition_table',
    dag = dag,
    op_kwargs = {'SQL' : INSERT_STG_ACCIDENT_CONDITION_SQL},
    python_callable=insert_update_table
)

insert_stg_airport_task = PythonOperator(
    task_id = 'insert_stg_airport_table',
    dag = dag,
    op_kwargs = {'SQL' : INSERT_STG_AIRPORT_SQL},
    python_callable=insert_update_table
)

insert_stg_weather_condition_task = PythonOperator(
    task_id = 'insert_stg_weather_condition_table',
    dag = dag,
    op_kwargs = {'SQL' : INSERT_STG_WEATHER_CONDITION_SQL},
    python_callable=insert_update_table
)

insert_stg_provider_task = PythonOperator(
    task_id = 'insert_stg_provider_table',
    dag = dag,
    op_kwargs = {'SQL' : INSERT_STG_PROVIDER_SQL},
    python_callable=insert_update_table
)

insert_stg_source_task = PythonOperator(
    task_id = 'insert_stg_source_table',
    dag = dag,
    op_kwargs = {'SQL' : INSERT_STG_SOURCE_SQL},
    python_callable=insert_update_table
)

insert_stg_accident_task = PythonOperator(
    task_id = 'insert_stg_accident_table',
    dag = dag,
    op_kwargs = {'SQL' : INSERT_STG_ACCIDENT_SQL},
    python_callable=insert_update_table
)

insert_stg_trip_task = PythonOperator(
    task_id = 'insert_stg_trip_table',
    dag = dag,
    op_kwargs = {'SQL' : INSERT_STG_TRIP_SQL},
    python_callable=insert_update_table
)

ins_upd_dim_date_task = PythonOperator(
    task_id = 'insert_update_dim_date_table',
    dag = dag,
    op_kwargs = {'SQL' : INSERT_UPDATE_DIM_DATE_SQL},
    python_callable=insert_update_table
)

ins_upd_dim_time_task = PythonOperator(
    task_id = 'insert_update_dim_time_table',
    dag = dag,
    op_kwargs = {'SQL' : INSERT_UPDATE_DIM_TIME_SQL},
    python_callable=insert_update_table
)

ins_upd_dim_address_task = PythonOperator(
    task_id = 'insert_update_dim_address_table',
    dag = dag,
    op_kwargs = {'SQL' : INSERT_UPDATE_DIM_ADDRESS_SQL},
    python_callable=insert_update_table
)

ins_upd_dim_acc_cond_task = PythonOperator(
    task_id = 'insert_update_dim_acc_cond_table',
    dag = dag,
    op_kwargs = {'SQL' : INSERT_UPDATE_DIM_ACC_COND_SQL},
    python_callable=insert_update_table
)

ins_upd_dim_airport_task = PythonOperator(
    task_id = 'insert_update_dim_airport_table',
    dag = dag,
    op_kwargs = {'SQL' : INSERT_UPDATE_DIM_AIRPORT_SQL},
    python_callable=insert_update_table
)

ins_upd_dim_wthr_cond_task = PythonOperator(
    task_id = 'insert_update_dim_wthr_cond_table',
    dag = dag,
    op_kwargs = {'SQL' : INSERT_UPDATE_DIM_WTHR_COND_SQL},
    python_callable=insert_update_table
)

ins_upd_lkp_provider_task = PythonOperator(
    task_id = 'insert_update_lkp_provider_table',
    dag = dag,
    op_kwargs = {'SQL' : INSERT_UPDATE_LKP_PROVIDER_SQL},
    python_callable=insert_update_table
)

ins_upd_lkp_source_task = PythonOperator(
    task_id = 'insert_update_lkp_source_table',
    dag = dag,
    op_kwargs = {'SQL' : INSERT_UPDATE_LKP_SOURCE_SQL},
    python_callable=insert_update_table
)

ins_upd_fact_accident_task = PythonOperator(
    task_id = 'insert_update_fact_accident_table',
    dag = dag,
    op_kwargs = {'SQL' : INSERT_UPDATE_FACT_ACCIDENT_SQL},
    python_callable=insert_update_table
)

ins_upd_fact_trip_task = PythonOperator(
    task_id = 'insert_update_fact_trip_table',
    dag = dag,
    op_kwargs = {'SQL' : INSERT_UPDATE_FACT_TRIP_SQL},
    python_callable=insert_update_table
)


# task to create index for faster query performance
create_indexes_task = PythonOperator(
    task_id = 'create_indexes',
    dag = dag,
    op_kwargs = {'SQL' : CREATE_INDEXES},
    python_callable=create_indexes
)

# task for validating the data in src tables
validate_row_cnt_stg_src_tables_task = PythonOperator(
    task_id = 'validate_row_cnt_stg_src_tables',
    dag = dag,
    op_kwargs={'schema': '"SRC_DB"'},
    python_callable = validate_row_count
)

# task for validating the data in stg tables
validate_row_cnt_stg_tables_task = PythonOperator(
    task_id = 'validate_row_cnt_stg_tables',
    dag = dag,
    op_kwargs={'schema': '"STG_DB"'},
    python_callable = validate_row_count
)

# task for validating the data in core tables
validate_row_cnt_core_tables_task = PythonOperator(
    task_id = 'validate_row_cnt_core_tables',
    dag = dag,
    op_kwargs={'schema': '"CORE_DB"'},
    python_callable = validate_row_count
)

# task for validating for duplicates based on natural key in core tables
validate_nat_keys_dup_tables_task = PythonOperator(
    task_id = 'validate_nat_keys_dup_core_tables',
    dag = dag,
    python_callable = validate_nat_keys_dup
)

# dummy task to link multiple sets of tasks
dummy_task = DummyOperator(
    task_id = 'stg_src_tables_created',
    dag = dag
)

# dummy task to link multiple sets of tasks
dummy_task1 = DummyOperator(
    task_id = 'stg_tables_created',
    dag = dag
)

# dummy task to link multiple sets of tasks
dummy_task2 = DummyOperator(
    task_id = 'core_tables_created',
    dag = dag
)

# final task at the end
end_task = DummyOperator(
    task_id = 'end',
    dag = dag
)

# Define order of execution of the tasks in the DAG

start_task >> create_airports_file_task >> [create_stg_src_airport_codes_table_task, create_stg_src_us_accidents_table_task,
                                            create_stg_src_dc_taxi_trips_table_task] >> dummy_task
dummy_task >> [create_stg_address_table_task, create_stg_accident_condition_table_task, create_stg_airport_table_task,
               create_stg_weather_condition_table_task, create_stg_provider_table_task, create_stg_source_table_task,
               create_stg_accident_table_task, create_stg_trip_table_task] >> dummy_task1
dummy_task1 >> [create_dim_date_table_task, create_dim_time_table_task, create_dim_address_table_task, create_dim_acc_cond_task,
                create_dim_airport_task, create_dim_wthr_cond_task, create_lkp_provider_task, create_lkp_source_task, create_fact_accident_task,
                create_fact_trip_task] >> dummy_task2
dummy_task2 >> [copy_stg_src_airport_codes_task, copy_stg_src_us_accidents_task, copy_stg_src_dc_taxi_trips_task] >> validate_row_cnt_stg_src_tables_task
validate_row_cnt_stg_src_tables_task >> [insert_stg_address_task, insert_stg_accident_condition_task, insert_stg_airport_task,
                                 insert_stg_weather_condition_task, insert_stg_provider_task, insert_stg_source_task,
                                 insert_stg_accident_task, insert_stg_trip_task] >> validate_row_cnt_stg_tables_task
validate_row_cnt_stg_tables_task >> [ins_upd_dim_date_task, ins_upd_dim_time_task, ins_upd_dim_address_task, ins_upd_dim_acc_cond_task,
                             ins_upd_dim_airport_task, ins_upd_dim_wthr_cond_task, ins_upd_lkp_provider_task, ins_upd_lkp_source_task] >> create_indexes_task
create_indexes_task >> [ins_upd_fact_accident_task, ins_upd_fact_trip_task] >> validate_row_cnt_core_tables_task >> validate_nat_keys_dup_tables_task >> end_task

