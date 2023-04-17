from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import json
import os
from nikescrapi import *
import pandas as pd
import glob
import os.path
from snowflake.connector import connect


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
dag = DAG('etl_dag', default_args=default_args, schedule_interval=None)
STAGING_TABLE = "nike_sales"
DROP_STAGING_TABLE = f"drop table if exists {STAGING_TABLE}"
CREATE_STAGING_TABLE = f"""create table if not exists {STAGING_TABLE}
 (id number autoincrement start 1 increment 1,
 productId varchar,
 colorNum number,
 title varchar,
 subtitle varchar,
 category varchar,
 type varchar,
 currency varchar,
 fullprice number, 
 currentPrice float,
 sale BOOLEAN,
 color_ID varchar, 
 color_Description varchar,
 color_BestSeller varchar,
 color_Discount varchar,
 color_MemberExclusive varchar
 )
                        """


def upload_to_snowflake(connection, data_frame, table_name):
    with connection.cursor() as cursor:
        query = f"INSERT INTO {table_name} (productID, colorNum, title, subtitle, category, type, currency, fullPrice , currentPrice, sale, color_ID, color_Description,color_BestSeller,color_Discount, color_MemberExclusive) VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s , %s, %s,%s, %s)"
        data = data_frame[['productID', 'colorNum', 'title', 'subtitle', 'category', 'type', 'currency',
                           'fullPrice', 'currentPrice', 'sale', 'color-ID', 'color-Description', 'color-BestSeller', 'color-Discount', 'color-MemberExclusive']].values.tolist()
        print(data_frame[['color-ID', 'color-Description',
              'color-BestSeller', 'color-Discount']].values.tolist()[1])
        cursor.executemany(query, data)


def create_staging_snowflake():
    csv_files = glob.glob(os.path.join("../data/", '*.csv'))
    if len(csv_files) > 0:
        nike = pd.read_csv(csv_files[0])
    else:
        nikeAPI = NikeScrAPI(max_pages=50, path='data')
        nike = nikeAPI.getData()
    with connect(
        account="NK31131",
        user="JESUSRAMOS1989",
        password="------",
        database="DB_TEST",
        warehouse="COMPUTE_WH",
        region="us-east-2.aws"
    ) as connection:
        cursor = connection.cursor()
        cursor.execute("USE DB_TEST")
        cursor.execute(DROP_STAGING_TABLE)
        cursor.execute(CREATE_STAGING_TABLE)
        upload_to_snowflake(connection, nike, STAGING_TABLE)
        connection.close()


def execute_staging_to_fact_dim():
    with connect(
        account="NK31131",
        user="JESUSRAMOS1989",
        password="Spark#@4321",
        database="DB_TEST",
        warehouse="COMPUTE_WH",
        region="us-east-2.aws"
    ) as connection:
        cursor = connection.cursor()
        cursor.execute("USE DB_TEST")
        cursor.execute("CALL insert_fact_dim_nike_sales()")


staging_to_snowflake = PythonOperator(
    task_id='stg_to_snflk',
    python_callable=create_staging_snowflake,
    dag=dag
)

snowflake_to_fact_and_dim = PythonOperator(
    task_id='stg_to_snflk',
    python_callable=execute_staging_to_fact_dim,
    dag=dag
)

# Set the task dependencies
staging_to_snowflake >> snowflake_to_fact_and_dim
