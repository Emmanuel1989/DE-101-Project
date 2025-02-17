# DE-101-Project

Project for DE-101 Course

## Project Objective

The objective of this project is to consume data from the Nike API provided on the nikescrapi.py file, build a data pipeline to insert the API data into a data warehouse, and create a data warehouse structure to store the data. Finally, we will write necessary queries to make reports from the data warehouse.

The project is divided into the following phases:

1. **Data Ingestion**:
   In this phase, we will ingest the data from the Nike API provided in the nikescrapi.py file.
2. **Data Processing**:
   In this phase, we will process the data using airflow as orchestrator or any other tool or framework. We will clean, transform, and aggregate the data as per the requirements.
3. **Data Ingestion into Data Warehouse**:
   In this phase, we will ingest the processed into the data warehouse. We will use the appropriate tools and frameworks for data ingestion into the data warehouse.
4. **Data Warehouse Structure**:
   In this phase, we will create the data warehouse structure to store the data. Design the schema and tables for the data warehouse as per the requirements.
5. **Querying the Datawarehouse**:
   In this phase, we will write necessary queries from the data warehouse. We will use appropriate tools and frameworks to create reports from the data warehouse.

## Requirements

- On the `Data Ingestion phase`, let's consume and store the API Data on the convenient place that you would like to use (it can be either locally, S3 or even on memory on a data frame, or any other storage location place)
- On the `Data Processing phase`, use the ingested data from the API and transform accordingly to be ready to be ingested into the Data Warehouse
- On the `Data ingestion into Data Warehouse phase`, lets upload the processed API data into the Data Warehouse
- The Data Warehouse will be used to calculate the sales of the company, the sales will be calculated as:

1. For all the products that the API ingested, sales will be the sum of the Current Price

- On the `Querying the Datawarehouse phase`, let's write the following queries:

1. Query the top 5 sales by product
2. Query the top 5 sales by category agrupation
3. Query the least 5 sales by category agrupation
4. Query the top 5 sales by title and subtitle agrupation
5. Query the top 3 products that has greatest sales by category

## Solution

- Open **nike_sales.sql** and create the table in snowflake **this will be our staging table**
- Open **fact_and_dimensions.sql** file under scripts folder and create the needed tables in snowflake.
- Create the store produre in snowflake **store_proc_fact_dim_tables.sql** before running the docker and dag process
- Open terminal in the working directory and run command _docker-compose up -d_
- Connect to airflow and install the package requirements

1.  Connect to worker airflow-worker and install snowflake connector
    - pip install snowflake-connector-python
2.  Check if airflow dag ran successfully

    **Note:** _The ariflow dag will move the data from local folder or in memory pandas DF to staging_table and it will then call store procedure to move data to fact and dimension tables_

- Last step execute the needed queries in **_dw_analysis_queries.sql_** file is located under scripts folder.
