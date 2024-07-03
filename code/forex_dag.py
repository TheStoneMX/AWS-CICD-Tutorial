from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators import S3KeySensor
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from io import StringIO, BytesIO
import requests
import boto3
import zipfile
import pandas as pd

# Configuration Variables
s3_bucket_name = 'my-forex-bucket'  # S3 bucket name for storing files
s3_key = 'forex/'  # S3 key prefix for files
redshift_cluster = 'my-redshift-cluster'  # Redshift cluster identifier
redshift_db = 'dev'  # Redshift database name
redshift_dbuser = 'awsuser'  # Redshift database user
redshift_table_name = 'forex_demo'  # Redshift table name
test_http = 'https://example.com/datasets/forex/latest/'  # URL to check forex data availability
download_http = 'http://example.com/datasets/forex/forex-latest-small.zip'  # URL to download forex data
athena_db = 'forex_athena_db'  # Athena database name
athena_results = 'athena-results/'  # S3 prefix for Athena query results

# Athena Queries for Forex Data
create_athena_forex_table_query = """
CREATE EXTERNAL TABLE IF NOT EXISTS Forex_Athena_DB.Forex_Data (
  `timestamp` bigint,
  `currency_pair` string,
  `bid` double,
  `ask` double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = ',',
  'field.delim' = ','
) LOCATION 's3://my-forex-bucket/forex/forex-latest-small.csv/'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  'skip.header.line.count'='1'
); 
"""  # Athena query to create an external table for forex data

join_tables_athena_query = """
SELECT currency_pair, AVG(bid) as avg_bid, AVG(ask) as avg_ask
FROM forex_athena_db.Forex_Data
GROUP BY currency_pair
"""  # Athena query to join and aggregate forex data

# Function to download and extract forex data
def download_zip():
    s3c = boto3.client('s3')  # Initialize S3 client
    indata = requests.get(download_http)  # Download the zip file from the specified URL
    with zipfile.ZipFile(BytesIO(indata.content)) as z:  # Open the zip file
        zList = z.namelist()  # Get the list of files in the zip
        for i in zList:
            zfiledata = BytesIO(z.read(i))  # Read each file in the zip
            s3c.put_object(Bucket=s3_bucket_name, Key=s3_key + i + '/' + i, Body=zfiledata)  # Upload file to S3

# Function to clean up CSV files
def clean_up_csv_fn(**kwargs):
    ti = kwargs['task_instance']  # Get the task instance
    queryId = ti.xcom_pull(key='return_value', task_ids='join_athena_tables')  # Pull the query ID from XCom
    athenaKey = athena_results + "join_athena_tables/" + queryId + ".csv"  # Construct the S3 key for the Athena query result
    cleanKey = athena_results + "join_athena_tables/" + queryId + "_clean.csv"  # Construct the S3 key for the cleaned CSV
    s3c = boto3.client('s3')  # Initialize S3 client
    obj = s3c.get_object(Bucket=s3_bucket_name, Key=athenaKey)  # Get the Athena query result from S3
    infileStr = obj['Body'].read().decode('utf-8')  # Read and decode the CSV data
    outfileStr = infileStr.replace('"e"', '')  # Example clean-up operation: remove unwanted characters
    outfile = StringIO(outfileStr)  # Convert the cleaned data to a StringIO object
    s3c.put_object(Bucket=s3_bucket_name, Key=cleanKey, Body=outfile.getvalue())  # Upload the cleaned CSV to S3

# Function to transform data
def transform_data_fn():
    s3c = boto3.client('s3')  # Initialize S3 client
    obj = s3c.get_object(Bucket=s3_bucket_name, Key=athena_results + "join_athena_tables/join_athena_tables.csv")  # Get the Athena query result from S3
    df = pd.read_csv(BytesIO(obj['Body'].read()))  # Read the CSV data into a DataFrame
    df['spread'] = df['avg_ask'] - df['avg_bid']  # Calculate the spread (ask - bid) and add it as a new column
    transformed_key = athena_results + "join_athena_tables/join_athena_tables_transformed.csv"  # Construct the S3 key for the transformed CSV
    df.to_csv('/tmp/join_athena_tables_transformed.csv', index=False)  # Save the transformed DataFrame to a local file
    s3c.upload_file('/tmp/join_athena_tables_transformed.csv', s3_bucket_name, transformed_key)  # Upload the transformed CSV to S3

# Function to load data into Redshift
def s3_to_redshift(**kwargs):
    ti = kwargs['task_instance']  # Get the task instance
    queryId = ti.xcom_pull(key='return_value', task_ids='join_athena_tables')  # Pull the query ID from XCom
    athenaKey = 's3://' + s3_bucket_name + "/" + athena_results + "join_athena_tables/" + queryId + "_clean.csv"  # Construct the S3 key for the cleaned CSV
    sqlQuery = f"copy {redshift_table_name} from '{athenaKey}' iam_role 'arn:aws:iam::163919838948:role/myRedshiftRole' CSV IGNOREHEADER 1;"  # Construct the Redshift COPY command
    rsd = boto3.client('redshift-data')  # Initialize Redshift Data API client
    resp = rsd.execute_statement(
        ClusterIdentifier=redshift_cluster,  # Redshift cluster identifier
        Database=redshift_db,  # Redshift database name
        DbUser=redshift_dbuser,  # Redshift database user
        Sql=sqlQuery  # SQL query to execute
    )
    return "OK"

# Function to create Redshift table if it does not exist
def create_redshift_table():
    rsd = boto3.client('redshift-data')  # Initialize Redshift Data API client
    resp = rsd.execute_statement(
        ClusterIdentifier=redshift_cluster,  # Redshift cluster identifier
        Database=redshift_db,  # Redshift database name
        DbUser=redshift_dbuser,  # Redshift database user
        Sql=f"CREATE TABLE IF NOT EXISTS {redshift_table_name} (currency_pair character varying, avg_bid double precision, avg_ask double precision, spread double precision);"  # SQL query to create table
    )
    return "OK"

DEFAULT_ARGS = {
    'owner': 'airflow',  # Owner of the DAG
    'depends_on_past': False,  # Whether the DAG depends on past runs
    'email': ['airflow@example.com'],  # Email notifications
    'email_on_failure': False,  # Disable email on failure
    'email_on_retry': False  # Disable email on retry
}

with DAG(
    dag_id='forex-market-dag',  # DAG ID
    default_args=DEFAULT_ARGS,  # Default arguments
    dagrun_timeout=timedelta(hours=2),  # Maximum runtime for the DAG
    start_date=days_ago(2),  # Start date for the DAG
    schedule_interval='*/10 * * * *',  # Schedule interval (every 10 minutes)
    tags=['athena', 'redshift'],  # Tags for the DAG
) as dag:
    check_s3_for_key = S3KeySensor(
        task_id='check_s3_for_key',  # Task ID
        bucket_key=s3_key,  # S3 key prefix to check
        wildcard_match=True,  # Enable wildcard matching
        bucket_name=s3_bucket_name,  # S3 bucket name
        s3_conn_id='aws_default',  # Airflow connection ID for S3
        timeout=20,  # Timeout for the sensor
        poke_interval=5,  # Interval between checks
        dag=dag  # DAG to which the task belongs
    )

    files_to_s3 = PythonOperator(
        task_id="files_to_s3",  # Task ID
        python_callable=download_zip  # Function to call
    )

    create_athena_forex_table = AWSAthenaOperator(
        task_id="create_athena_forex_table",  # Task ID
        query=create_athena_forex_table_query,  # Athena query to execute
        database=athena_db,  # Athena database name
        output_location='s3://' + s3_bucket_name + "/" + ath
