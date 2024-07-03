'''
DAG Definition:
The DAG and default arguments are defined at the beginning. The DAG is scheduled to run daily.
AWS Configuration:
AWS credentials and configuration are set up for Boto3 clients.

Extract Data from Redshift:
The extract_data function uses PostgresHook to connect to Redshift, execute a query, and save the result to a CSV file.

Clean Data:
The clean_data function reads the extracted data, performs a cleaning operation 
dropping rows with missing values), and saves the cleaned data to another CSV file.

Transform Data:
The transform_data function reads the cleaned data, performs a transformation (adding a new column), 
and saves the transformed data to another CSV file.

Load Data to SageMaker Feature Store:
The load_data_to_feature_store function reads the transformed data, 
creates a SageMaker Feature Group if it doesn't exist, waits for the feature group to be created, 
and ingests the data into the feature group.

Task Definitions:
The tasks for each step (extract, clean, transform, load) are defined using PythonOperator.

Task Dependencies:
The task dependencies are set to ensure the tasks execute in the correct order: extract -> clean -> transform -> load.

'''
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import boto3
import sagemaker
from sagemaker.feature_store.feature_group import FeatureGroup

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'depends_on_past': False,  # Do not wait for previous DAG runs
    'start_date': datetime(2023, 1, 1),  # Start date of the DAG
    'email_on_failure': False,  # Do not send email on failure
    'email_on_retry': False,  # Do not send email on retry
    'retries': 1,  # Number of retries on failure
    'retry_delay': timedelta(minutes=5),  # Delay between retries
}

# Define the DAG
dag = DAG(
    'redshift_to_sagemaker_feature_store',  # DAG name
    default_args=default_args,  # Default arguments
    description='ETL pipeline from Redshift to SageMaker Feature Store',  # Description of the DAG
    schedule_interval=timedelta(days=1),  # Interval at which the DAG runs
)

# AWS configuration
aws_access_key_id = '<your-access-key-id>'  # Your AWS access key ID
aws_secret_access_key = '<your-secret-access-key>'  # Your AWS secret access key
region_name = 'us-west-2'  # AWS region
s3_bucket_name = 'your-s3-bucket-name'  # S3 bucket name
s3_prefix = 'your/s3/prefix/'  # S3 prefix for storing data

# Initialize Boto3 clients
s3_client = boto3.client(
    's3',  # S3 service
    aws_access_key_id=aws_access_key_id,  # AWS access key ID
    aws_secret_access_key=aws_secret_access_key,  # AWS secret access key
    region_name=region_name  # AWS region
)

# Initialize SageMaker session and Feature Store client
sagemaker_session = sagemaker.Session()  # SageMaker session
featurestore_runtime = boto3.client(service_name='sagemaker-featurestore-runtime', region_name=region_name)  # SageMaker Feature Store runtime client
sagemaker_client = boto3.client(service_name='sagemaker', region_name=region_name)  # SageMaker client

# Define the feature group
feature_group_name = 'my-feature-group'  # Name of the feature group
record_identifier_name = 'customer_id'  # Record identifier name
event_time_feature_name = 'event_time'  # Event time feature name
feature_definitions = [  # Feature definitions
    {
        'FeatureName': 'customer_id',  # Feature name
        'FeatureType': 'Integral'  # Feature type
    },
    {
        'FeatureName': 'customer_name',  # Feature name
        'FeatureType': 'String'  # Feature type
    },
    {
        'FeatureName': 'total_spent',  # Feature name
        'FeatureType': 'Fractional'  # Feature type
    },
    {
        'FeatureName': 'event_time',  # Feature name
        'FeatureType': 'String'  # Feature type
    }
]

# Step 1: Extract data from Redshift
def extract_data():
    # Connect to Redshift
    redshift_hook = PostgresHook(postgres_conn_id='redshift_default')  # Create a PostgresHook to connect to Redshift
    sql = "SELECT * FROM source_table"  # SQL query to extract data
    connection = redshift_hook.get_conn()  # Get connection object
    cursor = connection.cursor()  # Create a cursor object
    cursor.execute(sql)  # Execute the SQL query
    records = cursor.fetchall()  # Fetch all records
    cursor.close()  # Close the cursor
    connection.close()  # Close the connection
    
    # Convert to DataFrame for transformation
    df = pd.DataFrame(records, columns=['customer_id', 'customer_name', 'total_spent', 'event_time'])  # Convert records to DataFrame
    df.to_csv('/tmp/extracted_data.csv', index=False)  # Save the DataFrame to a CSV file

# Step 2: Clean data
def clean_data():
    df = pd.read_csv('/tmp/extracted_data.csv')  # Read the extracted data from the CSV file
    
    # Example cleaning operation: drop rows with missing values
    df = df.dropna()  # Drop rows with missing values
    
    df.to_csv('/tmp/cleaned_data.csv', index=False)  # Save the cleaned data to another CSV file

# Step 3: Transform data
def transform_data():
    df = pd.read_csv('/tmp/cleaned_data.csv')  # Read the cleaned data from the CSV file
    
    # Example transformation: add a new column
    df['total_spent_double'] = df['total_spent'] * 2  # Add a new column with transformed data
    
    df.to_csv('/tmp/transformed_data.csv', index=False)  # Save the transformed data to another CSV file

# Step 4: Load data to SageMaker Feature Store
def load_data_to_feature_store():
    df = pd.read_csv('/tmp/transformed_data.csv')  # Read the transformed data from the CSV file
    
    # Initialize the feature group
    feature_group = FeatureGroup(name=feature_group_name, sagemaker_session=sagemaker_session)  # Create a FeatureGroup object
    
    # Create the feature group if it does not exist
    try:
        feature_group.create(
            s3_uri=f's3://{s3_bucket_name}/feature-store/',  # S3 URI for storing feature data
            record_identifier_name=record_identifier_name,  # Record identifier name
            event_time_feature_name=event_time_feature_name,  # Event time feature name
            role_arn='<your-iam-role-arn>',  # IAM role ARN with permissions for SageMaker Feature Store
            feature_definitions=feature_definitions,  # Feature definitions
            description='Customer feature group'  # Description of the feature group
        )
    except Exception as e:
        print(f'Feature group already exists or an error occurred: {e}')  # Handle feature group creation error
    
    # Wait for the feature group to be created
    while feature_group.describe().get('FeatureGroupStatus') != 'Created':  # Wait until the feature group status is 'Created'
        print('.', end='')  # Print a dot for each iteration (for visual feedback)
        sleep(5)  # Sleep for 5 seconds
    print('\nFeature Group Created')  # Print message when feature group is created
    
    # Ingest data into the feature group
    for index, row in df.iterrows():  # Iterate through each row in the DataFrame
        record = []  # Initialize an empty record list
        for feature_name in df.columns:  # Iterate through each column (feature) in the DataFrame
            record.append({'FeatureName': feature_name, 'ValueAsString': str(row[feature_name])})  # Append feature name and value to the record list
        try:
            featurestore_runtime.put_record(
                FeatureGroupName=feature_group_name,  # Name of the feature group
                Record=record  # Record to be ingested
            )
        except Exception as e:
            print(f'Error ingesting record: {e}')  # Handle record ingestion error

# Define the tasks
extract_task = PythonOperator(
    task_id='extract_data',  # Task ID
    python_callable=extract_data,  # Python function to be called
    dag=dag,  # DAG object
)

clean_task = PythonOperator(
    task_id='clean_data',  # Task ID
    python_callable=clean_data,  # Python function to be called
    dag=dag,  # DAG object
)

transform_task = PythonOperator(
    task_id='transform_data',  # Task ID
    python_callable=transform_data,  # Python function to be called
    dag=dag,  # DAG object
)

load_task = PythonOperator(
    task_id='load_data_to_feature_store',  # Task ID
    python_callable=load_data_to_feature_store,  # Python function to be called
    dag=dag,  # DAG object
)

# Set task dependencies
extract_task >> clean_task >> transform_task >> load_task  # Define the order of task execution
