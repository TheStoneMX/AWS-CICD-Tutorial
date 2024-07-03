


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime, timedelta
import pandas as pd
import boto3
import sagemaker
from sagemaker.estimator import Estimator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'redshift_to_sagemaker_training',
    default_args=default_args,
    description='ETL pipeline from Redshift to SageMaker training',
    schedule_interval=timedelta(days=1),
)

# Step 1: Extract data from Redshift
def extract_data():
    redshift_hook = PostgresHook(postgres_conn_id='redshift_default')
    sql = "SELECT * FROM source_table"
    connection = redshift_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(sql)
    records = cursor.fetchall()
    cursor.close()
    connection.close()
    
    df = pd.DataFrame(records, columns=['customer_id', 'customer_name', 'total_spent', 'event_time'])
    df.to_csv('/tmp/extracted_data.csv', index=False)

# Step 2: Clean data
def clean_data():
    df = pd.read_csv('/tmp/extracted_data.csv')
    df = df.dropna()
    df.to_csv('/tmp/cleaned_data.csv', index=False)

# Step 3: Transform data
def transform_data():
    df = pd.read_csv('/tmp/cleaned_data.csv')
    df['total_spent_double'] = df['total_spent'] * 2
    df.to_csv('/tmp/transformed_data.csv', index=False)

# Step 4: Upload to S3
def upload_to_s3():
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_hook.load_file(
        filename='/tmp/transformed_data.csv',
        key='transformed_data/transformed_data.csv',
        bucket_name='your-s3-bucket-name',
        replace=True
    )

# Step 5: Train SageMaker Model
def train_sagemaker_model():
    session = sagemaker.Session()
    role = 'your-sagemaker-execution-role'  # IAM role with SageMaker permissions
    input_data = 's3://your-s3-bucket-name/transformed_data/transformed_data.csv'
    
    # Define the estimator
    estimator = Estimator(
        image_uri='382416733822.dkr.ecr.us-west-2.amazonaws.com/linear-learner:latest',  # Example container image for SageMaker built-in algorithms
        role=role,
        instance_count=1,
        instance_type='ml.m4.xlarge',
        output_path='s3://your-s3-bucket-name/sagemaker_output/',
        sagemaker_session=session
    )
    
    # Define the data channels
    train_data = sagemaker.inputs.TrainingInput(
        s3_data=input_data,
        content_type='text/csv'
    )
    
    # Launch the training job
    estimator.fit({'train': train_data})

# Define the tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    dag=dag,
)

train_task = PythonOperator(
    task_id='train_sagemaker_model',
    python_callable=train_sagemaker_model,
    dag=dag,
)

# Set task dependencies
extract_task >> clean_task >> transform_task >> upload_task >> train_task
