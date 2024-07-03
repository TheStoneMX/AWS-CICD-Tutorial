import boto3
import pandas as pd
import io

# Configuration
redshift_cluster_id = 'my-redshift-cluster'
redshift_db = 'dev'
redshift_dbuser = 'awsuser'
s3_bucket = 'my-sagemaker-bucket'

# Redshift SQL queries to extract train, validation, and test datasets
sql_query_train = 'SELECT * FROM my_table WHERE split = \'train\''
sql_query_validation = 'SELECT * FROM my_table WHERE split = \'validation\''
sql_query_test = 'SELECT * FROM my_table WHERE split = \'test\''

# Initialize Redshift Data API client
redshift_data_client = boto3.client('redshift-data')

def execute_redshift_query(sql_query):
    # Execute the SQL query on Redshift
    response = redshift_data_client.execute_statement(
        ClusterIdentifier=redshift_cluster_id,
        Database=redshift_db,
        DbUser=redshift_dbuser,
        Sql=sql_query
    )

    # Get the query ID
    query_id = response['Id']

    # Wait for the query to finish
    finished = False
    while not finished:
        status_response = redshift_data_client.describe_statement(Id=query_id)
        status = status_response['Status']
        if status in ['FINISHED', 'FAILED']:
            finished = True

    # Check for query success
    if status == 'FINISHED':
        # Get the result as CSV
        result_response = redshift_data_client.get_statement_result(Id=query_id)
        records = result_response['Records']
        
        # Convert the result to a DataFrame
        column_names = [col['name'] for col in result_response['ColumnMetadata']]
        data = [[col['stringValue'] for col in record] for record in records]
        df = pd.DataFrame(data, columns=column_names)
        
        return df
    else:
        raise Exception(f'Query failed with status: {status}')

# Query train, validation, and test datasets
df_train = execute_redshift_query(sql_query_train)
df_validation = execute_redshift_query(sql_query_validation)
df_test = execute_redshift_query(sql_query_test)

# Save DataFrames to CSV files and upload to S3
def save_df_to_s3(df, s3_key):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=csv_buffer.getvalue())

save_df_to_s3(df_train, 'data/train/redshift-data-train.csv')
save_df_to_s3(df_validation, 'data/validation/redshift-data-validation.csv')
save_df_to_s3(df_test, 'data/test/redshift-data-test.csv')
