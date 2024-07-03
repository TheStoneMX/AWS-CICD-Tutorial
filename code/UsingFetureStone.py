'''
Python script that demonstrates how to create a feature group, ingest data, 
and retrieve features using Amazon SageMaker Feature Store.
Explanation
Initialization: Sets up the necessary SageMaker session and clients.
Feature Group Definition: Defines the schema for the feature group, including feature names and types.
Feature Group Creation: Creates the feature group in SageMaker Feature Store and waits for it to be available.
Data Preparation: Generates sample data for ingestion.
Data Ingestion: Ingests the sample data into the feature group.
Data Retrieval: Retrieves features for a specific record identifier (e.g., customer_id).
'''


import boto3
import sagemaker
from sagemaker.feature_store.feature_group import FeatureGroup
import pandas as pd
import numpy as np
from time import gmtime, strftime, sleep

# Initialize SageMaker session and client
session = sagemaker.Session()
region = boto3.Session().region_name
featurestore_runtime = boto3.client(service_name='sagemaker-featurestore-runtime', region_name=region)
sagemaker_client = boto3.client(service_name='sagemaker', region_name=region)

# Define feature group parameters
feature_group_name = 'my-feature-group'
record_identifier_name = 'customer_id'
event_time_feature_name = 'event_time'
bucket_name = '<your-s3-bucket>'

# Define schema
feature_definitions = [
    {
        'FeatureName': 'customer_id',
        'FeatureType': 'Integral'
    },
    {
        'FeatureName': 'customer_name',
        'FeatureType': 'String'
    },
    {
        'FeatureName': 'total_spent',
        'FeatureType': 'Fractional'
    },
    {
        'FeatureName': 'event_time',
        'FeatureType': 'String'
    }
]

# Create feature group
feature_group = FeatureGroup(name=feature_group_name, sagemaker_session=session)
try:
    feature_group.create(
        s3_uri=f's3://{bucket_name}/feature-store/',
        record_identifier_name=record_identifier_name,
        event_time_feature_name=event_time_feature_name,
        role_arn='<your-iam-role-arn>',
        feature_definitions=feature_definitions,
        description='Customer feature group'
    )
except Exception as e:
    print(f'Error creating feature group: {e}')

# Wait for the feature group to be created
print("Waiting for Feature Group Creation")
feature_group.describe()
while feature_group.describe().get('FeatureGroupStatus') != 'Created':
    print('.', end='')
    sleep(5)
print('\nFeature Group Created')

# Generate sample data
data = {
    'customer_id': [1, 2, 3],
    'customer_name': ['Alice', 'Bob', 'Charlie'],
    'total_spent': [120.5, 340.7, 500.0],
    'event_time': [strftime('%Y-%m-%dT%H:%M:%SZ', gmtime())] * 3
}

df = pd.DataFrame(data)

# Ingest data into feature group
def ingest_data(df, feature_group):
    records = []
    for _, row in df.iterrows():
        record = []
        for feature_name in df.columns:
            record.append({'FeatureName': feature_name, 'ValueAsString': str(row[feature_name])})
        records.append(record)

    for record in records:
        try:
            featurestore_runtime.put_record(
                FeatureGroupName=feature_group_name,
                Record=record
            )
        except Exception as e:
            print(f'Error ingesting record: {e}')

ingest_data(df, feature_group)
print("Data ingested into feature group")

# Retrieve features
def get_feature_values(feature_group_name, record_identifier_value):
    response = featurestore_runtime.get_record(
        FeatureGroupName=feature_group_name,
        RecordIdentifierValueAsString=str(record_identifier_value)
    )
    return response['Record']

# Example of retrieving features for customer_id=1
features = get_feature_values(feature_group_name, 1)
print("Retrieved features:", features)
