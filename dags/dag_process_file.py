# Import necessary libraries
from airflow import DAG, XComArg
from airflow.decorators import task
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import  (
    S3ListOperator, S3DeleteObjectsOperator 
)
import awswrangler as wr
import pandas as pd
import boto3
import os 

# DEFINING ENV VARIABLES

AWS_ACCESS_KEY = os.environ['AWS_KEY']
AWS_ACESS_SECRET_KEY = os.environ['AWS_SECRET_KEY']


# [START layout definition]
default_layout = {
        'id': (0, 5),
        'Nome': (5, 20),
        'telefone': (20, 30),
        'DtNascimento': (30, 40),
        'Endereço': (40, 50)
    }

# Function to process the files in the S3 Bucket, by the chosen layout (**kwargs). 
def process_file(sourcefile, **kwargs):
    json_output = []
    for line in sourcefile.splitlines():
        record = {}
        for key, value in kwargs.items():
            start, end = value
            record[key] = line[start:end].strip()
        json_output.append(record)
    return json_output


# [END layout definition]


# Define default values for DAG arguments
default_args = {
        'owner': 'joaopaulo',
        'depends_on_past': False,
        'start_date': datetime(2022, 11, 10),
        'email': ['joaopaulo96@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
      }


with DAG('process_s3_file', catchup=False,
        default_args=default_args,
        schedule_interval='*/30 * * * 1-5',
        max_active_runs=1)  as dag: 
        
        # Set the S3 key and bucket name
        s3_key_airflow = f"*.txt"
        bucket_airflow = 'airflowbucket'

        # Create a S3KeySensor to check if new files have arrived
        sensor_keys = S3KeySensor(
            task_id='check_for_files',
            bucket_name=bucket_airflow,
            bucket_key=s3_key_airflow,
            wildcard_match=True,
            aws_conn_id='aws_conn_airflow',
            timeout = 60 * 10,
            mode = 'reschedule'
        )

        # Create a S3ListOperator to list all files in the bucket
        key_list_s3_airflow = S3ListOperator(
            task_id = 'list_s3_files',
            bucket = bucket_airflow,
            aws_conn_id='aws_conn_airflow'
        )

        # Task to read the files from S3
        @task
        def read_files(key_list):
            s3_hook = S3Hook(aws_conn_id='aws_conn_airflow')
            key_values_all = []
            for key in key_list:
                key_value = s3_hook.read_key(
                    key = key,
                    bucket_name= bucket_airflow
                )
                # Process the file content, with its layout, and append it to the list
                file_cleaned = process_file(key_value, **default_layout)
                key_values_all.append(file_cleaned)
            
            return key_values_all
        
        # Call the read_files Task using XComArg
        files_content = read_files(XComArg(key_list_s3_airflow))

        # Task to save the processed files as Parquet files to S3
        @task
        def save_as_parquet(file_content):

            # Set up the AWS credentials for Boto3
            session = boto3.setup_default_session(
                aws_access_key_id=AWS_ACCESS_KEY,
                aws_secret_access_key=AWS_ACESS_SECRET_KEY
                )
            # Convert the file_content into a Pandas DataFrame
            df = pd.DataFrame(file_content)

            # Save the DataFrame as a Parquet file to S3 using the Wrangler library
            wr.s3.to_parquet(
                df,
                f"s3://airflowbucketparquet/parquet/{datetime.today().strftime('%Y-%m-%d')}/sample-{datetime.now().strftime('%H:%M:%S.%f')}.parquet",
                compression ='snappy'
            ) 
        # Delete the files from the Ingest Bucket, using Airflow Dynamic Task Mapping
        delete_from_ingest_bucket = S3DeleteObjectsOperator.partial(
            task_id = "delete_from_ingest_bucket",
            aws_conn_id = 'aws_conn_airflow',
            bucket = 'airflowbucket'
        ).expand(keys=XComArg(key_list_s3_airflow))

        # Call the save_as_parquet Task using the output of read_files (using Airflow Dynamic Task Mapping) and set the dependencies between the Tasks.
        save_as_parquet.expand(file_content=files_content) >> delete_from_ingest_bucket

        sensor_keys >> key_list_s3_airflow