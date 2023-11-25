from datetime import datetime, timedelta
from airflow import DAG
#from airflow.operators.python_operator import BranchPythonOperator
import pandas as pd
from airflow.providers.amazon.transfers.s3_to_s3 import S3ToS3Operator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook

# Define your S3 bucket and key
s3_bucket = 'myairflowbucket717'
input_key = 'dataset/input/organization.csv'
output_key = 'dataset/output/output_org.csv'

# Define your DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    's3_csv_operations',
    default_args=default_args,
    schedule_interval=None,  # Set the desired schedule interval
)


def read_and_drop_column(**kwargs):
    # Use S3Hook to read CSV from S3
    s3_hook = S3Hook(aws_conn_id='s3_connect')
    csv_data = s3_hook.read_key(key=input_key, bucket_name=s3_bucket)

    # Load CSV into DataFrame
    df = pd.read_csv(pd.compat.StringIO(csv_data))

    # Drop a column (replace 'column_to_drop' with the actual column name)
    out_df = df[['column1', 'column2']]

    # Save modified DataFrame back to S3
    out_df.to_csv('/tmp/output.csv', index=False)
    s3_hook.load_file(filename='/tmp/output.csv', key=output_key, bucket_name=s3_bucket, replace=True)


with dag:
    # Task to read, drop column, and save to S3
    read_and_drop_task = PythonOperator(
        task_id='read_and_drop_column',
        python_callable=read_and_drop_column,
    )

    # Task to copy modified file to another S3 location
    copy_to_s3_task = S3ToS3Operator(
        task_id='copy_to_s3',
        source_bucket_key=output_key,
        dest_bucket_key=output_key,
        source_bucket_name=s3_bucket,
        dest_bucket_name=s3_bucket,
        replace=True,
    )

read_and_drop_task >> copy_to_s3_task
