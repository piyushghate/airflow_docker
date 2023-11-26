import os
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
from io import StringIO


def read_from_s3(key: str, bucket_name: str, out_key: str) -> None:
    hook = S3Hook('aws_default')
    csv_content = hook.read_key(key, bucket_name)
    df = pd.read_csv(StringIO(csv_content))
    print(f"Schema of DF: {df.dtypes}")
    out_df = df[['Name', 'Website']]
    modified_csv_content = out_df.to_csv(index=False)
    hook.load_string(
        string_data=modified_csv_content,
        key=out_key,
        bucket_name=bucket_name,
        replace=True
    )


# def write_df_to_s3(df: pd.DataFrame, key: str, bucket_name: str) -> None:
#     hook = S3Hook('aws_default')
#
#     # Perform any DataFrame operations here if needed
#     # ...
#
#     # Convert DataFrame back to CSV in-memory
#     csv_buffer = StringIO()
#     df.to_csv(csv_buffer, index=False)
#
#     # Write the modified DataFrame back to S3
#     hook.load_string(
#         csv_buffer.getvalue(),
#         key=key,
#         bucket_name=bucket_name,
#         replace=True
#     )


with DAG(
        dag_id='s3_test_connection',
        schedule_interval='@daily',
        start_date=datetime(2023, 11, 24),
        catchup=False
) as dag:
    task_read_from_s3 = PythonOperator(
        task_id='read_from_s3',
        python_callable=read_from_s3,
        op_kwargs={
            'key': 'dataset/input/organization.csv',
            'bucket_name': 'myairflowbucket717',
            'out_key': 'dataset/output/output.csv'
        },
        provide_context = True
    )

    # task_write_df_to_s3= PythonOperator(
    #     task_id='rename_file_name',
    #     python_callable=write_df_to_s3,
    #     op_kwargs={
    #         'key': 'dataset/output/output.csv',
    #         'bucket_name': 'myairflowbucket717',
    #     },
    #     provide_context=True,
    # )

    #task_read_from_s3 >> task_write_df_to_s3
