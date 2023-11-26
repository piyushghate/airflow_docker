from airflow import DAG
#from airflow.providers.amazon.transfers.s3_to_local import S3ToLocalOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from io import StringIO

# Import S3Hook
from airflow.hooks.S3_hook import S3Hook

# Define default_args and DAG parameters
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pyspark_s3_parquet_example',
    default_args=default_args,
    description='A simple PySpark example using Airflow',
    schedule_interval=None,
)

def process_data(key:str, bucket_name: str, out_key: str)->None:
    # Create Spark session
    spark = SparkSession.builder.appName("S3ParquetExample").getOrCreate()

    # Use S3Hook to get the S3 credentials
    s3_hook = S3Hook(aws_conn_id='aws_default')  # Use the appropriate connection ID
    s3_credentials = s3_hook.get_credentials()
    print(f"s3_credentials:: {s3_credentials}")

    # Load data from S3
    input_s3_path = "s3a://myairflowbucket717/dataset/input/organization.csv"
    # s3_to_local = S3ToLocalOperator(
    #     task_id='s3_to_local',
    #     aws_conn_id='aws_default',  # Use the appropriate connection ID
    #     bucket_name='your-s3-bucket',
    #     prefix='path/to/input.csv',
    #     local_file='local_input.csv',
    #     dag=dag,
    # )
    # s3_to_local.execute(context=kwargs)

    #csv_content = s3_hook.read_key(key, bucket_name)
    # df = pd.read_csv(StringIO(csv_content))
    # print(f"Schema of DF: {df.dtypes}")

    # Read the local file into a Spark DataFrame
    df = spark.read.csv(input_s3_path, header=True, inferSchema=True)

    # Perform some transformations (example: converting column types)
    df_transformed = df.withColumn("new_column", df["existing_column"].cast("double"))

    # Save the transformed DataFrame to Parquet format on S3
    output_s3_path = "s3a://myairflowbucket717/dataset/output/output_spark.parquet"
    df_transformed.write.parquet(output_s3_path, mode="overwrite")

    # Stop the Spark session
    spark.stop()

# Define the PythonOperator
process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    op_kwargs={
                'key': 'dataset/input/organization.csv',
                'bucket_name': 'myairflowbucket717',
                'out_key': 'dataset/output/output_spark.parquet'
            },
    provide_context=True,
    dag=dag,
)

# Set task dependencies
process_data_task

if __name__ == "__main__":
    dag.cli()
