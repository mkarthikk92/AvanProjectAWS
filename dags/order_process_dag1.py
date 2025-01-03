from inspect import Arguments

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta

import os

import boto3


# Paths
BUCKET_NAME = "<INPUT BUCKET NAME>"
KEY_NAME = "data/medical_dataset.json"
TEMP_DIR=''
TARGET_BUCKET_NAME="<TARGET BUCKET NAME>"
TARGET_KEY_NAME="cleaned"
TARGET_PATH=f"s://{BUCKET_NAME}/{TARGET_KEY_NAME}/"
SCRIPT_LOCATION=f's3://{BUCKET_NAME}/scripts'

# IAM
AWS_REGION = "us-east-2"
CONN_ID = "aws_connection"
ROLE_ARN="<ROLE ARN>"
IAM_ROLE_NAME="<IAM ROLE NAME>"
SESSION_NAME="AvanProjectETLPipeline"

# GlUE
CRAWLER_NAME="<CRAWLER NAME>"
GLUE_JOB_NAME='unnest_date'
GLUE_VERSION="3.0"
WORKER_TYPE = "G.1X"
NUMBER_OF_WORKERS = 1


def get_sts_client():
    sts_client = boto3.client('sts')
    response = sts_client.assume_role(
        RoleArn=ROLE_ARN,
        RoleSessionName=SESSION_NAME
    )

    # Extract temporary credentials
    credentials = response['Credentials']
    return credentials


def get_glue_client(credentials):
    glue_client = boto3.client(
        'glue',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
        region_name=AWS_REGION,
    )
    return glue_client


def crawl_raw_bucket():
    credentials = get_sts_client()
    # Use temporary credentials to create a Glue client
    glue_client = get_glue_client(credentials)

    # Start the Glue crawler
    glue_client.start_crawler(Name=CRAWLER_NAME)


def unnest_raw_data():
    credentials = get_sts_client()
    glue_client = get_glue_client(credentials)

    response = glue_client.create_job(
        Name=GLUE_JOB_NAME,
        Role=ROLE_ARN,
        Command={
            "Name": "glueetl",
            "ScriptLocation": SCRIPT_LOCATION,
            "PythonVersion": "3"
        },
        DefaultArguments={
            "--TempDir": TEMP_DIR,
            "--job-bookmark-option": "job-bookmark-enable"
        },
        GlueVersion=GLUE_VERSION,
        WorkerType=WORKER_TYPE,
        NumberOfWorkers=NUMBER_OF_WORKERS,
        MaxRetries=2,
        Timeout=60
    )

    glue_client.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            "--JOB_NAME": GLUE_JOB_NAME,
            "--INPUT_PATH": f"s://{BUCKET_NAME}/{KEY_NAME}/",
            "--OUTPUT_PATH": TARGET_PATH
        }
    )


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
with DAG(
    dag_id='data-transform-etl-avan',
    default_args=default_args,
    description='A pipeline using S3KeySensor with assumed role',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # Task to monitor the presence of a specific S3 key
    s3_key_sensor = S3KeySensor(
        task_id='s3_key_sensor',
        bucket_name=BUCKET_NAME,
        bucket_key=KEY_NAME,
        aws_conn_id=CONN_ID,
        timeout=600,
        poke_interval=30,
    )

    run_glue_job = PythonOperator(
        task_id='unnest_raw_data',
        python_callable=unnest_raw_data
    )

    # crawl_raw_bucket = PythonOperator(
    #     task_id='crawl_raw_bucket',
    #     python_callable=crawl_raw_bucket
    # )

    # Task dependencies
    s3_key_sensor >> run_glue_job
