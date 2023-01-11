import tempfile
import requests
import py7zr
import os

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook


# Default settings applied to all tasks
default_args = {
    "owner": "trogalsk",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def download_data(key, bucket_name):
    with tempfile.NamedTemporaryFile() as f:
        response = requests.get("https://archive.org/download/stackexchange/english.stackexchange.com.7z")
        f.write(response.content)

        s3_hook = S3Hook("s3_conn")
        with py7zr.SevenZipFile(f.name, "r") as zf:
            with tempfile.TemporaryDirectory() as temp_dir:
                zf.extractall(path=temp_dir)

                for _, _, filenames in os.walk(temp_dir):
                    for file_name in filenames:
                        s3_hook.load_file(
                            filename=os.path.join(temp_dir, file_name),
                            bucket_name="english-stackexchange-com",
                            key=f"raw/{file_name}",
                            replace=True
                        )

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG("etl_english_stackexchange_com",
         start_date=datetime(2019, 1, 1),
         max_active_runs=3,
         schedule_interval=None,  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
         default_args=default_args,
         # catchup=False # enable if you don't want historical dag runs to run
         ) as dag:

    s3_create_bucket = S3CreateBucketOperator(
        task_id="s3_create_bucket",
        aws_conn_id="my_conn_s3",
        bucket_name="english-stackexchange-com",
    )

    s3_download_data = PythonOperator(
        task_id="s3_download_data",
        python_callable=download_data,
        op_kwargs={
            "key": "raw/english.stackexchange.com.7z",
            "bucket_name": "english-stackexchange-com"
        }
    )

    s3_create_bucket >> s3_download_data

