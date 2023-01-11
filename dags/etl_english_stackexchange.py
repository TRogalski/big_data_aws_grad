import tempfile
import requests
import py7zr
import os

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator,
    EmrAddStepsOperator
)
from airflow.providers.amazon.aws.sensors.emr import (
    EmrStepSensor
)
FILE_CONFIG = {
    "Badges": {
            "column_types": {
                "_Class": "integer",
                "_Id": "string",
                "_Name": "string",
                "_UserId": "string"
            }
    },
    "Comments": {
            "column_types": {
                "_ContentLicense": "string",
                "_CreationDate": "timestamp",
                "_Id": "string",
                "_PostId": "string",
                "_Score": "integer",
                "_Text": "string",
                "_UserDisplayName": "string",
                "_UserId": "string",
            }
    },
    "PostHistory": {
            "column_types": {
                "_Comment": "string",
                "_ContentLicense": "string",
                "_CreationDate": "timestamp",
                "_Id": "string",
                "_PostHistoryTypeId": "string",
                "_PostId": "string",
                "_RevisionGUID": "string",
                "_Text": "string",
                "_UserDisplayName": "string",
                "_UserId": "string"
            }
    },
    "PostLinks": {
            "column_types": {
                "_CreationDate": "timestamp",
                "_Id": "string",
                "_LinkTypeId": "string",
                "_PostId": "string",
                "_RelatedPostId": "string"
            }
    },
    "Posts": {
        "column_types": {
            "_AcceptedAnswerId": "string",
            "_AnswerCount": "integer",
            "_Body": "string",
            "_ClosedDate": "timestamp",
            "_CommentCount": "integer",
            "_CommunityOwnedDate": "timestamp",
            "_ContentLicense": "string",
            "_CreationDate": "timestamp",
            "_FavoriteCount": "integer",
            "_Id": "string",
            "_LastActivityDate": "timestamp",
            "_LastEditDate": "timestamp",
            "_LastEditorDisplayName": "string",
            "_LastEditorUserId": "string",
            "_OwnerDisplayName": "string",
            "_OwnerUserId": "string",
            "_ParentId": "string",
            "_PostTypeId": "string",
            "_Score": "integer",
            "_Tags": "string",
            "_Title": "string",
            "_ViewCount": "integer"
        }
    },
    "Tags": {
            "column_types": {
                "_Count": "integer",
                "_ExcerptPostId": "string",
                "_Id": "string",
                "_TagName": "string",
                "_WikiPostId": "string"
            }
        },
    "Users": {
            "column_types": {
                "_AboutMe": "string",
                "_AccountId": "string",
                "_CreationDate": "timestamp",
                "_DisplayName": "string",
                "_DownVotes": "string",
                "_Id": "string",
                "_LastAccessDate": "timestamp",
                "_Location": "string",
                "_ProfileImageUrl": "string",
                "_Reputation": "integer",
                "_UpVotes": "integer",
                "_Views": "integer",
                "_WebsiteUrl": "string"
            }
    },
    "Votes": {
            "column_types": {
                "_BountyAmount": "integer",
                "_CreationDate": "timestamp",
                "_Id": "string",
                "_PostId": "string",
                "_UserId": "string",
                "_VoteTypeId": "string"
            }
    },
}


JOB_FLOW_OVERRIDES = {
    "Name": "etl_english_stackexchange_com",
    "ReleaseLabel": "emr-6.7.0",
    "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
    "Configurations": [
        {
            "Classification": "spark-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                }
            ],
        }
    ],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master node",
                "Market": "SPOT",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.large",
                "InstanceCount": 1,
            },
            {
                "Name": "Core - 2",
                "Market": "SPOT",
                "InstanceRole": "CORE",
                "InstanceType": "m4.large",
                "InstanceCount": 2,
            },
        ],
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
    },
    "JobFlowRole": "EMR_EC2_DefaultRole",
    "ServiceRole": "EMR_DefaultRole",
}

SPARK_STEPS = [
    {
        "Name": f"save_{file}_xml_as_parquet",
        "ActionOnFailure": "CANCEL_AND_WAIT",
        "HadoopJarStep": {
        "Jar": "command-runner.jar",
        "Args": [
            "spark-submit",
            "--deploy-mode",
            "cluster",
            "--master",
            "yarn",
            "--conf",
            "spark.yarn.submit.waitAppCompletion=true",
            "--jars",
            "s3://english-stackexchange-com/packages/spark-xml_2.12-0.15.0.jar",
            "s3://english-stackexchange-com/spark/emr_save_s3_parquets_spark.py",
            "--source-name",
            f"{file}",
            "--cast-config",
            f"{FILE_CONFIG[file]['column_types']}",
            "--run-date",
            "{{ data_interval_end | ds }}"
        ]
        },
    }
    for file in FILE_CONFIG
]

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
                            bucket_name=bucket_name,
                            key=f"{key}/{file_name}",
                            replace=True
                        )

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG("etl_english_stackexchange",
         start_date=datetime(2019, 1, 1),
         max_active_runs=3,
         schedule_interval=None,
         default_args=default_args,
         ) as dag:

    s3_create_bucket = S3CreateBucketOperator(
        task_id="s3_create_bucket",
        aws_conn_id="s3_conn",
        bucket_name="english-stackexchange-com",
    )

    s3_download_data = PythonOperator(
        task_id="s3_download_data",
        python_callable=download_data,
        op_kwargs={
            "key": "raw/{{ data_interval_end | ds }}",
            "bucket_name": "english-stackexchange-com"
        }
    )

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id="aws_conn"
    )

    step_adder = EmrAddStepsOperator(
        task_id="add_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_conn",
        cluster_states=["RUNNING"],
        steps=SPARK_STEPS
    )

    step_sensor = EmrStepSensor(
        task_id="step_sensor",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[-1] }}",
        target_states=["COMPLETED"],
        failed_states=["FAILED", "CANCELLED"],
        aws_conn_id="aws_conn"
    )

    s3_create_bucket >> s3_download_data >> create_emr_cluster >> step_adder >> step_sensor

