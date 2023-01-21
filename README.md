# big_data_aws_grad
Big Data postgraduate project

Env versions:
- AWS EMR 6.7.0
- Spark 3.2.1

Python/Airflow dependencies:
- apache-airflow[amazon]
- py7zr

Additional JAR files:
- spark-xml

Airflow connection setup template:
- aws_conn {"region_name": "us-east-1", "session_kwargs": {"aws_session_token": ""}}
- s3_conn {"aws_access_key_id": "", "aws_secret_access_key": "", "aws_session_token": ""}

File config contains Json which should be put in airflow variable called file_config


