FROM quay.io/astronomer/ap-airflow:2.4.1-1
RUN pip install --no-cache-dir --user py7zr
RUN pip install --no-cache-dir --user 'apache-airflow[amazon]'

