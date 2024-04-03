from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
import os
import requests
import zipfile
import logging
import pandas as pd
import pyarrow.parquet as pq

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def download_and_extract_files(url, extract_folder):
    try:
        logging.info("Downloading file from URL...")
        os.makedirs(extract_folder, exist_ok=True)
        filename = os.path.join(extract_folder, url.split('/')[-1])
        response = requests.get(url, stream=True)
        with open(filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
        logging.info("File downloaded successfully.")
        
        logging.info("Extracting zip file...")
        with zipfile.ZipFile(filename, 'r') as zip_ref:
            zip_ref.extractall(extract_folder)
        logging.info("Extraction completed.")
        print(os.getcwd())
        print(filename)
        os.remove(filename)  # Remove the zip file after extraction
        logging.info("Zip file deleted.")
    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")


dag = DAG(
    'bart_hourly_riders',
    default_args=default_args,
    description='ETL',
    schedule_interval=None,
    catchup=False
)

url = 'https://github.com/pilanpra/DE-Projects/raw/main/parquet_files/Riders_20.zip'
extract_folder = './extracted/RidersMonthly'  # Use the current directory
transform_folder = './transform/RidersMonthly'  # Use the current directory

download_task = PythonOperator(
    task_id='ExtractTask',
    python_callable=download_and_extract_files,
    op_kwargs={'url': url, 'extract_folder': extract_folder},
    dag=dag
)

transform_task = BashOperator(
    task_id='TransformTask',
    bash_command='python3 /home/prasadpilankar/airflow/dags/tranform_to_parquet.py',
    dag=dag
)

load_to_gcs = BashOperator(
        task_id="GoogleCloudStorage_DataLakeTask",
        bash_command='python3 /home/prasadpilankar/airflow/dags/export_to_gcs.py',
        dag=dag
    )

clean_warehouse = BashOperator(
        task_id="GoogleBigQuery_DWCleanTask",
        bash_command='python3 /home/prasadpilankar/airflow/dags/clean_bgq.py',
        dag=dag
    )

load_to_bigquery = BashOperator(
        task_id="GoogleBigQuery_DWLoadTask",
        bash_command='python3 /home/prasadpilankar/airflow/dags/load_to_bgq.py',
        dag=dag
    )

download_task >> transform_task >> load_to_gcs >> clean_warehouse >>load_to_bigquery
 