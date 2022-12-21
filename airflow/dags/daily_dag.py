# Airflow Modules
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# External modules
from modules import etl
from datetime import timedelta, datetime
import logging


def extract_task(execution_date):    
    start = datetime.now()
    year = execution_date.strftime('%Y')
    etl.download_data(year, category = "daily")
    logging.info("Finished extract_task. All downloads saved to airflow/data/raw/%s. Task took up %s seconds", year, (datetime.now() - start).seconds)

def transform_task():
    # for year in airflow_dir/data/raw/
    # for file in year folder
    # Use modules.etl.transform(file)
    pass

def upsert_task():
    # for file in airflow_dir/data/year/clean
    # Use modules.etl.upsert(file)
    pass

################# DAG #################

# DAG configuration
local_workflow = DAG(
    "DailyData",
    schedule_interval="1 0 * * *", # Run at 00:01 Everyday
    start_date = days_ago(1),    
    dagrun_timeout=timedelta(minutes=60)
)


with local_workflow:  
    task1 = PythonOperator(
        task_id = "DownloadData",
        python_callable = extract_task
    )

    task2 = BashOperator(
        task_id = "ExtractArchive",
        bash_command = 'echo "Extract gz files from data/raw. (? Use XCOM to fetch the list of gz files). Remove .gz after extracting"'
    )

    task3 = PythonOperator(
        task_id = "TransformData",
        python_callable = transform_task
    )

    task1 >> task2 >> task3