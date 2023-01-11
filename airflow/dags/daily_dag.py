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
    """
    Download the objects modified within the last 24 hours inside an S3 bucket folder of current year.    
    """
    start = datetime.now()
    year = execution_date.strftime('%Y')
    # etl.download_data(year, category = "daily")
    logging.info(f"Starting task 1 (Downloading object archives) @ {datetime.today()}")
    etl.download_data("2022", category = "daily")
    logging.info("Finished extract_task. All downloads saved to airflow/data/raw/%s. Task took up %s seconds", year, (datetime.now() - start).seconds)

def transform_task():
    """
    Transform the extracted flat file (from downloaded objects in extract_task) into weather daily summaries.
    Save the clean version into a local CSV file inside data/clean/current_year.
    """
    # for year in airflow_dir/data/raw/
    # for file in year folder
    # Use modules.etl.transform(file)
    print("Hello from task 3 (Python)")
    pass

def upsert_task():
    """
    Upsert CSV file into PostgreSQL database.
    """
    # https://stackoverflow.com/questions/48019381/how-postgresql-copy-to-stdin-with-csv-do-on-conflic-do-update
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
        # bash_command = 'echo "Path to raw data files ${AIRFLOW_HOME:-/opt/airflow}/data/raw/{{ execution_date.strftime(\'%Y\') }}"'
        bash_command = 'echo Found $(eval "find ${AIRFLOW_HOME:-/opt/airflow}/data/raw/2022 -name \'*.gz\' | wc -l") .gz archives in /raw/2022 folder. Extracting them all now. && gunzip -fv ${AIRFLOW_HOME:-/opt/airflow}/data/raw/2022/*.gz'
    )

    task3 = PythonOperator(
        task_id = "TransformData",
        python_callable = transform_task
    )

    # Download the objects (archives) > Extract the files containing raw weather records > Transform the raw data and save to CSV file > Upsert CSV to PostgreSQL database
    task1 >> task2 >> task3