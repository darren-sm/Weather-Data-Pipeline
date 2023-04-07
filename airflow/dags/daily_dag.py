# Airflow Modules
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# External modules
from modules import noaa_isd
from modules import isd_io
import logging
from datetime import timedelta, datetime
import glob
import os

# @logger
def download_task(execution_date):    
    """
    Download the objects modified within the last 24 hours inside an S3 bucket folder of current year.    
    """
    # Get the list of objects to download
    year = execution_date.strftime('%Y')
    list_of_objects = noaa_isd.list_object_keys(f"isd-lite/data/{year}/")
    object_keys = (obj.key for index, obj in enumerate(noaa_isd.get_daily_list(list_of_objects)) if index < 5)

    # Folder to save the files. Create it if it does not exist
    directory = f"{noaa_isd.airflow_dir}/data/raw/{year}"
    if not os.path.exists(directory):
        logging.info("Folder for year %s raw data not found. Creating %s now", year, directory)
        os.makedirs(directory)

    # Download all the objects through multi-threading
    noaa_isd.download_multiple(object_keys)    
    logging.info("All objects for year %s retrieved from %s and saved to %s local directory", year, f"isd-lite/data/{year}/", f"airflow/data/raw/{year}/")


def transform_task(execution_date):
    """
    Transform the extracted flat file (from downloaded objects in extract_task) into weather daily summaries.
    Save the clean version into a local CSV file inside data/clean/current_year.
    """

    # Folder to save the clean files. Create it if it does not exist
    year = execution_date.strftime('%Y')
    directory = f"{noaa_isd.airflow_dir}/data/clean/{year}"    
    if not os.path.exists(directory):
        logging.info("Folder for year %s clean data not found. Creating %s now", year, directory)
        os.makedirs(directory)

    # List out all the raw files that are not yet cleaned
    raw_files_directory = f"{noaa_isd.airflow_dir}/data/raw/{execution_date.strftime('%Y')}"
    for filename in glob.glob(f"{raw_files_directory}/*"):
        # Transform every non-csv file that are modified within the last 24 hours
        if not filename.endswith("csv") and datetime.now().timestamp() - os.path.getmtime(filename) <= 86400:
            isd_io.transform(filename, year)        
    

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
        python_callable = download_task
    )

    task2 = BashOperator(
        task_id = "ExtractArchive",
        # bash_command = 'echo "Path to raw data files ${AIRFLOW_HOME:-/opt/airflow}/data/raw/{{ execution_date.strftime(\'%Y\') }}"'
        bash_command = 'echo Found $(eval "find ${AIRFLOW_HOME:-/opt/airflow}/data/raw/{{ execution_date.strftime(\"%Y\") }} -name \'*.gz\' | wc -l") .gz archives in /raw/{{ execution_date.strftime(\"%Y\") }} folder. Extracting them all now. && gunzip -fv ${AIRFLOW_HOME:-/opt/airflow}/data/raw/{{ execution_date.strftime(\"%Y\") }}/*.gz'
    )

    task3 = PythonOperator(
        task_id = "TransformData",
        python_callable = transform_task
    )

    # Download the objects (archives) > Extract the files containing raw weather records > Transform the raw data and save to CSV file > Upsert CSV to PostgreSQL database
    task1 >> task2 >> task3