# Airflow Modules
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# External modules
from modules import noaa_isd
from modules import isd_io
from modules import postgres
import logging
from datetime import timedelta, datetime
import glob
import os

CLEAN_CSV_DIRECTORY = f"{noaa_isd.airflow_dir}/data/clean" 
RAW_FILES_DIRECTORY = f"{noaa_isd.airflow_dir}/data/raw"

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
    if not os.path.exists(f"{RAW_FILES_DIRECTORY}/{year}"):
        logging.info("Folder for year %s raw data not found. Creating %s now", year, f"{RAW_FILES_DIRECTORY}/{year}")
        os.makedirs(f"{RAW_FILES_DIRECTORY}/{year}")

    # Download all the objects through multi-threading
    noaa_isd.download_multiple(object_keys)    
    logging.info("All objects for year %s retrieved from %s and saved to %s local directory", year, f"isd-lite/data/{year}/", f"~/data/raw/{year}/")


def transform_task(execution_date):
    """
    Transform the extracted flat file (from downloaded objects in extract_task) into weather daily summaries.
    Save the clean version into a local CSV file inside data/clean/current_year.
    """

    # Folder to save the clean files. Create it if it does not exist
    year = execution_date.strftime('%Y')
       
    if not os.path.exists(f"{CLEAN_CSV_DIRECTORY}/{year}"):
        logging.info("Folder for year %s clean data not found. Creating %s now", year, CLEAN_CSV_DIRECTORY)
        os.makedirs(f"{CLEAN_CSV_DIRECTORY}/{year}")
   
    for filename in glob.glob(f"{RAW_FILES_DIRECTORY}/{year}/*"):
        # Transform every non-csv file that are modified within the last 24 hours
        if not filename.endswith("csv") and datetime.now().timestamp() - os.path.getmtime(filename) <= 86400:
            isd_io.transform(filename, year)        
    

def upsert_weather_task(execution_date):
    """
    Upsert CSV file into PostgreSQL database.
    """
    # Create a database connection
    credentials = {
        "host": "app-db",        
        "user": os.environ['POSTGRES_USER'],
        "password": os.environ['POSTGRES_PASSWORD'],
        "database": os.environ['POSTGRES_DB']
    }
    engine = postgres.PsqlEngine(credentials)

    # TSV Files (clean data to upsert)
    year = execution_date.strftime('%Y')
    tsv_files = glob.glob(f"{CLEAN_CSV_DIRECTORY}/{year}/*")

    # Upsert the clean csv data to weather table
    try:
        for tsv_file in tsv_files:
            # Select only those that contains the weather data (size not in filename) and modified within the last 24 hours
            if tsv_file.endswith("tsv") and datetime.now().timestamp() - os.path.getmtime(tsv_file) <= 86400:                
                with open(tsv_file, 'r') as f:
                    next(f)
                    engine.upsert("weather", f)
    finally:
        del engine


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

    task4 = PythonOperator(
        task_id = "IngestWeatherData",
        python_callable = upsert_weather_task
    )

    # Download the objects (archives) > Extract the files containing raw weather records > Transform the raw data and save to CSV file > Upsert CSV to PostgreSQL database
    task1 >> task2 >> task3 >> task4