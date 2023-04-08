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
DB_CREDENTIALS = {
    "host": "app-db",        
    "user": os.environ['POSTGRES_USER'],
    "password": os.environ['POSTGRES_PASSWORD'],
    "database": os.environ['POSTGRES_DB']
}

# @logger
def download_task(execution_date):    
    """
    Download the objects modified within the last 24 hours inside an S3 bucket folder of current year.    
    """
    # Get the list of objects to download
    year = execution_date.strftime('%Y')
    list_of_objects = noaa_isd.list_object_keys(f"isd-lite/data/{year}/")
    object_keys = (obj.key for obj in noaa_isd.get_daily_list(list_of_objects))

    # Folder to save the raw files. Create it if it does not exist    
    if not os.path.exists(f"{RAW_FILES_DIRECTORY}/{year}"):
        logging.info("Folder for year %s raw data not found. Creating %s now", year, f"{RAW_FILES_DIRECTORY}/{year}")
        os.makedirs(f"{RAW_FILES_DIRECTORY}/{year}")
    # Folder to save the clean files. 
    if not os.path.exists(f"{CLEAN_CSV_DIRECTORY}/{year}"):
        logging.info("Folder for year %s clean data not found. Creating %s now", year, CLEAN_CSV_DIRECTORY)
        os.makedirs(f"{CLEAN_CSV_DIRECTORY}/{year}")
   
    # Download all the objects through multi-threading
    noaa_isd.download_multiple(object_keys)    
    logging.info("All objects for year %s retrieved from %s and saved to %s local directory", year, f"isd-lite/data/{year}/", f"~/data/raw/{year}/")


def transform_task(execution_date):
    """
    Transform the extracted flat file (from downloaded objects in extract_task) into weather daily summaries.
    Save the clean version into a local CSV file inside data/clean/current_year.
    """    
    year = execution_date.strftime('%Y')          
    for filename in glob.glob(f"{RAW_FILES_DIRECTORY}/{year}/*"):
        # Transform every non-csv file that are modified within the last 24 hours
        if not filename.endswith("csv") and datetime.now().timestamp() - os.path.getmtime(filename) <= 86400:
            isd_io.transform(filename, year)        
    
def count_record_task(execution_date):
    """
    Record the count of data for each day and station
    """    
    year = execution_date.strftime('%Y')           
    for filename in glob.glob(f"{RAW_FILES_DIRECTORY}/{year}/*"):
        # Transform every csv file that are modified within the last 24 hours
        if not filename.endswith("csv") and datetime.now().timestamp() - os.path.getmtime(filename) <= 86400:
            isd_io.count_record(filename, year)      

def upsert_weather_task(execution_date):
    """
    Upsert CSV file into PostgreSQL database.
    """
    # Create a database connection    
    engine = postgres.PsqlEngine(DB_CREDENTIALS)

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

def upsert_count_task(execution_date):
    """
    Append the record count to the database
    """
    # Create a database connection    
    engine = postgres.PsqlEngine(DB_CREDENTIALS)
    year = execution_date.strftime('%Y')

    # Upsert the clean csv data to weather table
    try:
        for csv_file in glob.glob(f"{CLEAN_CSV_DIRECTORY}/{year}/*"):
            # Select only those that contains the weather data (size not in filename) and modified within the last 24 hours
            if csv_file.endswith("csv") and datetime.now().timestamp() - os.path.getmtime(csv_file) <= 86400:                
                with open(csv_file, 'r') as f:                    
                    engine.upsert("records_count", f)
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

    task3a = PythonOperator(
        task_id = "TransformData",
        python_callable = transform_task
    )

    task3b = PythonOperator(
        task_id = "CountRecords",
        python_callable = count_record_task
    )

    task4a = PythonOperator(
        task_id = "IngestWeatherData",
        python_callable = upsert_weather_task
    )

    task4b = PythonOperator(
        task_id = "IngestCountData",
        python_callable = upsert_weather_task
    )

    # Download the objects (archives) > Extract the files containing raw weather records > Transform the raw data and save to CSV file > Upsert CSV to PostgreSQL database
    task1 >> task2 >> [task3a, task3b]
    task3a >> task4a
    task3b >> task4b