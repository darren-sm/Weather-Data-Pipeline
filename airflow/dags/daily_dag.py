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

YEAR = '{{ data_interval_start.strftime(\"%Y\") }}'
TODAY = datetime.now().strftime("%Y-%m-%d")
CLEAN_CSV_DIRECTORY = f"{noaa_isd.airflow_dir}/data/clean" 
RAW_FILES_DIRECTORY = f"{noaa_isd.airflow_dir}/data/raw"

# @logger
def download_task(data_interval_start):    
    """
    Download the objects modified within the last 24 hours inside an S3 bucket folder of current year.    
    """
    # Get the list of objects to download
    year = data_interval_start.strftime('%Y')
    list_of_objects = noaa_isd.list_object_keys(f"isd-lite/data/{year}/")
    object_keys = (obj.key for index, obj in enumerate(list_of_objects) if index < 600)
    # object_keys = (obj.key for obj in noaa_isd.get_daily_list(list_of_objects))

    # Folder to save the raw files. Create it if it does not exist        
    os.makedirs(f"{RAW_FILES_DIRECTORY}/{year}")    
    os.makedirs(f"{CLEAN_CSV_DIRECTORY}/{year}")
   
    # Download all the objects through multi-threading
    noaa_isd.download_multiple(object_keys)    
    logging.info("All objects for year %s retrieved from %s and saved to %s local directory", year, f"isd-lite/data/{year}/", f"~/data/raw/{year}/")


def transform_task(data_interval_start):
    """
    Transform the extracted flat file (from downloaded objects in extract_task) into weather daily summaries.
    Save the clean version into a local CSV file inside data/clean/current_year.
    """    
    year = data_interval_start.strftime("%Y")
    filename = f"{RAW_FILES_DIRECTORY}/{year}/{TODAY}.txt"

    for filename in glob.glob(f"{RAW_FILES_DIRECTORY}/{year}/*.txt"):
        if TODAY in filename:    
            logging.info("Transforming stage on target %s", filename)
            isd_io.transform(filename, year)        

def upsert_weather_task(data_interval_start):
    """
    Upsert CSV file into PostgreSQL database.
    """
    # Create a database connection    
    engine = postgres.PsqlEngine({
        "host": "postgres",        
        "user": os.environ['POSTGRES_USER'],
        "password": os.environ['POSTGRES_PASSWORD'],
        "database": os.environ['POSTGRES_DB']
    }        
    )

    # TSV Files (clean data to upsert)
    year = data_interval_start.strftime('%Y')
    tsv_files = glob.glob(f"{CLEAN_CSV_DIRECTORY}/{year}/*")

    # Upsert the clean csv data to weather table
    try:
        for tsv_file in tsv_files:
            # Select only those that contains the weather data (size not in filename) and modified within the last 24 hours
            if all(x in tsv_file for x in ['tsv', TODAY]):        
                logging.info("Upserting tsv file %s", tsv_file)        
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
        # bash_command = 'echo "Path to raw data files ${AIRFLOW_HOME:-/opt/airflow}/data/raw/{{ data_interval_start.strftime(\'%Y\') }}"'
        bash_command = f"""
        echo Found $(eval "find {RAW_FILES_DIRECTORY}/{YEAR} -name \'*.gz\' | wc -l") .gz archives in /raw/{YEAR} folder. Extracting them all now. && gunzip -fv {RAW_FILES_DIRECTORY}/{YEAR}/*.gz
        """
    )

    task3 = BashOperator(
        task_id = "CombineFiles",
        bash_command = f"""
        raw_dir={RAW_FILES_DIRECTORY}/{YEAR}                
        
        # count the number of files to be combined
        echo $(find $raw_dir -type f -mtime -1 | wc -l) files to be edited inside $raw_dir/$year
        
        # for every file in the directory, add the station_id (modified filename) as prefix at every line
        # Set the number of processes to use
        NUM_PROCESSES=4

        # Get the list of files without extensions
        FILES=$(find $raw_dir -type f -mtime -1 ! -name "*.*")

        # Define the function to add the filename prefix to each line in place
        function add_prefix {{
            BASENAME=$(basename $1)
            PREFIX=$(echo $BASENAME | sed 's/-2023//')
            sed -i "s/^/${{PREFIX}} /" $1	
        }}

        # Export the function so it can be used by subprocesses
        export -f add_prefix

        # Use xargs to run the function in parallel on each file
        echo "$FILES" | xargs -I{{}} -P $NUM_PROCESSES bash -c 'add_prefix "$@"' _ {{}}

        echo "Adding prefix finished. Now combining every 500 files into a single .txt file"
              
        find $raw_dir -maxdepth 1 -type f ! -name "*.*" | sort -V > file_list.txt
        split -d -l 500 file_list.txt file_list_
        for file in file_list_*; do
            echo Combining 500 files into $raw_dir/combined-"${{file##*_}}".txt
            cat "$file" | xargs -I{{}} bash -c 'cat {{}} >> {RAW_FILES_DIRECTORY}/{YEAR}/{TODAY}-'"${{file##*_}}".txt &            
        done

        wait

        echo "Concatenation finished"        
        
        rm file_list*
        find $raw_dir -type f -not -name "*.*" -delete

        echo "Original raw files deleted"    
        """
    )

    task4 = PythonOperator(
        task_id = "TransformData",
        python_callable = transform_task
    )

    task5 = PythonOperator(
        task_id = "IngestWeatherData",
        python_callable = upsert_weather_task
    )


    # Download the objects (archives) > Extract the files containing raw weather records > combine the files into single .txt file >> Transform the raw data and save to CSV file > Upsert CSV to PostgreSQL database
    task1 >> task2 >> task3 >> task4 >> task5