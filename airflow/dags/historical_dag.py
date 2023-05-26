# Airflow modules
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# External Modules
from modules import noaa_isd
from datetime import datetime, timedelta
import logging
import glob
import os
import psycopg2

# Global variables
CLEAN_FILES_DIRECTORY = f"{noaa_isd.airflow_dir}/data/clean"
RAW_FILES_DIRECTORY = f"{noaa_isd.airflow_dir}/data/raw"
CONN = psycopg2.connect(**{
    "host": "postgres",
    "user": os.environ['POSTGRES_USER'],
    "password": os.environ['POSTGRES_PASSWORD'],
    "database": os.environ['POSTGRES_DB']
})

def download_task(data_interval_start):
    """
    Download the objects inside the S3 Bucket for a given year stated in data_interval_start
    """
    # List of objects
    year = data_interval_start.strftime("%Y")
    list_of_objects = noaa_isd.list_object_keys(f"isd-lite/data/{year}")

    # Save the files inside the raw directory. Transformed data will be in clean directory
    if not os.path.exists(f"{RAW_FILES_DIRECTORY}/{year}"):
        os.makedirs(f"{RAW_FILES_DIRECTORY}/{year}")    
        os.makedirs(f"{CLEAN_FILES_DIRECTORY}/{year}")
    
    # Download the objects through multi-threading
    noaa_isd.download_multiple((obj.key for obj in list_of_objects))
    logging.info("All objects for year %s retrieved from %s and saved to %s local directory", year, f"isd-lite/data/{year}/", f"~/data/raw/{year}/")

def ingest(data_interval_start, db):
    """
    Ingest the content of the raw-text based files into a "temporary" table before upserting it to the real table
    """
    year = data_interval_start.strftime("%Y")
    cursor = db.cursor()
    logging.info("Cursor created")

    # Create the temporary table
    table_name = f"tmp_weather_{year}"
    with db:        
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name}(
                station_id char(12) not null,
                year integer,
                month integer,
                day integer,
                hour integer,
                air_temperature integer, 
                dew_point integer, 
                sea_lvl_pressure integer, 
                wind_direction integer, 
                wind_speed integer, 
                sky_condition integer, 
                one_hour_precipitation integer, 
                six_hour_precipitation integer
            )
            """
        )
        logging.info("Temporary table %s created", table_name)
    # Copy the content of all .txt files into temp table
    for filename in glob.glob(f"{RAW_FILES_DIRECTORY}/{year}/*.txt"):
        with open(filename, 'r') as f:
            logging.info("Now ingesting %s file (%s MB) into `%s`", filename, round(os.stat(filename).st_size / 1024 / 1024, 2), table_name)
            cursor.copy_from(f, table_name, sep = ' ', null='-9999')
            CONN.commit()

        
def upsert(data_interval_start, db):
    """
    Upsert the summarized content of "temporary" table (for the year of data_interval_start) into `weather` table.
    """
    year = data_interval_start.strftime("%Y")
    cursor = db.cursor()
    
    # Modify and upsert to real table
    table_name = f"tmp_weather_{year}"
    with db:
        logging.info("Summarizing the content of `tmp_weather`")
        cursor.execute(
            f"""
            INSERT INTO weather
            SELECT 
                station_id, 
                make_date(year, month,day) as date, 
                count(hour) as n_records,
                avg(air_temperature) as air_temperature_avg, 
                min(air_temperature) as air_temperature_min, 
                max(air_temperature) as air_temperature_max,
                avg(dew_point) as dew_point_avg, 
                min(dew_point) as dew_point_min, 
                max(dew_point) as dew_point_max,
                avg(sea_lvl_pressure) as sea_lvl_pressure_avg, 
                min(sea_lvl_pressure) as sea_lvl_pressure_min, 
                max(sea_lvl_pressure) as sea_lvl_pressure_max,
                avg(wind_direction) as wind_direction,
                avg(wind_speed) as wind_speed_avg, 
                min(wind_speed) as wind_speed_min, 
                max(wind_speed) as wind_speed_max,
                round(avg(sky_condition)) as sky_condition, 
                avg(one_hour_precipitation) as one_hour_precipitation_avg,
                min(one_hour_precipitation) as one_hour_precipitation_min, 
                max(one_hour_precipitation) as one_hour_precipitation_max,
                avg(six_hour_precipitation) as six_hour_precipitation_avg, 
                min(six_hour_precipitation) as six_hour_precipitation_min, 
                max(six_hour_precipitation) as six_hour_precipitation_max
            FROM {table_name} GROUP BY station_id, make_date(year, month,day) HAVING count(hour) > 3;
            """
        )
        logging.info("Aggregated data upserted")
        CONN.commit()

        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        CONN.commit()
        logging.info(f"`{table_name}` table deleted")


################# DAG #################

# DAG configuration
local_workflow = DAG(
    "HistoricalData",
    schedule_interval="0 0 1 1 *", # Run at 00:00 every year
    start_date = datetime(1999, 1, 1, 0, 0),
    dagrun_timeout=timedelta(minutes=60),
    catchup=True
)

with local_workflow:    
    year_template = '{{ data_interval_start.strftime(\'%Y\') }}'

    # Download the objects from the S3 Bucket
    task1 = PythonOperator(
        task_id = "DownloadData",
        python_callable = download_task
    )

    # Extract the content(s) of .gz archives    
    task2 = BashOperator(
        task_id = "ExtractArchive",
        do_xcom_push = False,
        bash_command= f"""
        echo Found $(eval "find {RAW_FILES_DIRECTORY}/{year_template} -name \'*.gz\' | wc -l") .gz archives in /raw/{year_template} folder. Extracting them all now. && gunzip -fv {RAW_FILES_DIRECTORY}/{year_template}/*.gz
        """
    )

    # Add the filename as prefix for every line on all the extracted text-based files 
    task3 = BashOperator(
        task_id = 'AddPrefix',
        do_xcom_push = False,
        bash_command = f"""
        # Get the list of files without extension
        FILES=$(find {RAW_FILES_DIRECTORY}/{year_template} -type f ! -name "*.*")
        
        # Define the function to add the filename as prefix to each line in place. Also, replace multiple spaces with a single one
        function add_prefix {{
            BASENAME=$(basename $1)
            PREFIX=$(echo $BASENAME | sed 's/-{year_template}//')
            sed -i "s/^/${{PREFIX}} /" $1
            sed -i "s/[[:space:]]\+/ /g" $1
        }}

        # Export the function so it can be used by subprocesses
        export -f add_prefix

        # Use xargs to run the function in parallel
        echo "$FILES" | xargs -I{{}} -P 4 bash -c 'add_prefix "$@"' _ {{}}
        """    
    )

    # Combine every 500 files into a single .txt file
    task4 = BashOperator(
        task_id = "CombineFiles",
        do_xcom_push = False,
        bash_command= f"""
        # Save the name of files
        find {RAW_FILES_DIRECTORY}/{year_template} -maxdepth 1 -type f ! -name "*.*" | sort -V > file_list.txt
        # Split every 500 into new list
        split -d -l 500 file_list.txt file_list_

        # Loop over every file list (containing 500 names) then combine the content of those files
        for file in file_list_*; do
            echo Combining 500 files into {RAW_FILES_DIRECTORY}/{year_template}/combined-"${{file##*_}}".txt
            cat "$file" | xargs -I{{}} bash -c 'cat {{}} >> {RAW_FILES_DIRECTORY}/{year_template}/{year_template}_'"${{file##*_}}".txt &
        done
        wait

        echo "Concatenation finished"

        rm file_list*
        
        find {RAW_FILES_DIRECTORY}/{year_template} -type f -not -name "*.*" -delete
        echo "Original raw files deleted"  
        """
    )

    # Ingest the content of the .txt files into "temporary" table for every year in data_interval_start
    task5 = PythonOperator(
        task_id = "IngestData",
        python_callable=ingest,
        op_kwargs={
            "db": CONN
        }
    )

    # Upsert the summarized content of the "temporary" table into `weather` table
    task6 = PythonOperator(
        task_id = "UpsertData",
        python_callable= upsert,
        op_kwargs={
            "db": CONN
        }
    )

    task1 >> task2 >> task3 >> task4 >> task5 >> task6