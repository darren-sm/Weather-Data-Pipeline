# Airflow Modules
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# External modules
from modules import noaa_isd
import logging
from datetime import timedelta, datetime
import glob
import psycopg2
import os

# Global variables
YEAR = datetime.now().strftime("%Y")
TODAY = datetime.now().strftime("%Y-%m-%d")
CLEAN_CSV_DIRECTORY = f"{noaa_isd.airflow_dir}/data/clean" 
RAW_FILES_DIRECTORY = f"{noaa_isd.airflow_dir}/data/raw"
CONN = psycopg2.connect(**{
    "host": "postgres",        
    "user": os.environ['POSTGRES_USER'],
    "password": os.environ['POSTGRES_PASSWORD'],
    "database": os.environ['POSTGRES_DB']
})


def download_task():    
    """
    Download the objects modified within the last 24 hours inside an S3 bucket folder of current year.    
    """
    # Get the list of objects to download
    list_of_objects = noaa_isd.list_object_keys(f"isd-lite/data/{YEAR}/")    
    object_keys = (obj.key for obj in noaa_isd.get_daily_list(list_of_objects))

    # Folder to save the raw files. Create it if it does not exist        
    if not os.path.exists(f"{RAW_FILES_DIRECTORY}/{YEAR}"):
        os.makedirs(f"{RAW_FILES_DIRECTORY}/{YEAR}")    
        os.makedirs(f"{CLEAN_CSV_DIRECTORY}/{YEAR}")
   
    # Download all the objects through multi-threading
    noaa_isd.download_multiple(object_keys)    
    logging.info("All objects for year %s retrieved from %s and saved to %s local directory", YEAR, f"isd-lite/data/{YEAR}/", f"~/data/raw/{YEAR}/")


def ingest(db):
    """
    Ingest the content of the raw-text based files into a "temporary" table before upserting it to the real table
    """
    cursor = db.cursor()
    logging.info("Cursor created.")

    with db:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS tmp_weather(
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
            );
        """        
        )
        logging.info("Temporary table tmp_weather created")
    # Copy all .txt files into temp table
    for filename in glob.glob(f'{RAW_FILES_DIRECTORY}/{YEAR}/*.txt'):
        if TODAY in filename:            
            with open(filename, 'r') as f:
                logging.info("Now ingesting %s file (%s MB) into `tmp_weather`", filename, round(os.stat(filename).st_size / 1024 / 1024, 2))
                cursor.copy_from(f, "tmp_weather", sep = ' ', null='-9999')
                CONN.commit()


def upsert(db):
    """
    Upsert the summarized content of `tmp_weather` table into `weather` table.
    """
    cursor = db.cursor()
    with db:
        # Modify and upsert to real table
        logging.info("Summarizing the content of `tmp_weather`")
        cursor.execute(
            """
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
            FROM tmp_weather GROUP BY station_id, make_date(year, month,day) HAVING count(hour) > 3;
            """
        )
        logging.info("Aggregated data upserted")
        CONN.commit()

        # Drop temp table
        cursor.execute('DROP TABLE IF EXISTS tmp_weather;')
        CONN.commit()
        logging.info("`tmp_weather` table deleted")        

    

################# DAG #################

# DAG configuration
local_workflow = DAG(
    "DailyData",
    schedule_interval="1 0 * * *", # Run at 00:01 Everyday
    start_date = days_ago(1),    
    dagrun_timeout=timedelta(minutes=60),
    catchup=False
)


with local_workflow:  
    # Download the objects from the S3 Bucket
    task1 = PythonOperator(
        task_id = "DownloadData",
        python_callable = download_task
    )

    # Extract the content(s) of .gz archives
    task2 = BashOperator(
        task_id = "ExtractArchive",   
        do_xcom_push = False,     
        bash_command = f"""
        echo Found $(eval "find {RAW_FILES_DIRECTORY}/{YEAR} -name \'*.gz\' | wc -l") .gz archives in /raw/{YEAR} folder. Extracting them all now. && gunzip -fv {RAW_FILES_DIRECTORY}/{YEAR}/*.gz
        """
    )

    # Add the filename as prefix for every line on all the extracted text-based files
    task3 = BashOperator(
        task_id = "AddPrefix",
        do_xcom_push = False,
        bash_command = f"""
        # Get the list of files without extensions
        FILES=$(find {RAW_FILES_DIRECTORY}/{YEAR} -type f -mtime -1 ! -name "*.*")

        # Define the function to add the filename prefix to each line in place. Also, remove multiple spaces
        function add_prefix {{
            BASENAME=$(basename $1)
            PREFIX=$(echo $BASENAME | sed 's/-{YEAR}//')
            sed -i "s/^/${{PREFIX}} /" $1
            sed -i "s/[[:space:]]\+/ /g" $1
        }}

        # Export the function so it can be used by subprocesses
        export -f add_prefix

        # Use xargs to run the function in parallel on each file
        echo "$FILES" | xargs -I{{}} -P 4 bash -c 'add_prefix "$@"' _ {{}}
        """
    )

    # Combine every 500 files into a single .txt file
    task4 = BashOperator(
        task_id = "CombineFiles",
        do_xcom_push = False,
        bash_command = f"""
        find {RAW_FILES_DIRECTORY}/{YEAR} -maxdepth 1 -type f ! -name "*.*" | sort -V > file_list.txt
        split -d -l 500 file_list.txt file_list_
        for file in file_list_*; do
            echo Combining 500 files into {RAW_FILES_DIRECTORY}/{YEAR}/combined-"${{file##*_}}".txt
            cat "$file" | xargs -I{{}} bash -c 'cat {{}} >> {RAW_FILES_DIRECTORY}/{YEAR}/{TODAY}_'"${{file##*_}}".txt &            
        done
        wait

        echo "Concatenation finished"     

        rm file_list*
        find {RAW_FILES_DIRECTORY}/{YEAR} -type f -not -name "*.*" -delete

        echo "Original raw files deleted"        
        """
    )

    # Ingest the content of the .txt files into "temporary" table `tmp_weather`
    task5 = PythonOperator(
        task_id = "IngestData",
        python_callable = ingest,
        op_kwargs={
            "db": CONN
        }
    )

    # Upsert the summarized content of `tmp_weather` into `weather` table
    task6 = PythonOperator(
        task_id = "UpsertData",
        python_callable = upsert,
        op_kwargs={
            "db": CONN
        }
    )

    task1 >> task2 >> task3 >> task4 >> task5 >> task6