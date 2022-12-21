import pandas as pd
from modules import isd_io
from modules import noaa_isd
import logging
import numpy as np
from datetime import datetime, timedelta

def _get_daily_list(folder_objects):
    for index, obj in enumerate(folder_objects):
        yesterday = datetime.now() - timedelta(hours=24)
        # Check if the object's last modified date is more than 24 hours ago
        if yesterday < obj.last_modified.replace(tzinfo=None):            
            yield obj
    logging.info("Found %s objects modified within the last 24 hours since today %s", index + 1, datetime.now().strftime("%Y-%m-%d %H:%M"))

def download_data(year, category = "historical"):
    list_of_objects = noaa_isd.list_object_keys(f"isd-lite/data/{year}/")
    if category == "daily":
        logging.info("Category set to daily. Now filtering the list of objects to items modified within the last 24 hours since today %s", datetime.now().strftime("%Y-%m-%d %H:%M"))
        list_of_objects = _get_daily_list(list_of_objects)
    # object_keys = (obj.key for obj in list_of_objects)
    object_keys = (obj.key for index, obj in enumerate(list_of_objects) if index < 5)
    noaa_isd.download_multiple(object_keys)    
    logging.info("All objects for year %s retrieved from %s and saved to %s local directory", year, f"isd-lite/data/{year}/", f"airflow/data/raw/{year}/")

def _get_sky_condition(key):
    """
    Fetch the value for an integer sky condition
    
    Returns
    --------- 
    string
        - Sky condition

    Example
    --------- 
    >>> print(get_sky_condition(2))
    One okta
    """
    mapping = {        
        0: "SKC or CLR",
        1: "One okta",
        2: "Two okta",
        3: "Three okta",
        4: "Four okta",
        5: "Five okta",
        6: "Six okta",
        7: "Seven okta",
        8: "Eight okta",
        9: "Sky obscured, or cloud amount cannot be estimated",
        10: "Partial obscuration",
        11: "Thin scattered",
        12: "Scattered",
        13: "Dark scattered",
        14: "Thin broken",
        15: "Broken",
        16: "Dark broken",
        17: "Thin overcast",
        18: "Overcast",
        19: "Dark overcast"
    }
    return mapping[int(key)]

def transform(filename):
    # Create a generator for reading the content of flat file
    file_data = isd_io.read_isd(filename)

    # Create a pandas dataframe from the generator
    df = pd.DataFrame(file_data)

    # Create a count column to check the no. of records of a given date
    df['count'] = df.groupby('date')['date'].transform('count')

    # If a day has only less than 4 records, drop them all
    df = df[df['count'] > 3]

    # Drop 'count' column
    df = df.drop('count', axis = 1)

    # Summarize the dataset using the `mean` aggregate function
    df = df.groupby(['station_id', 'date']).mean(numeric_only= False).round(2)

    # Map the floor(value) of sky condition column
    df['sky_condition'] = np.floor(df['sky_condition'])
    df['sky_condition'] = df['sky_condition'].apply(lambda x: _get_sky_condition(x) if pd.notnull(x) else x)

    # Save in f"{airflow_dir}/data/clean/year/station_id.csv"
    df.to_csv(f"{filename}.csv", encoding='utf-8', index=False)

def upsert(filename):
    
    print("Upsert the local CSV file to PostgreSQL database")