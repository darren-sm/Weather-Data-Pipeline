import pandas as pd
import isd_io
import numpy as np


def download_data(list_of_bucket_files):
    # list_of_bucket_files is provided in individual dags. History dag will fetch all while daily dag will fetch only objects which are not older than 24 hours
    # Use multi-threading to download the files to f"{airflow_dir}/data/raw/year/flat_file"
    pass

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
    # Create a generator for reading the content of 888890-99999-2022 flat file
    file_data = isd_io.read_isd("temp/888890-99999-2022") # use filename in read_isd (e.g. f"{airflow_dir}/data/raw/year/flat_file")

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

def upsert(filename):
    
    pass