"""
Methods to read and transform the content of text-based extracted files from ISD archives
"""

import datetime 
import logging 
import os
from modules.decorator import logger
import pandas as pd
import numpy as np

AIRFLOW_DIR = os.environ.get("AIRFLOW_HOME", "/opt/airflow")    

def _parse_value(value, category):
    """
    Validate the value of weather record on a given category. Values must be in reasonable range. Range is made up based on the current world record + 40% allowance.

    Parameter:
    ----------
    value: float        
    category: string
        Weather category (e.g. air temperature, pressure, dew point, etc.)

    Returns:
    ----------
    None if value is missing (-9999 in raw file) or beyond the reasonable range else return the value as is

    Examples:
    ----------
    >>> # Validate the given air temperature 
    >>> _parse_value(-9999, "air_temperature")
    None
    >>> # Validate the given dew point
    >>> _parse_value(20.1, "dew_point")
    20.1
    """
    # Parsing the value for numerical records including Air Temperature, Dew Point Temperature, Sea Level Pressure, Wind Speed, One hour precipitation, and Six hour precipitation
    reasonable_ranges = {
        # I am only making up these numbers based on the set bounds or current world records + 40% allowance for future record breakers
        "air_temperature": (-125.16, 79.38),
        "dew_point": (-46.48, 49),
        "sea_lvl_pressure": (522, 1517.32),
        "wind_direction": (0,360),
        "wind_speed": (0, 158.2),        
        "sky_condition": (0, 19),
        "one_hour_precipitation": (0, 561.4),
        "six_hour_precipitation": (0, 2000)
    }

    min_val, max_val = reasonable_ranges[category]
    # Missing value = -9999
    if value == -9999 or min_val > value or max_val < value:        
        return None
    return value


sky_conditions = {        
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

@logger 
def count_record(filename, year):
    # File base name and Airflow directory
    base_name = os.path.basename(filename)    
    
    # Create a generator for reading the content of flat file and make a DataFrame out of it
    file_data = read_isd(filename)
    df = pd.DataFrame(file_data)

    # Get the count of each row by date and station_id then save it to a CSV
    file_size = df.groupby(['station_id', 'date']).size()
    file_size.to_csv(f"{AIRFLOW_DIR}/data/clean/{year}/{base_name}-size.csv", encoding='utf-8', index=True, header=False)

@logger
def transform(filename, year):
    """
    Transform the content of a text-based flat file into day summarization of hourly records. Save the transformed data into a local CSV flat file.

    Parameter:
    ----------
    filename: string
        Name of the flat file containing raw hourly weather data.

    Returns:
    ----------
    None

    Example:
    ----------
    >>> # Transform the content of "010010-99999-2022" flat file inside "data/daw/2022" folder
    >>> transform("data/raw/2022/010010-99999-2022")
    >>> # Transformed data saved in "data/clean/2022/010010-99999-2022.tsv"
    """
    # File base name and Airflow directory
    base_name = os.path.basename(filename)    
    
    # Create a generator for reading the content of flat file and make a DataFrame out of it
    file_data = read_isd(filename)
    df = pd.DataFrame(file_data)

    # Remove the rows with each date and station_id having less than 4 records
    df = df.groupby(['station_id', 'date']).filter(lambda x: len(x) > 3).reset_index(drop=True)

    # Get the summarization of data (min, mean, max)
    df = df.groupby(['station_id', 'date']).agg(
        air_temperature_avg = ('air_temperature', 'mean'),
        air_temperature_min = ('air_temperature', 'min'),
        air_temperature_max = ('air_temperature', 'max'),
        dew_point_avg = ('dew_point', 'mean'),
        dew_point_min = ('dew_point', 'min'),
        dew_point_max = ('dew_point', 'max'),
        sea_lvl_pressure_avg = ('sea_lvl_pressure', 'mean'),
        sea_lvl_pressure_min = ('sea_lvl_pressure', 'min'),
        sea_lvl_pressure_max = ('sea_lvl_pressure', 'max'),
        wind_direction_avg = ('wind_direction', 'mean'),
        wind_direction_min = ('wind_direction', 'min'),
        wind_direction_max = ('wind_direction', 'max'),
        wind_speed_avg = ('wind_speed', 'mean'),
        wind_speed_min = ('wind_speed', 'min'),
        wind_speed_max = ('wind_speed', 'max'),    
        sky_condition = ('sky_condition', 'mean'),
        one_hour_precipitation_avg = ('one_hour_precipitation', 'mean'),
        one_hour_precipitation_min = ('one_hour_precipitation', 'min'),
        one_hour_precipitation_max = ('one_hour_precipitation', 'max'),
        six_hour_precipitation_avg = ('six_hour_precipitation', 'mean'),
        six_hour_precipitation_min = ('six_hour_precipitation', 'min'),
        six_hour_precipitation_max = ('six_hour_precipitation', 'max')
    )

    # Map the floor(value) of sky condition column
    df['sky_condition'] = np.round(df['sky_condition'])
    df['sky_condition'] = df['sky_condition'].apply(lambda x: sky_conditions[x] if pd.notnull(x) else x)

    # Save as TSV file
    df.to_csv(f"{AIRFLOW_DIR}/data/clean/{year}/{base_name}.tsv", sep="\t", encoding='utf-8', index=True)

        
def read_isd(filename):
    """
    Reads a local text-based flat file containing the raw hourly weather records. 

    Parameter
    ---------
    filename: string
        Name of the flat file containing the raw weather data
    
    Yields
    ---------
    dictionary containing weather categories and their values. 

    Example
    ---------
    >>> file_data = read_isd("data/raw/2022/010010-99999-2022")
    >>> print(file_data)
    {
        "station_id": "010010-99999",
        "current_date": datetime.date(2022, 01, 01)
        "air_temperature": 40,
        ...
    }
    """
    
    station_id, wban, _ = os.path.basename(filename).split("-")

    logging.info("Reading the content of %s ISD", filename)
    with open(filename, "r", encoding="UTF-8") as f:
        for _, line in enumerate(f, start = 1):
            # Check if each line follows the set Data Format (each line must occupy 61 positions)
            if len(line.strip()) != 61:
                # logging.warning("Line %s has invalid length of %s instead of 61. Skipping this line.", index, len(line.strip()))
                continue
            
            # split the line by whitespace
            line_content = [float(i) for i in line.split()]

            # Date = first three elements of line_content in order: year, month, day
            current_date = datetime.date(*(int(i) for i in line_content[:3]))

            # Parse and yield line data
            yield {
                "station_id": f"{station_id}-{wban}",
                "date": current_date,
                "air_temperature": _parse_value(line_content[4], "air_temperature"),
                "dew_point": _parse_value(line_content[5], "dew_point"),
                "sea_lvl_pressure": _parse_value(line_content[6], "sea_lvl_pressure"),
                "wind_direction": _parse_value(line_content[7], "wind_direction"),
                "wind_speed": _parse_value(line_content[8], "wind_speed"),
                "sky_condition": _parse_value(line_content[9], "sky_condition"),
                "one_hour_precipitation": _parse_value(line_content[10], "one_hour_precipitation"),
                "six_hour_precipitation": _parse_value(line_content[11], "six_hour_precipitation")
            }
