"""
Methods to read and transform the content of text-based extracted files from ISD archives
"""

import datetime 
import logging 
import os
from modules.decorator import logger
import pandas as pd
import numpy as np

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
        logging.warning("Value %s missing or unreasonable exceeding the set range %s", value, (min_val, max_val))
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
def transform(filename):
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
    >>> # Transformed data saved in "data/clean/2022/010010-99999-2022.csv"
    """
    # Create a generator for reading the content of flat file    
    file_data = read_isd(filename)

    # Using pandas, create a temp count column which contains the no. of weather records in a given day
    # If a day has only less than 4 records, drop them all    
    df = pd.DataFrame(file_data)    
    df['count'] = df.groupby('date')['date'].transform('count')    
    df = df[df['count'] > 3]    
    df = df.drop('count', axis = 1)

    # Summarize the dataset using the `mean` aggregate function
    df = df.groupby(['station_id', 'date']).mean(numeric_only= False).round(2)

    # Map the floor(value) of sky condition column
    df['sky_condition'] = np.floor(df['sky_condition'])
    df['sky_condition'] = df['sky_condition'].apply(lambda x: sky_conditions[x] if pd.notnull(x) else x)

    # Save in f"{airflow_dir}/data/clean/year/station_id.csv"
    df.to_csv(f"{filename}.csv", encoding='utf-8', index=False)

        
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
