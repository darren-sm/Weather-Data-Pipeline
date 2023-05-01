"""
Methods to read and transform the content of text-based extracted files from ISD archives
"""
import os
from modules.decorator import logger
import dask.dataframe as dd
import logging

AIRFLOW_DIR = os.environ.get("AIRFLOW_HOME", "/opt/airflow")    

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

def _read_file(filename):
    """
    Read the content of the file as dataframe
    """
     
    colnames = ["station_id", "year", "month", "day", "hour", "air_temperature", "dew_point", "sea_lvl_pressure", "wind_direction", "wind_speed", "sky_condition", "one_hour_precipitation", "six_hour_precipitation"]
    df = dd.read_csv(
    filename, 
    header = None, 
    names = colnames, 
    sep='\s+', 
    engine='python', 
    na_values=[-9999],
    dtype={
        'year': 'int64',
        'month': 'int64',
        'day': 'int64',
        'hour': 'int64',
        'air_temperature': 'float64',
        'dew_point': 'float64',
        'sea_lvl_pressure': 'float64',
        'wind_direction': 'float64',
        'wind_speed': 'float64',
        'sky_condition': 'float64',
        'one_hour_precipitation': 'float64',
        'six_hour_precipitation': 'float64'
        }
    )

    # Create date column from the concatenation of 3 
    df['date'] =  df['year'].astype(str).str.cat([df['month'].astype(str), df['day'].astype(str)], sep='-')
    df = df.drop(['year', 'month', 'day'], axis=1)
    logging.info("Date column created.")

    return df



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
    
    base_name = os.path.splitext(os.path.basename(filename))[0]    
    df = _read_file(filename)
    
    # Remove the rows with each date and station_id having less than 4 records
    grouped = df.groupby(['station_id', 'date']).count()
    selected = grouped[grouped['hour'] > 3].reset_index()[['station_id', 'date']]
    result = df.merge(selected, on=['station_id', 'date'], how='inner')

    logging.info('Stations and dates having less than 3 records dropped')

    # Get the summarization of data (min, mean, max)
    df = result.groupby(['station_id', 'date']).agg(
        n_records = ('hour', 'count'),
        air_temperature_avg=('air_temperature', 'mean'),
        air_temperature_min=('air_temperature', 'min'),
        air_temperature_max=('air_temperature', 'max'),
        dew_point_avg=('dew_point', 'mean'),
        dew_point_min=('dew_point', 'min'),
        dew_point_max=('dew_point', 'max'),
        sea_lvl_pressure_avg=('sea_lvl_pressure', 'mean'),
        sea_lvl_pressure_min=('sea_lvl_pressure', 'min'),
        sea_lvl_pressure_max=('sea_lvl_pressure', 'max'),
        wind_direction_avg=('wind_direction', 'mean'),
        wind_direction_min=('wind_direction', 'min'),
        wind_direction_max=('wind_direction', 'max'),
        wind_speed_avg=('wind_speed', 'mean'),
        wind_speed_min=('wind_speed', 'min'),
        wind_speed_max=('wind_speed', 'max'),
        sky_condition=('sky_condition', 'mean'),
        one_hour_precipitation_avg=('one_hour_precipitation', 'mean'),
        one_hour_precipitation_min=('one_hour_precipitation', 'min'),
        one_hour_precipitation_max=('one_hour_precipitation', 'max'),
        six_hour_precipitation_avg=('six_hour_precipitation', 'mean'),
        six_hour_precipitation_min=('six_hour_precipitation', 'min'),
        six_hour_precipitation_max=('six_hour_precipitation', 'max')
    )
    
    logging.info("Hourly data summarized")

    # Map the floor(value) of sky condition column
    df['sky_condition'] = df['sky_condition'].round().fillna(-1)
    df['sky_condition'] = df['sky_condition'].replace(sky_conditions)

    # Save as TSV file
    df = df.astype(str)
    logging.info("Saving output to clean/%s/%s_0.tsv", year, base_name)
    df.to_csv(f"{AIRFLOW_DIR}/data/clean/{year}/{base_name}.tsv", sep="\t", encoding='utf-8', index=True, single_file = True)