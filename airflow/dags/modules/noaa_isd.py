"""
Collection of methods for listing and downloading the objects in NOAA ISD bucket
https://noaa-isd-pds.s3.amazonaws.com/index.html
"""

import os
import logging
from datetime import datetime, timedelta
import concurrent.futures
from modules.decorator import logger

# Modules to interact with S3 bucket
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.handlers import disable_signing

# Airflow directory
airflow_dir = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

# BOTO3 Resources
S3 = boto3.resource("s3", config = Config(max_pool_connections=50,))
S3.meta.client.meta.events.register('choose-signer.s3.*', disable_signing)
PUBLIC_BUCKET = S3.Bucket("noaa-isd-pds")

def list_object_keys(prefix, bucket = PUBLIC_BUCKET):
    """
    Generate the key of objects inside a given folder in S3 bucket.
    
    Parameter:
    ----------
    prefix: string
        "Path" inside the S3 bucket containing the objects

    Yields:
    ----------
    S3.Object
        Objects inside the folder

    Example:
    ----------
    >>> # List out all the objects for year 2022 (full path is "isd-lite/data/2022")
    >>> list_object_keys("isd-lite/data/2022")
    """
    try:
        logging.info("Generating the list of all the objects in %s folder", prefix)
        for index, obj in enumerate(bucket.objects.filter(Prefix=prefix)):
            yield obj
        if 'index' in locals():
            logging.info("Found %s objects in %s folder", index + 1, prefix)
        else:
            logging.info("No object found in %s folder", prefix)
    except ClientError as ce:
        logging.error("ERROR: %s", ce)


def get_daily_list(folder_objects):
    """
    Filter the list of object keys from S3 bucket to download for daily data. If the object's last modified date is more than 24 hours ago, skip it. Else, include it in download list.

    Parameter:
    ----------
    folder_objects: generator
        Generator for object keys (string) inside a folder in S3 bucket

    Yields:
    ----------
    S3.Object
        Objects modified within the last 24 hours
    """
    modified_objects = 0
    for index, obj in enumerate(folder_objects):        
        # Check if the object's last modified date is more than 24 hours ago
        yesterday = datetime.now() - timedelta(hours=24)
        if yesterday < obj.last_modified.replace(tzinfo=None):            
            modified_objects += 1
            yield obj
    if 'index' in locals():
        logging.info("Found %s / %s objects modified within the last 24 hours since today %s", modified_objects, index + 1, datetime.now().strftime("%Y-%m-%d %H:%M"))
    else: 
        logging.info("List of objects empty. No keys for daily data filtered.")


def _download_file(object_key, bucket = PUBLIC_BUCKET):  
    """
    Download an object in a bucket (defaults to noaa-isd-pds) using boto3 resource.

    Parameters:
    ----------
    object_key: string
        Name of the object in bucket to download
    
    Optional Parameters
    ----------
    directory: string 
        Path in local machine where the file to be downloaded is saved
        Defaults to None
    local_name: string
        Base name of the downloaded file in local machine
        Defaults to None
    bucket: S3.Bucket
        Bucket containing the file to be downloaded
        Defaults to PUBLIC_BUCKET (noaa-isd-pds S3 bucket)

    Returns:
    ----------
    local_name: string
        Name of the file downloaded

    Example:
    ----------
    >>> download_file("isd-lite/data/2022/010010-99999-2022.gz")    
    >>> # file saved in /opt/airflow/data/raw/010010-99999-2022.gz
    """ 
    
    # Set directory and local name of file to be downloaded    
    year, local_name = object_key.split("/")[2:]        
    directory = f"{airflow_dir}/data/raw/{year}"
    filename = f"{directory}/{local_name}"

    # Download s3 object 
    try:
        bucket.download_file(object_key, filename)
    except ClientError as ce:
        logging.error("ERROR: %s", ce)

    return filename

@logger
def download_multiple(list_of_files):
    """
    Download multiple S3 bucket objects at the same time using multi-threading of _download_file function

    Parameter:
    ----------
    list_of_files: generator / list
        Contains the list of object keys to be downloaded
    
    Returns:
    ----------
    None

    Example:
    ----------
    >>> # Download all the objects for 2022 data (stored in "isd-lite/data/2022/" path)
    >>> objects = list_object_keys("isd-lite/data/2022/")
    >>> object_keys = (obj.key for obj in object_keys)
    >>> download_multiple(object_keys)
    """
    logging.info("Generator for list of objects to download now set. Starting download with multi-threading.")
    with concurrent.futures.ThreadPoolExecutor() as execturor:
        execturor.map(_download_file, list_of_files)        