# Collection of methods for fetching the object in NOAA ISD bucket
# https://noaa-isd-pds.s3.amazonaws.com/index.html

import os
import logging
import concurrent.futures

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
        logging.info("Found %s objects in %s folder", index + 1, prefix)
    except ClientError as ce:
        logging.error("ERROR: %s", ce)

def _download_file(object_key, directory = None, local_name = None, bucket = PUBLIC_BUCKET):  
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
    if local_name is None:
        year, local_name = object_key.split("/")[2:]
        if directory is None:
            directory = f"{airflow_dir}/data/raw/{year}"
    filename = f"{directory}/{local_name}"

    # Create raw data directory if it does not exist
    if not os.path.exists(directory):
        logging.debug("Folder for year %s raw data not found. Creating %s now", year, directory)
        os.makedirs(directory)

    # Download s3 object 
    try:
        bucket.download_file(object_key, filename)
    except ClientError as ce:
        logging.error("ERROR: %s", ce)

    return filename

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