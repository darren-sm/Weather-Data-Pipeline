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

    Returns:
    ----------
    local_name: string
        Name of the file downloaded

    Example:
    ----------
    >>> download_file("readme.txt", "readme.txt")
    "readme.txt"
    >>> # file saved in /opt/airflow/data/raw/readme.txt
    """ 
    if local_name is None:
        year, local_name = object_key.split("/")[2:]

        if directory is None:                        
            directory = f"{airflow_dir}/data/raw/{year}"

    if not os.path.exists(directory):
        logging.debug("Folder for year %s raw data not found. Creating %s now", year, directory)
        os.makedirs(directory)

    filename = f"{directory}/{local_name}"

    try:
        bucket.download_file(object_key, filename)
    except ClientError as ce:
        logging.error("ERROR: %s", ce)

    return 

def download_multiple(list_of_files):
    """
    Download multiple S3 bucket objects at the same time using multi-threading 
    """
    logging.info("Generator for list of objects to download now set. Starting download with multi-threading.")
    with concurrent.futures.ThreadPoolExecutor() as execturor:
        for result in execturor.map(_download_file, list_of_files):
            logging.debug("%s object downloaded.", result)