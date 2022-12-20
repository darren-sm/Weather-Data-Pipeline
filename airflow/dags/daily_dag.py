def extract_task():    
    # Use modules.etl.download_data(list_of_items)
    # with list_of_items as all items not older than 24 hours in the isd-lite folder of the NOAA-ISD bucket
    pass

def transform_task():
    # for year in airflow_dir/data/raw/
    # for file in year folder
    # Use modules.etl.transform(file)
    pass

def upsert_task():
    # for file in airflow_dir/data/year/clean
    # Use modules.etl.upsert(file)
    pass