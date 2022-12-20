# Weather Data Pipeline

This project is a data pipeline for [NOAA Integrated Surface Database (ISD)](https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database) which is a part of [NOAA Big Data Program](https://registry.opendata.aws/collab/noaa/). The Integrated Surface Database (ISD) consists of global hourly and synoptic observations compiled from numerous sources into a gzipped fixed width format. It is hosted in multiple Cloud Service Providers, but our target for this project is hosted in AWS S3 that can be found in [this link](https://registry.opendata.aws/noaa-isd/).

## Dashboard

> No dashboard for now. Will include a live link here or screenshot/recording 

## ETL

This is an ***Extract-Transform-Load*** project. 

The raw flat files are in `.nc` format which are used to save climate data in multi-dimensions layer including [NetCDF](http://www.agrimetsoft.com/help-netcdf) (Network Common Data Form). A [NetCDF](http://www.agrimetsoft.com/help-netcdf) dataset contains dimensions, variables, and attributes, which all have both a  name and an ID number by which they are identified. 

[netCDF4](https://unidata.github.io/netcdf4-python/) Python library to access the content of the `.nc` files which contains hourly weather data. Transformation processes include data quality checking and aggregating the datapoints (e.g. average temperature of the day instead of multiple hourly records). Lastly, the clean data is ingested in a PostgreSQL database.

All of this is orchestrated through [Apache Airflow](https://airflow.apache.org/)

## Setup

**Pre-requisites**: Docker

- Clone the project and `cd Weather-Data-Pipeline/`
- Create a `.env` file and write variables based on the values of `sample.env`
  - Feeling lazy? Simply rename `sample.env` to `.env`

- Build and run. 
  - `docker compose build`
    - **Note:** This is only needed at the first run
  - `docker compose up -d`
    - **Note:** First run will take a few minutes due to downloading the services' image
- Airflow web server: http://localhost:8080
- Shut down
  - `docker compose down`

**Further development**:

- If apache airflow is not installed in your machine, do the following for code linting:
  - Create a virtual environment and activate it
    - `python -m venv venv`
    - `source venv/bin/activate`
  - Install apache airflow
    - `pip install "apache-airflow=={AIRFLOW-VERSION}" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-{AIRFLOW-VERSION}/constraints-{PYTHON-VERSION}.txt"`
      - Airflow version in Dockerfile
      - Python version: `python --version`. Get only the `x.y` in `x.y.z`
    - Example: `pip install "apache-airflow==2.3.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.3.0/constraints-3.8.txt"`

​	