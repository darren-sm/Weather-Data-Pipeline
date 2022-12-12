# Weather Data Pipeline

This project is a data pipeline for [NOAA Integrated Surface Database (ISD)](https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database) which is a part of [NOAA Big Data Program](https://registry.opendata.aws/collab/noaa/). The Integrated Surface Database (ISD) consists of global hourly and synoptic observations compiled from numerous sources into a gzipped fixed width format. It is hosted in multiple Cloud Service Providers, but our target for this project is hosted in AWS S3 that can be found in [this link](https://registry.opendata.aws/noaa-isd/).

## Dashboard

> No dashboard for now. Will include a live link here or screenshot/recording 

## ETL

This is an ***Extract-Transform-Load*** project. 

The raw flat files are in `.nc` format which are used to save climate data in multi-dimensions layer including [NetCDF](http://www.agrimetsoft.com/help-netcdf) (Network Common Data Form). A [NetCDF](http://www.agrimetsoft.com/help-netcdf) dataset contains dimensions, variables, and attributes, which all have both a  name and an ID number by which they are identified. 

[netCDF4](https://unidata.github.io/netcdf4-python/) Python library to access the content of the `.nc` files which contains hourly weather data. Transformation processes include data quality checking and aggregating the datapoints (e.g. average temperature of the day instead of multiple hourly records). Lastly, the clean data is ingested in a PostgreSQL database.

All of this is orchestrated through [Apache Airflow](https://airflow.apache.org/)