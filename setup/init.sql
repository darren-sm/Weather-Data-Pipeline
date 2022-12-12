-- Initialize the application database
CREATE DATABASE noaaisd;

-- create the user for the application database
CREATE USER appuser with encrypted password 'secretpassword';
GRANT all privileges ON DATABASE noaaisd to appuser;

-- READ the csv file downloaded by station-info.py