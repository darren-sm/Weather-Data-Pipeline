-- weather TABLE
    --Contains the daily summarization (avg, mean, max) of several weather variables (e.g. temperature)
CREATE TABLE weather(    
    station_id char(12) not null ,
    date date not null,
	air_temperature_avg decimal (15, 5),
	air_temperature_min decimal (15, 5),
	air_temperature_max decimal (15, 5),
	dew_point_avg decimal (15, 5),
	dew_point_min decimal (15, 5),
	dew_point_max decimal (15, 5),
	sea_lvl_pressure_avg decimal (15, 5),
	sea_lvl_pressure_min decimal (15, 5),
	sea_lvl_pressure_max decimal (15, 5),
	wind_direction_avg decimal (15, 5),
	wind_direction_min decimal (15, 5),
	wind_direction_max decimal (15, 5),
	wind_speed_avg decimal (15, 5),
	wind_speed_min decimal (15, 5),
	wind_speed_max decimal (15, 5),
	sky_condition varchar(64),
	one_hour_precipitation_avg decimal (15, 5),
	one_hour_precipitation_min decimal (15, 5),
	one_hour_precipitation_max decimal (15, 5),
	six_hour_precipitation_avg decimal (15, 5),
	six_hour_precipitation_min decimal (15, 5),
	six_hour_precipitation_max decimal (15, 5),
    primary key(station_id, date)
);

-- records_count TABLE
	-- Contains the number of hourly records are there for each day and station before they are summarized
	-- Separate to weather because weather table only takes in days which have 4 or more records
CREATE TABLE records_count(
	station_id char(12) not null,
	date date not null,
	count integer,
	primary key(station_id, date)
)