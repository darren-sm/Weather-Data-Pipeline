-- weather TABLE
    --Contains the daily summarization (avg, mean, max) of several weather variables (e.g. temperature)
CREATE TABLE weather(    
    station_id VARCHAR(13) CONSTRAINT check_station_id_length CHECK (LENGTH(station_id) = 12 OR LENGTH(station_id) = 13),
    date date not null,
	n_records integer,
	air_temperature_avg decimal (15, 5),
	air_temperature_min decimal (15, 5),
	air_temperature_max decimal (15, 5),
	dew_point_avg decimal (15, 5),
	dew_point_min decimal (15, 5),
	dew_point_max decimal (15, 5),
	sea_lvl_pressure_avg decimal (15, 5),
	sea_lvl_pressure_min decimal (15, 5),
	sea_lvl_pressure_max decimal (15, 5),
	wind_direction decimal (15, 5),
	wind_speed_avg decimal (15, 5),
	wind_speed_min decimal (15, 5),
	wind_speed_max decimal (15, 5),
	sky_condition integer,
	one_hour_precipitation_avg decimal (15, 5),
	one_hour_precipitation_min decimal (15, 5),
	one_hour_precipitation_max decimal (15, 5),
	six_hour_precipitation_avg decimal (15, 5),
	six_hour_precipitation_min decimal (15, 5),
	six_hour_precipitation_max decimal (15, 5),
    primary key(station_id, date)
);
