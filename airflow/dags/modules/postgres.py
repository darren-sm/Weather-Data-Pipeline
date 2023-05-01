import psycopg2
import csv

# Postgresql engine
class PsqlEngine:
    def __init__(self, credentials) -> None:        
        # Create database connection and cursor
        self.__conn = psycopg2.connect(**credentials)
        self.__cursor = self.__conn.cursor()

    def upsert(self, table_name, file):
        with self.__conn:            
            
            if table_name == "weather":
                # Create temp table with the same structure as original `weather` table
                self.__cursor.execute("""
                DROP TABLE IF EXISTS tmp_tbl;
                CREATE TABLE tmp_tbl AS (SELECT * FROM weather WHERE 1 = 2);
                """)

                # Copy the content of file into the tmp_tbl
                self.__cursor.copy_from(file, "tmp_tbl", sep = "\t", null='')

                # Upsert from tmp_tbl into weather table
                self.__cursor.execute(
                    """
                    INSERT INTO weather (station_id, date, n_records, air_temperature_avg, air_temperature_min, air_temperature_max, dew_point_avg, dew_point_min, dew_point_max, sea_lvl_pressure_avg, sea_lvl_pressure_min, sea_lvl_pressure_max, wind_direction_avg, wind_direction_min, wind_direction_max, wind_speed_avg, wind_speed_min, wind_speed_max, sky_condition, one_hour_precipitation_avg, one_hour_precipitation_min, one_hour_precipitation_max, six_hour_precipitation_avg, six_hour_precipitation_min, six_hour_precipitation_max)
                    SELECT * FROM tmp_tbl 
                    ON CONFLICT (station_id, date) DO UPDATE SET
                        n_records = EXCLUDED.n_records,
                        air_temperature_avg = EXCLUDED.air_temperature_avg,
                        air_temperature_min = EXCLUDED.air_temperature_min,
                        air_temperature_max = EXCLUDED.air_temperature_max,
                        dew_point_avg = EXCLUDED.dew_point_avg,
                        dew_point_min = EXCLUDED.dew_point_min,
                        dew_point_max = EXCLUDED.dew_point_max,
                        sea_lvl_pressure_avg = EXCLUDED.sea_lvl_pressure_avg,
                        sea_lvl_pressure_min = EXCLUDED.sea_lvl_pressure_min,
                        sea_lvl_pressure_max = EXCLUDED.sea_lvl_pressure_max,
                        wind_direction_avg = EXCLUDED.wind_direction_avg,
                        wind_direction_min = EXCLUDED.wind_direction_min,
                        wind_direction_max = EXCLUDED.wind_direction_max,
                        wind_speed_avg = EXCLUDED.wind_speed_avg,
                        wind_speed_min = EXCLUDED.wind_speed_min,
                        wind_speed_max = EXCLUDED.wind_speed_max,
                        sky_condition = EXCLUDED.sky_condition,
                        one_hour_precipitation_avg = EXCLUDED.one_hour_precipitation_avg,
                        one_hour_precipitation_min = EXCLUDED.one_hour_precipitation_min,
                        one_hour_precipitation_max = EXCLUDED.one_hour_precipitation_max,
                        six_hour_precipitation_avg = EXCLUDED.six_hour_precipitation_avg,
                        six_hour_precipitation_min = EXCLUDED.six_hour_precipitation_min,
                        six_hour_precipitation_max = EXCLUDED.six_hour_precipitation_max;
                    """
                )
                self.__cursor.execute("DROP TABLE IF EXISTS tmp_tbl;")
            else:
                raise SystemExit(f"ERROR: No `{table_name}` table name found in the database")            

    def __del__(self):
        # Close the connection on object deletion
        self.__conn.close()