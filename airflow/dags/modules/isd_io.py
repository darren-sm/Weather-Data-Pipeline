import datetime 
from collections import defaultdict
import logging 


def _parse_value(value, category):
    # Parsing the value for numerical records including Air Temperature, Dew Point Temperature, Sea Level Pressure, Wind Speed, One hour precipitation, and Six hour precipitation
    reasonable_ranges = {
        # I am only making up these numbers based on the set bounds or current world records + 40% allowance for future record breakers
        "air_temperature": (-125.16, 79.38),
        "dew_point": (-46.48, 49),
        "sea_lvl_pressure": (522, 1517.32),
        "wind_direction": (0,360),
        "wind_speed": (0, 158.2),        
        "sky_condition": (0, 19),
        "one_hour_precipitation": (0, 561.4),
        "six_hour_precipitation": (0, 2000)
    }

    min_val, max_val = reasonable_ranges[category]
    # Missing value = -9999
    if value == -9999 or min_val > value or max_val < value:
        return None
    return value
        
def read_isd(filename):
    """
    Yields
    ---------
    dictionary
    """
    
    station_id, wban, _ = filename.split("-")

    with open(filename, "r", encoding="UTF-8") as f:
        for index, line in enumerate(f, start = 1):
            # Check if each line follows the set Data Format (each line must occupy 61 positions)
            if len(line.strip()) != 61:
                logging.warning("Line %s has invalid length of %s instead of 61. Skipping this line.", index, len(line.strip()))
                continue
            
            # split the line by whitespace
            line_content = [float(i) for i in line.split()]

            # Date = first three elements of line_content in order: year, month, day
            current_date = datetime.date(*(int(i) for i in line_content[:3]))

            # Parse and yield line data
            yield {
                "station_id": f"{station_id}-{wban}",
                "date": current_date,
                "air_temperature": _parse_value(line_content[4], "air_temperature"),
                "dew_point": _parse_value(line_content[5], "dew_point"),
                "sea_lvl_pressure": _parse_value(line_content[6], "sea_lvl_pressure"),
                "wind_direction": _parse_value(line_content[7], "wind_direction"),
                "wind_speed": _parse_value(line_content[8], "wind_speed"),
                "sky_condition": _parse_value(line_content[9], "sky_condition"),
                "one_hour_precipitation": _parse_value(line_content[10], "one_hour_precipitation"),
                "six_hour_precipitation": _parse_value(line_content[11], "six_hour_precipitation")
            }

def _get_sky_condition(key):
    mapping = {        
        0: "SKC or CLR",
        1: "One okta",
        2: "Two okta",
        3: "Three okta",
        4: "Four okta",
        5: "Five okta",
        6: "Six okta",
        7: "Seven okta",
        8: "Eight okta",
        9: "Sky obscured, or cloud amount cannot be estimated",
        10: "Partial obscuration",
        11: "Thin scattered",
        12: "Scattered",
        13: "Dark scattered",
        14: "Thin broken",
        15: "Broken",
        16: "Dark broken",
        17: "Thin overcast",
        18: "Overcast",
        19: "Dark overcast"
    }

    return mapping[key]