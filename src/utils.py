import iso8601
import zipfile
import shutil
import math
import os
import re

"""
All kind of small functions
"""

def distance(point1, point2):
    """Calculate distance in km between two nodes using haversine forumla
    
    :param point1: First node
    :type point1: tuple with 2 numbers

    param point2: Second node
    :type point2: tuple with 2 numbers
    """
    lat1, lon1 = point1[0], point1[1]
    lat2, lon2 = point2[0], point2[1]
    
    # go to radiand
    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    d = math.sin(dlat * 0.5) ** 2 + \
        math.cos(lat1) * math.cos(lat2) * math.sin(dlon * 0.5) ** 2

    return math.asin(math.sqrt(d)) * 12742

def text_color(color):
    """Given a color, estimate if it's better to
    show block or white text on top of it.

    :param color: A string of six hex digits (RRGGBB)
                  representing the background color

    :returns: A string of six hex digits (RRGGBB)
              representing the estimated text color
    """

    r = int(color[0:2], base=16)
    g = int(color[2:4], base=16)
    b = int(color[4:6], base=16)
    yiq = 0.299 * r + 0.587 * g + 0.114 * b

    return "000000" if yiq > 128 else "FFFFFF"

def clear_directory(dir_name):
    """Makes sure that the provided directory
    exists and is empty.
    
    :param dir_name: Directory to clear
    :type dir_name: str or PathLike
    """
    print_log(f"Clearing directory {dir_name}")

    if not os.path.exists(dir_name):
        os.mkdir(dir_name)

    else:
        for entry in os.scandir(dir_name):
            if entry.is_dir():
                shutil.rmtree(entry.path)
            else:
                os.remove(entry.path)

def timetable_item_station(ttable_item):
    """Return the stop_id for the corresponding item of odpt:trainTimetableObject"""
    station = ttable_item.get("odpt:departureStation") or ttable_item.get("odpt:arrivalStation")
    return station.split(":")[1]

def compress(target_file):
    """Compress the contents of gtfs/ directory to target_file.

    :param target_file: Path to the new archive
    :type target_file: str or file-like or path-like
    """

    print_log(f"Compressing to {target_file}")

    with zipfile.ZipFile(target_file, mode="w", compression=zipfile.ZIP_DEFLATED) as arch:
        for file in os.scandir("gtfs"):
            if file.name.endswith(".txt"):
                arch.write(file.path, arcname=file.name)

def carmel_to_title(carmel_string):
    return re.sub(r"(?!^)([A-Z][a-z]+)", r" \1", carmel_string)

def iso_date_to_tstamp(iso_date):
    if iso_date is None:
        return None
    else:
        return round(iso8601.parse_date(iso_date).timestamp())

def iso_date_to_ymdhm(iso_date):
    if iso_date is None:
        return None
    else:
        return iso8601.parse_date(iso_date).strftime("%Y-%m-%d %H:%M")

def print_log(message, severity=0):
    """Prints message with nice colors
    
    :param int severity:
        0 - Info (replaced by next log message)
        1 - Warning (usually data issue)
    """

    if severity == 0:
        formatted_message = message
        end_value = "\n"

    elif severity == 1:
        formatted_message = "\033[31m" "Warning! " "\033[33m" + message + "\033[0m"
        end_value = "\n\n"

    else:
        raise ValueError("print_log only supports severity==0 or ==1")

    print("\033[1A\033[K" + formatted_message, end=end_value)
