from datetime import datetime, date, timedelta
from collections import OrderedDict
from bs4 import BeautifulSoup
from warnings import warn
from copy import copy
import argparse
import requests
import zipfile
import ijson
import json
import math
import time
import csv
import re
import io
import os

__author__ = "Mikołaj Kuranowski"
__email__ = "mikolaj@mkuran.pl"
__license__ = "CC BY 4.0"

GTFS_HEADERS = {
    "agency.txt": ["agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang"],
    "stops.txt": ["stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon", "zone_id", "location_type", "parent_station"],
    "routes.txt": ["agency_id", "route_id", "route_short_name", "route_long_name", "route_type", "route_color", "route_text_color"],
    "trips.txt": ["route_id", "trip_id", "service_id", "trip_short_name", "trip_headsign", "direction_id", "direction_name", "block_id", "train_realtime_id"],
    "stop_times.txt": ["trip_id", "stop_sequence", "stop_id", "platform", "arrival_time", "departure_time"],
    "calendar_dates.txt": ["service_id", "date", "exception_type"],
    #"fare_attributes.txt": ["agency_id", "fare_id", "price", "currency_type", "payment_method", "transfers"],
    #"fare_rules.txt": ["fare_id", "contains_id"],
    "translations.txt": ["trans_id", "lang", "translation"]
}

BUILT_IN_CALENDARS = {"Weekday", "SaturdayHoliday", "Holiday", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"}

def _text_color(route_color: str):
    """Calculate if route_text_color should be white or black"""
    # This isn't perfect, but works for what we're doing
    red, green, blue = int(route_color[:2], base=16), int(route_color[2:4], base=16), int(route_color[4:6], base=16)
    yiq = 0.299 * red + 0.587 * green + 0.114 * blue

    if yiq > 128: return "000000"
    else: return "FFFFFF"

def _holidays(year):
    request = requests.get("https://www.officeholidays.com/countries/japan/{}.php".format(year), timeout=30)
    soup = BeautifulSoup(request.text, "html.parser")
    holidays = {datetime.strptime(h.find("time").string, "%Y-%m-%d").date() for h in soup.find_all("tr", class_="holiday")}
    return holidays

def _distance(point1, point2):
    """Calculate distance in km between two nodes using haversine forumla"""
    lat1, lon1 = point1[0], point1[1]
    lat2, lon2 = point2[0], point2[1]
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    d = math.sin(math.radians(dlat) * 0.5) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(math.radians(dlon) * 0.5) ** 2
    return math.asin(math.sqrt(d)) * 12742

class _Time(object):
    "Represent a time value"
    def __init__(self, seconds):
        self.m, self.s = divmod(int(seconds), 60)
        self.h, self.m = divmod(self.m, 60)

    def __str__(self):
        "Return GTFS-compliant string representation of time"
        return ":".join(["0" + i if len(i) == 1 else i for i in map(str, [self.h, self.m, self.s])])

    def __int__(self): return self.h * 3600 + self.m * 60 + self.s
    def __add__(self, other): return _Time(self.__int__() + int(other))
    def __lt__(self, other): return self.__int__() < int(other)
    def __le__(self, other): return self.__int__() <= int(other)
    def __gt__(self, other): return self.__int__() > int(other)
    def __ge__(self, other): return self.__int__() >= int(other)
    def __eq__(self, other): return self.__int__() == int(other)
    def __ne__(self, other): return self.__int__() != int(other)

    @classmethod
    def from_str(cls, string):
        str_split = list(map(int, string.split(":")))
        if len(str_split) == 2:
            return cls(str_split[0]*3600 + str_split[1]*60)
        elif len(str_split) == 3:
            return cls(str_split[0]*3600 + str_split[1]*60 + str_split[2])
        else:
            raise ValueError("invalid string for _Time.from_str(), {} (should be HH:MM or HH:MM:SS)".format(string))

class TrainParser:
    def __init__(self, apikey, use_osm=False, verbose=True):
        self.apikey = apikey
        self.use_osm = use_osm
        self.verbose = verbose

        self.valid_stops = set()
        self.stop_names = {}
        self.blocks = {}
        self.block_enum = 0

        self.english_strings = {}

        # Clean gtfs/ directory
        if not os.path.exists("gtfs"): os.mkdir("gtfs")
        for file in os.listdir("gtfs"): os.remove("gtfs/" + file)

        # Get info on which routes to parse
        self.operators = []
        self.route_data = {}
        with open("data/train_routes.csv", mode="r", encoding="utf8", newline="") as buffer:
            reader = csv.DictReader(buffer)
            for row in reader:
                self.route_data[row["route_id"]] = row
                if row["operator"] not in self.operators:
                    self.operators.append(row["operator"])

        # Calendars
        self.startdate = date.today()
        self.enddate = self.startdate + timedelta(days=180)
        self.used_calendars = OrderedDict()

    def _trainTypes(self):
        ttypes_req = requests.get("https://api-tokyochallenge.odpt.org/api/v4/odpt:TrainType.json", params={"acl:consumerKey": self.apikey}, timeout=30, stream=True)
        ttypes_req.raise_for_status()
        ttypes = ijson.items(ttypes_req.raw, "item")
        ttypes_dict = {i["owl:sameAs"]: (i["dc:title"], i.get("odpt:trainTypeTitle", {}).get("en", "")) for i in ttypes}
        ttypes_req.close()
        return ttypes_dict

    def _trainDirections(self):
        tdirs_req = requests.get("https://api-tokyochallenge.odpt.org/api/v4/odpt:RailDirection.json", params={"acl:consumerKey": self.apikey}, timeout=30, stream=True)
        tdirs_req.raise_for_status()
        tdirs = ijson.items(tdirs_req.raw, "item")
        tdirs_dict = OrderedDict()
        for i in tdirs: tdirs_dict[i["owl:sameAs"]] = i["dc:title"]
        tdirs_req.close()
        return tdirs_dict

    def _blockid(self, trips):
        for trip in trips:
            if trip in self.blocks:
                block = self.blocks[trip]
                break
        else:
            self.block_enum += 1
            block = str(self.block_enum)

        for trip in trips:
            if trip not in self.blocks:
                self.blocks[trip] = block

        return block

    def _legal_calendars(self):
        calendars_req = requests.get("https://api-tokyochallenge.odpt.org/api/v4/odpt:Calendar.json", params={"acl:consumerKey": self.apikey}, timeout=30, stream=True)
        calendars_req.raise_for_status()
        calendars = ijson.items(calendars_req.raw, "item")

        valid_calendars = set()
        for calendar in calendars:
            calendar_id = calendar["owl:sameAs"].split(":")[1]

            if calendar_id in BUILT_IN_CALENDARS:
                valid_calendars.add(calendar_id)

            elif "odpt:day" in calendar:
                dates = [datetime.strptime(i, "%Y-%m-%d").date() for i in calendar["odpt:day"]]
                if min(dates) <= self.enddate and max(dates) >= self.startdate:
                    valid_calendars.add(calendar_id)

            else:
                warn("\033[1mno dates defined for calendar {}\033[0m".format(calendar_id))

        calendars_req.close()
        return valid_calendars

    def agencies(self):
        buffer = open("gtfs/agency.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(buffer, GTFS_HEADERS["agency.txt"], extrasaction="ignore")
        writer.writeheader()

        with open("data/operators.csv", mode="r", encoding="utf8", newline="") as add_info_buff:
            additional_info = {i["operator"]: i for i in csv.DictReader(add_info_buff)}

        # Iterate over agencies
        for operator in self.operators:
            # Get data fro moperators.csv
            operator_data = additional_info.get(operator, {})
            if not operator_data: warn("\033[1mno data defined for operator {}\033[0m".format(operator))

            # Translations
            if "name_en" in operator_data:
                self.english_strings[operator_data["name"]] = operator_data["name_en"]

            # Write to agency.txt
            writer.writerow({
                "agency_id": operator,
                "agency_name": operator_data.get("name", operator),
                "agency_url": operator_data.get("website", ""),
                "agency_timezone": "Asia/Tokyo", "agency_lang": "ja"
            })

        buffer.close()

    def feed_info(self):
        with open(os.path.join("gtfs", "feed_info.txt"), mode="w", encoding="utf8", newline="") as file_buff:
            file_wrtr = csv.writer(file_buff)
            file_wrtr.writerow(["feed_publisher_name", "feed_publisher_url", "feed_lang"])
            if self.use_osm:
                file_wrtr.writerow([
                    "Mikołaj Kuranowski (via TokyoGTFS); Data provders: Open Data Challenge for Public Transportation in Tokyo, © OpenStreetMap contributors (under ODbL license)",
                    "https://github.com/MKuranowski/TokyoGTFS",
                    "ja"
                ])
            else:
                file_wrtr.writerow([
                    "Mikołaj Kuranowski (via TokyoGTFS); Data provided by Open Data Challenge for Public Transportation in Tokyo",
                    "https://github.com/MKuranowski/TokyoGTFS",
                    "ja"
                ])

    def stops(self):
        """Parse stops"""
        # Get list of stops
        stops_req = requests.get("https://api-tokyochallenge.odpt.org/api/v4/odpt:Station.json", params={"acl:consumerKey": self.apikey}, timeout=90, stream=True)
        stops_req.raise_for_status()
        stops = ijson.items(stops_req.raw, "item")

        # Load OSM data
        if self.use_osm:
            with open("data/train_osm_stops.csv", mode="r", encoding="utf8", newline="") as f:
                osm_data = [i for i in csv.DictReader(f)]

        else:
            osm_data = []

        # Open files
        buffer = open("gtfs/stops.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(buffer, GTFS_HEADERS["stops.txt"], extrasaction="ignore")
        writer.writeheader()

        broken_stops_buff = open("broken_stops.csv", mode="w", encoding="utf8", newline="")
        broken_stops_wrtr = csv.writer(broken_stops_buff)
        broken_stops_wrtr.writerow(["stop_id", "stop_name", "stop_name_en", "stop_code", "is_in_osm"])

        # Iterate over stops
        for stop in stops:
            stop_id = stop["owl:sameAs"].split(":")[1]
            stop_code = stop.get("odpt:stationCode", "").replace("-", "")
            stop_name, stop_name_en = stop["dc:title"], stop.get("odpt:stationTitle", {}).get("en", "")
            stop_lat, stop_lon = None, None

            if self.verbose: print("\033[1A\033[KParsing stops:", stop_id)

            self.stop_names[stop_id] = stop_name

            # Stop name translation
            if stop_name_en: self.english_strings[stop_name] = stop_name_en

            # Ignore stops that belong to ignored routes
            if stop["odpt:railway"].split(":")[1] not in self.route_data:
                continue

            # Correct stop position
            if "geo:lat" in stop and "geo:long" in stop:
                stop_lat = stop["geo:lat"]
                stop_lon = stop["geo:long"]

            # No position given, try to get data from OSM
            elif self.use_osm:
                # Try to match by ref
                found = list(filter(lambda i: stop_code in i.get("ref", ""), osm_data)) if stop_code else []

                # If not station was found, try to find by name
                if not found:
                    found = list(filter(lambda i: stop_name==i["name"], osm_data))

                # If a matching station was found, get its lat and lon
                if found:
                    stop_lat, stop_lon = found[0]["@lat"], found[0]["@lon"]
                    broken_stops_wrtr.writerow([stop_id, stop_name, stop_name_en, stop_code, 1])


            # Output to GTFS or to incorrect stops
            if stop_lat and stop_lon:
                self.valid_stops.add(stop_id)
                writer.writerow({
                    "stop_id": stop_id, "stop_code": stop_code, "zone_id": stop_id,
                    "stop_name": stop_name, "stop_lat": stop_lat, "stop_lon": stop_lon,
                    "location_type": 0
                })

            else:
                broken_stops_wrtr.writerow([stop_id, stop_name, stop_name_en, stop_code, 0])

        stops_req.close()
        buffer.close()

    def routes(self):
        routes_req = requests.get("https://api-tokyochallenge.odpt.org/api/v4/odpt:Railway.json", params={"acl:consumerKey": self.apikey}, timeout=90, stream=True)
        routes_req.raise_for_status()
        routes = ijson.items(routes_req.raw, "item")

        buffer = open("gtfs/routes.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(buffer, GTFS_HEADERS["routes.txt"], extrasaction="ignore")
        writer.writeheader()

        for route in routes:
            route_id = route["owl:sameAs"].split(":")[1]
            if route_id not in self.route_data: continue

            if self.verbose: print("\033[1A\033[KParsing routes:", route_id)

            # Get color from train_routes.csv
            route_info = self.route_data[route_id]
            operator = route_info["operator"]
            route_color = route_info["route_color"].upper()
            route_text = _text_color(route_color)

            # Translation
            self.english_strings[route_info["route_name"]] = route_info["route_en_name"]

            # Output to GTFS
            writer.writerow({
                "agency_id": operator,
                "route_id": route_id,
                "route_short_name": route_info.get("route_code", ""),
                "route_long_name": route_info["route_name"],
                "route_type": route_info.get("route_type", "") or "2",
                "route_color": route_color,
                "route_text_color": route_text
            })

        routes_req.close()
        buffer.close()

    def trips(self):
        """Parse trips & stop_times"""
        # Some variables
        train_types = self._trainTypes()
        train_directions = self._trainDirections()
        available_calendars = self._legal_calendars()
        main_direction = ""

        # Get all trips
        trips_req = requests.get("https://api-tokyochallenge.odpt.org/api/v4/odpt:TrainTimetable.json", params={"acl:consumerKey": self.apikey}, timeout=90, stream=True)
        trips_req.raise_for_status()
        trips = ijson.items(trips_req.raw, "item")

        # Open GTFS trips
        buffer_trips = open("gtfs/trips.txt", mode="w", encoding="utf8", newline="")
        writer_trips = csv.DictWriter(buffer_trips, GTFS_HEADERS["trips.txt"], extrasaction="ignore")
        writer_trips.writeheader()

        buffer_times = open("gtfs/stop_times.txt", mode="w", encoding="utf8", newline="")
        writer_times = csv.DictWriter(buffer_times, GTFS_HEADERS["stop_times.txt"], extrasaction="ignore")
        writer_times.writeheader()

        # Iteratr over trips
        for trip in trips:
            route = trip["odpt:railway"].split(":")[1]
            trip_id = trip["owl:sameAs"].split(":")[1]
            calendar = trip["odpt:calendar"].split(":")[1]
            service_id = route + "/" + calendar
            train_rt_id = trip["odpt:train"].split(":")[1] if "odpt:train" in trip else ""

            if self.verbose: print("\033[1A\033[KParsing times:", trip_id)

            # Ignore ignored routes and non_active calendars
            if route not in self.route_data or calendar not in available_calendars:
                continue

            # Add calendar
            if route not in self.used_calendars: self.used_calendars[route] = set()
            self.used_calendars[route].add(calendar)

            # Ignore one-stop trips without any previous/next timetables
            if len(trip["odpt:trainTimetableObject"]) < 2 and \
            not (trip.get("odpt:previousTrainTimetable", []) or trip.get("odpt:nextTrainTimetable", [])):
                continue

            # Train name
            trip_short_name = trip["odpt:trainNumber"]
            train_name = trip.get("odpt:trainName", {}).get("ja", "")
            if train_name:
                trip_short_name = "{}「{}」".format(trip_short_name, train_name)
                if trip.get("odpt:trainName", {}).get("en", ""):
                    self.english_strings[trip_short_name] = '{} "{}"'.format(trip["odpt:trainNumber"], trip["odpt:trainName"]["en"])

            # Train Direction
            if "odpt:railDirection" in trip:
                if not main_direction: main_direction = trip["odpt:railDirection"]
                direction_name = train_directions.get(trip["odpt:railDirection"], "")
                direction_id = 0 if trip["odpt:railDirection"] == main_direction else 1

            else:
                direction_id, direction_name == "", ""

            # Train headsign
            if "odpt:destinationStation" in trip:
                last_stop_id = trip["odpt:destinationStation"][-1].split(":")[1]

            else:
                last_stop_id = (trip["odpt:trainTimetableObject"][-1].get("odpt:arrivalStation", "") or \
                                trip["odpt:trainTimetableObject"][-1].get("odpt:departureStation", "")).split(":")[1]

            if last_stop_id in self.stop_names:
                trip_headsign = self.stop_names[last_stop_id]
            else:
                trip_headsign = re.sub(r"(?!^)([A-Z][a-z]+)", r" \1", last_stop_id.split(".")[-1])
                warn("\033[1mno name for stop {}\033[0m".format(last_stop_id))
                self.stop_names[last_stop_id] = trip_headsign

            trip_headsign_en = self.english_strings.get(trip_headsign, "")

            trip_type, trip_type_en = train_types.get(trip.get("odpt:trainType", ""), ("", ""))
            if trip_type:
                trip_headsign = "（{}）{}".format(trip_type, trip_headsign)
                if trip_headsign_en and trip_type_en:
                    self.english_strings[trip_headsign] = "({}) {}".format(trip_type_en, trip_headsign_en)

            # Block ID
            all_trips = trip.get("odpt:previousTrainTimetable", []) + [trip["owl:sameAs"]] + trip.get("odpt:nextTrainTimetable", [])
            if len(all_trips) > 1:
                block_id = self._blockid(all_trips)

            else:
                block_id = ""

            # Write to trips.txt
            writer_trips.writerow({
                "route_id": route, "trip_id": trip_id, "service_id": service_id,
                "trip_short_name": trip_short_name, "trip_headsign": trip_headsign,
                "direction_id": direction_id, "direction_name": direction_name,
                "block_id": block_id, "train_realtime_id": train_rt_id
            })


            # Times
            prev_departure = _Time(0)
            for idx, stop_time in enumerate(trip["odpt:trainTimetableObject"]):
                stop_id = stop_time.get("odpt:departureStation") or stop_time.get("odpt:arrivalStation")
                platform = stop_time.get("odpt:platformNumber", "")

                # Be sure stop_id exist
                if stop_id: stop_id = stop_id.split(":")[1]
                else: continue

                if stop_id not in self.valid_stops:
                    warn("\033[1mreference to a non-existing stop, {}\033[0m".format(stop_id))
                    continue

                # Get time
                arrival = stop_time.get("odpt:arrivalTime") or stop_time.get("odpt:departureTime")
                departure = stop_time.get("odpt:departureTime") or stop_time.get("odpt:arrivalTime")

                if arrival: arrival = _Time.from_str(arrival)
                if departure: departure = _Time.from_str(departure)

                # Be sure arrival and departure exist
                if not (arrival and departure): continue

                # Fix for after-midnight trips. GTFS requires "24:23", while JSON data contains "00:23"
                if arrival < prev_departure: arrival += 86400
                if departure < arrival: departure += 86400
                prev_departure = copy(departure)

                writer_times.writerow({
                    "trip_id": trip_id, "stop_sequence": idx, "stop_id": stop_id, "platform": platform,
                    "arrival_time": str(arrival), "departure_time": str(departure)
                })

        trips_req.close()
        buffer_trips.close()
        buffer_times.close()

    def translations(self):
        buffer = open("gtfs/translations.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(buffer, GTFS_HEADERS["translations.txt"], extrasaction="ignore")
        writer.writeheader()

        for ja_string, en_string in self.english_strings.items():
            writer.writerow({"trans_id": ja_string, "lang": "ja", "translation": ja_string})
            writer.writerow({"trans_id": ja_string, "lang": "en", "translation": en_string})

        buffer.close()

    def calendars(self):
        calendars_req = requests.get("https://api-tokyochallenge.odpt.org/api/v4/odpt:Calendar.json", params={"acl:consumerKey": self.apikey}, timeout=30, stream=True)
        calendars_req.raise_for_status()
        calendars = ijson.items(calendars_req.raw, "item")

        # Get info on specific calendars
        calendar_dates = {}
        for calendar in calendars:
            calendar_id = calendar["owl:sameAs"].split(":")[1]
            if "odpt:day" in calendar:
                dates = [datetime.strptime(i, "%Y-%m-%d").date() for i in calendar["odpt:day"]]
                dates = [i for i in dates if self.startdate <= i <= self.enddate]
                for date in dates:
                    if date not in calendar_dates: calendar_dates[date] = set()
                    calendar_dates[date].add(calendar_id)

        # Get info about holidays
        if self.startdate.year == self.enddate.year: holidays = _holidays(self.startdate.year)
        else: holidays = _holidays(self.startdate.year) | _holidays(self.enddate.year)

        # Open file
        buffer = open("gtfs/calendar_dates.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(buffer, GTFS_HEADERS["calendar_dates.txt"], extrasaction="ignore")
        writer.writeheader()

        # Dump data
        for route, services in self.used_calendars.items():
            if self.verbose: print("\033[1A\033[KParsing calendars:", route)
            working_date = copy(self.startdate)
            while working_date <= self.enddate:
                active_services = []
                if calendar_dates.get(working_date, set()).intersection(services):
                    active_services = [i for i in calendar_dates[working_date].intersection(services)]

                elif working_date in holidays and "Holiday" in services:
                    active_services = ["Holiday"]

                elif working_date.isoweekday() == 7 and working_date not in holidays:
                    if "Sunday" in services: active_services = ["Sunday"]
                    elif "Holiday" in services: active_services = ["Sunday"]

                elif working_date.isoweekday() == 6 and working_date not in holidays and "Saturday" in services:
                    active_services = ["Saturday"]

                elif working_date.isoweekday() == 5 and working_date not in holidays and "Friday" in services:
                    active_services = ["Friday"]

                elif working_date.isoweekday() == 4 and working_date not in holidays and "Thursday" in services:
                    active_services = ["Thursday"]

                elif working_date.isoweekday() == 3 and working_date not in holidays and "Wednesday" in services:
                    active_services = ["Wednesday"]

                elif working_date.isoweekday() == 2 and working_date not in holidays and "Tuesday" in services:
                    active_services = ["Tuesday"]

                elif working_date.isoweekday() == 1 and working_date not in holidays and "Monday" in services:
                    active_services = ["Monday"]

                elif (working_date.isoweekday() >= 6 or working_date in holidays) and "SaturdayHoliday" in services:
                    active_services = ["SaturdayHoliday"]

                elif working_date.isoweekday() <= 5 and working_date not in holidays and "Weekday" in services:
                    active_services = ["Weekday"]

                if active_services:
                    for service in active_services:
                        writer.writerow({"service_id": route+"/"+service, "date": working_date.strftime("%Y%m%d"), "exception_type": 1})
                working_date += timedelta(days=1)

        calendars_req.close()
        buffer.close()

    def stops_postprocess(self):
        stops = OrderedDict()
        names = {}
        avg = lambda i: round(sum(i)/len(i), 10)

        # Read file
        buffer = open("gtfs/stops.txt", mode="r", encoding="utf8", newline="")
        reader = csv.DictReader(buffer)

        for row in reader:
            stop_name_id = row["stop_id"].split(".")[-1]
            names[stop_name_id] = row["stop_name"]
            stop_id_suffix = -1

            close_enough = False
            while not close_enough:
                stop_id_suffix += 1
                stop_id_wsuffix = stop_name_id + "." + str(stop_id_suffix) if stop_id_suffix else stop_name_id

                # If there's no stop with such ID, start a new merge group
                if stop_id_wsuffix not in stops:
                    stops[stop_id_wsuffix] = []
                    close_enough = True

                # If there is; check distance between current stop and other stop in such merge group
                else:
                    saved_location = stops[stop_id_wsuffix][0]["lat"], stops[stop_id_wsuffix][0]["lon"]
                    row_location = float(row["stop_lat"]), float(row["stop_lon"])

                    # Append current stop to merge group only if it's up to 500m close.
                    # If current stop is further, try next merge group
                    if _distance(saved_location, row_location) <= 1:
                        close_enough = True

            stops[stop_id_wsuffix].append({
                "id": row["stop_id"], "code": row["stop_code"],
                "lat": float(row["stop_lat"]), "lon": float(row["stop_lon"])
            })

        buffer.close()

        # Write new stops.txt
        buffer = open("gtfs/stops.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(buffer, GTFS_HEADERS["stops.txt"], extrasaction="ignore")
        writer.writeheader()

        for merge_group_id, merge_group_stops in stops.items():
            # If there's only 1 entry for a station in API: just write it to stops.txt
            if len(merge_group_stops) == 1:
                writer.writerow({
                    "stop_id": merge_group_stops[0]["id"], "zone_id": merge_group_stops[0]["id"],
                    "stop_code": merge_group_stops[0]["code"], "stop_name": names[merge_group_id.split(".")[0]],
                    "stop_lat": merge_group_stops[0]["lat"], "stop_lon": merge_group_stops[0]["lon"]
                })

            # If there are more then 2 entries, create a station (location_type=1) to merge all stops
            else:
                # Calculate some info about the station
                station_id = "Merged." + merge_group_id
                station_name = names[merge_group_id.split(".")[0]]
                station_lat = avg([i["lat"] for i in merge_group_stops])
                station_lon = avg([i["lon"] for i in merge_group_stops])

                codes = "/".join([i["code"] for i in merge_group_stops if i["code"]])

                writer.writerow({
                    "stop_id": station_id, "zone_id": "",
                    "stop_code": codes, "stop_name": station_name,
                    "stop_lat": station_lat, "stop_lon": station_lon,
                    "location_type": 1, "parent_station": ""
                })

                # Dump info about each stop
                for stop in merge_group_stops:
                    writer.writerow({
                        "stop_id": stop["id"], "zone_id": stop["id"],
                        "stop_code": stop["code"], "stop_name": station_name,
                        "stop_lat": stop["lat"], "stop_lon": stop["lon"],
                        "location_type": 0, "parent_station": station_id
                    })

        buffer.close()

    def parse(self):
        if self.verbose: print("Parsing agencies")
        self.agencies()
        self.feed_info()

        if self.verbose: print("\033[1A\033[KParsing stops")
        self.stops()
        if self.verbose: print("\033[1A\033[KParsing stops: finished")

        if self.verbose: print("\033[1A\033[KParsing routes")
        self.routes()
        if self.verbose: print("\033[1A\033[KParsing routes: finished")

        if self.verbose: print("\033[1A\033[KParsing times")
        self.trips()
        if self.verbose: print("\033[1A\033[KParsing times: finished")

        if self.verbose: print("\033[1A\033[KParsing translations")
        self.translations()

        if self.verbose: print("\033[1A\033[KParsing calendars")
        self.calendars()
        if self.verbose: print("\033[1A\033[KParsing calendars: finished")

        if self.verbose: print("\033[1A\033[KPost-processing stops")
        self.stops_postprocess()

        if self.verbose: print("\033[1A\033[KParsing finished!")

    def compress(self):
        "Compress all created files to tokyo_trains.zip"
        archive = zipfile.ZipFile("tokyo_trains.zip", mode="w", compression=zipfile.ZIP_DEFLATED)
        for file in os.listdir("gtfs"):
            if file.endswith(".txt"):
                archive.write(os.path.join("gtfs", file), arcname=file)
        archive.close()

if __name__ == "__main__":
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("-a", "--apikey", metavar="YOUR_APIKEY", help="apikey from developer-tokyochallenge.odpt.org")
    args_parser.add_argument("-v", "--verbose", action="store_true", help="use ANSI escape codes to verbose *a lot*")
    args_parser.add_argument("-o", "-osm", "--use-osm", action="store_true", help="use OSM data to fix stations without lat/lon")
    args = args_parser.parse_args()

    if args.apikey:
        apikey = args.apikey

    elif os.path.exists("apikey.txt"):
        with open("apikey.txt", mode="r", encoding="utf8") as f:
            apikey = f.read().strip()

    else:
        raise RuntimeError("No apikey!\n              Provide it inside command line argument '--apikey',\n              Or put it inside a file named 'apikey.txt'.")

    start_time = time.time()
    print("""
    |  _____     _                 ____ _____ _____ ____   |
    | |_   _|__ | | ___   _  ___  / ___|_   _|  ___/ ___|  |
    |   | |/ _ \| |/ / | | |/ _ \| |  _  | | | |_  \___ \  |
    |   | | (_) |   <| |_| | (_) | |_| | | | |  _|  ___) | |
    |   |_|\___/|_|\_\\\\__, |\___/ \____| |_| |_|   |____/  |
    |                 |___/                                |
    """)
    print("=== Trains GTFS: Starting! ===")


    print("Initializing parser")
    parser = TrainParser(apikey=apikey, use_osm=args.use_osm, verbose=args.verbose)

    print("Starting data parse... This might take some time...")
    parser.parse()

    print("Compressing to tokyo_trains.zip")
    parser.compress()

    total_time = time.time() - start_time
    print("=== TokyoGTFS: Finished in {} s ===".format(round(total_time, 2)))
