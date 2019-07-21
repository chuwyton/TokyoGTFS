from datetime import datetime, date, timedelta
from collections import OrderedDict
from bs4 import BeautifulSoup
from pykakasi import kakasi
from warnings import warn
from copy import copy
import argparse
import requests
import zipfile
import shutil
import ijson
import json
import math
import time
import csv
import re
import io
import os

__title__ = "TokyoGTFS: Trains-GTFS"
__author__ = "Mikołaj Kuranowski"
__email__ = "mikolaj@mkuran.pl"
__license__ = "CC BY 4.0"

ADDITIONAL_ENGLISH = {}

GTFS_HEADERS = {
    "agency.txt": ["agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang"],
    "stops.txt": ["stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon", "location_type", "parent_station"],
    "routes.txt": ["agency_id", "route_id", "route_short_name", "route_long_name", "route_type", "route_color", "route_text_color"],
    "trips.txt": ["route_id", "trip_id", "service_id", "trip_short_name", "trip_headsign", "direction_id", "direction_name", "block_id", "train_realtime_id"],
    "stop_times.txt": ["trip_id", "stop_sequence", "stop_id", "platform", "arrival_time", "departure_time"],
    "calendar_dates.txt": ["service_id", "date", "exception_type"],
    "fare_attributes.txt": ["agency_id", "fare_id", "price", "currency_type", "payment_method", "transfers"],
    "fare_rules.txt": ["fare_id", "origin_id", "destination_id", "contains_id"],
    "translations.txt": ["trans_id", "lang", "translation"]
}

BUILT_IN_CALENDARS = {"Weekday", "SaturdayHoliday", "Holiday", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"}

SEPARATE_STOPS = {"Waseda", "Kuramae", "Nakanobu", "Suidobashi", "HongoSanchome", "Ryogoku", "Kumanomae"}


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

def _train_name(names, lang):
    if type(names) is dict: names = [names]

    sep = "・" if lang == "ja" else " / "
    name = sep.join([i[lang] for i in names if i.get(lang)])

    return name

def _clear_dir(dir):
    if os.path.isdir(dir):
        for file in os.listdir(dir):
            filepath = os.path.join(dir, file)
            if os.path.isdir(filepath): _clear_dir(filepath)
            else: os.remove(filepath)
        os.rmdir(dir)
    elif os.path.isfile(dir):
        os.remove(dir)

def trip_generator(apikey):
    # First, the ODPT trips
    trips_req = requests.get("https://api-tokyochallenge.odpt.org/api/v4/odpt:TrainTimetable.json", params={"acl:consumerKey": apikey}, timeout=90, stream=True)
    trips_req.raise_for_status()
    odpt_trips = ijson.items(trips_req.raw, "item")
    parsed_trips = set()

    for trip in odpt_trips:
        prev_trips = trip.get("odpt:previousTrainTimetable", [])
        next_trips = trip.get("odpt:nextTrainTimetable", [])

        # Avoid duplicate trips, it sometimes happens
        if trip["owl:sameAs"] in parsed_trips:
            continue

        parsed_trips.add(trip["owl:sameAs"])

        if len(prev_trips) > 1:
            assert len(next_trips) <= 1, "trip {} has multiple previous and multiple next timetables - that's not supported".format(i["owl:sameAs"])

            for suffix, prev_trip_id in enumerate(prev_trips):
                trip_for_this_train = copy(trip)
                trip_for_this_train["owl:sameAs"] = trip_for_this_train["owl:sameAs"] + "." + str(suffix + 1)
                trip_for_this_train["odpt:previousTrainTimetable"] = [prev_trip_id]

                yield trip_for_this_train

        elif len(next_trips) > 1:
            assert len(prev_trips) <= 1, "trip {} has multiple previous and multiple next timetables - that's not supported".format(i["owl:sameAs"])

            for suffix, next_trip_id in enumerate(next_trips):
                trip_for_this_train = copy(trip)
                trip_for_this_train["owl:sameAs"] = trip_for_this_train["owl:sameAs"] + "." + str(suffix + 1)
                trip_for_this_train["odpt:nextTrainTimetable"] = [next_trip_id]

                yield trip_for_this_train

        else:
            yield trip

class _Time:
    "Represent a time value"
    def __init__(self, seconds):
        self.m, self.s = divmod(int(seconds), 60)
        self.h, self.m = divmod(self.m, 60)

    def __str__(self):
        "Return GTFS-compliant string representation of time"
        return ":".join(["0" + i if len(i) == 1 else i for i in map(str, [self.h, self.m, self.s])])

    def __repr__(self): return "<Time " + self.__str__() + ">"
    def __int__(self): return self.h * 3600 + self.m * 60 + self.s
    def __add__(self, other): return _Time(self.__int__() + int(other))
    def __sub__(self, other): return self.__int__() - int(other)
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
    def __init__(self, apikey, verbose=True):
        self.apikey = apikey
        self.verbose = verbose

        # Stations stuff
        self.valid_stops = set()
        self.station_names = {}
        self.station_positions = {}

        # Blocks stuff
        self.switch_blocks = {}
        self.block_enum = 0
        self.blocks = {}

        # Translations stuff
        kakasi_loader = kakasi()
        kakasi_loader.setMode("H", "a")
        kakasi_loader.setMode("K", "a")
        kakasi_loader.setMode("J", "a")
        kakasi_loader.setMode("r", "Hepburn")
        kakasi_loader.setMode("s", True)
        kakasi_loader.setMode("C", True)
        self.kakasi_conv = kakasi_loader.getConverter()
        self.english_strings = {}

        # Clean gtfs/ directory
        _clear_dir("gtfs")
        os.mkdir("gtfs")

        # Get info on which routes to parse
        self.operators = []
        self.route_data = {}
        with open("data/train_routes.csv", mode="r", encoding="utf8", newline="") as buffer:
            reader = csv.DictReader(buffer)
            for row in reader:

                # Only save info on routes which will have timetables data available:
                # Those in odpt:TrainTimetable or, if enabled, in ekikara
                if not row["train_timetable_available"] == "1":
                    continue

                # TODO: N'EX route creator
                if row["route_code"] == "N'EX":
                    continue

                self.route_data[row["route_id"]] = row
                if row["operator"] not in self.operators:
                    self.operators.append(row["operator"])

        # Calendars
        self.startdate = date.today()
        self.enddate = self.startdate + timedelta(days=180)
        self.used_calendars = OrderedDict()

    def _train_types(self):
        ttypes_req = requests.get("https://api-tokyochallenge.odpt.org/api/v4/odpt:TrainType.json", params={"acl:consumerKey": self.apikey}, timeout=30, stream=True)
        ttypes_req.raise_for_status()
        ttypes = ijson.items(ttypes_req.raw, "item")

        ttypes_dict = {}
        for ttype in ttypes:
            ja_name = ttype["dc:title"]

            if ttype.get("odpt:trainTypeTitle", {}).get("en", ""):
                en_name = ttype.get("odpt:trainTypeTitle", {}).get("en", "")
            else:
                en_name = self._english(ttype["dc:title"])

            ttypes_dict[ttype["owl:sameAs"].split(":")[1]] = (ja_name, en_name)

        ttypes_req.close()
        return ttypes_dict

    def _train_directions(self):
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

            if trip in self.blocks and self.blocks[trip] != block:
                self.switch_blocks[self.blocks[trip]] = block

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

            elif calendar.get("odpt:day", []) != []:
                dates = [datetime.strptime(i, "%Y-%m-%d").date() for i in calendar["odpt:day"]]
                if min(dates) <= self.enddate and max(dates) >= self.startdate:
                    valid_calendars.add(calendar_id)

            else:
                warn("\033[1mno dates defined for calendar {}\033[0m".format(calendar_id))

        calendars_req.close()
        return valid_calendars

    def _stop_name(self, stop_id):
        if stop_id in self.station_names:
            return self.station_names[stop_id]

        else:
            name = re.sub(r"(?!^)([A-Z][a-z]+)", r" \1", stop_id.split(".")[-1])
            self.station_names[stop_id] = name
            warn("\033[1mno name for stop {}\033[0m".format(stop_id))
            return name

    def _english(self, text):
        if text in self.english_strings:
            return self.english_strings[text]

        elif text in ADDITIONAL_ENGLISH:
            self.english_strings[text] = ADDITIONAL_ENGLISH[text]
            return ADDITIONAL_ENGLISH[text]

        else:
            english = self.kakasi_conv.do(text)
            english = english.title()
            # Fix for hepburn macrons (Ooki → Ōki)
            english = english.replace("Uu", "Ū").replace("uu", "ū")
            english = english.replace("Oo", "Ō").replace("oo", "ō")
            english = english.replace("Ou", "Ō").replace("ou", "ō")

            # Fix for katakana chōonpu (ta-minaru → taaminaru)
            english = english.replace("A-", "Aa").replace("a-", "aa")
            english = english.replace("I-", "Ii").replace("i-", "ii")
            english = english.replace("U-", "Ū").replace("u-", "ū")
            english = english.replace("E-", "Ee").replace("e-", "ee")
            english = english.replace("O-", "Ō").replace("o-", "ō")

            english = english.title()

            self.english_strings[text] = english
            warn("\033[1mno english for string {} (generated: {})\033[0m".format(text, english))
            return english

    def agencies(self):
        buffer = open("gtfs/agency.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(buffer, GTFS_HEADERS["agency.txt"], extrasaction="ignore")
        writer.writeheader()

        with open("data/operators.csv", mode="r", encoding="utf8", newline="") as add_info_buff:
            additional_info = {i["operator"]: i for i in csv.DictReader(add_info_buff)}

        # Iterate over agencies
        for operator in self.operators:
            # Get data from operators.csv
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

        # Load fixed positions
        position_fixer = {}

        with open("data/train_stations_fixes.csv", mode="r", encoding="utf8", newline="") as f:
            for row in csv.DictReader(f):
                position_fixer[row["id"]] = (row["lat"], row["lon"])

        # Open files
        buffer = open("gtfs/stops.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(buffer, GTFS_HEADERS["stops.txt"], extrasaction="ignore")
        writer.writeheader()

        broken_stops_buff = open("broken_stops.csv", mode="w", encoding="utf8", newline="")
        broken_stops_wrtr = csv.writer(broken_stops_buff)
        broken_stops_wrtr.writerow(["stop_id", "stop_name", "stop_name_en", "stop_code"])

        # Iterate over stops
        for stop in stops:
            stop_id = stop["owl:sameAs"].split(":")[1]
            stop_code = stop.get("odpt:stationCode", "").replace("-", "")
            stop_name, stop_name_en = stop["dc:title"], stop.get("odpt:stationTitle", {}).get("en", "")
            stop_lat, stop_lon = None, None

            if self.verbose: print("\033[1A\033[KParsing stops:", stop_id)

            self.station_names[stop_id] = stop_name

            # Stop name translation
            if stop_name_en: self.english_strings[stop_name] = stop_name_en

            # Ignore stops that belong to ignored routes
            if stop["odpt:railway"].split(":")[1] not in self.route_data:
                continue

            # Stop Position
            stop_lat, stop_lon = position_fixer.get(stop_id, (stop.get("geo:lat"), stop.get("geo:long")))

            # Output to GTFS or to incorrect stops
            if stop_lat and stop_lon:
                self.valid_stops.add(stop_id)
                self.station_positions[stop_id] = (float(stop_lat), float(stop_lon))
                writer.writerow({
                    "stop_id": stop_id, "stop_code": stop_code, "stop_name": stop_name,
                    "stop_lat": stop_lat, "stop_lon": stop_lon, "location_type": 0
                })

            else:
                broken_stops_wrtr.writerow([stop_id, stop_name, stop_name_en, stop_code])

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

            # Stops
            self.route_data[route_id]["stops"] = \
                [stop["odpt:station"].split(":")[1] for stop in sorted(route["odpt:stationOrder"], key=lambda i: i["odpt:index"])]

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
        timetable_item_station = lambda i: (i.get("odpt:departureStation") or i.get("odpt:arrivalStation")).split(":")[1]

        train_types = self._train_types()
        train_directions = self._train_directions()
        available_calendars = self._legal_calendars()
        main_direction = ""

        # Get all trips
        trips = trip_generator(self.apikey)

        # Open GTFS trips
        buffer_trips = open("gtfs/trips.txt", mode="w", encoding="utf8", newline="")
        writer_trips = csv.DictWriter(buffer_trips, GTFS_HEADERS["trips.txt"], extrasaction="ignore")
        writer_trips.writeheader()

        buffer_times = open("gtfs/stop_times.txt", mode="w", encoding="utf8", newline="")
        writer_times = csv.DictWriter(buffer_times, GTFS_HEADERS["stop_times.txt"], extrasaction="ignore")
        writer_times.writeheader()

        # Iteratr over trips
        for trip in trips:
            route_id = trip["odpt:railway"].split(":")[1]
            trip_id = trip["owl:sameAs"].split(":")[1]
            calendar = trip["odpt:calendar"].split(":")[1]
            service_id = route_id + "/" + calendar
            train_rt_id = trip["odpt:train"].split(":")[1] if "odpt:train" in trip else ""
            block_id = None

            if self.verbose: print("\033[1A\033[KParsing times:", trip_id)

            # Ignore ignored routes and non_active calendars
            if route_id not in self.route_data or calendar not in available_calendars:
                continue

            # Add calendar
            if route_id not in self.used_calendars: self.used_calendars[route_id] = set()
            self.used_calendars[route_id].add(calendar)

            # Destination staion
            if trip.get("odpt:destinationStation") not in ["", None]:
                destination_stations = [self._stop_name(i.split(":")[1]) for i in trip["odpt:destinationStation"]]

            else:
                destination_stations = [self._stop_name(timetable_item_station(trip["odpt:trainTimetableObject"][-1]))]

            ### BLOCK_ID ###
            # ↓ If there's any previousTrainTimetable or nextTrainTimetable
            if trip.get("odpt:previousTrainTimetable", []) not in [[], None] or trip.get("odpt:nextTrainTimetable", []) not in [[], None]:

                all_trips = [trip_id] + [
                    i.split(":")[1] for i in
                    (trip.get("odpt:previousTrainTimetable", []) + trip.get("odpt:nextTrainTimetable", []))
                ]

                block_id = self._blockid(all_trips)

            else:
                block_id = ""

            # Ignore one-stop that are not part of a block
            if len(trip["odpt:trainTimetableObject"]) < 2 and block_id == "":
                continue

            ### TEXT INFO ###
            # Train Direction
            if route_id == "TokyoMetro.Chiyoda":
                # Chiyoda line — special case.
                # This line is really 2 lines: YoyogiUehara↔Ayase and Ayase↔KitaAyase
                # This makes 3 directions in the ODPT data: YoyogiUehara, Ayase and KitaAyase

                stations_of_trip = [timetable_item_station(i) for i in trip["odpt:trainTimetableObject"]]

                if trip["odpt:railDirection"] == "odpt.RailDirection:TokyoMetro.YoyogiUehara":
                    # Ayase → YoyogiUehara
                    direction_id, direction_name = "0", train_directions.get(trip["odpt:railDirection"], "")

                elif trip["odpt:railDirection"] == "odpt.RailDirection:TokyoMetro.KitaAyase":
                    # Ayase → KitaAyase
                    direction_id, direction_name = "1", train_directions.get(trip["odpt:railDirection"], "")

                elif trip["odpt:railDirection"] == "odpt.RailDirection:TokyoMetro.Ayase" and \
                                            "TokyoMetro.Chiyoda.KitaAyase" in stations_of_trip:
                    # KitaAyase → Ayase
                    direction_id, direction_name = "0", train_directions.get(trip["odpt:railDirection"], "")

                elif trip["odpt:railDirection"] == "odpt.RailDirection:TokyoMetro.Ayase":
                    # YoyogiUehara → Ayase
                    direction_id, direction_name = "1", train_directions.get(trip["odpt:railDirection"], "")

                else:
                    raise ValueError("error while resolving directions of TokyoMetro.Chiyoda line train {}. please report this issue on GitHub.".format(trip_id))

            elif "odpt:railDirection" in trip:
                if not main_direction: main_direction = trip["odpt:railDirection"]
                direction_name = train_directions.get(trip["odpt:railDirection"], "")
                direction_id = 0 if trip["odpt:railDirection"] == main_direction else 1

            else:
                direction_id, direction_name == "", ""

            # Train name
            trip_short_name = trip["odpt:trainNumber"]
            train_names = trip.get("odpt:trainName", {})

            train_name = _train_name(train_names, "ja")
            train_name_en = _train_name(train_names, "en")

            if train_name:
                trip_short_name = trip_short_name + " " + train_name
                if train_name_en:
                    self.english_strings[trip_short_name] = trip["odpt:trainNumber"] + " " + train_name_en

            # Headsign
            destination_station = "・".join(destination_stations)
            destination_station_en = " / ".join([self._english(i) for i in destination_stations])

            if route_id == "JR-East.Yamanote":
                # Special case - JR-East.Yamanote line
                # Here, we include the direction_name, as it's important to users
                if direction_name == "内回り" and trip.get("odpt:nextTrainTimetable", []) in [[], None]:
                    trip_headsign = "内回り・{}".format(destination_station)
                    trip_headsign_en = "Inner Loop ⟲: {}".format(destination_station_en)

                if direction_name == "外回り" and trip.get("odpt:nextTrainTimetable", []) in [[], None]:
                    trip_headsign = "外回り・{}".format(destination_station)
                    trip_headsign_en = "Outer Loop ⟳: {}".format(destination_station_en)

                elif direction_name == "内回り":
                    trip_headsign = "内回り"
                    trip_headsign_en = "Inner Loop ⟲"

                elif direction_name == "外回り":
                    trip_headsign = "外回り"
                    trip_headsign_en = "Outer Loop ⟳"

                else:
                    raise ValueError("error while creating headsign of JR-East.Yamanote line train {}. please report this issue on GitHub.".format(trip_id))

            else:
                trip_headsign = destination_station
                trip_headsign_en = destination_station_en

                trip_type, trip_type_en = train_types.get(trip.get("odpt:trainType", ""), ("", ""))

                if trip_type:
                    trip_headsign = "（{}）{}".format(trip_type, destination_station)
                    if trip_headsign_en and trip_type_en:
                        trip_headsign_en = "({}) {}".format(trip_type_en, trip_headsign_en)
                    else:
                        trip_headsign_en = None

            if trip_headsign_en is not None:
                self.english_strings[trip_headsign_en] = trip_headsign_en

            # TODO: N'EX route creator
            #tofrom_narita_airport = lambda i: "odpt.Station:JR-East.NaritaAirportBranch.NaritaAirportTerminal1" in i.get("odpt:originStation", []) or \
            #                                  "odpt.Station:JR-East.NaritaAirportBranch.NaritaAirportTerminal1" in i.get("odpt:destinationStation", [])

            #if rotue_id.startswith("JR-East") and trip_type == "特急" and tofrom_narita_airport(trip):
            #    route_id = "JR-East.NaritaExpress"

            # Write to trips.txt
            writer_trips.writerow({
                "route_id": route_id, "trip_id": trip_id, "service_id": service_id,
                "trip_short_name": trip_short_name, "trip_headsign": trip_headsign,
                "direction_id": direction_id, "direction_name": direction_name,
                "block_id": block_id, "train_realtime_id": train_rt_id
            })

            # Times
            prev_departure = _Time(0)
            for idx, stop_time in enumerate(trip["odpt:trainTimetableObject"]):
                stop_id = timetable_item_station(stop_time)
                platform = stop_time.get("odpt:platformNumber", "")

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

                # Fix for after-midnight trips. GTFS requires "24:23", while ODPT data contains "00:23"
                if arrival < prev_departure: arrival += 86400
                if departure < arrival: departure += 86400
                prev_departure = copy(departure)

                writer_times.writerow({
                    "trip_id": trip_id, "stop_sequence": idx, "stop_id": stop_id, "platform": platform,
                    "arrival_time": str(arrival), "departure_time": str(departure)
                })

        buffer_trips.close()
        buffer_times.close()

    def fares(self):
        """Gets fares from odpt and converts it to gtfs. Each station has a unique zone; the fares are created by specifying fare for each station. Ticket fares are used. (No IC fare, no child fare)"""
        buffer_attributes = open("gtfs/fare_attributes.txt", mode="w", encoding="utf8", newline="")
        writer_attributes = csv.DictWriter(buffer_attributes, GTFS_HEADERS["fare_attributes.txt"], extrasaction="ignore")
        writer_attributes.writeheader()

        buffer_rules = open("gtfs/fare_rules.txt", mode="w", encoding="utf8", newline="")
        writer_rules = csv.DictWriter(buffer_rules, GTFS_HEADERS["fare_rules.txt"], extrasaction="ignore")
        writer_rules.writeheader()

        # Get list of fares
        fares_req = requests.get("https://api-tokyochallenge.odpt.org/api/v4/odpt:RailwayFare.json", params={"acl:consumerKey": self.apikey}, timeout=90, stream=True)
        fares_req.raise_for_status()
        fares = ijson.items(fares_req.raw, "item")

        # Iterate over fares
        for fare in fares:
            origin_id = fare["odpt:fromStation"].split(":")[1]
            destination_id = fare["odpt:toStation"].split(":")[1]
            fare_id = f"!{origin_id}_to_{destination_id}"
            fare_amt = fare["odpt:ticketFare"]

            agency_id = origin_id.split(".")[0]
            # As of 2019/07/21, none of these fares are inter-agency
            # Exceptions to Total = Agency A + Agency B:
            #    . Toei - TokyoMetro

            # The purpose of odpt:viaStation is not very clear, but it is assumed to have no harmful effects to using it as a contains_id in fare_rules
            if "odpt:viaStation" in fare:
                contains_id = fare["odpt:viaStation"].split(":")[1]
            else:
                contains_id = ""

            if self.verbose: print("\033[1A\033[KParsing fares:", fare_id)

            # Write to GTFS
            writer_attributes.writerow({
               "agency_id": agency_id, "fare_id": fare_id, "price": fare_amt, "currency_type": "JPY", "payment_method": 1, "transfers": ""
               })
            writer_rules.writerow({
                "fare_id": fare_id, "origin_id": origin_id, "destination_id": destination_id, "contains_id": contains_id})

        fares_req.close()
        buffer_attributes.close()
        buffer_rules.close()



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
        avg = lambda i: round(sum(i)/len(i), 8)

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

                # Special case for stations with the same name that are pretty close, but shouldn't be merged anyway
                elif stop_name_id in SEPARATE_STOPS:
                    close_enough = False

                # If there is; check distance between current stop and other stop in such merge group
                else:
                    saved_location = stops[stop_id_wsuffix][0]["lat"], stops[stop_id_wsuffix][0]["lon"]
                    row_location = float(row["stop_lat"]), float(row["stop_lon"])

                    # Append current stop to merge group only if it's up to 1km close.
                    # If current stop is further, try next merge group
                    if _distance(saved_location, row_location) <= 1:
                        close_enough = True

                    else:
                        close_enough = False

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
                    "stop_id": merge_group_stops[0]["id"],
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
                    "stop_id": station_id,
                    "stop_code": codes, "stop_name": station_name,
                    "stop_lat": station_lat, "stop_lon": station_lon,
                    "location_type": 1, "parent_station": ""
                })

                # Dump info about each stop
                for stop in merge_group_stops:
                    writer.writerow({
                        "stop_id": stop["id"],
                        "stop_code": stop["code"], "stop_name": station_name,
                        "stop_lat": stop["lat"], "stop_lon": stop["lon"],
                        "location_type": 0, "parent_station": station_id
                    })

        buffer.close()

    def trips_postprocesss(self):
        os.rename("gtfs/trips.txt", "gtfs/trips.txt.old")

        # Old file
        in_buffer = open("gtfs/trips.txt.old", mode="r", encoding="utf8", newline="")
        reader = csv.DictReader(in_buffer)

        # New file
        out_buffer = open("gtfs/trips.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(out_buffer, GTFS_HEADERS["trips.txt"], extrasaction="ignore")
        writer.writeheader()

        for row in reader:
            row["block_id"] = self.switch_blocks.get(row["block_id"], row["block_id"])

            writer.writerow(row)

        in_buffer.close()
        out_buffer.close()

        os.remove("gtfs/trips.txt.old")

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

        if self.verbose: print("\033[1A\033[KParsing fares")
        self.fares()
        if self.verbose: print("\033[1A\033[KParsing fares: finished")

        if self.verbose: print("\033[1A\033[KPost-processing stops")
        self.stops_postprocess()

        if self.verbose: print("\033[1A\033[KPost-processing trips")
        self.trips_postprocesss()

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
    args_parser.add_argument("--no-verbose", action="store_false", default=True, dest="verbose", help="don't verbose")
    args = args_parser.parse_args()

    # Apikey checks
    if args.apikey:
        apikey = args.apikey

    elif os.path.exists("apikey.txt"):
        with open("apikey.txt", mode="r", encoding="utf8") as f:
            apikey = f.read().strip()

    else:
        raise RuntimeError("No apikey!\n              Provide it inside command line argument '--apikey',\n              Or put it inside a file named 'apikey.txt'.")

    start_time = time.time()
    print(r"""
|  _____     _                 ____ _____ _____ ____   |
| |_   _|__ | | ___   _  ___  / ___|_   _|  ___/ ___|  |
|   | |/ _ \| |/ / | | |/ _ \| |  _  | | | |_  \___ \  |
|   | | (_) |   <| |_| | (_) | |_| | | | |  _|  ___) | |
|   |_|\___/|_|\_\\__, |\___/ \____| |_| |_|   |____/  |
|                 |___/                                |
    """)
    print("=== Trains GTFS: Starting! ===")

    print("Initializing parser")
    parser = TrainParser(apikey=apikey, verbose=args.verbose)

    print("Starting data parse... This might take some time...")
    parser.parse()

    print("Compressing to tokyo_trains.zip")
    parser.compress()

    total_time = time.time() - start_time
    print("=== TokyoGTFS: Finished in {} s ===".format(round(total_time, 2)))
