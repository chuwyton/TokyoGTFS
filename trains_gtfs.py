try: import ijson.backends.yajl2_c as ijson
except: import ijson

from datetime import datetime, date, timedelta, timezone
from collections import OrderedDict
from bs4 import BeautifulSoup
from pykakasi import kakasi
from warnings import warn
from copy import copy
import argparse
import requests
import zipfile
import iso8601
import shutil
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
__license__ = "MIT"

ADDITIONAL_ENGLISH = {}

GTFS_HEADERS = {
    "agency.txt": ["agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang"],
    "stops.txt": ["stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon", "location_type", "parent_station"],
    "routes.txt": ["agency_id", "route_id", "route_short_name", "route_long_name", "route_type", "route_color", "route_text_color"],
    "trips.txt": ["route_id", "trip_id", "service_id", "trip_short_name", "trip_headsign", "direction_id", "direction_name", "block_id", "train_realtime_id"],
    "stop_times.txt": ["trip_id", "stop_sequence", "stop_id", "platform", "arrival_time", "departure_time"],
    "calendar_dates.txt": ["service_id", "date", "exception_type"],
    #"fare_attributes.txt": ["agency_id", "fare_id", "price", "currency_type", "payment_method", "transfers"],
    #"fare_rules.txt": ["fare_id", "contains_id"],
    "translations.txt": ["trans_id", "lang", "translation"]
}

BUILT_IN_CALENDARS = {"Weekday", "SaturdayHoliday", "Holiday", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Everyday"}

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
        # Check if trip has not expired
        if "dct:valid" in trip:
            valid_until = iso8601.parse_date(trip["dct:valid"])
            current_time_in_japan = datetime.now(timezone(timedelta(hours=9)))

            if valid_until <= current_time_in_japan:
                continue

        # Avoid duplicate trips, it sometimes happens
        if trip["owl:sameAs"] in parsed_trips:
            continue

        parsed_trips.add(trip["owl:sameAs"])

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

class DataAssumptionError(ValueError):
    pass

class TrainParser:
    def __init__(self, apikey, verbose=True):
        self.apikey = apikey
        self.verbose = verbose

        # Stations stuff
        self.valid_stops = set()
        self.station_names = {}
        self.station_positions = {}

        # Blocks stuff
        self.train_blocks = {}

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

        # Postprocess Stuff
        self.outputted_clendars = set()
        self.remove_trips = set()

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

    def _train_headsigns(self, train_types, route_id, trip, destination_stations, direction_name):
        destination_station = "・".join(destination_stations)
        destination_station_en = " / ".join([self._english(i) for i in destination_stations])
        trip_id = trip["owl:sameAs"].split(":")[1]

        if route_id == "JR-East.Yamanote":
            # Special case - JR-East.Yamanote line
            # Here, we include the direction_name, as it's important to users
            if direction_name == "内回り" and not trip.get("odpt:nextTrainTimetable"):
                trip_headsign = f"（内回り）{destination_station}"
                trip_headsign_en = f"(Inner Loop ⟲) {destination_station_en}"

            if direction_name == "外回り" and not trip.get("odpt:nextTrainTimetable"):
                trip_headsign = f"（外回り）{destination_station}"
                trip_headsign_en = f"(Outer Loop ⟳) {destination_station_en}"

            elif direction_name == "内回り":
                trip_headsign = "内回り"
                trip_headsign_en = "Inner Loop ⟲"

            elif direction_name == "外回り":
                trip_headsign = "外回り"
                trip_headsign_en = "Outer Loop ⟳"

            else:
                raise DataAssumptionError(
                    "error while creating headsign of JR-East.Yamanote line " \
                    f"train {trip_id}. please report this issue on GitHub."
                )

        else:
            trip_headsign = destination_station
            trip_headsign_en = destination_station_en

            trip_type_id = trip.get("odpt:trainType", ":").split(":")[1]
            trip_type, trip_type_en = train_types.get(trip_type_id, ("", ""))

            if trip_type:
                trip_headsign = "（{}）{}".format(trip_type, destination_station)
                if trip_headsign_en and trip_type_en:
                    trip_headsign_en = "({}) {}".format(trip_type_en, trip_headsign_en)
                else:
                    trip_headsign_en = None

        if trip_headsign_en is not None:
            self.english_strings[trip_headsign] = trip_headsign_en

        return trip_headsign

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

            #else:
            #    warn("\033[1mno dates defined for calendar {}\033[0m".format(calendar_id))

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
            file_buff.write(",".join(["feed_publisher_name", "feed_publisher_url", "feed_lang"]) + "\n")
            file_buff.write(",".join([
                '"Mikołaj Kuranowski (via TokyoGTFS); Data provided by Open Data Challenge for Public Transportation in Tokyo"',
                '"https://github.com/MKuranowski/TokyoGTFS"',
                "ja"
            ]) + "\n")

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

    def blocks(self):
        """
        Solve blocks - properly order trains into blocks

        This block of code only supports 4 types of ODPT-data blocks:
        0: No through service

        1: TRIP 1 ━━ ... ━━ TRIP N
           (simple through service)

        2. TRIP A/1 ━━ ... ━━ TRIP A/n ━━┓
                                         ┣━ TRIP C/1 ━━ ... ━━ TRIP C/n
           TRIP B/1 ━━ ... ━━ TRIP B/n ━━┛
           (2 trips merging into 1 trip)

        3.                              ┏━ TRIP B/1 ━━ ... ━━ TRIP B/n
           TRIP A/1 ━━ ... ━━ TRIP A/n ━┫
                                        ┗━ TRIP C/1 ━━ ... ━━ TRIP C/n
           (1 trip splitting into 2 trips)

        And also, r/badcode. Seriously, I just can be bothered to write this nicely.
        """

        trains = trip_generator(self.apikey)

        self.train_blocks = {}

        through_trains = {}
        undef_trains = {}
        block_id = 0

        ### LOAD THROUGH TRAIN DATA ###
        for trip in trains:
            trip_id = trip["owl:sameAs"].split(":")[1]

            if self.verbose: print("\033[1A\033[K" + "Loading to block_id solver:", trip_id)

            next_trains = trip.get("odpt:nextTrainTimetable", [])
            prev_trains = trip.get("odpt:previousTrainTimetable", [])

            destinations = trip.get("odpt:destinationStation", [])
            origins = trip.get("odpt:originStation", [])

            # Convert all to lists
            if type(next_trains) is str: next_trains = [next_trains]
            if type(prev_trains) is str: prev_trains = [prev_trains]
            if type(destinations) is str: destinations = [destinations]
            if type(origins) is str: destinations = [origins]


            # Change ids to match GTFS ids
            next_trains = [i.split(":")[1] for i in next_trains]
            prev_trains = [i.split(":")[1] for i in prev_trains]
            destinations = [i.split(":")[1] for i in destinations]
            origins = [i.split(":")[1] for i in origins]

            # Trains with absolutely no nextTrainTimetable and previousTrainTimetable
            # cannot belong to any blocks.
            if (not next_trains) and (not prev_trains):
                undef_trains[trip_id] = {
                    "previous"   : prev_trains,
                    "next"       : next_trains,
                    "destination": destinations,
                }

                continue

            # No splitting AND merging is supported
            if len(origins) > 1 and len(destinations) > 1:
                raise ValueError(f"{trip_id} has more then 2 destinations and origins - no idea how to map this to GTFS blocks")

            if len(next_trains) > 1 and len(prev_trains) > 1:
                raise ValueError(f"{trip_id} has more then 2 prevTimetables and nextTrains - no idea how to map this to GTFS blocks")

            through_trains[trip_id] = {
                "previous"   : prev_trains,
                "next"       : next_trains,
                "destination": destinations,
            }

        ### SOLVE TRAIN BLOCKS ###
        while through_trains:
            block_id += 1

            trains_in_block = {}

            # First Trip
            main_trip, main_tripdata = through_trains.popitem()
            trip, data = main_trip, main_tripdata
            trains_in_block[trip] = data

            fetch_trips = set(data["previous"]).union(data["next"])
            failed_fetch = False

            if self.verbose: print("\033[1A\033[K" + "Solving block for:", main_trip)

            # Save all connected trips into trains_in_block
            while fetch_trips:

                trip = fetch_trips.pop()
                try:
                    data = through_trains.pop(trip)
                except:
                    try:
                        data = undef_trains.pop(trip)
                    except:
                        warn("\033[1m" +
                            f"following through services for {main_trip}\n" +
                            " "*8 + f"reaches {trip}, " +
                            "which is already used in a different block" +
                            "\033[0m")

                        failed_fetch = True
                        break

                trains_in_block[trip] = data

                # Look over through_trains to find which trips reference current trip and add them too
                additional_trips = (
                    i[0] for i in through_trains.items() if \
                    trip in i[1]["previous"] or trip in i[1]["next"])

                # Add direct references from current trip to additional_trips
                # And remove trips we already fetched
                fetch_trips = fetch_trips.union(data["previous"]) \
                                         .union(data["next"])     \
                                         .union(additional_trips) \
                                         .difference(trains_in_block.keys())

            # Failed Fetch
            if failed_fetch:
                continue

            # Check how many splits and merges exist in this block
            max_merges = max([len(i["previous"]) for i in trains_in_block.values()])
            max_splits = max([len(i["next"]) for i in trains_in_block.values()])

            # Linear Block - No splitting or merging
            if max_splits == 1 and max_merges == 1:
                for trip_id in trains_in_block:
                    self.train_blocks[trip_id] = block_id

                continue

            # Both split & merge - todo, but currently not found in real life
            elif max_splits > 1 and max_merges > 1:
                raise DataAssumptionError(
                    "the below block of trips both splits and merges - " \
                    "handling this logic is not implented." \
                    f"Here's the list of trains in this block: {list(trains_in_block.keys())}"
                )

            ### SPLITTING TRAIN ###
            elif max_splits > 1:
                split_train_id = [i[0] for i in trains_in_block.items() if len(i[1]["next"]) > 1]

                if len(split_train_id) > 1:
                    raise DataAssumptionError(
                        "encountered a block of trains with more then 2 splits -" \
                        "handling this logic is not implemented."
                        f"Here's the list of trains in this block: {list(trains_in_block.keys())}"
                    )

                elif len(split_train_id) == 0:
                    raise RuntimeError("it's impossible to get here. if you see this error call an exorcist.")

                split_train_id = split_train_id[0]
                split_train_data = trains_in_block.pop(split_train_id)

                for train_after_split in split_train_data["next"]:

                    next_train_id = train_after_split
                    next_train_data = trains_in_block.pop(next_train_id)

                    destination = next_train_data["destination"]

                    if not destination:
                        raise DataAssumptionError(
                            f"expected train {next_train_id} " \
                            "to have destinationStation defined, " \
                            "as it's the first trip after a train split"
                        )

                    # Add block_id to trips after split, while popping them from trains_in_block
                    while next_train_id:
                        self.train_blocks[next_train_id] = block_id

                        # Try to find the next train by odpt:nextTrainTimetable
                        if next_train_data["next"]:
                            next_train_id = next_train_data["next"][0]
                            next_train_data = trains_in_block.pop(next_train_id)

                        # If this fails, try to find the current train in other trips' odpt:previousTrainTimetable
                        else:
                            for potential_train_id, potential_train_data in trains_in_block.items():
                                if next_train_id in potential_train_data["previous"]:

                                    next_train_id = potential_train_id
                                    next_train_data = potential_train_data

                                    del trains_in_block[next_train_id]

                                    break
                            else:
                                next_train_id = None
                                next_train_data = None


                    # Add the splitting train to block
                    if split_train_id not in self.train_blocks:
                        self.train_blocks[split_train_id] = []

                    self.train_blocks[split_train_id].append((block_id, destination))

                    # Add block_id to trips before split
                    if len(split_train_data["previous"]) == 0:
                        block_id += 1
                        continue

                    prev_train_id = split_train_data["previous"][0]
                    prev_train_data = trains_in_block[prev_train_id]

                    # Add block_id to trips after split, while popping them from trains_in_block
                    while prev_train_id:

                        if prev_train_id not in self.train_blocks:
                            self.train_blocks[prev_train_id] = []

                        self.train_blocks[prev_train_id].append((block_id, destination))

                        # Try to find the previous leg by odpt:previousTrainTimetable
                        if prev_train_data["previous"]:
                            prev_train_id = prev_train_data["previous"][0]
                            prev_train_data = trains_in_block[prev_train_id]

                        # If that fails, try to find prev_train_id in trips' odpt:nextTrainTimetable
                        else:
                            for potential_train_id, potential_train_data in trains_in_block.items():
                                if prev_train_id in potential_train_data["next"]:

                                    prev_train_id = potential_train_id
                                    prev_train_data = potential_train_data

                                    break
                            else:
                                prev_train_id = None
                                prev_train_data = None

                    block_id += 1

            ### MERGING TRAIN ###
            elif max_merges > 1:
                merge_train_id = [i[0] for i in trains_in_block.items() if len(i[1]["previous"]) > 1]

                if len(merge_train_id) > 1:
                    raise DataAssumptionError(
                        "encountered a block of trains with more then 2 merges -" \
                        "handling this logic is not implemented."
                        f"Here's the list of trains in this block: {list(trains_in_block.keys())}"
                    )

                elif len(merge_train_id) == 0:
                    raise RuntimeError("it's impossible to get here. if you see this error call an exorcist.")

                merge_train_id = merge_train_id[0]
                merge_train_data = trains_in_block.pop(merge_train_id)

                for train_before_split in merge_train_data["previous"]:

                    prev_train_id = train_before_split
                    prev_train_data = trains_in_block.pop(prev_train_id)

                    # Add block_id to trips before the split, while popping them from trains_in_block
                    while prev_train_id:
                        self.train_blocks[prev_train_id] = block_id

                        # Try to find the previous leg by odpt:previousTrainTimetable
                        if prev_train_data["previous"]:
                            prev_train_id = prev_train_data["previous"][0]
                            prev_train_data = trains_in_block.pop(prev_train_id)

                        # If that fails, try to find next_trip_id in trips' odpt:nextTrainTimetable
                        else:
                            for potential_train_id, potential_train_data in trains_in_block.items():
                                if prev_train_id in potential_train_data["next"]:

                                    prev_train_id = potential_train_id
                                    prev_train_data = potential_train_data

                                    del trains_in_block[prev_train_id]

                                    break
                            else:
                                prev_train_id = None
                                prev_train_data = None

                    # Add the merging train to block
                    if split_train_id not in self.train_blocks:
                        self.train_blocks[split_train_id] = []

                    self.train_blocks[split_train_id].append((block_id, None))

                    # Add block_id to trips after merge
                    if len(merge_train_data["next"]) == 0:
                        block_id += 1
                        continue

                    next_train_id = merge_train_data["next"][0]
                    next_train_data = trains_in_block[next_train_id]

                    # Add block_id to trips after split
                    while next_train_id:

                        if next_train_id not in self.train_blocks:
                            self.train_blocks[next_train_id] = []

                        self.train_blocks[next_train_id].append((block_id, None))

                        # Try to find the next train by odpt:nextTrainTimetable
                        if next_train_data["next"]:
                            next_train_id = next_train_data["next"][0]
                            next_train_data = trains_in_block[next_train_id]

                        # If this fails, try to find the current train in other trips' odpt:previousTrainTimetable
                        else:
                            for potential_train_id, potential_train_data in trains_in_block.items():
                                if next_train_id in potential_train_data["previous"]:

                                    next_train_id = potential_train_id
                                    next_train_data = potential_train_data

                                    break
                            else:
                                next_train_id = None
                                next_train_data = None


                    block_id += 1

    def trips(self):
        """Parse trips & stop_times"""
        # Some variables
        timetable_item_station = lambda i: (i.get("odpt:departureStation") or i.get("odpt:arrivalStation")).split(":")[1]

        train_types = self._train_types()
        train_directions = self._train_directions()
        available_calendars = self._legal_calendars()
        main_direction = {}

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
            service_id = route_id + "." + calendar
            train_rt_id = trip["odpt:train"].split(":")[1] if "odpt:train" in trip else ""

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

            # Block
            block_id = self.train_blocks.get(trip_id, "")

            # Ignore one-stop that are not part of a block
            if len(trip["odpt:trainTimetableObject"]) < 2 and block_id == "":
                continue

            # Train Direction
            if route_id == "TokyoMetro.Chiyoda":
                # Chiyoda line — special case.
                # This line is really 2 lines: YoyogiUehara↔Ayase and Ayase↔KitaAyase
                # This makes 3 directions in the ODPT data: YoyogiUehara, Ayase and KitaAyase

                stations_of_trip = [timetable_item_station(i) for i in trip["odpt:trainTimetableObject"]]

                if trip["odpt:railDirection"] == "odpt.RailDirection:TokyoMetro.YoyogiUehara":
                    # Ayase → YoyogiUehara
                    direction_id, direction_name = "0", train_directions.get("odpt.RailDirection:TokyoMetro.YoyogiUehara", "")

                elif trip["odpt:railDirection"] == "odpt.RailDirection:TokyoMetro.KitaAyase":
                    # Ayase → KitaAyase
                    direction_id, direction_name = "1", train_directions.get("odpt.RailDirection:TokyoMetro.KitaAyase", "")

                elif trip["odpt:railDirection"] == "odpt.RailDirection:TokyoMetro.Ayase" and \
                                            "TokyoMetro.Chiyoda.KitaAyase" in stations_of_trip:
                    # KitaAyase → Ayase
                    direction_id, direction_name = "0", train_directions.get("odpt.RailDirection:TokyoMetro.Ayase", "")

                elif trip["odpt:railDirection"] == "odpt.RailDirection:TokyoMetro.Ayase":
                    # YoyogiUehara → Ayase
                    direction_id, direction_name = "1", train_directions.get("odpt.RailDirection:TokyoMetro.Ayase", "")

                else:
                    raise DataAssumptionError(
                        "error while resolving directions of TokyoMetro.Chiyoda " \
                        f"line train {trip_id}. please report this issue on GitHub."
                    )

            elif "odpt:railDirection" in trip:
                if route_id not in main_direction:
                    main_direction[route_id] = trip["odpt:railDirection"]
                direction_name = train_directions.get(trip["odpt:railDirection"], "")
                direction_id = 0 if trip["odpt:railDirection"] == main_direction[route_id] else 1

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
            trip_headsign = self._train_headsigns(train_types, route_id, trip,
                                           destination_stations, direction_name)

            # TODO: N'EX route creator
            #tofrom_narita_airport = lambda i: "odpt.Station:JR-East.NaritaAirportBranch.NaritaAirportTerminal1" in i.get("odpt:originStation", []) or \
            #                                  "odpt.Station:JR-East.NaritaAirportBranch.NaritaAirportTerminal1" in i.get("odpt:destinationStation", [])

            #if rotue_id.startswith("JR-East") and trip_type == "特急" and tofrom_narita_airport(trip):
            #    route_id = "JR-East.NaritaExpress"

            # Times
            times = []
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

                times.append({
                    "stop_sequence": idx, "stop_id": stop_id, "platform": platform,
                    "arrival_time": str(arrival), "departure_time": str(departure)
                })

            # Output
            if type(block_id) is not list:
                writer_trips.writerow({
                    "route_id": route_id, "trip_id": trip_id, "service_id": service_id,
                    "trip_short_name": trip_short_name, "trip_headsign": trip_headsign,
                    "direction_id": direction_id, "direction_name": direction_name,
                    "block_id": block_id, "train_realtime_id": train_rt_id
                })

                for row in times:
                    row["trip_id"] = trip_id
                    writer_times.writerow(row)

            else:
                for suffix, (new_block_id, new_dest_ids) in enumerate(block_id):
                    new_trip_id = f"{trip_id}.Block{new_block_id}"

                    if new_dest_ids:
                        new_dest_stations = [self._stop_name(i) for i in new_dest_ids]

                        new_headsign = self._train_headsigns(
                            train_types, route_id, trip,
                            new_dest_stations, direction_name)

                    else:
                        new_headsign = trip_headsign

                    writer_trips.writerow({
                        "route_id": route_id, "trip_id": new_trip_id, "service_id": service_id,
                        "trip_short_name": trip_short_name, "trip_headsign": new_headsign,
                        "direction_id": direction_id, "direction_name": direction_name,
                        "block_id": new_block_id, "train_realtime_id": train_rt_id
                    })

                    for row in times:
                        row["trip_id"] = new_trip_id
                        writer_times.writerow(row)

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

                elif "Everyday" in services:
                    active_services = ["Everyday"]

                if active_services:
                    for service in active_services:
                        service_id = route + "." + service

                        self.outputted_clendars.add(service_id)
                        writer.writerow({"service_id": service_id, "date": working_date.strftime("%Y%m%d"), "exception_type": 1})

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
            if row["service_id"] not in self.outputted_clendars:
                self.remove_trips.add(row["trip_id"])

            else:
                writer.writerow(row)

        in_buffer.close()
        out_buffer.close()

        os.remove("gtfs/trips.txt.old")

    def times_postprocesss(self):
        os.rename("gtfs/stop_times.txt", "gtfs/stop_times.txt.old")

        # Old file
        in_buffer = open("gtfs/stop_times.txt.old", mode="r", encoding="utf8", newline="")
        reader = csv.DictReader(in_buffer)

        # New file
        out_buffer = open("gtfs/stop_times.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(out_buffer, GTFS_HEADERS["stop_times.txt"], extrasaction="ignore")
        writer.writeheader()

        for row in reader:
            if row["trip_id"] in self.remove_trips:
                continue

            else:
                writer.writerow(row)

        in_buffer.close()
        out_buffer.close()

        os.remove("gtfs/stop_times.txt.old")

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

        if self.verbose: print("\033[1A\033[KParsing blocks")
        self.blocks()
        if self.verbose: print("\033[1A\033[KParsing blocks: finished")

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

        if self.verbose: print("\033[1A\033[KPost-processing trips")
        self.trips_postprocesss()

        if self.verbose: print("\033[1A\033[KPost-processing stop_times")
        self.times_postprocesss()

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
