try: import ijson.backends.yajl2_cffi as ijson
except: import ijson

from datetime import datetime, date, timedelta
from collections import OrderedDict
from bs4 import BeautifulSoup
from warnings import warn
from copy import copy
from urllib.request import urlopen
import argparse
import requests
import zipfile
import json
import math
import time
import csv
import re
import io
import os

__title__ = "TokyoGTFS: Buses-GTFS"
__author__ = "Mikołaj Kuranowski"
__email__ = "mikolaj@mkuran.pl"
__license__ = "CC BY 4.0"

GTFS_HEADERS = {
    "agency.txt": ["agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang"],
    "stops.txt": ["stop_id", "stop_name", "stop_code", "stop_lat", "stop_lon", "zone_id"],
    "routes.txt": ["agency_id", "route_id", "route_short_name", "route_long_name", "route_type", "route_color", "route_text_color"],
    "trips.txt": ["route_id", "trip_id", "service_id", "trip_headsign", "trip_pattern_id", "wheelchair_accessible"],
    "stop_times.txt": ["trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time", "pickup_type", "drop_off_type"],
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

class BusesParser:
    def __init__(self, apikey, verbose=True):
        self.apikey = apikey
        self.verbose = verbose

        self.valid_stops = set()
        self.stop_names = {}
        self.pattern_map = {}
        self.english_strings = {}

        self.carmel_to_title = lambda i: re.sub(r"(?!^)([A-Z][a-z]+)", r" \1", i)

        # Clean gtfs/ directory
        if not os.path.exists("gtfs"): os.mkdir("gtfs")
        for file in os.listdir("gtfs"): os.remove("gtfs/" + file)

        # Get info on which routes to parse
        self.operators = OrderedDict()
        with open("data/bus_data.csv", mode="r", encoding="utf8", newline="") as buffer:
            reader = csv.DictReader(buffer)
            for row in reader:
                if row["route_timetables_available"] != "1": continue # Ignores agencies without BusTimetables
                self.operators[row["operator"]] = (row["color"].upper(), _text_color(row["color"]))

        # Calendars
        self.startdate = date.today()
        self.enddate = self.startdate + timedelta(days=180)
        self.used_calendars = OrderedDict()

    def _legal_calendars(self):
        calendars = requests.get("http://api-tokyochallenge.odpt.org/api/v4/odpt:Calendar.json", params={"acl:consumerKey": self.apikey}, timeout=30, stream=True)
        calendars.raise_for_status()
        calendars = ijson.items(calendars.raw, "item")

        valid_calendars = set()
        for calendar in calendars:
            calendar_id = calendar["owl:sameAs"].split(":")[1]

            if calendar_id in BUILT_IN_CALENDARS:
                valid_calendars.add(calendar_id)

            elif "odpt:day" in calendar and calendar["odpt:day"] != []:
                dates = [datetime.strptime(i, "%Y-%m-%d").date() for i in calendar["odpt:day"]]
                if min(dates) <= self.enddate and max(dates) >= self.startdate:
                    valid_calendars.add(calendar_id)

            else:
                warn("\033[1mno dates defined for calendar {}\033[0m".format(calendar_id))

        calendars.close()
        return valid_calendars

    def agencies(self):
        buffer = open("gtfs/agency.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(buffer, GTFS_HEADERS["agency.txt"], extrasaction="ignore")
        writer.writeheader()

        with open("data/operators.csv", mode="r", encoding="utf8", newline="") as add_info_buff:
            additional_info = {i["operator"]: i for i in csv.DictReader(add_info_buff)}

        # Iterate over agencies
        for operator in self.operators.keys():
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
            file_wrtr.writerow([
                '"Mikołaj Kuranowski (via TokyoGTFS); Data provided by Open Data Challenge for Public Transportation in Tokyo"',
                '"https://github.com/MKuranowski/TokyoGTFS"',
                "ja"
            ])

    def stops(self):
        """Parse stops"""
        # Get list of stops
        stops = requests.get("http://api-tokyochallenge.odpt.org/api/v4/odpt:BusstopPole.json", params={"acl:consumerKey": self.apikey}, timeout=30, stream=True)
        stops.raise_for_status()
        stops = ijson.items(stops.raw, "item")

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
            stop_code = stop.get("odpt:busstopPoleNumber", "")
            stop_name = stop["dc:title"]
            stop_name_en = self.carmel_to_title(stop_id.split(".")[1])

            if self.verbose: print("\033[1A\033[KParsing stops:", stop_id)

            self.stop_names[stop_id] = stop_name

            # Stop name translation
            if stop_name_en: self.english_strings[stop_name] = stop_name_en

            # Stop operators
            if type(stop["odpt:operator"]) is list:
                operators = [i.split(":")[1] for i in stop["odpt:operator"]]
            else:
                operators = [stop["odpt:operator"].split(":")[1]]

            # Ignore stops that belong to ignored agencies
            if not set(operators).intersection(self.operators):
                continue

            # Correct stop position
            if "geo:lat" in stop and "geo:long" in stop:
                stop_lat = stop["geo:lat"]
                stop_lon = stop["geo:long"]

            # Output to GTFS or to incorrect stops
            if stop_lat and stop_lon:
                self.valid_stops.add(stop_id)
                writer.writerow({
                    "stop_id": stop_id, "stop_code": stop_code, "zone_id": stop_id,
                    "stop_name": stop_name, "stop_lat": stop_lat, "stop_lon": stop_lon,
                })

            else:
                broken_stops_wrtr.writerow([stop_id, stop_name, stop_name_en, stop_code])

        stops.close()
        buffer.close()

    def routes(self):
        patterns = requests.get("http://api-tokyochallenge.odpt.org/api/v4/odpt:BusroutePattern.json", params={"acl:consumerKey": self.apikey}, timeout=30, stream=True)
        patterns.raise_for_status()
        patterns = ijson.items(patterns.raw, "item")

        buffer = open("gtfs/routes.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(buffer, GTFS_HEADERS["routes.txt"], extrasaction="ignore")
        writer.writeheader()

        self.parsed_routes = set()

        for pattern in patterns:
            pattern_id = pattern["owl:sameAs"].split(":")[1]

            if type(pattern["odpt:operator"]) is list: operator = pattern["odpt:operator"][0].split(":")[1]
            else: operator = pattern["odpt:operator"].split(":")[1]

            if operator not in self.operators: continue
            if self.verbose: print("\033[1A\033[KParsing route patterns:", pattern_id)

            # Get route_id
            if "odpt:busroute" in pattern:
                route_id = pattern["odpt:busroute"].split(":")[1]

            else:
                if operator == "JRBusKanto":
                    route_id = operator + "." + \
                               pattern_id.split(".")[1] + "." + \
                               pattern_id.split(".")[2]

                else:
                    route_id = operator + "." + pattern_id.split(".")[1]

            # Map pattern → route_id, as BusTimetable references patterns instead of routes
            self.pattern_map[pattern_id] = route_id

            # Get color from bus_colors.csv
            route_code = pattern["dc:title"].split(" ")[0] # Toei appends direction to BusroutePattern's dc:title
            route_color, route_text = self.operators[operator]

            # Output to GTFS
            if route_id not in self.parsed_routes:
                self.parsed_routes.add(route_id)
                writer.writerow({
                    "agency_id": operator,
                    "route_id": route_id,
                    "route_short_name": route_code,
                    "route_type": 3,
                    "route_color": route_color,
                    "route_text_color": route_text
                })

        patterns.close()
        buffer.close()

    def trips(self):
        """Parse trips & stop_times"""
        # Some variables
        available_calendars = self._legal_calendars()

        # Get all trips
        trips = requests.get("http://api-tokyochallenge.odpt.org/api/v4/odpt:BusTimetable.json", params={"acl:consumerKey": self.apikey}, timeout=90, stream=True)
        trips.raise_for_status()
        trips = ijson.items(trips.raw, "item")

        # Open GTFS trips
        buffer_trips = open("gtfs/trips.txt", mode="w", encoding="utf8", newline="")
        writer_trips = csv.DictWriter(buffer_trips, GTFS_HEADERS["trips.txt"], extrasaction="ignore")
        writer_trips.writeheader()

        buffer_times = open("gtfs/stop_times.txt", mode="w", encoding="utf8", newline="")
        writer_times = csv.DictWriter(buffer_times, GTFS_HEADERS["stop_times.txt"], extrasaction="ignore")
        writer_times.writeheader()

        # Iteratr over trips
        for trip in trips:
            operator = trip["odpt:operator"].split(":")[1]
            pattern_id = trip["odpt:busroutePattern"].split(":")[1]

            # Get route_id
            if pattern_id in self.pattern_map:
                route_id = self.pattern_map[pattern_id]

            else:
                if operator == "JRBusKanto":
                    route_id = operator + "." + \
                               pattern_id.split(".")[1] + "." + \
                               pattern_id.split(".")[2]

                else:
                    route_id = operator + "." + pattern_id.split(".")[1]

            trip_id = trip["owl:sameAs"].split(":")[1]
            calendar = trip["odpt:calendar"].split(":")[1]
            service_id = route_id + "/" + calendar

            if self.verbose: print("\033[1A\033[KParsing times:", trip_id)

            # Ignore non-parsed routes and non_active calendars
            if operator not in self.operators:
                continue

            if route_id not in self.parsed_routes:
                warn("\033[1mno route for pattern {}\033[0m".format(pattern_id))
                continue

            if calendar not in available_calendars:
                continue

            # Add calendar
            if route_id not in self.used_calendars: self.used_calendars[route_id] = set()
            self.used_calendars[route_id].add(calendar)

            # Ignore one-stop trips
            if len(trip["odpt:busTimetableObject"]) < 2:
                continue

            # Bus headsign
            headsigns = [i["odpt:destinationSign"] for i in trip["odpt:busTimetableObject"] if i.get("odpt:destinationSign") != None]

            if headsigns:
                trip_headsign = headsigns[0]

            else:
                last_stop_id = trip["odpt:busTimetableObject"][-1]["odpt:busstopPole"].split(":")[1]

                if last_stop_id in self.stop_names:
                    trip_headsign = self.stop_names[last_stop_id]

                else:
                    trip_headsign = re.sub(r"(?!^)([A-Z][a-z]+)", r" \1", last_stop_id.split(".")[1])
                    warn("\033[1mno name for stop {}\033[0m".format(last_stop_id))
                    self.stop_names[last_stop_id] = trip_headsign

            trip_headsign_en = self.english_strings.get(trip_headsign, "")

            # Non-step bus (wheelchair accesibility)
            if any([i.get("odpt:isNonStepBus") == False for i in trip["odpt:busTimetableObject"]]):
                wheelchair = "2"

            elif any([i.get("odpt:isNonStepBus") == True for i in trip["odpt:busTimetableObject"]]):
                wheelchair = "1"

            else:
                wheelchair = "0"

            # Do we start after midnight?
            prev_departure = _Time(0)
            if trip["odpt:busTimetableObject"][0].get("odpt:isMidnight", False):
                first_time = trip["odpt:busTimetableObject"][0].get("odpt:departureTime") or \
                             trip["odpt:busTimetableObject"][0].get("odpt:arrivalTime")
                # If that's a night bus, and the trip starts before 6 AM
                # Add 24h to departure, as the trip starts "after-midnight"
                if int(first_time.split(":")[0]) < 6: prev_departure = _Time(86400)

            # Filter stops to include only active stops
            trip["odpt:busTimetableObject"] = sorted([
                    i for i in trip["odpt:busTimetableObject"]
                    if i["odpt:busstopPole"].split(":")[1] in self.valid_stops
                ], key=lambda i: i["odpt:index"])

            # Ignore trips with less then 1 stop
            if len(trip["odpt:busTimetableObject"]) <= 1:
                #warn("\033[1mno correct stops in trip {}\033[0m".format(trip_id))
                continue

            # Write to trips.txt
            writer_trips.writerow({
                "route_id": route_id, "trip_id": trip_id,
                "service_id": service_id, "trip_headsign": trip_headsign,
                "trip_pattern_id": pattern_id, "wheelchair_accessible": wheelchair
            })

            # Times
            for idx, stop_time in enumerate(trip["odpt:busTimetableObject"]):
                stop_id = stop_time["odpt:busstopPole"].split(":")[1]

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

                # Can get on/off?
                # None → no info → fallbacks to True, but bool(None) == False, so we have to explicitly comapre the value to False
                pickup = "1" if stop_time.get("odpt:CanGetOn") == False else "0"
                dropoff = "1" if stop_time.get("odpt:CanGetOff") == False else "0"

                writer_times.writerow({
                    "trip_id": trip_id, "stop_sequence": idx, "stop_id": stop_id,
                    "arrival_time": str(arrival), "departure_time": str(departure),
                    "pickup_type": pickup, "drop_off_type": dropoff
                })

        trips.close()
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
        calendars = requests.get("http://api-tokyochallenge.odpt.org/api/v4/odpt:Calendar.json", params={"acl:consumerKey": self.apikey}, timeout=30, stream=True)
        calendars.raise_for_status()
        calendars = ijson.items(calendars.raw, "item")

        # Get info on specific calendars
        calendar_dates = {}
        for calendar in calendars:
            calendar_id = calendar["owl:sameAs"].split(":")[1]
            if "odpt:day" in calendar and calendar["odpt:day"] != []:
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

        calendars.close()
        buffer.close()

    def trips_calendars_crosscheck(self):
        # Sometimes BusTimetable references the "Holiday" service, which is »valid«,
        # But sometimes specific calendars override every holiday inside the GTFS peroid
        # This functions checks if service_id of every trips is inside calendar_dates.txt

        valid_services = set()
        remove_trips = set()

        # Read valid services
        if self.verbose: print("\033[1A\033[KTrips×Calendars cross-check: reading calendar_dates.txt")

        buff = open("gtfs/calendar_dates.txt", "r", encoding="utf8")
        reader = csv.DictReader(buff)
        for row in reader:
            valid_services.add(row["service_id"])
        buff.close()

        ### FIX TRIPS.TXT ###
        if self.verbose: print("\033[1A\033[KTrips×Calendars cross-check: rewriting trips.txt")
        os.rename("gtfs/trips.txt", "gtfs/trips.txt.old")

        # Old file
        in_buffer = open("gtfs/trips.txt.old", mode="r", encoding="utf8", newline="")
        reader = csv.DictReader(in_buffer)

        # New file
        out_buffer = open("gtfs/trips.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(out_buffer, GTFS_HEADERS["trips.txt"], extrasaction="ignore")
        writer.writeheader()

        for row in reader:
            if row["service_id"] in valid_services:
                writer.writerow(row)
            else:
                remove_trips.add(row["trip_id"])

        in_buffer.close()
        out_buffer.close()

        os.remove("gtfs/trips.txt.old")
        del valid_services

        ### FIX STOP_TIMES.TXT ###
        if self.verbose: print("\033[1A\033[KTrips×Calendars cross-check: rewriting stop_times.txt")
        os.rename("gtfs/stop_times.txt", "gtfs/stop_times.txt.old")

        # Old file
        in_buffer = open("gtfs/stop_times.txt.old", mode="r", encoding="utf8", newline="")
        reader = csv.DictReader(in_buffer)

        # New file
        out_buffer = open("gtfs/stop_times.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(out_buffer, GTFS_HEADERS["stop_times.txt"], extrasaction="ignore")
        writer.writeheader()

        for row in reader:
            if row["trip_id"] in remove_trips:
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

        if self.verbose: print("\033[1A\033[KParsing times")
        self.trips()
        if self.verbose: print("\033[1A\033[KParsing times: finished")

        if self.verbose: print("\033[1A\033[KParsing translations")
        self.translations()

        if self.verbose: print("\033[1A\033[KParsing calendars")
        self.calendars()
        if self.verbose: print("\033[1A\033[KParsing calendars: finished")

        if self.verbose: print("\033[1A\033[KTrips×Calendars cross-check")
        self.trips_calendars_crosscheck()
        if self.verbose: print("\033[1A\033[KTrips×Calendars cross-check: finished")

        if self.verbose: print("\033[1A\033[KParsing finished!")

    def compress(self):
        "Compress all created files to tokyo_trains.zip"
        archive = zipfile.ZipFile("tokyo_buses.zip", mode="w", compression=zipfile.ZIP_DEFLATED)
        for file in os.listdir("gtfs"):
            if file.endswith(".txt"):
                archive.write(os.path.join("gtfs", file), arcname=file)
        archive.close()

if __name__ == "__main__":
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("-a", "--apikey", metavar="YOUR_APIKEY", help="apikey from developer-tokyochallenge.odpt.org")
    args_parser.add_argument("--no-verbose", action="store_false", dest="verbose", help="don't verbose")
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
    print("=== Buses GTFS: Starting! ===")


    print("Initializing parser")
    parser = BusesParser(apikey=apikey, verbose=args.verbose)

    print("Starting data parse... This might take some time...")
    parser.parse()

    print("Compressing to tokyo_buses.zip")
    parser.compress()

    total_time = time.time() - start_time
    print("=== TokyoGTFS: Finished in {} s ===".format(round(total_time, 2)))
