from collections import OrderedDict
from datetime import datetime
from copy import copy
import pytz
import csv
import re
import os

from .handlers import TimeValue, ApiHandler, CalendarHandler

from .utils import distance, text_color, clear_directory, \
                   carmel_to_title, compress, print_log

from .const import GTFS_HEADERS_BUS
from .err import DataAssertion

class BusParser:
    """Object responsible for parsing bus data

    :param apikey: Key to the ODPT API
    :type apikey: str
    """

    def __init__(self, apikey):
        """Init the BusParser"""
        self.apikey = apikey
        self.api = ApiHandler(apikey)

        # Separate handlers
        self.today = datetime.now(pytz.timezone("Asia/Tokyo"))

        self.calendars = CalendarHandler(self.api, self.today.date())

        # Route & Operator data
        self.operators = OrderedDict()
        self.parsed_routes = set()

        # Trips data
        self.pattern_map = {}
        
        # Stops data
        self.stop_names = {}
        self.valid_stops = set()

    def load_operators(self):
        """Load operator data from bus_data.csv"""

        print_log("Loading data from bus_data.csv")

        with open("data/bus_data.csv", mode="r", encoding="utf8", newline="") as buffer:
            reader = csv.DictReader(buffer)
            for row in reader:
                # Ignore agencies without BusTimetables
                if row["route_timetables_available"] != "1":
                    continue 
                
                self.operators[row["operator"]] = (row["color"].upper(), text_color(row["color"]))

    def agencies(self):
        """Load data from operators.csv and export them to agencies.txt"""
        buffer = open("gtfs/agency.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(buffer, GTFS_HEADERS_BUS["agency.txt"])
        writer.writeheader()

        with open("data/operators.csv", mode="r", encoding="utf8", newline="") as add_info_buff:
            additional_info = {i["operator"]: i for i in csv.DictReader(add_info_buff)}

        # Iterate over agencies
        for operator in self.operators.keys():
            # Get data fro moperators.csv
            operator_data = additional_info.get(operator, {})
            if not operator_data:
                print_log(f"no data defined for operator {operator}", 1)

            # Write to agency.txt
            writer.writerow({
                "agency_id": operator,
                "agency_name": operator_data.get("name", operator),
                "agency_url": operator_data.get("website", ""),
                "agency_timezone": "Asia/Tokyo",
                "agency_lang": "ja"
            })

        buffer.close()

    def feed_info(self):
        """Create file gtfs/feed_info.txt"""
        print_log("Exporting feed_info")
        version = self.today.strftime("%Y-%m-%d %H:%M:%S")

        with open("gtfs/feed_info.txt", mode="w", encoding="utf8", newline="\r\n") as file_buff:
            file_buff.write("feed_publisher_name,feed_publisher_url,feed_lang,feed_version\n")
            
            file_buff.write(
                '"TokyoGTFS (provided by Mikołaj Kuranowski)",'
                '"https://github.com/MKuranowski/TokyoGTFS",'
                f"pl,{version}\n"
            )

    def attributions(self):
        """Create file gtfs/attributions.txt"""
        print_log("Exporting attributions")

        with open("gtfs/attributions.txt", mode="w", encoding="utf8", newline="\r\n") as file_buff:
            file_buff.write(
                "attribution_id,organization_name,attribution_url"
                "is_producer,is_authority,is_data_source\n"
            )
            
            file_buff.write(
                '0,"TokyoGTFS (provided by Mikołaj Kuranowski)",'
                '"https://github.com/MKuranowski/TokyoGTFS",'
                '1,0,0\n'
            )

            file_buff.write(
                '1,"Open Data Challenge for Public Transportation in Tokyo",'
                '"http://tokyochallenge.odpt.org/",'
                '0,1,1\n'
            )

    def stops(self):
        """Parse stops and export them to gtfs/stops.txt"""
        # Get list of stops
        stops = self.api.get("BusstopPole")

        # Open files
        print_log("Exporting stops")

        buffer = open("gtfs/stops.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(buffer, GTFS_HEADERS_BUS["stops.txt"])
        writer.writeheader()

        broken_stops_buff = open("broken_stops.csv", mode="w", encoding="utf8", newline="")
        broken_stops_wrtr = csv.writer(broken_stops_buff)
        broken_stops_wrtr.writerow(["stop_id", "stop_name", "stop_code"])

        # Iterate over stops
        for stop in stops:
            stop_id = stop["owl:sameAs"].split(":")[1]
            stop_code = stop.get("odpt:busstopPoleNumber", "")
            stop_name = stop["dc:title"]

            print_log(f"Exporting stops: {stop_id}")

            self.stop_names[stop_id] = stop_name

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
                    "stop_id": stop_id, "stop_code": stop_code,
                    "stop_name": stop_name, "stop_lat": stop_lat, "stop_lon": stop_lon,
                })

            else:
                broken_stops_wrtr.writerow([stop_id, stop_name, stop_code])

        buffer.close()
        broken_stops_buff.close()

    def routes(self):
        """Parse routes and export them to routes.txt"""
        patterns = self.api.get("BusroutePattern")

        print_log("Parsing patterns & exporting routes")

        buffer = open("gtfs/routes.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(buffer, GTFS_HEADERS_BUS["routes.txt"])
        writer.writeheader()

        self.parsed_routes = set()

        for pattern in patterns:
            pattern_id = pattern["owl:sameAs"].split(":")[1]

            if type(pattern["odpt:operator"]) is list:
                operator = pattern["odpt:operator"][0].split(":")[1]
            else:
                operator = pattern["odpt:operator"].split(":")[1]

            if operator not in self.operators:
                continue
            
            print_log(f"Parsing patterns & exporting routes: {pattern_id}")

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

            # Output to GTFS
            if route_id not in self.parsed_routes:

                # Toei appends direction to BusroutePattern's dc:title
                route_code = pattern["dc:title"].split(" ")[0]
                
                # Get color from bus_colors.csv
                route_color, route_text = self.operators[operator]

                self.parsed_routes.add(route_id)

                writer.writerow({
                    "agency_id": operator,
                    "route_id": route_id,
                    "route_short_name": route_code,
                    "route_type": 3,
                    "route_color": route_color,
                    "route_text_color": route_text
                })

        buffer.close()

    def trips(self):
        """Parse trips & stop_times and export them"""
        # Get all trips
        trips = self.api.get("BusTimetable")

        print_log("Exporting trips")

        # Open GTFS trips
        buffer_trips = open("gtfs/trips.txt", mode="w", encoding="utf8", newline="")
        writer_trips = csv.DictWriter(buffer_trips, GTFS_HEADERS_BUS["trips.txt"], extrasaction="ignore")
        writer_trips.writeheader()

        buffer_times = open("gtfs/stop_times.txt", mode="w", encoding="utf8", newline="")
        writer_times = csv.DictWriter(buffer_times, GTFS_HEADERS_BUS["stop_times.txt"], extrasaction="ignore")
        writer_times.writeheader()

        # Iterate over trips
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
            calendar_id = trip["odpt:calendar"].split(":")[1]
            service_id = self.calendars.use(route_id, calendar_id)

            print_log(f"Exporting trips: {trip_id}")

            # Ignore non-parsed routes and non_active calendars
            if operator not in self.operators or service_id is None:
                continue

            if route_id not in self.parsed_routes:
                print_log("no route for pattern {pattern_id}", 1)
                continue

            # Ignore one-stop trips
            if len(trip["odpt:busTimetableObject"]) <= 1:
                continue

            # Bus headsign
            headsigns = [
                i["odpt:destinationSign"] for i in trip["odpt:busTimetableObject"] \
                if i.get("odpt:destinationSign") is not None
            ]

            last_stop_id = trip["odpt:busTimetableObject"][-1]["odpt:busstopPole"].split(":")[1]
            
            if headsigns:
                trip_headsign = headsigns[0]
                
                # Exclude "bound for" from the headsign
                trip_headsign = re.sub(r"(行|行き|ゆき)$", "", trip_headsign)

            else:

                if last_stop_id in self.stop_names:
                    trip_headsign = self.stop_names[last_stop_id]


                else:
                    trip_headsign = ""
                    print_log(f"no name for stop {last_stop_id}", 1)

            # Non-step bus (wheelchair accesibility)
            nonstep_values = [
                i.get("odpt:isNonStepBus") for i in trip["odpt:busTimetableObject"]
            ]

            if any((i is False for i in nonstep_values)):
                wheelchair = "2"

            elif any((i is True for i in nonstep_values)):
                wheelchair = "1"

            else:
                wheelchair = "0"

            # Do we start after midnight?
            prev_departure = TimeValue(0)
            if trip["odpt:busTimetableObject"][0].get("odpt:isMidnight", False):
                first_time = trip["odpt:busTimetableObject"][0].get("odpt:departureTime") or \
                             trip["odpt:busTimetableObject"][0].get("odpt:arrivalTime")
                
                # If that's a night bus, and the trip starts before 6 AM
                # Add 24h to departure, as the trip starts "after-midnight"
                if int(first_time.split(":")[0]) < 6:
                    prev_departure = TimeValue(86400)

            # Sort timetable entries
            trip["odpt:busTimetableObject"] = sorted(
                trip["odpt:busTimetableObject"],
                key=lambda i: i["odpt:index"]
            )

            # Filter to only include valid stops
            trip["odpt:busTimetableObject"] = [
                i for i in trip["odpt:busTimetableObject"]
                if i["odpt:busstopPole"].split(":")[1] in self.valid_stops
            ]

            # Ignore trips with less then 1 stop
            if len(trip["odpt:busTimetableObject"]) <= 1:
                continue

            # Write to trips.txt
            writer_trips.writerow({
                "route_id": route_id, "trip_id": trip_id,
                "service_id": service_id, "trip_headsign": trip_headsign,
                "trip_pattern_id": pattern_id, "wheelchair_accessible": wheelchair,
            })

            # Times
            for idx, stop_time in enumerate(trip["odpt:busTimetableObject"]):
                stop_id = stop_time["odpt:busstopPole"].split(":")[1]

                # Get time
                arrival = stop_time.get("odpt:arrivalTime")
                departure = stop_time.get("odpt:departureTime")

                # Fallback to other time values
                arrival = arrival or departure
                departure = departure or arrival

                if arrival: arrival = TimeValue.from_str(arrival)
                if departure: departure = TimeValue.from_str(departure)

                # Be sure arrival and departure exist
                if not (arrival and departure): continue

                # Fix for after-midnight trips. GTFS requires "24:23", while ODPT data contains "00:23"
                if arrival < prev_departure: arrival += 86400
                if departure < arrival: departure += 86400
                prev_departure = copy(departure)

                # Can get on/off?
                # None → no info → fallbacks to True
                pickup = "1" if stop_time.get("odpt:CanGetOn") is False else "0"
                dropoff = "1" if stop_time.get("odpt:CanGetOff") is False else "0"

                writer_times.writerow({
                    "trip_id": trip_id, "stop_sequence": idx, "stop_id": stop_id,
                    "arrival_time": str(arrival), "departure_time": str(departure),
                    "pickup_type": pickup, "drop_off_type": dropoff,
                })

        buffer_trips.close()
        buffer_times.close()

    def trips_calendars_check(self):
        """Check trips against used calendars"""
        remove_trips = set()

        ### FIX TRIPS.TXT ###
        print_log("Checking trips against calendars: trips.txt")
        os.rename("gtfs/trips.txt", "gtfs/trips.txt.old")

        # Old file
        in_buffer = open("gtfs/trips.txt.old", mode="r", encoding="utf8", newline="")
        reader = csv.DictReader(in_buffer)

        # New file
        out_buffer = open("gtfs/trips.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(out_buffer, GTFS_HEADERS_BUS["trips.txt"])
        writer.writeheader()

        for row in reader:
            if self.calendars.was_exported(row["service_id"]):
                writer.writerow(row)
            else:
                remove_trips.add(row["trip_id"])

        in_buffer.close()
        out_buffer.close()

        os.remove("gtfs/trips.txt.old")

        ### FIX STOP_TIMES.TXT ###
        print_log("Checking trips against calendars: stop_times.txt")
        os.rename("gtfs/stop_times.txt", "gtfs/stop_times.txt.old")

        # Old file
        in_buffer = open("gtfs/stop_times.txt.old", mode="r", encoding="utf8", newline="")
        reader = csv.DictReader(in_buffer)

        # New file
        out_buffer = open("gtfs/stop_times.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(out_buffer, GTFS_HEADERS_BUS["stop_times.txt"])
        writer.writeheader()

        for row in reader:
            if row["trip_id"] in remove_trips:
                continue
            else:
                writer.writerow(row)

        in_buffer.close()
        out_buffer.close()

        os.remove("gtfs/stop_times.txt.old")

    @classmethod
    def parse(cls, apikey, target_file="tokyo_buses.zip"):
        """Automatically create Tokyo trains GTFS.
        
        :param apikey: Key to the ODPT API
        :type apikey: str

        :param target_file: Path to the result GTFS archive,
                            defaults to tokyo_trains.zip
        :type target_file: str or path-like or file-like
        """
        clear_directory("gtfs")

        self = cls(apikey)

        self.load_operators()

        self.agencies()
        self.feed_info()
        self.attributions()
        self.stops()
        self.routes()
        self.trips()
        self.calendars.export()
        self.trips_calendars_check()

        compress(target_file)
