from collections import OrderedDict
from datetime import datetime
from copy import copy
import pytz
import csv
import re
import os

from .handlers import TimeValue, ApiHandler, CalendarHandler, \
                      TranslationHandler, TrainTripIterator

from .utils import distance, text_color, clear_directory, \
                   timetable_item_station, compress, print_log

from .const import GTFS_HEADERS_TRAIN, SEPARATE_STOPS
from .err import DataAssertion

class TrainParser:
    """Object responsible for parsing train data
    
    :param apikey: Key to the ODPT API
    :type apikey: str
    """

    def __init__(self, apikey):
        """Init the TrainParser"""
        self.apikey = apikey
        self.api = ApiHandler(self.apikey)

        # Set-up separate handlers
        self.today = datetime.now(pytz.timezone("Asia/Tokyo"))

        self.translations = TranslationHandler()
        self.calendars = CalendarHandler(self.api, self.today.date())

        # Route & Operator data
        self.operators = []
        self.route_data = OrderedDict()

        # Trips data
        self.train_blocks = {}
        self.train_directions = {}
        self.train_types = {}

        # Stops data
        self.station_positions = {}
        self.station_names = {}
        self.valid_stops = set()

        # Postprocess data
        self.remove_trips = set()

    def load_routes(self):
        """Load route data from data/train_routes.csv"""

        print_log("Loading data from train_routes.csv")

        with open("data/train_routes.csv", mode="r", encoding="utf8", newline="") as buffer:
            reader = csv.DictReader(buffer)
            for row in reader:

                # Only save info on routes which will have timetables data available
                if not row["train_timetable_available"] == "1":
                    continue

                self.route_data[row["route_id"]] = row
                
                if row["operator"] not in self.operators:
                    self.operators.append(row["operator"])

    def load_train_types(self):
        """Return a dictionary mapping each TrainType to
        a tuple (japanese_name, english_name)"""

        ttypes = self.api.get("TrainType")

        for ttype in ttypes:
            ja_name = ttype["dc:title"]

            if ttype.get("odpt:trainTypeTitle", {}).get("en", ""):
                en_name = ttype.get("odpt:trainTypeTitle", {}).get("en", "")
            else:
                en_name = self.translations.get_english(ttype["dc:title"], True)

            self.train_types[ttype["owl:sameAs"].split(":")[1]] = (ja_name, en_name)

    def load_train_directions(self):
        """Save a dictionary mapping RailDirection to
        its name to self.train_directions"""
        tdirs = self.api.get("RailDirection")
       
        for i in tdirs:
            self.train_directions[i["owl:sameAs"]] = i["dc:title"]

    def get_stop_name(self, stop_id):
        """Returns the name of station with the provided id"""
        saved_name = self.station_names.get(stop_id)

        if saved_name is not None:
            return saved_name
        
        else:
            name = re.sub(r"(?!^)([A-Z][a-z]+)", r" \1", stop_id.split(".")[-1])
            self.station_names[stop_id] = name
            print_log(f"no name for stop {stop_id}", 1)
            return name

    @staticmethod
    def get_train_name(names, lang):
        """Returns the train_name of given the names and language"""
        if type(names) is dict: names = [names]

        sep = "・" if lang == "ja" else " / "
        name = sep.join([i[lang] for i in names if i.get(lang)])

        return name

    def get_train_headsigns(self, route_id, trip, destinations, direction):
        """Returns the trip_headsign given all trip data"""
        destination = "・".join(destinations)
        destination_en = " / ".join([self.translations.get_english(i) for i in destinations])
        trip_id = trip["owl:sameAs"].split(":")[1]

        if route_id == "JR-East.Yamanote":
            # Special case - JR-East.Yamanote line
            # Here, we include the direction_name, as it's important to users
            if direction == "内回り" and not trip.get("odpt:nextTrainTimetable"):
                trip_headsign = f"（内回り）{destination}"
                trip_headsign_en = f"(Inner Loop ⟲) {destination_en}"

            if direction == "外回り" and not trip.get("odpt:nextTrainTimetable"):
                trip_headsign = f"（外回り）{destination}"
                trip_headsign_en = f"(Outer Loop ⟳) {destination_en}"

            elif direction == "内回り":
                trip_headsign = "内回り"
                trip_headsign_en = "Inner Loop ⟲"

            elif direction == "外回り":
                trip_headsign = "外回り"
                trip_headsign_en = "Outer Loop ⟳"

            else:
                raise DataAssertion(
                    "error while creating headsign of JR-East.Yamanote line " \
                    f"train {trip_id}. please report this issue on GitHub."
                )

        else:
            trip_headsign = destination
            trip_headsign_en = destination_en

            trip_type_id = trip.get("odpt:trainType", ":").split(":")[1]
            trip_type, trip_type_en = self.train_types.get(trip_type_id, ("", ""))

            if trip_type:
                trip_headsign = f"（{trip_type}）{destination}"
                
                if trip_headsign_en and trip_type_en:
                    trip_headsign_en = f"({trip_type_en}) {destination_en}"
                
                else:
                    trip_headsign_en = None

        if trip_headsign_en is not None:
            self.translations.set_english(trip_headsign, trip_headsign_en)

        return trip_headsign

    def agencies(self):
        """Load data from operators.csv and export them to agencies.txt"""
        buffer = open("gtfs/agency.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(buffer, GTFS_HEADERS_TRAIN["agency.txt"], extrasaction="ignore")
        writer.writeheader()

        print_log("Loading data from operators.csv")

        with open("data/operators.csv", mode="r", encoding="utf8", newline="") as add_info_buff:
            additional_info = {i["operator"]: i for i in csv.DictReader(add_info_buff)}

        print_log("Exporting agencies")

        for operator in self.operators:
            # Get data from operators.csv
            operator_data = additional_info.get(operator, {})
            if not operator_data:
                print_log(f"no data defined for operator {operator}", 1)

            # Translations
            if "name_en" in operator_data:
                self.translations.set_english(operator_data["name"], operator_data["name_en"])

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
        stops = self.api.get("Station")

        # Load fixed positions
        print_log("loading train_stations_fixes.csv")
        position_fixer = {}

        with open("data/train_stations_fixes.csv", mode="r", encoding="utf8", newline="") as f:
            for row in csv.DictReader(f):
                position_fixer[row["id"]] = (row["lat"], row["lon"])

        # Open files
        print_log("Exporting stops")

        buffer = open("gtfs/stops.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(buffer, GTFS_HEADERS_TRAIN["stops.txt"])
        writer.writeheader()

        broken_stops_buff = open("broken_stops.csv", mode="w", encoding="utf8", newline="")
        broken_stops_wrtr = csv.writer(broken_stops_buff)
        broken_stops_wrtr.writerow(["stop_id", "stop_name", "stop_name_en", "stop_code"])

        # Iterate over stops
        for stop in stops:
            stop_id = stop["owl:sameAs"].split(":")[1]
            stop_code = stop.get("odpt:stationCode", "").replace("-", "")
            stop_name = stop["dc:title"]
            stop_name_en = stop.get("odpt:stationTitle", {}).get("en", "")
            stop_lat, stop_lon = None, None

            self.station_names[stop_id] = stop_name

            # Stop name translation
            if stop_name_en:
                self.translations.set_english(stop_name, stop_name_en)

            # Ignore stops that belong to ignored routes
            if stop["odpt:railway"].split(":")[1] not in self.route_data:
                continue

            print_log(f"Exporting stops: {stop_id}")

            # Stop Position
            stop_lat, stop_lon = position_fixer.get(
                stop_id,
                (stop.get("geo:lat"), stop.get("geo:long"))
            )

            # Output to GTFS or to incorrect stops
            if stop_lat and stop_lon:
                self.valid_stops.add(stop_id)
                self.station_positions[stop_id] = (float(stop_lat), float(stop_lon))
                writer.writerow({
                    "stop_id": stop_id, "stop_code": stop_code, "stop_name": stop_name,
                    "stop_lat": stop_lat, "stop_lon": stop_lon, "location_type": 0,
                })

            else:
                broken_stops_wrtr.writerow([stop_id, stop_name, stop_name_en, stop_code])

        buffer.close()
        broken_stops_buff.close()

    def routes(self):
        """Parse routes and export them to routes.txt"""
        routes = self.api.get("Railway")

        print_log("Exporting routes")

        buffer = open("gtfs/routes.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(buffer, GTFS_HEADERS_TRAIN["routes.txt"], extrasaction="ignore")
        writer.writeheader()

        for route in routes:
            route_id = route["owl:sameAs"].split(":")[1]
            
            if route_id not in self.route_data:
                continue

            print_log(f"Exporting routes: {route_id}")

            # Get color from train_routes.csv
            route_info = self.route_data[route_id]
            operator = route_info["operator"]
            route_color = route_info["route_color"].upper()
            route_text = text_color(route_color)

            # Translation
            self.translations.set_english(route_info["route_name"], route_info["route_en_name"])

            # Stops
            self.route_data[route_id]["stops"] = [
                stop["odpt:station"].split(":")[1] for stop in sorted(route["odpt:stationOrder"],
                key=lambda i: i["odpt:index"])
            ]

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

        trains = TrainTripIterator(self.api, self.today)

        self.train_blocks = {}

        through_trains = {}
        undef_trains = {}
        block_id = 0

        ### LOAD THROUGH TRAIN DATA ###
        for trip in trains:
            trip_id = trip["owl:sameAs"].split(":")[1]

            print_log(f"Loading trip to block solver: {trip_id}")

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
                raise DataAssertion(
                    f"{trip_id} has more then 2 destinations and origins - "
                    "no idea how to map this to GTFS blocks"
                )

            if len(next_trains) > 1 and len(prev_trains) > 1:
                raise DataAssertion(
                    f"{trip_id} has more then 2 prevTimetables and nextTrains - "
                    "no idea how to map this to GTFS blocks"
                )

            through_trains[trip_id] = {
                "previous"   : prev_trains,
                "next"       : next_trains,
                "destination": destinations,
            }

        ### SOLVE TRAIN BLOCKS ###
        while through_trains:
            trains_in_block = {}
            block_id += 1

            # First Trip
            main_trip, main_tripdata = through_trains.popitem()
            trip, data = main_trip, main_tripdata
            trains_in_block[trip] = data

            fetch_trips = set(data["previous"]).union(data["next"])
            failed_fetch = False

            print_log(f"Solving block for: {main_trip}")

            # Save all connected trips into trains_in_block
            while fetch_trips:

                trip = fetch_trips.pop()
                try:
                    data = through_trains.pop(trip)
                except:
                    try:
                        data = undef_trains.pop(trip)
                    except:
                        print_log(
                            f"following through services for {main_trip}\n" +
                            " "*8 + f"reaches {trip}, " +
                            "which is already used in a different block",
                            severity=1
                        )

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
                raise DataAssertion(
                    "the below block of trips both splits and merges - " \
                    "handling this logic is not implented." \
                    f"Here's the list of trains in this block: {list(trains_in_block.keys())}"
                )

            ### SPLITTING TRAIN ###
            elif max_splits > 1:
                split_train_id = [i[0] for i in trains_in_block.items() if len(i[1]["next"]) > 1]

                if len(split_train_id) > 1:
                    raise DataAssertion(
                        "encountered a block of trains with more then 2 splits -" \
                        "handling this logic is not implemented."
                        f"Here's the list of trains in this block: {list(trains_in_block.keys())}"
                    )

                elif len(split_train_id) == 0:
                    raise RuntimeError(
                        "it's impossible to get here. "
                        "if you see this error call an exorcist."
                    )

                split_train_id = split_train_id[0]
                split_train_data = trains_in_block.pop(split_train_id)

                for train_after_split in split_train_data["next"]:

                    next_train_id = train_after_split
                    next_train_data = trains_in_block.pop(next_train_id)

                    destination = next_train_data["destination"]

                    if not destination:
                        raise DataAssertion(
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
                    raise DataAssertion(
                        "encountered a block of trains with more then 2 merges -" \
                        "handling this logic is not implemented."
                        f"Here's the list of trains in this block: {list(trains_in_block.keys())}"
                    )

                elif len(merge_train_id) == 0:
                    raise RuntimeError(
                        "it's impossible to get here. "
                        "if you see this error call an exorcist."
                    )

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
        self.load_train_directions()
        self.load_train_types()
        main_directions = {}

        # Get all trips
        print_log("Parsing trips")
        trips = TrainTripIterator(self.api, self.today)

        # Open GTFS trips
        buffer_trips = open("gtfs/trips.txt", mode="w", encoding="utf8", newline="")
        writer_trips = csv.DictWriter(buffer_trips, GTFS_HEADERS_TRAIN["trips.txt"])
        writer_trips.writeheader()

        buffer_times = open("gtfs/stop_times.txt", mode="w", encoding="utf8", newline="")
        writer_times = csv.DictWriter(buffer_times, GTFS_HEADERS_TRAIN["stop_times.txt"])
        writer_times.writeheader()

        # Iteratr over trips
        for trip in trips:
            route_id = trip["odpt:railway"].split(":")[1]
            trip_id = trip["owl:sameAs"].split(":")[1]
            calendar = trip["odpt:calendar"].split(":")[1]
            train_rt_id = trip["odpt:train"].split(":")[1] if "odpt:train" in trip else ""

            print_log(f"Parsing trips: {trip_id}")

            # Ignore ignored routes and non_active calendars
            if route_id not in self.route_data:
                continue

            # Add calendar
            service_id = self.calendars.use(route_id, calendar)
            if service_id is None:
                continue

            # Destination staion
            if trip.get("odpt:destinationStation") not in ["", None]:
                destination_stations = [
                    self.get_stop_name(i.split(":")[1]) for i in trip["odpt:destinationStation"]
                ]

            else:
                destination_stations = [
                    self.get_stop_name(timetable_item_station(trip["odpt:trainTimetableObject"][-1]))
                ]

            # Block
            block_id = self.train_blocks.get(trip_id, "")

            # Ignore one-stop that are not part of a block
            if len(trip["odpt:trainTimetableObject"]) < 2 and block_id == "":
                continue

            # Rail Direction
            if route_id not in main_directions:
                main_directions[route_id] = trip["odpt:railDirection"]
            direction_name = self.train_directions.get(trip["odpt:railDirection"], "")
            direction_id = 0 if trip["odpt:railDirection"] == main_directions[route_id] else 1

            # Train name
            trip_short_name = trip["odpt:trainNumber"]
            train_names = trip.get("odpt:trainName", {})

            train_name = self.get_train_name(train_names, "ja")
            train_name_en = self.get_train_name(train_names, "en")

            if train_name:
                trip_short_name = trip_short_name + " " + train_name
                
                if train_name_en:
                    trip_short_name_en = trip["odpt:trainNumber"] + " " + train_name_en
                    self.translations.set_english(trip_short_name, trip_short_name_en)

            # Headsign
            trip_headsign = self.get_train_headsigns(route_id, trip,
                                           destination_stations, direction_name)

            # TODO: N'EX route creator
            #tofrom_narita_airport = lambda i: "odpt.Station:JR-East.NaritaAirportBranch.NaritaAirportTerminal1" in i.get("odpt:originStation", []) or \
            #                                  "odpt.Station:JR-East.NaritaAirportBranch.NaritaAirportTerminal1" in i.get("odpt:destinationStation", [])

            #if rotue_id.startswith("JR-East") and trip_type == "特急" and tofrom_narita_airport(trip):
            #    route_id = "JR-East.NaritaExpress"

            # Times
            times = []
            prev_departure = TimeValue(0)
            for idx, stop_time in enumerate(trip["odpt:trainTimetableObject"]):
                stop_id = timetable_item_station(stop_time)
                platform = stop_time.get("odpt:platformNumber", "")

                if stop_id not in self.valid_stops:
                    print_log(f"reference to a non-existing stop, {stop_id}", 1)
                    continue

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
                for new_block_id, new_dest_ids in block_id:
                    new_trip_id = f"{trip_id}.Block{new_block_id}"

                    if new_dest_ids:
                        new_dest_stations = [self.get_stop_name(i) for i in new_dest_ids]

                        new_headsign = self.get_train_headsigns(
                            route_id, trip,
                            new_dest_stations, direction_name
                        )

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

    def stops_postprocess(self):
        """Process stops and create parent nodes for stations"""
        stops = OrderedDict()
        names = {}
        avg = lambda i: round(sum(i)/len(i), 8)

        # Read file
        print_log("Merging stations: loading")

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
                    if distance(saved_location, row_location) <= 1:
                        close_enough = True

                    else:
                        close_enough = False

            stops[stop_id_wsuffix].append({
                "id": row["stop_id"], "code": row["stop_code"],
                "lat": float(row["stop_lat"]), "lon": float(row["stop_lon"])
            })

        buffer.close()

        # Write new stops.txt
        print_log("Merging stations: exporting")

        buffer = open("gtfs/stops.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(buffer, GTFS_HEADERS_TRAIN["stops.txt"], extrasaction="ignore")
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

    def trips_postprocess(self):
        """Process trips.txt and remove trips with inactive services"""
        print_log("Processing trips")
        os.rename("gtfs/trips.txt", "gtfs/trips.txt.old")

        # Old file
        in_buffer = open("gtfs/trips.txt.old", mode="r", encoding="utf8", newline="")
        reader = csv.DictReader(in_buffer)

        # New file
        out_buffer = open("gtfs/trips.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(out_buffer, GTFS_HEADERS_TRAIN["trips.txt"], extrasaction="ignore")
        writer.writeheader()

        for row in reader:
            if self.calendars.was_exported(row["service_id"]):
                writer.writerow(row)

            else:
                self.remove_trips.add(row["trip_id"])

        in_buffer.close()
        out_buffer.close()

        os.remove("gtfs/trips.txt.old")

    def times_postprocess(self):
        """Process stop_times.txt and remove trips with inactive services.
        Has to be called after calling trainparser.trips_postprocess()"""
        print_log("Processing stop_times.txt")
        os.rename("gtfs/stop_times.txt", "gtfs/stop_times.txt.old")

        # Old file
        in_buffer = open("gtfs/stop_times.txt.old", mode="r", encoding="utf8", newline="")
        reader = csv.DictReader(in_buffer)

        # New file
        out_buffer = open("gtfs/stop_times.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(out_buffer, GTFS_HEADERS_TRAIN["stop_times.txt"], extrasaction="ignore")
        writer.writeheader()

        for row in reader:
            if row["trip_id"] in self.remove_trips:
                continue

            else:
                writer.writerow(row)

        in_buffer.close()
        out_buffer.close()

        os.remove("gtfs/stop_times.txt.old")

    @classmethod
    def parse(cls, apikey, target_file="tokyo_trains.zip"):
        """Automatically create Tokyo trains GTFS.
        
        :param apikey: Key to the ODPT API
        :type apikey: str

        :param target_file: Path to the result GTFS archive,
                            defaults to tokyo_trains.zip
        :type target_file: str or path-like or file-like
        """
        clear_directory("gtfs")

        self = cls(apikey)

        self.load_routes()

        self.agencies()
        self.feed_info()
        self.attributions()
        self.stops()
        self.routes()
        self.blocks()
        self.trips()
        self.translations.export()
        self.calendars.export()
        self.stops_postprocess()
        self.trips_postprocess()
        self.times_postprocess()

        compress(target_file)
