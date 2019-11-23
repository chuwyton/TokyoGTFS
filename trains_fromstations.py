from warnings import warn
import argparse
import requests
import shutil
import ijson
import time
import csv
import os
import re

# {
#     "owl:sameAs": "odpt.StationTimetable:Tobu.TobuUrbanPark.Atago.Inbound.SaturdayHoliday",
#     "odpt:railway": "odpt.Railway:Tobu.TobuUrbanPark",
#     "odpt:station": "odpt.Station:Tobu.TobuUrbanPark.Atago",
#     "odpt:calendar": "odpt.Calendar:SaturdayHoliday",
#     "odpt:operator": "odpt.Operator:Tobu",
#     "odpt:railDirection": "odpt.RailDirection:Inbound",
#     "odpt:stationTimetableObject": [
#       {
#         "odpt:trainType": "odpt.TrainType:Tobu.Local",
#         "odpt:departureTime": "05:20",
#         "odpt:destinationStation": [
#           "odpt.Station:Tobu.TobuUrbanPark.Omiya"
#         ]
#       }
#      ]
# }

def _safe_path_part(path_part):
    if "/" in path_part or "\\" in path_part:
        raise ValueError("incorrect path_part: {}".format(path_part))

    else:
        return path_part

def _train_name(names, lang):
    if type(names) is dict: names = [names]

    sep = "・" if lang == "ja" else " / "
    name = sep.join([i[lang] for i in names if i.get(lang)])

    return name

class _Time:
    "Represent a time value"
    def __init__(self, seconds):
        self.m, self.s = divmod(int(seconds), 60)
        self.h, self.m = divmod(self.m, 60)

    def __str__(self):
        "Return GTFS-compliant string representation of time"
        return "{:0>2}:{:0>2}:{:0>2}".format(self.h, self.m, self.s)

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
        str_split = [int(i) for i in string.split(":")]
        if len(str_split) == 2:
            return cls(str_split[0]*3600 + str_split[1]*60)
        elif len(str_split) == 3:
            return cls(str_split[0]*3600 + str_split[1]*60 + str_split[2])
        else:
            raise ValueError("invalid string for _Time.from_str(), {} (should be HH:MM or HH:MM:SS)".format(string))

class StationTablesToTrains:
    def __init__(self, apikey):
        self.apikey = apikey
        self.verbose = True

        # Clear the temp directory
        if os.path.exists("temp"):
            shutil.rmtree("temp")

        # Get info on which routes to parse
        self.routes = {}
        with open("data/train_routes.csv", mode="r", encoding="utf8", newline="") as buffer:
            reader = csv.DictReader(buffer)
            for row in reader:

                # Ignore routes with TrainTimetables available
                if row["train_timetable_available"] == "1" or row["route_code"] == "N'EX":
                    continue

                self.routes[row["route_id"]] = row

        # Add station list for every route
        self._load_routes_stations()

    def _load_routes_stations(self):
        routes_req = requests.get("https://api-tokyochallenge.odpt.org/api/v4/odpt:Railway.json", params={"acl:consumerKey": self.apikey}, timeout=30, stream=True)
        routes_req.raise_for_status()
        routes = ijson.items(routes_req.raw, "item")

        for route in routes:

            route_id = route["owl:sameAs"].split(":")[1]

            # We only need station list of parsed routes
            if route_id not in self.routes: continue

            station_list = sorted(route["odpt:stationOrder"], key=lambda i: i["odpt:index"])
            station_list = [i["odpt:station"].split(":")[1] for i in station_list]

            assert station_list, "route {} has an empty station list!".format(route_id)

            # Check if we have the AscendingOrder or DescedingOrder provided
            if "odpt:ascendingRailDirection" in route and "odpt:descendingRailDirection" in route:
                up_dir = route["odpt:ascendingRailDirection"].split(":")[1]
                down_dir = route["odpt:descendingRailDirection"].split(":")[1]

                self.routes[route_id]["stations:" + up_dir] = station_list.copy()

                self.routes[route_id]["stations:" + down_dir] = station_list.copy()
                self.routes[route_id]["stations:" + down_dir].reverse()

            self.routes[route_id]["stations"] = station_list

        routes_req.close()

    def get_tables(self):
        tables_req = requests.get("https://api-tokyochallenge.odpt.org/api/v4/odpt:StationTimetable.json", params={"acl:consumerKey": self.apikey}, timeout=30, stream=True)
        tables_req.raise_for_status()
        tables = ijson.items(tables_req.raw, "item")

        for table in tables:

            table_id = table["owl:sameAs"].split(":")[1]
            route = table["odpt:railway"].split(":")[1]

            # Check if we want this route
            if route not in self.routes: continue

            # Direction, station and calendar has to be defined
            # This data is listed as "optional" by ODPT
            if "odpt:calendar" not in table:
                warn("StationTimetable {} has no calendar!".format(table_id))
                continue

            if "odpt:railDirection" not in table:
                warn("StationTimetable {} has no railDirection!".format(table_id))
                continue

            if "odpt:station" not in table:
                warn("StationTimetable {} has no station!".format(table_id))
                continue

            if self.verbose: print("\033[1A\033[K" + "Downloading StationTimetable:", table_id)

            direction = table["odpt:railDirection"].split(":")[1]
            calendar = table["odpt:calendar"].split(":")[1]
            station = table["odpt:station"].split(":")[1]

            # Save the table as CSV
            # temp / route_id / direction / calendar / station.json
            path_parts = ["temp", route, direction, calendar, station+".csv"]
            path_parts = [_safe_path_part(i) for i in path_parts]

            # Make target directory
            os.makedirs(os.path.join(*path_parts[:-1]), exist_ok=True)

            # Write station to CSV
            with open(os.path.join(*path_parts), mode="w", encoding="utf-8", newline="") as f:
                w = csv.writer(f)

                w.writerow([
                    "arrival", "departure", "type",
                    "destination", "name_ja", "name_en"
                ])

                previous_time = _Time(0)

                for train in table["odpt:stationTimetableObject"]:

                    arr = train.get("odpt:arrivalTime", "") or train.get("odpt:departureTime", "")
                    dep = train.get("odpt:departureTime", "") or train.get("odpt:arrivalTime", "")

                    assert arr and dep, "some departures in StationTimetable {} do not have any time defined!".format(table_id)

                    arr, dep = map(_Time.from_str, [arr, dep])

                    # Fix for trains breaking through midnight
                    # Basically, if any time value is smaller then the previous value,
                    # Adds 24 hours to the _Time value
                    if arr < previous_time: arr += 86_400
                    if dep < arr: dep += 86_400

                    previous_time = dep

                    name_ja = _train_name(train.get("odpt:trainName", []), "ja")
                    name_en = _train_name(train.get("odpt:trainName", []), "en")

                    train_type = train["odpt:trainType"].split(":")[1]

                    # odpt:destinationStation should be an array
                    if type(train["odpt:destinationStation"]) == str:
                        train["odpt:destinationStation"] = [train["odpt:destinationStation"]]

                    destinations = ";".join([i.split(":")[1] for i in train["odpt:destinationStation"]])

                    w.writerow([
                        arr, dep, train_type,
                        destinations, name_ja, name_en
                    ])

        tables_req.close()

    def transpose_tables(self):

        for route_id in os.listdir("temp"):

            directions = os.listdir(os.path.join("temp", route_id))

            assert len(directions) == 2, "route {} does not have 2 directions!".format(route_id)

            for direction in directions:

                for calendar in os.listdir(os.path.join("temp", route_id, direction)):

                    if self.verbose: print("\033[1A\033[K" + "Transposing: {}.{}.{}".format(route_id, direction, calendar))


                    # If direction was provided in routes data, then just copy the station list
                    if ("stations:" + direction) in self.routes[route_id]:
                        station_order = self.routes[route_id]["stations:" + direction]

                    # Else, try to auto-detect the direction
                    else:
                        # Detect if we traverse the self.routes[route_id]["stations"] forward or backwards
                        # Fortunately (usually) there are no stationtimetables for arrival-only station - for last stations

                        first_station_exists = os.path.exists(os.path.join("temp", route_id, direction, calendar, self.routes[route_id]["stations"][0] + ".csv"))
                        last_station_exists = os.path.exists(os.path.join("temp", route_id, direction, calendar, self.routes[route_id]["stations"][-1] + ".csv"))

                        if first_station_exists and last_station_exists:
                            raise RuntimeError("{}/{}/{} has both first and last stations of station list — unable to detect the direction!".format(route_id, direction, calendar))

                        elif first_station_exists:
                            station_order = self.routes[route_id]["stations"].copy()

                        elif last_station_exists:
                            station_order = self.routes[route_id]["stations"].copy()
                            station_order.reverse()

                        else:
                            raise RuntimeError("{}/{}/{} has neither the first or the last stations of station list — unable to detect the direction!".format(route_id, direction, calendar))

                        del first_station_exists, last_station_exists

                    # Transpose ("rotate") station tables into train timetables
                    # Tries to avoid timetravel by picking the next train matching metadata
                    #
                    # |   Sta. A    |   Sta. B    |   Sta. C    |
                    # |-------------|-------------|-------------|
                    # | Local 05:12 | Local 05:15 | Rapid 05:24 |
                    # | Rapid 05:16 | Local 05:23 | Local 05:25 |
                    # | Local 05:20 | Local 05:27 | Local 05:33 |
                    # | Local 05:24 |             | Rapid 05:36 |
                    # | Rapid 05:28 |             | Local 05:37 |
                    #                      ↓
                    # |        | Local | Rapid | Local | Local | Rapid |
                    # |--------|-------|-------|-------|-------|-------|
                    # | Sta. A | 05:12 | 05:16 | 05:20 | 05:24 | 05:28 |
                    # | Sta. B | 05:15 |   -   | 05:23 | 05:27 |   -   |
                    # | Sta. C | 05:25 | 05:24 | 05:33 | 05:37 | 05:36 |

                    # trains[i] = {
                    #   "name_ja": str, "name_en": str,
                    #   "type": str
                    #   "destination": str,
                    #   "stations": [{"sta": str, "arr": _Time, "dep": _Time}, …],
                    #   "finished": bool
                    # }

                    trains = []

                    # Ignore last station when iterating, there are
                    # no StationTimetable published for last station.
                    for station in station_order:

                        if self.verbose: print("\033[1A\033[K" + "Transposing: {}.{}.{}, station {}".format(route_id, direction, calendar, station))

                        station_file = os.path.join("temp", route_id, direction, calendar, station + ".csv")

                        if os.path.exists(station_file):
                            with open(station_file, mode="r", encoding="utf8", newline="") as f:
                                reader = csv.DictReader(f)

                                # First: mark any train that has its destination == current station as "finished"
                                for idx, train in enumerate(trains):
                                    if train["destination"] == station:
                                        trains[idx]["finished"] = True

                                        trains[idx]["stations"].append({
                                            "sta": station,
                                            "arr": "tbd", "dep": "tbd"
                                        })

                                # Second: match any departures to existing trains
                                for departure in reader:

                                    departure["arrival"] = _Time.from_str(departure["arrival"])
                                    departure["departure"] = _Time.from_str(departure["departure"])

                                    # Train with a name can only match to a train with the same name
                                    if departure["name_ja"]:

                                        # Only trains with the same name,
                                        # trains that don't already stop at this station
                                        # and trains that won't travel back in time
                                        train_isok = lambda i: (not i["finished"])                  and \
                                                               i["stations"][-1]["sta"] != station  and \
                                                               i["name_ja"] == departure["name_ja"] and \
                                                               i["stations"][-1]["dep"] < departure["arrival"]

                                        # We have to store the original index inside `trains`
                                        # in order to append the current departure
                                        matching_trains = [(i, j) for (i, j) in enumerate(trains) if train_isok(j)]

                                        # Sort the matching_trains by last departure time.
                                        # We match with the fastest train
                                        matching_trains = sorted(matching_trains, key=lambda i: i[1]["stations"][-1]["dep"], reverse=True)

                                        if matching_trains:
                                            idx, train = matching_trains[0]
                                            trains[idx]["stations"].append({
                                                "sta": station,
                                                "arr": departure["arrival"],
                                                "dep": departure["departure"]
                                            })

                                        else:
                                            trains.append({
                                                "name_ja": departure["name_ja"],
                                                "name_en": departure["name_en"],
                                                "type": departure["type"],
                                                "destination": departure["destination"],
                                                "finished": False,
                                                "stations": [{
                                                    "sta": station,
                                                    "arr": departure["arrival"],
                                                    "dep": departure["departure"]
                                                }]
                                            })


                                    # Other trains are matched by type & destination
                                    else:

                                        # Only trains with the same type & destination,
                                        # trains that don't already stop at this station
                                        # and trains that won't travel back in time
                                        # can be macthed
                                        train_isok = lambda i: (not i["finished"])                          and \
                                                               i["stations"][-1]["sta"] != station          and \
                                                               i["type"] == departure["type"]               and \
                                                               i["destination"] == departure["destination"] and \
                                                               i["stations"][-1]["dep"] < departure["arrival"]

                                        # We have to store the original index inside `trains`
                                        # in order to append the current departure
                                        matching_trains = [(i, j) for (i, j) in enumerate(trains) if train_isok(j)]

                                        # Sort the matching_trains by last departure time.
                                        # We match with the fastest train
                                        matching_trains = sorted(matching_trains, key=lambda i: i[1]["stations"][-1]["dep"], reverse=True)

                                        if matching_trains:
                                            idx, train = matching_trains[0]
                                            trains[idx]["stations"].append({
                                                "sta": station,
                                                "arr": departure["arrival"],
                                                "dep": departure["departure"]
                                            })

                                        else:
                                            trains.append({
                                                "name_ja": departure["name_ja"],
                                                "name_en": departure["name_en"],
                                                "type": departure["type"],
                                                "destination": departure["destination"],
                                                "finished": False,
                                                "stations": [{
                                                    "sta": station,
                                                    "arr": departure["arrival"],
                                                    "dep": departure["departure"]
                                                }]
                                            })

                        # Add tbd;tbd timestamp to last stations, sometimes they're not provided
                        elif station == station_order[-1]:
                            for idx, train in enumerate(trains):
                                if train["destination"] == station:
                                    trains[idx]["finished"] = True

                                    trains[idx]["stations"].append({
                                        "sta": station,
                                        "arr": "tbd", "dep": "tbd"
                                    })

                        else:
                            raise RuntimeError("{}/{}/{} misses a file for station {}".format(route_id, direction, calendar, station))

                    # At the end: export the table

                    if self.verbose: print("\033[1A\033[K" + "Transposing: {}.{}.{}: exporting".format(route_id, direction, calendar))

                    table_file = os.path.join("temp", route_id, direction, calendar + ".csv")

                    header = ["type", "destination", "finished", "name_ja", "name_en"] + station_order

                    with open(table_file, mode="w", encoding="utf8", newline="") as f:
                        writer = csv.DictWriter(f, header)
                        writer.writeheader()

                        for train in trains:

                            train["finished"] = "1" if train["finished"] else "0"

                            # stations to dict
                            for timepoint in train["stations"]:

                                train[timepoint["sta"]] = str(timepoint["arr"]) + ";" + str(timepoint["dep"])

                            del train["stations"]

                            writer.writerow(train)

                    shutil.rmtree(os.path.join("temp", route_id, direction, calendar))

if __name__ == "__main__":
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("-a", "--apikey", metavar="YOUR_APIKEY", help="apikey from developer-tokyochallenge.odpt.org")
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
    print("=== Trains FromStations: Starting! ===")

    print("Initializing parser")
    parser = StationTablesToTrains(apikey=apikey)

    print("Starting StationTimetables → TrainTimetables conversion")
    parser.get_tables()
    parser.transpose_tables()

    total_time = time.time() - start_time
    print("=== TokyoGTFS: Finished in {} s ===".format(round(total_time, 2)))
