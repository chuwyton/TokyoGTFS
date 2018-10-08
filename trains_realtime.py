from google.transit import gtfs_realtime_pb2 as gtfs_rt
from datetime import datetime, date, timedelta
import argparse
import requests
import zipfile
import iso8601
import time
import pytz
import csv
import io
import os

EFFECTS = {"運転見合わせ": 1, "運転被約": 2, "遅延": 3, "運行情報あり": 6}
CAUSES = {
    "車両点検": 9, "車輪空転": 3, "大雨": 8, "大雪": 8, "地震": 6, "線路に支障物": 6, "シカと衝突": 6,
    "接続待合せ": 3,
}

class TrainRealtime:
    def __init__(self, apikey, gtfs_arch="tokyo_trains.zip"):
        self.apikey = apikey
        self.timezone = pytz.timezone("Asia/Tokyo")
        self.active_routes = set()
        self.active_operators = set()

        # Get list of active routes
        with open("data/train_routes.csv", mode="r", encoding="utf8", newline="") as buff:
            for row in csv.DictReader(buff):
                self.active_routes.add(row["route_id"])
                self.active_operators.add(row["operator"])

        # Get map realtime_trip_id → trip_id
        self.trip_map_date = datetime.now(tz=self.timezone).strftime("%Y%m%d")
        self.trip_map = {}
        with zipfile.ZipFile(gtfs_arch, mode="r") as arch:
            # Get active calendars
            with arch.open("calendar_dates.txt") as buff:
                reader = csv.DictReader(io.TextIOWrapper(buff, encoding="utf8", newline=""))
                active_services = {i["service_id"] for i in reader if i["date"] == self.trip_map_date}

            # Map tr_trip_id → trip_id
            with arch.open("trips.txt") as buff:
                reader = csv.DictReader(io.TextIOWrapper(buff, encoding="utf8", newline=""))
                for row in reader:
                    if row["service_id"] in active_services and row["train_realtime_id"]:
                        if row["train_realtime_id"] not in self.trip_map:
                            self.trip_map[row["train_realtime_id"]] = []
                        self.trip_map[row["train_realtime_id"]].append(row["trip_id"])

    def delays(self, container):
        now = datetime.now(tz=self.timezone)
        if self.trip_map_date != now.strftime("%Y%m%d"):
            self.__init__()

        trains = requests.get("https://api-tokyochallenge.odpt.org/api/v4/odpt:Train", params={"acl:consumerKey": self.apikey}, timeout=10)
        trains.raise_for_status()
        trains = trains.json()

        for train in trains:
            train_id = train["owl:sameAs"].split(":")[1]
            trips = self.trip_map.get(train_id, [])

            # Assume the train maps to some trip
            if not trips:
                continue

            # Load some info about train
            delay = train.get("odpt:delay")
            current_stop = train.get("odpt:fromStation")
            next_stop = train.get("odpt:toStation")
            route = train["odpt:Railway"].split(":")
            update_timestamp = round(iso8601.parse_date(train["dc:date"]).timestamp())

            # Be sure data is not too old
            if "dct:valid" in train:
                if now > iso8601.parse_date(train["dct:valid"]):
                    continue

            # Make sure we have info about delay/current stop
            if delay == None or current_stop == None:
                continue

            for trip_id in trips:
                trip_belongs_to_current_route = trip_id.split(".")[1]  == route.split(".")[1]

                entity = container.entity.add()
                entity.id = train["@id"] + "/" + trip_id
                if delay != None:
                    trip_update = entity.trip_update
                    trip_update.trip.trip_id = trip_id
                    trip_update.delay = delay
                    trip_update.timestamp = update_timestamp

                if next_stop and trip_belongs_to_current_route:
                    vehicle = entity.vehicle
                    vehicle.trip.trip_id = trip_id
                    vehicle.stop_id = next_stop.split(":")[1]
                    vehicle.current_status = 2
                    vehicle.timestamp = update_timestamp

                elif current_stop and trip_belongs_to_current_route:
                    vehicle = entity.vehicle
                    vehicle.trip.trip_id = trip_id
                    vehicle.stop_id = current_stop.split(":")[1]
                    vehicle.current_status = 1
                    vehicle.timestamp = update_timestamp

        return container

    def alerts(self, container):
        alerts = requests.get("https://api-tokyochallenge.odpt.org/api/v4/odpt:TrainInformation", params={"acl:consumerKey": self.apikey}, timeout=10)
        alerts.raise_for_status()
        alerts = alerts.json()

        for alert in alerts:
            # Load basic info about the alert
            operator = alert["odpt:operator"].split(":")[1]
            route = alert["odpt:railway"].split(":")[1] if "odpt:railway" in alert else ""

            # Load info about validaty time
            start_time = round(iso8601.parse_date(alert["odpt:timeOfOrigin"]).timestamp()) if "odpt:timeOfOrigin" in alert else None
            end_time = round(iso8601.parse_date(alert["dct:valid"]).timestamp()) if "dct:valid" in alert else None
            recovery_time = round(iso8601.parse_date(alert["odpt:resumeEstimate"]).strftime("%Y-%m-%d %H:%M")) if "odpt:resumeEstimate" in alert else None


            # Ignore alerts that denote normal service status
            if alert.get("odpt:trainInformationStatus", {}).get("ja", "平常") == "平常":
                continue

            # Ignore alerts for inactive operators and inactive routes
            if operator not in self.active_operators or (route and route not in self.active_routes):
                continue

            # Data
            cause = alert.get("odpt:trainInformationCauseTitle", {}) or alert.get("odpt:trainInformationCause", {})
            direction = alert.get("odpt:trainInformationLineTitle", {}) or alert.get("odpt:trainInformationLine", {})
            area = alert.get("odpt:trainInformationAreaTitle", {}) or alert.get("odpt:trainInformationArea", {})

            # Create GTFS-RT entity
            entity = container.entity.add()
            entity.id = alert["@id"]

            # Add info about alerted routes
            informed = entity.alert.informed_entity.add()
            if not route: informed.agency_id = operator
            else: informed.route_id = route

            # Load info about validaty time
            if start_time or end_time: period = entity.alert.active_period.add()
            if start_time: period.start = start_time
            if end_time: period.end = end_time

            # Try to guess the cause and effect, defaulting to UNKNOWN_CAUSE and UNKNOWN_EFFECT
            entity.alert.cause = CAUSES.get(cause.get("ja", ""), 1)
            entity.alert.effect = EFFECTS.get(alert.get("odpt:trainInformationStatus", {}).get("ja", ""), 8)

            # Get alert header
            header_ja = alert["odpt:trainInformationStatus"]["ja"]

            translation = entity.alert.header_text.translation.add()
            translation.language, translation.text = "ja", header_ja

            if "en" in alert["odpt:trainInformationStatus"]:
                translation = entity.alert.header_text.translation.add()
                translation.language, translation.text = "en", alert["odpt:trainInformationStatus"]["en"]

            # Contrusct alert body
            # Append main info
            ja_body, en_body = alert["odpt:trainInformationText"]["ja"], alert["odpt:trainInformationText"].get("en", "")
            ja_body += "\n\n"
            if en_body: en_body += "\n\n"


            # Add cause, if it's defined
            if "ja" in cause: ja_body += "発生理由：" + cause["ja"] + "\n"
            if "en" in cause: en_body += "Cause: " + cause["en"] + "\n"

            # Add direction, if it's defined
            if "ja" in direction: ja_body += "列車の運転方向：" + direction["ja"] + "\n"
            if "en" in direction: en_body += "Direction: " + direction["en"] + "\n"

            # Add affected area, if it's defined
            if "ja" in area: ja_body += "発生エリア：" + area["ja"] + "\n"
            if "en" in area: en_body += "Affected area: " + area["en"] + "\n"

            # Add recovery time, if it's defined
            if recovery_time:
                ja_body += "復旧見込み時刻：" + recovery_time + "\n"
                en_body += "Estimated Recovery Time: " + recovery_time + "\n"

            # Add body to alert
            translation = entity.alert.description_text.translation.add()
            translation.language, translation.text = "ja", ja_body.strip()

            if en_body:
                translation = entity.alert.description_text.translation.add()
                translation.language, translation.text = "en", en_body.strip()

        return container

    def parse(self, human_readable=False):
        container = gtfs_rt.FeedMessage()
        header = container.header
        header.gtfs_realtime_version = "2.0"
        header.incrementality = 0
        header.timestamp = round(datetime.today().timestamp())

        container = self.delays(container)
        container = self.alerts(container)

        mode = "w" if human_readable else "wb"
        with open("tokyo_trains_rt.pb", mode=mode) as f:
            if human_readable: f.write(str(container))
            else: f.write(container.SerializeToString())

if __name__ == "__main__":
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("-a", "--apikey", metavar="YOUR-APIKEY", help="apikey from developer-tokyochallenge.odpt.org")
    args_parser.add_argument("-g", "--gtfs", metavar="PATH-TO-TRAINS-GTFS.zip", default="tokyo_trains.zip", help="path to GTFS created by trains_gtfs.py")
    args_parser.add_argument("-hr", "--human-readable", action="store_true", help="output gtfs-realtime file as human-readable instead of binary")
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
    print("=== Trains GTFS-RT: Starting! ===")
    parser = TrainRealtime(apikey=apikey, gtfs_arch=args.gtfs)
    parser.parse(human_readable=args.human_readable)
    total_time = time.time() - start_time
    print("=== TokyoGTFS: Finished in {} s ===".format(round(total_time, 2)))
