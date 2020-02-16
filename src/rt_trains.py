from google.transit import gtfs_realtime_pb2 as gtfs_rt
from datetime import datetime
import zipfile
import iso8601
import pytz
import csv
import io

from .handlers import ApiHandler
from .utils import iso_date_to_tstamp, iso_date_to_ymdhm, print_log
from .const import ALERT_EFFECTS, ALERT_CAUSES

class TrainRealtime:
    """Object responsible for creating GTFS-Realtime data

    :param apikey: Key to the ODPT API
    :type apikey: str

    :param human_readable: Should the create GTFS-RT protobuff be human readable?
    :type human_readable: bool

    :param path_to_gtfs: Path to the GTFS file.
                         The GTFS has to have a column train_realtime_id in trips.txt
    :type path_to_gtfs: str or path-like
    """
    def __init__(self, apikey, human_readable=False, path_to_gtfs="tokyo_trains.zip"):
        """Init the parser"""
        self.apikey = apikey
        self.api = ApiHandler(self.apikey)

        self.human_readable = human_readable

        self.path_to_gtfs = path_to_gtfs

        self.active_operators = set()
        self.active_routes = set()

        self.date = None
        self.trip_map = {}

        self.train_dirs = {}

    def load_active(self):
        """Load active routes and operators from train_routes.csv"""
        print_log("Loading valid operators & routes data")

        self.active_operators = set()
        self.active_routes = set()

        with open("data/train_routes.csv", mode="r", encoding="utf8", newline="") as buff:
            for row in csv.DictReader(buff):
                self.active_routes.add(row["route_id"])
                self.active_operators.add(row["operator"])

    def load_trip_map(self):
        """Map Train IDs to trip_ids. Should be called everyday around 2AM.
        """
        self.date = datetime.now(pytz.timezone("Asia/Tokyo")).strftime("%Y%m%d")
        self.trip_map = {}

        print_log("Loading data from GTFS")
        
        with zipfile.ZipFile(self.path_to_gtfs, mode="r") as arch:
            # Get active calendars
            with arch.open("calendar_dates.txt") as buff:
                reader = csv.DictReader(io.TextIOWrapper(buff, encoding="utf8", newline=""))
                active_services = {i["service_id"] for i in reader if i["date"] == self.date}

            # Map train_id → trip_id
            with arch.open("trips.txt") as buff:
                reader = csv.DictReader(io.TextIOWrapper(buff, encoding="utf8", newline=""))
                for row in reader:
                    if row["service_id"] in active_services and row["train_realtime_id"]:
                        if row["train_realtime_id"] not in self.trip_map:
                            self.trip_map[row["train_realtime_id"]] = []
                        self.trip_map[row["train_realtime_id"]].append(row["trip_id"])

    def load_train_directions(self):
        """Creates a mapping in self.train_dirs
        from odpt:railDirection id to
        {"ja": "japanese_dir_name", "en": "english_dir_name"}.
        """
        tdirs = self.api.get("RailDirection")

        print_log("Loading rail direction data")

        for tdir in tdirs:
            tdir_names = {}
            titles = tdir.get("odpt:railDirectionTitle", {})

            title_ja = titles.get("ja")
            title_en = titles.get("en")

            if title_ja is None:
                title_ja = tdir["dc:title"]

            if title_ja: tdir_names["ja"] = title_ja
            if title_en: tdir_names["en"] = title_en

            self.train_dirs[tdir["owl:sameAs"]] = tdir_names

    def delays(self, container):
        """Add delay data to the GTFS-RT container.

        :param container: The GTFS-RT FeedMessage to append the TripUpdates to
        :type container: google.transit.gtfs_realtime_pb2.FeedMessage
        """

        now = datetime.now(pytz.timezone("Asia/Tokyo"))

        trains = self.api.get("Train", data_dump=False, force_vanilla_json=True)

        print_log("Parsing Train delays")

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
            route = train["odpt:railway"].split(":")[1]
            update_timestamp = round(iso8601.parse_date(train["dc:date"]).timestamp())

            # Be sure data is not too old
            if "dct:valid" in train:
                if now > iso8601.parse_date(train["dct:valid"]):
                    continue

            # Make sure we have info about delay/current stop
            if delay is None or current_stop is None:
                continue

            for trip_id in trips:
                trip_belongs_to_current_route = trip_id.split(".")[1] == route.split(".")[1]

                entity = container.entity.add()
                entity.id = train["@id"] + "/" + trip_id
                if delay is not None:
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
        alerts = self.api.get("TrainInformation", data_dump=False, force_vanilla_json=True)

        for alert in alerts:
            # Load basic info about the alert
            operator = alert["odpt:operator"].split(":")[1]
            route = alert["odpt:railway"].split(":")[1] if "odpt:railway" in alert else ""

            # Load info about validaty time
            start_time = iso_date_to_tstamp(alert.get("odpt:timeOfOrigin"))
            end_time = iso_date_to_tstamp(alert.get("dct:valid"))
            recovery_time = iso_date_to_ymdhm(alert.get("odpt:resumeEstimate"))

            status = alert.get("odpt:trainInformationStatus")

            # Ignore alerts that denote normal service status
            if status is None or status.get("ja", "平常") == "平常":
                continue

            # Ignore alerts for inactive operators and inactive routes
            if operator not in self.active_operators or (route and route not in self.active_routes):
                continue

            # Data
            cause = alert.get("odpt:trainInformationCauseTitle", {}) \
                    or alert.get("odpt:trainInformationCause", {})
            
            direction = self.train_dirs.get(alert.get("odpt:railDirection", None), {})
            
            area = alert.get("odpt:trainInformationAreaTitle", {}) \
                   or alert.get("odpt:trainInformationArea", {})

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
            entity.alert.cause = ALERT_CAUSES.get(cause.get("ja", ""), 1)
            entity.alert.effect = ALERT_EFFECTS.get(status.get("ja", ""), 8)

            # Get alert header
            header_ja = alert["odpt:trainInformationStatus"]["ja"]

            translation = entity.alert.header_text.translation.add()
            translation.language, translation.text = "ja", header_ja

            if "en" in alert["odpt:trainInformationStatus"]:
                translation = entity.alert.header_text.translation.add()
                translation.language = "en"
                translation.text = alert["odpt:trainInformationStatus"]["en"]

            # Contrusct alert body
            # Append main info
            ja_body = alert["odpt:trainInformationText"]["ja"]
            en_body = alert["odpt:trainInformationText"].get("en")

            ja_body += "\n\n"
            if en_body: en_body += "\n\n"

            # Add cause, if it's defined
            if "ja" in cause: ja_body += "発生理由：" + cause["ja"] + "\n"
            if en_body and "en" in cause: en_body += "Cause: " + cause["en"] + "\n"

            # Add direction, if it's defined
            if "ja" in direction: ja_body += "列車の運転方向：" + direction["ja"] + "\n"
            if en_body and "en" in direction: en_body += "Direction: " + direction["en"] + "\n"

            # Add affected area, if it's defined
            if "ja" in area: ja_body += "発生エリア：" + area["ja"] + "\n"
            if en_body and "en" in area: en_body += "Affected area: " + area["en"] + "\n"

            # Add recovery time, if it's defined
            if recovery_time:
                ja_body += "復旧見込み時刻：" + recovery_time + "\n"
                
                if en_body:
                    en_body += "Estimated Recovery Time: " + recovery_time + "\n"

            # Add body to alert
            translation = entity.alert.description_text.translation.add()
            translation.language, translation.text = "ja", ja_body.strip()

            if en_body:
                translation = entity.alert.description_text.translation.add()
                translation.language, translation.text = "en", en_body.strip()

        return container

    def create(self):
        container = gtfs_rt.FeedMessage()
        header = container.header
        header.gtfs_realtime_version = "2.0"
        header.incrementality = 0
        header.timestamp = round(datetime.now(pytz.timezone("Asia/Tokyo")).timestamp())

        container = self.delays(container)
        container = self.alerts(container)

        mode = "w" if self.human_readable else "wb"
        with open("tokyo_trains_rt.pb", mode=mode) as f:
            if self.human_readable: f.write(str(container))
            else: f.write(container.SerializeToString())

    @classmethod
    def parse_once(cls, apikey, human_readable=False, path_to_gtfs="tokyo_trains.zip"):
        """Parse realtime data and create the GTFS-RT .pb file in tokyo_trains_rt.pb

        :param apikey: Key to the ODPT API
        :type apikey: str

        :param human_readable: Should the create GTFS-RT protobuff be human readable?
        :type human_readable: bool

        :param path_to_gtfs: Path to the GTFS file.
                            The GTFS has to have a column train_realtime_id in trips.txt
        :type path_to_gtfs: str or path-like
        """
        self = cls(apikey, human_readable, path_to_gtfs)
        self.load_active()
        self.load_trip_map()
        self.load_train_directions()
        self.create()

