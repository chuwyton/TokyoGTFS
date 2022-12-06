try: import ijson.backends.yajl2_c as ijson
except: import ijson

from datetime import datetime, timedelta, date
from warnings import warn
from bs4 import BeautifulSoup
from copy import copy
from tempfile import TemporaryDirectory
import pykakasi
import requests
import iso8601
import json
import csv
import io
import os
import re

from .err import DataAssertion
from .utils import print_log
from .const import GTFS_HEADERS, ADDITIONAL_ENGLISH, GET_TIMEOUT

"""
This file contains object that are used to
_handle_ some more complicated stuff.

Currently used to simplify:
- Getting and caching data from API
- handling calendars
- handling translations
- calculating time-realted things
"""

def JSONIterator(buffer):
    """Creates a ijson iterator over all items,
    then automatically closes the provided buffer.
    """
    try:
        yield from ijson.items(buffer, "item")
    finally:
        buffer.close()

def TrainTripIterator(api, conversion_time):
    odpt_trips = api.get("TrainTimetable")
    parsed_trips = set()

    for trip in odpt_trips:
        # Check if trip has not expired
        if "dct:valid" in trip:
            valid_until = iso8601.parse_date(trip["dct:valid"])

            if valid_until <= conversion_time:
                continue

        # Avoid duplicate trips, it sometimes happens
        if trip["owl:sameAs"] in parsed_trips:
            continue

        parsed_trips.add(trip["owl:sameAs"])

        yield trip

class TimeValue:
    """An object representing a GTFS time value.

    :param seconds: The amount of seconds since 12:00 - 12 hours.
    :type secnonds: int
    """
    def __init__(self, seconds):
        self.m, self.s = divmod(int(seconds), 60)
        self.h, self.m = divmod(self.m, 60)

    def __str__(self):
        "Return GTFS-compliant string representation of time"
        return f"{self.h:0>2}:{self.m:0>2}:{self.s:0>2}"

    def __repr__(self): return "<Time " + self.__str__() + ">"
    def __int__(self): return self.h * 3600 + self.m * 60 + self.s
    def __add__(self, other): return TimeValue(self.__int__() + int(other))
    def __sub__(self, other): return TimeValue(self.__int__() - int(other))
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

class ApiHandler:
    """An object to request and cache API data dumps

    :param apikey: Apikey to ODPT
    :type apikey: str
    """

    def __init__(self, apikey):
        self.session = requests.Session()
        self.dir = TemporaryDirectory()
        self.apikey = apikey

    def get(self, endpoint, data_dump=True, force_vanilla_json=False):
        """Get the `endpoint` data dump.
        If `cache` is truthy, the data dump is cached for later use.
        If `data_dump` is truthy requests the data dump of the given endpoint.
        """

        if data_dump:
            endpoint = endpoint + ".json"

        print_log(f"Requesting data dump for {endpoint}")

        req = self.session.get(
            f"https://api.odpt.org/api/v4/odpt:{endpoint}",
            params={"acl:consumerKey": self.apikey}, timeout=GET_TIMEOUT,
            stream=True,
        )

        req.raise_for_status()
        buffer = req.raw

        if force_vanilla_json:
            return (i for i in req.json())
        else:
            return JSONIterator(buffer)

class CalendarHandler:
    """An object which handles services,
    calendars and all that kind of stuff.

    :param apihandler: The ApiHandler object
    :type apihandler: ApiHandler

    :param start_date: Calendar start date
    :type start_date: datetime.date

    :param end_date: Calendar end date. If not provded assumed
                     to be 180 days after start_date
    :type end_date: datetime.date
    """

    def __init__(self, apihandler, start_date, end_date=None):
        """Inits the CalednarHandler
        """
        if end_date is None:
            end_date = start_date + timedelta(days=180)

        self.apihandler = apihandler
        self.start = start_date
        self.end = end_date

        self.used = {}
        self.valid = set()
        self.holidays = set()
        self.special = {}
        self.outputted = set()

        self.built_ins = {
            "Everyday", "Weekday", "SaturdayHoliday", "Holiday",
            "Sunday", "Monday", "Tuesday", "Wednesday",
            "Thursday", "Friday", "Saturday", "Sunday"
        }

        # Load holidays
        self.load_valid()
        self.load_holidays()

    def load_holidays(self):
        """Loads Japan holidays into self.holidays.
        Data comes from Japan's Cabinet Office:
        https://www8.cao.go.jp/chosei/shukujitsu/gaiyou.html

        Only holdays within self.start and self.end are saved.
        """
        print_log("Loading calendar holidays")

        req = requests.get("https://www8.cao.go.jp/chosei/shukujitsu/syukujitsu.csv")
        req.raise_for_status()
        req.encoding = "shift-jis"

        buffer = io.StringIO(req.text)
        reader = csv.DictReader(buffer)

        for row in reader:
            date_str = row["国民の祝日・休日月日"]
            date_val = datetime.strptime(date_str, "%Y/%m/%d").date()

            if self.start <= date_val <= self.end:
                self.holidays.add(date_val)

        buffer.close()

    def load_valid(self):
        """Loads list of **usable** calendars into self.valid
        in order to ensure that each trips points to a
        service_id active on at least one day.
        """
        calendars = self.apihandler.get("Calendar")

        for calendar in calendars:
            calendar_id = calendar["owl:sameAs"].split(":")[1]

            if calendar_id in self.built_ins:
                self.valid.add(calendar_id)

            elif calendar.get("odpt:day", []) != []:
                dates = [datetime.strptime(i, "%Y-%m-%d").date() for i in calendar["odpt:day"]]
                dates = [i for i in dates if self.start <= i <= self.end]

                if dates:

                    # Save dates of special calendars
                    for date in dates:
                        if date not in self.special: self.special[date] = set()
                        self.special[date].add(calendar_id)

                    # Add this special calendar to self.valid
                    self.valid.add(calendar_id)

    def use(self, route_id, calendar_id):
        """Checks if this pair of route_id and calendar_id can be used.
        If yes, returns the service_id to be used in the GTFS.
        If no, returns None
        """

        if calendar_id in self.valid:
            service_id = route_id + "." + calendar_id

            # List calendars used by this route
            if route_id not in self.used:
                self.used[route_id] = set()

            self.used[route_id].add(calendar_id)

            return service_id

        else:
            return None

    def was_exported(self, service_id):
        """Check if this service_id (route_id.calendar_id)
        was exported to calendar_dates.txt

        :rtype: bool
        """
        if service_id in self.outputted:
            return True

        else:
            return False

    def export(self):
        """Exports all used services into
        gtfs/calendar_dates.txt
        """
        # Open file
        buffer = open("gtfs/calendar_dates.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(buffer, GTFS_HEADERS["calendar_dates.txt"], extrasaction="ignore")
        writer.writeheader()

        for route_id, calendars_used in self.used.items():

            print_log(f"Exporting calendars: {route_id}")

            working_date = copy(self.start)

            while working_date <= self.end:

                active_services = []
                weekday = working_date.weekday()
                is_holiday = working_date in self.holidays
                special_calendars = calendars_used.intersection(self.special[working_date])

                # == DIFFERENT ACTIVE SERIVCE SWITCH-CASE == #
                if special_calendars:
                    active_services = list(special_calendars)

                # Holidays
                elif is_holiday and "Holiday" in calendars_used:
                    active_services = ["Holiday"]

                elif is_holiday and "SaturdayHoliday" in calendars_used:
                    active_services = ["SaturdayHoliday"]

                # Specific weekdays
                elif weekday == 0 and "Monday" in calendars_used:
                    active_services = ["Monday"]

                elif weekday == 1 and "Tuesday" in calendars_used:
                    active_services = ["Tuesday"]

                elif weekday == 2 and "Wednesday" in calendars_used:
                    active_services = ["Wednesday"]

                elif weekday == 3 and "Thursday" in calendars_used:
                    active_services = ["Thursday"]

                elif weekday == 4 and "Friday" in calendars_used:
                    active_services = ["Friday"]

                elif weekday == 5 and "Saturday" in calendars_used:
                    active_services = ["Saturday"]

                elif weekday == 6 and "Sunday" in calendars_used:
                    active_services = ["Sunday"]

                # Weekend vs Workday
                elif weekday <= 4 and "Weekday" in calendars_used:
                    active_services = ["Weekday"]

                elif weekday >= 5 and "SaturdayHoliday" in calendars_used:
                    active_services = ["SaturdayHoliday"]

                # Everyday
                elif "Everyday" in calendars_used:
                    active_services = ["Everyday"]

                # == END SWITCH-CASE == #

                for active_service in active_services:
                    service_id = route_id + "." + active_service
                    self.outputted.add(service_id)

                    writer.writerow({
                        "service_id": service_id,
                        "date": working_date.strftime("%Y%m%d"),
                        "exception_type": "1",
                    })

                working_date += timedelta(days=1)

        buffer.close()

class TranslationHandler:
    """An object to handle translations"""

    def __init__(self):
        """Sets up the TranslationHandler
        """
        kakasi_loader = pykakasi.kakasi()
        kakasi_loader.setMode("H", "a")
        kakasi_loader.setMode("K", "a")
        kakasi_loader.setMode("J", "a")
        kakasi_loader.setMode("r", "Hepburn")
        kakasi_loader.setMode("s", True)
        kakasi_loader.setMode("C", True)

        self.converter = kakasi_loader.getConverter()
        self.print_warns = False

        self.strings = {}

    def get_english(self, japanese, dont_save=False):
        """Given a Japanese text,
        returns the corresponding English string.
        """
        # Ignore empty strings
        if japanese is None or japanese == "":
            return japanese

        # Check if this text is already known
        inside_self = self.strings.get(japanese)

        if inside_self:
            return inside_self

        # Check if it is defined in ADDITIONAL_ENGLISH
        inside_additional = ADDITIONAL_ENGLISH.get(japanese)

        if inside_additional:
            self.strings[japanese] = inside_additional
            return inside_additional

        # Fallback to using pykakasi
        english = self.converter.do(japanese)
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

        if not dont_save:
            self.strings[japanese] = english

        if self.print_warns:
            print_log(f"no english for string {japanese} (generated {english})", 1)

        return english

    def get_headsign_english(self, japanese, dont_save=False):
        """Given a Japanese text,
        returns the corresponding English string,
        with optimization for trip_headsign.
        """
        # Check if this text is already known
        inside_self = self.strings.get(japanese)

        if inside_self:
            return inside_self

        # Check if it is defined in ADDITIONAL_ENGLISH
        inside_additional = ADDITIONAL_ENGLISH.get(japanese)

        if inside_additional:
            self.strings[japanese] = inside_additional
            return inside_additional

        # Analyze the text (bonus points for using regex!)
        jap_parsed = japanese.replace("）", ")").replace("（", "(")
        via = re.search(r"(\w+)経由", jap_parsed)
        brackets = re.search(r"(\()(\w+)(\))", jap_parsed)

        if via:
            via_txt = via[1]
            jap_parsed = jap_parsed.replace(via[0], "")
        else:
            via_txt = None

        if brackets:
            brackets_txt = brackets[2]
            jap_parsed = jap_parsed.replace(brackets[0], "")
        else:
            brackets_txt = None

        destination = re.sub(r"(行|行き|ゆき)$", "", jap_parsed.strip())

        # Translate parts into english
        destination_en = self.get_english(destination, True)
        via_en = self.get_english(via_txt, True)
        brackets_en = self.get_english(brackets_txt, True)

        # Stich all parts together
        english = destination_en

        if via_en: english += f" via {via_en}"
        if brackets_en: english += f" ({brackets_en})"

        if not dont_save:
            self.strings[japanese] = english

        print_log(f"auto-generated headsign for {japanese} is {english}", 1)

        return english

    def set_english(self, japanese, english):
        """Save the english tranlation of this japanese text
        """
        self.strings[japanese] = english

    def export(self):
        """Export all known translations to
        gtfs/translations.txt
        """
        print_log("Exporting translations")

        buffer = open("gtfs/translations.txt", mode="w", encoding="utf8", newline="")
        writer = csv.DictWriter(buffer, GTFS_HEADERS["translations.txt"], extrasaction="ignore")
        writer.writeheader()

        for ja_string, en_string in self.strings.items():
            writer.writerow({"trans_id": ja_string, "lang": "ja", "translation": ja_string})
            writer.writerow({"trans_id": ja_string, "lang": "en", "translation": en_string})

        buffer.close()
