import xml.etree.ElementTree as etree
from collections import OrderedDict
import html5lib
import requests
import ijson
import json
import csv
import re
import os

EKIKARA_ROUTES = OrderedDict([
    ("Keisei.Main", "1302011"),
    ("Keisei.Kanamachi", "1302031"),
    ("Keisei.Oshiage", "1302021"),
    ("Keisei.Chiba", "1302041"),
    ("Keisei.Chihara", "1302051"),
    ("Keisei.HigashiNarita", "1302011"),
    ("Keisei.NaritaSkyAccess", "1204011"),
    ("Tokyu.Meguro", "1307021"),
    ("Tokyu.DenEnToshi", "1307051"),
    ("Tokyu.Toyoko", "1307011"),
    ("Tokyu.Oimachi", "1307061"),
    ("Tokyu.Ikegami", "1307071"),
    ("Tokyu.TokyuTamagawa", "1307031"),
    ("Tokyu.Setagaya", "1307091"),
    ("Tokyu.Kodomonokuni", "1307081"),
    ("Tobu.Tojo", "1303111"),
    ("Tobu.Ogose", "1303121"),
    ("Tobu.TobuSkytree", "1303011"),
    ("Tobu.TobuSkytreeBranch", "1303011"),
    ("Tobu.Kameido", "1303081"),
    ("Tobu.Daishi", "1303091"),
    ("Tobu.Isesaki", "1303011"),
    ("Tobu.Sano", "1303051"),
    ("Tobu.Koizumi", "1303061"),
    ("Tobu.KoizumiBranch", "1303062"),
    ("Tobu.Kiryu", "1303071"),
    ("Tobu.Nikko", "1303021"),
    ("Tobu.Utsunomiya", "1303041"),
    ("Tobu.Kinugawa", "1303031"),
    ("Tobu.TobuUrbanPark", "1303101"),
    ("Odakyu.Odawara", "1306011"),
    ("Odakyu.Enoshima", "1306021"),
    ("Odakyu.Tama", "1306031"),
    ("Seibu.Ikebukuro", "1304011"),
    ("Seibu.Sayama", "1304041"),
    ("Seibu.SeibuChichibu", "1304021"),
    ("Seibu.SeibuYurakucho", "1304051"),
    ("Seibu.Toshima", "1304031"),
    ("Seibu.Shinjuku", "1304061"),
    ("Seibu.Haijima", "1304081"),
    ("Seibu.Tamako", "1304091"),
    ("Seibu.Kokubunji", "1304101"),
    ("Seibu.Seibuen", "1304071"),
    ("Seibu.Yamaguchi", "1304121"),
    ("Seibu.Tamagawa", "1304111"),
    ("Keikyu.Main", "1308011"),
    ("Keikyu.Airport", "1308021"),
    ("Keikyu.Daishi", "1308051"),
    ("Keikyu.Kurihama", "1308041"),
    ("Keikyu.Zushi", "1308031"),
    ("Yurikamome.Yurikamome", "1314011"),
])

ETREE_NS = {"html": "http://www.w3.org/1999/xhtml"}
VERBOSE = True

def _elem_string(element):
    txt = etree.tostring(element, encoding="unicode")
    txt = re.sub(r"<.+?>", "", txt)
    return txt

def _clear_dir(dir):
    if os.path.isdir(dir):
        for file in os.listdir(dir):
            filepath = os.path.join(dir, file)
            if os.path.isdir(filepath): _clear_dir(filepath)
            else: os.remove(filepath)
        os.rmdir(dir)
    elif os.path.isfile(dir):
        os.remove(dir)

def through_train_info(route_ekikara_id, train_url, breakpoints, route_id):
    url = "http://ekikara.jp/newdata/line/{}/{}".format(route_ekikara_id, train_url)

    req = requests.get(url, timeout=90)
    req.encoding = "shift-jis"

    if VERBOSE: print("\033[1A\033[K" + "Parsing times: scrapping ekikara rt {} for train through info, url {}".format(route_id, url))

    breakpoints = {i: (None, None) for i in breakpoints}

    assert req.url != "http://ekikara.jp/error.htm", "ekikara returned an error!"

    html = html5lib.parse(req.text)
    table = html.find(".//html:td[@class='lowBg01']", ETREE_NS).find("html:table/html:tbody", ETREE_NS)

    list_of_stations = []
    arr, dep = None, None

    rows = list(table.findall("html:tr", ETREE_NS))

    # Rows 0÷7 are train meta-data
    for tr in rows[8:-1]:

        for col_idx, td in enumerate(tr.findall("html:td", ETREE_NS)):

            if col_idx == 0:

                station_name = td.find(".//html:span[@class='textBold']", ETREE_NS)
                if station_name:
                    station_name = etree.tostring(station_name, encoding="unicode").split("<html:br />")[0]
                    station_name = re.sub(r"<[^<]+?>", "", station_name)
                    station_name = re.sub(r"\(\d+\)", "", station_name)
                    station_name = station_name.strip()

                else:
                    station_name == ""

                list_of_stations.append(station_name)

            # The case is only entered when station_name == breakpoint_station_name
            elif col_idx == 1 and station_name in breakpoints:
                arr_deps = td.findall("html:span[@class='l']", ETREE_NS)
                arr_deps = [re.sub(r"<.+?>", "", etree.tostring(i, encoding="unicode")) for i in arr_deps]

                if len(arr_deps) < 2:
                    arr_deps = ["", ""]

                arr = arr_deps[0].strip()
                dep = arr_deps[1].strip()

                arr_match = re.search(r"\d\d:\d\d", arr)
                dep_match = re.search(r"\d\d:\d\d", dep)

                if arr_match: arr = arr_match[0]
                else: arr = None

                if dep_match: dep = dep_match[0]
                else: dep = None

                breakpoints[station_name] = (arr, dep)

    return breakpoints, list_of_stations[0], list_of_stations[1]

def stops_by_line(apikey, position_fixer_loc=os.path.join("data", "train_stations_fixes.csv")):
    """Parse stops"""
    # Get list of stops
    stops_req = requests.get("https://api-tokyochallenge.odpt.org/api/v4/odpt:Station.json", params={"acl:consumerKey": apikey}, timeout=90, stream=True)
    stops_req.raise_for_status()
    stops = ijson.items(stops_req.raw, "item")

    stops_by_line = {i: {} for i in EKIKARA_ROUTES.keys()}

    # Load Fixer data
    position_fixer = {}
    with open(position_fixer_loc, mode="r", encoding="utf8", newline="") as f:
        for row in csv.DictReader(f):
            position_fixer[row["id"]] = (float(row["lat"]), float(row["lon"]))

    # Iterate over stops
    for stop in stops:
        stop_id = stop["owl:sameAs"]
        stop_id_split = stop_id.split(":")[1]
        stop_name = stop["dc:title"]
        stop_line = stop["odpt:railway"].split(":")[1]

        # Only care about routes from ekikara
        if stop_line not in stops_by_line:
            continue

        # Stop Position
        stop_lat, stop_lon = position_fixer.get(stop_id_split, (stop.get("geo:lat"), stop.get("geo:long")))

        if stop_lat and stop_lon:
            stops_by_line[stop_line][stop_name] = stop_id

    stops_req.close()

    stops_by_line["Keisei.NaritaSkyAccess"]["京成上野"] = "odpt.Station:Keisei.Main.KeiseiUeno"
    stops_by_line["Keisei.NaritaSkyAccess"]["日暮里"] = "odpt.Station:Keisei.Main.Nippori"
    stops_by_line["Keisei.NaritaSkyAccess"]["青砥"] = "odpt.Station:Keisei.Main.Aoto"

    return stops_by_line

def parse_tbody(tbody, stop_ids):
    train_data = {}
    ttable_row_stops = {}
    late_night = False
    row_enum = -1

    rows = list(tbody.findall("html:tr", ETREE_NS))

    assert len(rows) >= 8
    next_train_row = len(rows) - 1

    for tr in rows:

        col_enum = -1
        row_enum += 1

        # row 0 → table header
        # row 3 → calendar excpetion
        if row_enum in {0, 3}: continue

        for td in tr.findall("html:td", ETREE_NS):

            col_enum += int(td.get("colspan", "1"))

            # first cell is row label (expect for timetable!), with colspan=2
            if row_enum in [1, 2, 4, 5, next_train_row] and col_enum in [0, 1]:
                continue

            if col_enum not in train_data:
                train_data[col_enum] = {"times": []}

            # row 1 → train number
            if row_enum == 1:
                train_data[col_enum]["number"] = _elem_string(td).strip()

            # row 2 → [train_type] train_name
            elif row_enum == 2:
                train_data[col_enum]["type"] = _elem_string(td.find(".//html:span[@class='s']", ETREE_NS)).strip().lstrip("[").rstrip("]")
                train_data[col_enum]["name"] = _elem_string(td.find(".//html:span[@class='m']", ETREE_NS)).strip()

            # row 4 → trip view
            elif row_enum == 4:
                train_data[col_enum]["train_view"] = td.find(".//html:a", ETREE_NS).get("href", "")

            # row 5 → previous trip view
            elif row_enum == 5:
                link = td.find(".//html:a", ETREE_NS)
                if link: train_data[col_enum]["previous"] = link.get("href")
                else: train_data[col_enum]["previous"] = None

            # last row → next trip view
            elif row_enum == next_train_row:
                link = td.find(".//html:a", ETREE_NS)
                if link: train_data[col_enum]["next"] = link.get("href")
                else: train_data[col_enum]["next"] = None

            # rows 6÷(last - 1) col 0 → timetable: station names
            elif col_enum == 0:

                stop_names = etree.tostring(td, encoding="unicode").split("<html:br />")[:-1]
                stop_names = [re.sub("<[^<]+?>", "", i) for i in stop_names]

                for ttable_idx, stop_name in enumerate(stop_names):

                    stop_name = stop_name.strip()

                    if stop_name == "〃":
                        arr_dep = "DEP"
                        stop_name = ttable_row_stops[ttable_idx - 1][0]
                        stop_id = ttable_row_stops[ttable_idx - 1][1]

                    else:
                        arr_dep = "ARR"
                        stop_id = stop_ids.get(stop_name.strip(), None)

                    ttable_row_stops[ttable_idx] = (stop_name, stop_id, arr_dep)

            # rows 6÷(last - 1) col 0 → timetable: arrival/departure charachter
            # this is handled when col_enum == 0
            elif col_enum == 1:
                continue

            # rows 6÷(last - 1) col > 1 → timetable: train times
            else:
                times = etree.tostring(td, encoding="unicode").split("<html:br />")[:-1]
                times = [re.sub("<[^<]+?>", "", i).strip() for i in times]

                for ttable_idx, time_str in enumerate(times):

                    time_str = time_str.strip()
                    stop_name, stop_id, arr_dep = ttable_row_stops[ttable_idx]

                    if stop_id == None: continue
                    if not re.match(r"\d\d:\d\d", time_str): continue

                    if time_str.startswith("22") or time_str.startswith("23"):
                        late_night = True

                    if late_night and time_str.startswith("00") or time_str.startswith("01") or time_str.startswith("02") or time_str.startswith("03"):
                        hr, min = map(int, time_str.split(":"))
                        hr += 24

                        time_str = "{:0>2}:{:0>2}".format(hr, min)

                    if arr_dep == "DEP" and train_data[col_enum]["times"] != [] and train_data[col_enum]["times"][-1]["s"] == stop_id:
                        train_data[col_enum]["times"][-1]["d"] = time_str

                    else:
                        train_data[col_enum]["times"].append({"s": stop_id, "n": stop_name, "a": time_str, "d": time_str})


    trains = [train_data[i] for i in sorted(train_data.keys()) if len(train_data[i]["times"]) > 1]
    return trains

def parse_route(apikey, route_id):
    if route_id not in EKIKARA_ROUTES:
        raise ValueError("{} route's ekikara ID is not defined!".format(route_id))

    route_ekikara_id = EKIKARA_ROUTES[route_id]
    stops = stops_by_line(apikey)
    trains = []

    for service_id, service_suffix in [("Weekday", ""), ("Saturday", "_sat"), ("Holiday", "_holi")]:

        for direction_id, direction_prefix in [("Outbound", "down1_"), ("Inbound", "up1_")]:

            page_counter = 0
            while True:
                page_counter += 1

                url = "http://ekikara.jp/newdata/line/{}/{}.htm".format(route_ekikara_id, direction_prefix + str(page_counter) + service_suffix)

                req = requests.get(url)
                req.encoding = "shift-jis"
                if req.url == "http://ekikara.jp/error.htm": break

                html = html5lib.parse(req.text)

                if VERBOSE: print("\033[1A\033[K" + "Parsing times: scrapping ekikara for {}, url {}".format(route_id, url))

                list_of_trains = parse_tbody(html.find(".//html:td[@class='lowBg14']", ETREE_NS).find("html:table/html:tbody", ETREE_NS), stops[route_id])

                # create pseudo-ODPT data from ekikara tranis:
                for train in list_of_trains:

                    # Ekikara Keisei SkyAccess ID and Hokuso Railway ID are the same
                    # for SkyAccess we only care about Skyliner and Access Rapid services
                    if route_id == "Keisei.NaritaSkyAccess" and not ("スカイライナー" in train["type"] or "アクセス特急" in train["type"]):
                        continue

                    # Keisei Narita SkyAccess services on Keisei Main line are parsed with Keisei.NaritaSkyAccess
                    if route_id == "Keisei.Main" and ("スカイライナー" in train["type"] or "アクセス特急" in train["type"]):
                        continue

                    if route_id == "Keisei.NaritaSkyAccess":
                        breakpoints, first_station, last_station = through_train_info(
                            route_ekikara_id,
                            train["train_view"],
                            ["京成上野", "日暮里", "青砥", train["times"][0]["n"], train["times"][-1]["n"]],
                            route_id
                        )


                    # {"1302011", "1303011"} ekikara IDs map to many ODPT routes, so we check if this train continues onto the other routes with same ekikara id
                    elif train["next"] or train["previous"] or route_ekikara_id in {"1302011", "1303011"}:
                        breakpoints, first_station, last_station = through_train_info(
                            route_ekikara_id,
                            train["train_view"],
                            [train["times"][0]["n"], train["times"][-1]["n"]],
                            route_id
                        )

                    else:
                        breakpoints, first_station, last_station = {}, train["times"][0]["n"], train["times"][-1]["n"]

                    pseudo_odpt = {
                        "owl:sameAs": "odpt.TrainTimetable:" + ".".join([route_id, train["number"], service_id]),
                        "odpt:operator": "odpt.Operator:" + route_id.split(".")[0],
                        "odpt:railway": "odpt.Railway:" + route_id,
                        "odpt:railDirection": "odpt.RailDirection:" + direction_id,
                        "odpt:calendar": "odpt.Calendar:" + service_id,
                        "odpt:trainNumber": train["number"],
                        "tokyogtfs:trainTypeName": train["type"],
                        #"odpt:trainName": {"ja": train["name"]}
                    }

                    if train["name"] != "":
                        pseudo_odpt["odpt:trainName"] = {"ja": train["name"]}

                    pseudo_odpt["tokyogtfs:destinationStationName"] = last_station
                    pseudo_odpt["tokyogtfs:originStationName"] = first_station

                    first_breakpoint = breakpoints.get(train["times"][0]["n"], (None, None))
                    last_breakpoint = breakpoints.get(train["times"][-1]["n"], (None, None))

                    if first_breakpoint:
                        if first_breakpoint[0]: train["times"][0]["a"] = first_breakpoint[0]
                        if first_breakpoint[1]: train["times"][0]["d"] = first_breakpoint[1]

                    if last_breakpoint:
                        if last_breakpoint[0]: train["times"][-1]["a"] = last_breakpoint[0]
                        if last_breakpoint[1]: train["times"][-1]["d"] = last_breakpoint[1]

                    # Append KeiseiUeno, Nippori and Aoto station to SkyAccess trains
                    if route_id == "Keisei.NaritaSkyAccess":
                        additional_stations = []
                        if breakpoints.get("京成上野", (None, None)) != (None, None):
                            arr, dep = breakpoints["京成上野"]
                            arr = arr or dep
                            dep = dep or arr

                            additional_stations.append({"s": "odpt.Station:Keisei.Main.KeiseiUeno", "n": "京成上野", "d": dep, "a": arr})

                        if breakpoints.get("日暮里", (None, None)) != (None, None):
                            arr, dep = breakpoints["日暮里"]
                            arr = arr or dep
                            dep = dep or arr

                            additional_stations.append({"s": "odpt.Station:Keisei.Main.Nippori", "n": "日暮里", "d": dep, "a": arr})

                        if breakpoints.get("青砥", (None, None)) != (None, None):
                            arr, dep = breakpoints["青砥"]
                            arr = arr or dep
                            dep = dep or arr

                            additional_stations.append({"s": "odpt.Station:Keisei.Main.Aoto", "n": "青砥", "d": dep, "a": arr})

                        if additional_stations:
                            if train["times"][-1]["s"] == "odpt.Station:Keisei.NaritaSkyAccess.NaritaAirportTerminal1":
                                train["times"] = additional_stations + train["times"]

                            else:
                                additional_stations.reverse()
                                train["times"] = train["times"] + additional_stations

                            first_breakpoint = breakpoints[train["times"][0]["n"]]
                            last_breakpoint = breakpoints[train["times"][-1]["n"]]


                    pseudo_odpt["odpt:trainTimetableObject"] = [
                        {"odpt:departureStation": i["s"], "odpt:departureTime": i["d"], "odpt:arrivalTime": i["a"]} \
                        for i in train["times"]
                    ]

                    trains.append(pseudo_odpt)

    return trains

def main(apikey, verbose=True):
    global VERBOSE
    VERBOSE = verbose

    _clear_dir("ekikara")
    os.mkdir("ekikara")

    if VERBOSE: print("", end="\n\n")

    for route_id in EKIKARA_ROUTES.keys():
        if VERBOSE: print("\033[2A\033[K" + "Route: {}".format(route_id) + "\033[1E\033[K")
        t = parse_route(apikey, route_id)
        with open("ekikara/{}.json".format(route_id), "w", encoding="utf-8") as f: json.dump(t, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    with open("apikey.txt", mode="r", encoding="utf8") as f:
        apikey = f.read().strip()

    main(apikey, True)
