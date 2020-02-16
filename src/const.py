"""
This file defiens some constants,
that are used by all over the place.
"""

GTFS_HEADERS = {
    "agency.txt": ["agency_id", "agency_name", "agency_url", "agency_timezone", "agency_lang"],
    
    "stops.txt": ["stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon"],
    
    "routes.txt": ["agency_id", "route_id", "route_short_name", "route_long_name",
                   "route_type", "route_color", "route_text_color"],
    
    "calendar_dates.txt": ["service_id", "date", "exception_type"],
    
    "translations.txt": ["trans_id", "lang", "translation"]
    
    #"fare_attributes.txt": ["agency_id", "fare_id", "price", "currency_type",
    #                        "payment_method", "transfers"],
    
    #"fare_rules.txt": ["fare_id", "contains_id"],
}

GTFS_HEADERS_TRAIN = GTFS_HEADERS.copy()
GTFS_HEADERS_TRAIN.update({
    "trips.txt": ["route_id", "trip_id", "service_id", "trip_short_name",
                  "trip_headsign", "direction_id", "direction_name",
                  "block_id", "train_realtime_id"],
    
    "stop_times.txt": ["trip_id", "stop_sequence", "stop_id",
                       "platform", "arrival_time", "departure_time",],

    "stops.txt": ["stop_id", "stop_code", "stop_name", "stop_lat", "stop_lon",
                  "location_type", "parent_station"],
})

GTFS_HEADERS_BUS = GTFS_HEADERS.copy()
GTFS_HEADERS_BUS.update({
    "trips.txt": ["route_id", "trip_id", "service_id", "trip_headsign",
                  "wheelchair_accessible", "trip_pattern_id"],
    
    "stop_times.txt": ["trip_id", "stop_sequence", "stop_id",
                       "arrival_time", "departure_time",
                       "pickup_type", "drop_off_type"],
})

SEPARATE_STOPS = {
    "Waseda", "Kuramae", "Nakanobu",
    "Suidobashi", "HongoSanchome",
    "Ryogoku", "Kumanomae"
}

ALERT_EFFECTS = {
    "運転見合わせ": 1, "運転被約": 2, "遅延": 3, "運行情報あり": 6, "お知らせ": 6, "直通運転中止": 1,
}

ALERT_CAUSES = {
    "車両点検": 9, "車輪空転": 3, "大雨": 8, "大雪": 8, "地震": 6, "線路に支障物": 6, "シカと衝突": 6,
    "接続待合せ": 3, "異音の確認": 3, "架線点検": 3, "踏切に支障物": 6, "架線に支障物": 6, "事故": 6,
}

ADDITIONAL_ENGLISH = {
    "循環": "Loop",
}

GET_TIMEOUT = 30

HEADER = r"""
|  _____     _                 ____ _____ _____ ____   |
| |_   _|__ | | ___   _  ___  / ___|_   _|  ___/ ___|  |
|   | |/ _ \| |/ / | | |/ _ \| |  _  | | | |_  \___ \  |
|   | | (_) |   <| |_| | (_) | |_| | | | |  _|  ___) | |
|   |_|\___/|_|\_\\__, |\___/ \____| |_| |_|   |____/  |
|                 |___/                                |
"""
