ABOUT FILES
-----------

- **bus_data.csv**: Lists all bus operators and assignes some additional data to them;
- **operators.csv**: Lists all parsable transit operators in ODPT, with some additional info about them;
- **train_routes.csv**: List of all train routes to export to GTFS, with additional data about them;
- **train_station_fixes.csv**: Some stations have incorrect positions. This table lists better station coordinates for such stations;
- **train_stop_headsigns.csv**: Lists stop_headsign fileds for trains, which actually have a changing headsign throughout the trip. If the `only_if_next_station_is` is empty, the headsign applies to all trips of given route and direction at the station.


LICENSING OF FILES IN THIS DIRECTORY
------------------------------------

All files in this directory are shared under the [CC0 1.0 Universal](https://creativecommons.org/publicdomain/zero/1.0/) license, included in the file *license.md*.
