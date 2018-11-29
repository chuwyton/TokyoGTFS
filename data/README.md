ABOUT FILES
-----------

- **bus_colors.csv**: Gives each bus operator a color to all bus routes;
- **operators.csv**: Lists all parsable transit operators in ODPT, with some additional info about them;
- **through_service.csv**: If a train's journey abruplty stops at `from_station`, and the destanation doesn't belong to the route, try looking for this train at `to_station`. **Warning!** This file doen't list transfers if both routes have `odpt:TrainTimetable` data available (JR-East÷JR-East, JR-East÷TokyoMetro, Toei÷Keio are **not** included);
- **train_osm_stops.csv**: Export of train stations around Tokyo from [OpenStreetMap](https://openstreetmap.org);
- **train_routes.csv**: List of all train routes to export to GTFS, with additional data about them.


LICENSING OF FILES IN THIS DIRECTORY
------------------------------------

#### train_osm_stops.csv
This file includes data from [© OpenStreetMap contributors](https://www.openstreetmap.org/copyright/en), licensed under the [Open data Commons Open Database License](https://opendatacommons.org/licenses/odbl/).


#### bus_colors.csv & operators.csv & train_routes.csv & through_service.csv
These files are shared under the [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) license, included in the file *license.md*.

Data in those files may be used as long as you give an appropiate contribution, by providing:
- Author's name: Mikołaj Kuranowski,
- Link to TokyoGTFS project: https://github.com/MKuranowski/TokyoGTFS,
- Link to CC BY 4.0 license: https://creativecommons.org/licenses/by/4.0/,
- List of changes made to the original data.
