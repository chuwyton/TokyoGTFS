ABOUT FILES
-----------

- **bus_colors.csv**: Gives each bus operator a color to all bus routes;
- **operators.csv**: Lists all parsable transit operators in ODPT, with some additional info about them;
- **train_routes.csv**: List of all train routes to export to GTFS, with additional data about them;
- **train_station_fixes.csv**: Some stations have incorrect positions. This table lists better station coordinates for such stations.
- **train_through_service.csv**: If a train's journey abruplty stops at `from_station`, and the destanation doesn't belong to parsed route, try looking for this train at `to_station`. **Warning!** This file doen't list transfers if both routes have `odpt:TrainTimetable` data available (JR-East÷JR-East, JR-East÷TokyoMetro, JR-East÷TWR, Toei÷Keio are **not** included);


LICENSING OF FILES IN THIS DIRECTORY
------------------------------------

All files in this directory are shared under the [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) license, included in the file *license.md*.

Data in those files may be used as long as you give an appropiate contribution, by providing:
- Author's name: Mikołaj Kuranowski,
- Link to TokyoGTFS project: https://github.com/MKuranowski/TokyoGTFS,
- Link to CC BY 4.0 license: https://creativecommons.org/licenses/by/4.0/,
- List of changes made to the original data (if any).
