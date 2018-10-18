TokyoGTFS
==========



Descrpition
-----------

Make GTFS and GTFS-Realtime feeds for Tokyo from data provided by [Open Data Challenge for Public Transportation in Tokyo](https://tokyochallenge.odpt.org/).



Precautions
-----------
Before using this script you're going to have to get an apikey for Open Data Challenge.
You can do this at the [OPDT website](https://tokyochallenge.odpt.org/en/index.html#entry).

Then put this apikey in a file called `apikey.txt` where python scripts are provided, or provide it as command line arguments for the script, like `python3 <script_name>.py -k YOUR-APIKEY`.



Running
-------

TokyoGTFS is written in [Python3](https://python.org) and depends on several external modules:
- [Requests](http://docs.python-requests.org/en/master/),
- [BeautifulSoup4](https://www.crummy.com/software/BeautifulSoup/),
- [pytz](http://pytz.sourceforge.net/),
- [iso8601](https://pypi.org/project/iso8601/).

Before launching install those using `pip3 install -r requirements.txt`.

Currently there are 2 scripts available:
- *trains_gtfs.py*: to create static train data in GTFS format,
- *trains_realtime.py*: to create dynamic train data in GTFS-Realtime format.



Launch the desired script with `python3 <script_file>.py`. Please make sure you've provided the apikey as written earlier.

All of the scripts have more options available. For a description of them run `python3 <script_file>.py --help`.


**NOTE**:
Windows users may need to run `py -m pip ...` instead of `pip3 ...` and `py ...` instead of `python3 ...`.



Attributions
------------
Use created data according to [API Use Guidelines](https://developer-tokyochallenge.odpt.org/en/terms/api_guideline.html),
[API Use Permission Rules](https://developer-tokyochallenge.odpt.org/en/terms/terms_api_usage.html), [TokyoGTFS data license](https://github.com/MKuranowski/TokyoGTFS/tree/master/data) and, if appropiate, [OSM copyright](https://www.openstreetmap.org/copyright/en).

The source of data used for creating GTFS and GTFS-Realtime scripts is the Open Data Challenge for Public Transportation in Tokyo.
They are based on the data provided by the public transportation operators.
The accuracy and integrity of the data are not guaranteed.
Please do not contact the public transportation operators directly regarding the content of created GTFS/GTFS-Realtime feeds.
For inquiries on this script, use the [GitHub's Issues page](https://github.com/MKuranowski/TokyoGTFS/issues/).

Produced GTFS and GTFS-Realtime feeds include data from Mikołaj Kuranowski's [TokyoGTFS](https://github.com/MKuranowski/TokyoGTFS/) project, shared under the [CC BY 4.0 license](https://creativecommons.org/licenses/by/4.0/).


Also, with a special command line option (`--use-osm` or `-osm`),
the tokyo_trains.zip GTFS *will* include data from [© OpenStreetMap contributors](https://www.openstreetmap.org/copyright/en), licensed under the [Open Data Commons Open Database License](https://opendatacommons.org/licenses/odbl/).



License
-------

TokyoGTFS is shared under the [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) license, included in the file *license.md*.

This script may be used for any use as long as alongside the sciprt a contribution is stated, that includes:
- Author's name: Mikołaj Kuranowski,
- Link to TokyoGTFS project: https://github.com/MKuranowski/TokyoGTFS,
- Link to CC BY 4.0 license: https://creativecommons.org/licenses/by/4.0/,
- List of changes made to the original script.
