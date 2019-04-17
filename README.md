TokyoGTFS
==========



Descrpition
-----------

Make GTFS and GTFS-Realtime feeds for Tokyo from data provided by [Open Data Challenge for Public Transportation in Tokyo](https://tokyochallenge.odpt.org/).



Precautions
-----------
Before using this script you're going to have to get an apikey for Open Data Challenge.
You can do this at the [OPDT website](https://tokyochallenge.odpt.org/en/index.html#entry).

Then put this apikey in a file called `apikey.txt` where python scripts are provided, or provide it as command line arguments for the script, like `python3 <script_name>.py -a YOUR-APIKEY`.



Running
-------

TokyoGTFS is written in [Python3](https://python.org) and depends on several external modules:
- [ijson](https://pypi.org/project/ijson/)
- [Requests](http://docs.python-requests.org/en/master/),
- [html5lib](https://pypi.org/project/html5lib/),
- [BeautifulSoup4](https://www.crummy.com/software/BeautifulSoup/),
- [pykakasi](https://pypi.org/project/pykakasi/),
- [iso8601](https://pypi.org/project/iso8601/),
- [gtfs-realtime-bindings](https://github.com/google/gtfs-realtime-bindings/tree/master/python),

Before launching install those using `pip3 install -r requirements.txt`.

Currently there are 4 scripts available:
- *trains_gtfs.py*: to create train schedules in GTFS format,
- *trains_ekikara.py*: to load timetables for trains without odpt:TrainTimetable data available.  
  This file isn't meant to be run standalone, its functions are incoroporated with *trains_gtfs.py* script.
- *trains_realtime.py*: to create GTFS-Realtime feed for trains, based on GTFS feed created by *trains_gtfs.py*.
- *buses_gtfs.py*: to create bus schedules in GTFS format.



Launch the desired script with `python3 <script_file>.py`. Please make sure you've provided the apikey as written earlier.

Major scripts have more options available. For a description of them run `python3 <script_file>.py --help`.


**NOTE**:
Windows users may need to run `py -m pip ...` instead of `pip3 ...` and `py ...` instead of `python3 ...`.



Attributions
------------
Use created data according to [API Use Guidelines](https://developer-tokyochallenge.odpt.org/en/terms/api_guideline.html),
[API Use Permission Rules](https://developer-tokyochallenge.odpt.org/en/terms/terms_api_usage.html) and [TokyoGTFS data license](https://github.com/MKuranowski/TokyoGTFS/tree/master/data).

The source of data used for GTFS-creating scripts is the Open Data Challenge for Public Transportation in Tokyo.
They are based on the data provided by the public transportation operators.
The accuracy and integrity of the data are not guaranteed.
Please do not contact the public transportation operators directly regarding the content of created GTFS/GTFS-Realtime feeds.
For inquiries on this script, use the [GitHub's Issues page](https://github.com/MKuranowski/TokyoGTFS/issues/).

Produced GTFS feeds include data from Mikołaj Kuranowski's [TokyoGTFS](https://github.com/MKuranowski/TokyoGTFS/) project, shared under the [CC BY 4.0 license](https://creativecommons.org/licenses/by/4.0/).



License
-------

TokyoGTFS is shared under the [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) license, included in the file *license.md*.

This script may be used for any use as long as alongside the script a contribution is stated, that includes:
- Author's name: Mikołaj Kuranowski,
- Link to TokyoGTFS project: https://github.com/MKuranowski/TokyoGTFS,
- Link to CC BY 4.0 license: https://creativecommons.org/licenses/by/4.0/,
- List of changes made to the original script.
