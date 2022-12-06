TokyoGTFS
==========

UPDATING TOKYO GTFS R2R PACKAGE
----------

- Run `trains_gtfs.py` and `buses_gtfs.py` as described below
- Add the output `trains.zip` and `buses.zip` to the [GTFS Feed Manager](https://content.rome2rio.com/gtfs-feed-manager) to `jp.tokyo.buses` and `jp.tokyo.trains` respectively and save each one
- Wait until the GTFS Scraper & Transit Builder next run, and then these new files will be added to the product

Docker build process
-----------
* Build the container image first
  * `docker build -t r2r/tokyogtfs:latest .`
* Then run the container with the command line args and set the api key. The API key can be found in 1password or you will need to sign up here [Public Transportation Open Data Centre](https://www.odpt.org/en/)
  * Double check the mounted directory matches the fulle path to the output folder that you want. It is the left hand side of the `-v` arg up to the colon `:`
  * `docker run -v C:\src\TokyoGTFS\output:/app/output --name tokyogtfs r2r/tokyogtfs:latest run_gtfs.py -a YOUR-APIKEY`
* Check the output folder `output` for the zip files and add them to the JIRA ticket for a content user to perform the upload and testing

Approximate time for completion as of 2022-12-06:
* trains - 80 seconds
* busses - 12 minutes


Original ReadMe
-----------

Description
-----------

Make GTFS and GTFS-Realtime feeds for Tokyo from data provided by [Public Transportation Open Data Centre](https://www.odpt.org/en/).

Precautions
-----------
Before using this script you're going to have to get an apikey for Open Data Challenge.
You can do this at the [ODPT website](https://tokyochallenge.odpt.org/en/index.html#entry).

+++ API Key can be found here: https://api.odpt.org/oauth/applications?locale=en +++
Then put this apikey in a file called `apikey.txt` where python scripts are provided, or provide it as command line arguments for the script, like `python3 <script_name>.py -a YOUR-APIKEY`.

+++ This Key can also be found in LastPass, under "Tokyo GTFS Key" in Shared-Content Tools


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

Currently there are 3 scripts available:
- *trains_gtfs.py*: to create train schedules in GTFS format,
- *trains_realtime.py*: to create GTFS-Realtime feed for trains, based on GTFS feed created by *trains_gtfs.py*.
- *buses_gtfs.py*: to create bus schedules in GTFS format.



Launch the desired script with `python3 <script_file>.py`. Please make sure you've provided the apikey as written earlier.

Major scripts have more options available. For a description of them run `python3 <script_file>.py --help`.


**NOTE**:
Windows users may need to run `py -m pip ...` instead of `pip3 ...` and `py ...` instead of `python3 ...`.



Attributions
------------
Use created data according to [API Use Guidelines](https://api.odpt.org/en/terms/api_guideline.html),
[API Use Permission Rules](https://api.odpt.org/en/terms/terms_api_usage.html).

The source of data used for GTFS-creating scripts is the Open Data Challenge for Public Transportation in Tokyo.
They are based on the data provided by the public transportation operators.
The accuracy and integrity of the data are not guaranteed.
Please do not contact the public transportation operators directly regarding the content of created GTFS/GTFS-Realtime feeds.
For inquiries on this script, use the [GitHub's Issues page](https://github.com/MKuranowski/TokyoGTFS/issues/).



License
-------

TokyoGTFS is shared under the [MIT License](https://github.com/MKuranowski/TokyoGTFS/blob/master/LICENSE.md) license, included in the file *license.md*.
