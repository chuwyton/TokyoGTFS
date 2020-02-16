import argparse
import time
import sys
import os

from src.rt_trains import TrainRealtime
from src.const import HEADER

if __name__ == "__main__":
    args_parser = argparse.ArgumentParser()

    args_parser.add_argument(
        "-a", "--apikey", metavar="YOUR-APIKEY",
        help="apikey from developer-tokyochallenge.odpt.org"
    )

    args_parser.add_argument(
        "-g", "--gtfs", metavar="PATH-TO-TRAINS-GTFS.zip", default="tokyo_trains.zip",
        help="path to GTFS created by trains_gtfs.py"
    )

    args_parser.add_argument(
        "-hr", "--human-readable", action="store_true",
        help="output gtfs-realtime file as human-readable instead of binary"
    )

    args = args_parser.parse_args()

    # Apikey checks
    if args.apikey:
        apikey = args.apikey

    elif os.path.exists("apikey.txt"):
        with open("apikey.txt", mode="r", encoding="utf8") as f:
            apikey = f.read().strip()

    else:
        sys.exit(
            "No apikey!\n"
            "Provide it inside command line argument '--apikey',\n"
            "Or put it inside a file named 'apikey.txt'."
        )

    start_time = time.time()
    print(HEADER)
    print("=== Trains GTFS-RT: Starting! ===")
    
    print("Warming up")
    TrainRealtime.parse_once(apikey, args.human_readable, args.gtfs)
    
    total_time = time.time() - start_time
    print("=== TokyoGTFS: Finished in {} s ===".format(round(total_time, 2)))
