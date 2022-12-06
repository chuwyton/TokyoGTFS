import argparse
import time
import sys
import os

from src.static_buses import BusParser
from src.const import HEADER

if __name__ == "__main__":
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument("-a", "--apikey", metavar="YOUR_APIKEY", 
                             help="apikey from api.odpt.org")
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
    print("=== Buses GTFS: Starting! ===")

    print("Warming up")
    BusParser.parse(apikey)

    total_time = time.time() - start_time
    print("=== Buses: Finished in {} s ===".format(round(total_time, 2)))
