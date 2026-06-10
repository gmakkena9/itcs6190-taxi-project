"""Download a real NYC TLC Yellow Taxi monthly file for the validation run.

The full TLC trip records are *not* committed to this repository (see
``data/external.txt``); development uses the synthetic sample. This helper
fetches one real monthly Parquet file plus the official Taxi Zone Lookup CSV
into ``data/real/`` so the same pipeline can be validated against authentic data
(see ``docs/real_data_validation.md``).

Usage
-----
    python3 data/download_tlc.py              # default: 2024-01
    python3 data/download_tlc.py 2024-03      # a specific YYYY-MM

Then run the pipeline against it:
    TAXI_SOURCE=data/real/yellow_tripdata_2024-01.parquet \
    TAXI_ZONES=data/real/taxi_zone_lookup.csv \
    bash run.sh

The files are published by the City of New York as open data. URLs follow the
documented CloudFront pattern used on the TLC trip-record page:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
"""
from __future__ import annotations

import os
import sys
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
REAL_DIR = os.path.join(HERE, "real")

TRIPS_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{ym}.parquet"
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def _download(url: str, dest: str) -> None:
    if os.path.exists(dest):
        print(f"  already present: {dest}")
        return
    print(f"  downloading {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "itcs6190-taxi-project"})
    with urllib.request.urlopen(req, timeout=120) as resp, open(dest, "wb") as out:
        out.write(resp.read())
    size_mb = os.path.getsize(dest) / 1e6
    print(f"  saved {dest} ({size_mb:.1f} MB)")


def main(ym: str = "2024-01") -> None:
    os.makedirs(REAL_DIR, exist_ok=True)
    trips_dest = os.path.join(REAL_DIR, f"yellow_tripdata_{ym}.parquet")
    zones_dest = os.path.join(REAL_DIR, "taxi_zone_lookup.csv")
    print(f"Fetching real TLC data for {ym} into {REAL_DIR}/")
    _download(TRIPS_URL.format(ym=ym), trips_dest)
    _download(ZONES_URL, zones_dest)
    print("\nDone. Validate the pipeline against it with:")
    print(f"  TAXI_SOURCE={trips_dest} \\")
    print(f"  TAXI_ZONES={zones_dest} \\")
    print("  bash run.sh")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "2024-01")
