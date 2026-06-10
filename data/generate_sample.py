"""Generate a small, representative NYC Yellow Taxi sample for local runs.

The real dataset (TLC Trip Record Data) is hundreds of millions of rows and is
stored externally (see ``data/external.txt``). To keep the repository runnable
offline with a single command, this script synthesizes a small sample that
mirrors the official schema and contains realistic statistical structure:

* trip duration is driven by distance and time-of-day congestion (so the
  regression model has genuine signal to learn),
* a taxi-zone lookup table enables a DataFrame join,
* a small fraction of dirty rows is injected so the cleaning stage is meaningful.

The generator is seeded, so results are reproducible.

Outputs
-------
data/sample/yellow_tripdata_sample.csv : trip records (TLC-style schema)
data/sample/taxi_zone_lookup.csv       : LocationID -> Borough/Zone lookup
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

SEED = 42
N_TRIPS = 40_000
START = datetime(2024, 1, 1)
DAYS = 31

HERE = os.path.dirname(os.path.abspath(__file__))
SAMPLE_DIR = os.path.join(HERE, "sample")

# A representative subset of the official 265 TLC taxi zones.
ZONES = [
    (4, "Manhattan", "Alphabet City", "Yellow Zone"),
    (13, "Manhattan", "Battery Park City", "Yellow Zone"),
    (24, "Manhattan", "Bloomingdale", "Yellow Zone"),
    (41, "Manhattan", "Central Harlem", "Boro Zone"),
    (43, "Manhattan", "Central Park", "Yellow Zone"),
    (48, "Manhattan", "Clinton East", "Yellow Zone"),
    (50, "Manhattan", "Clinton West", "Yellow Zone"),
    (68, "Manhattan", "East Chelsea", "Yellow Zone"),
    (79, "Manhattan", "East Village", "Yellow Zone"),
    (90, "Manhattan", "Flatiron", "Yellow Zone"),
    (100, "Manhattan", "Garment District", "Yellow Zone"),
    (107, "Manhattan", "Gramercy", "Yellow Zone"),
    (113, "Manhattan", "Greenwich Village North", "Yellow Zone"),
    (125, "Manhattan", "Hudson Sq", "Yellow Zone"),
    (132, "Queens", "JFK Airport", "Airports"),
    (138, "Queens", "LaGuardia Airport", "Airports"),
    (140, "Manhattan", "Lenox Hill East", "Yellow Zone"),
    (142, "Manhattan", "Lincoln Square East", "Yellow Zone"),
    (161, "Manhattan", "Midtown Center", "Yellow Zone"),
    (163, "Manhattan", "Midtown North", "Yellow Zone"),
    (170, "Manhattan", "Murray Hill", "Yellow Zone"),
    (186, "Manhattan", "Penn Station/Madison Sq West", "Yellow Zone"),
    (230, "Manhattan", "Times Sq/Theatre District", "Yellow Zone"),
    (236, "Manhattan", "Upper East Side North", "Yellow Zone"),
    (237, "Manhattan", "Upper East Side South", "Yellow Zone"),
    (162, "Manhattan", "Midtown East", "Yellow Zone"),
    (114, "Manhattan", "Greenwich Village South", "Yellow Zone"),
    (158, "Manhattan", "Meatpacking/West Village West", "Yellow Zone"),
    (33, "Brooklyn", "Brooklyn Heights", "Boro Zone"),
    (256, "Brooklyn", "Williamsburg (South Side)", "Boro Zone"),
    (255, "Brooklyn", "Williamsburg (North Side)", "Boro Zone"),
    (61, "Brooklyn", "Crown Heights North", "Boro Zone"),
    (7, "Queens", "Astoria", "Boro Zone"),
    (82, "Queens", "Elmhurst", "Boro Zone"),
    (260, "Queens", "Woodside", "Boro Zone"),
]


def _hour_congestion(hour: np.ndarray) -> np.ndarray:
    """Average speed (mph) as a function of pickup hour.

    Rush-hour windows (7-10, 16-19) are slow; the dead of night is fast.
    """
    base = np.full(hour.shape, 18.0)
    morning = (hour >= 7) & (hour <= 10)
    evening = (hour >= 16) & (hour <= 19)
    night = (hour >= 0) & (hour <= 5)
    base[morning] = 11.0
    base[evening] = 9.5
    base[night] = 24.0
    return base


def generate() -> None:
    rng = np.random.default_rng(SEED)
    os.makedirs(SAMPLE_DIR, exist_ok=True)

    loc_ids = np.array([z[0] for z in ZONES])
    # Weight Manhattan midtown/airport zones as more popular pickups.
    weights = np.ones(len(loc_ids), dtype=float)
    for i, z in enumerate(ZONES):
        if z[2] in ("Midtown Center", "Times Sq/Theatre District", "Penn Station/Madison Sq West"):
            weights[i] = 4.0
        elif z[1] == "Manhattan":
            weights[i] = 2.0
        elif z[3] == "Airports":
            weights[i] = 2.5
    weights /= weights.sum()

    n = N_TRIPS
    pu = rng.choice(loc_ids, size=n, p=weights)
    do = rng.choice(loc_ids, size=n, p=weights)

    # Pickup times: hour distribution weighted toward daytime / rush hours.
    hour_p = np.array(
        [0.4, 0.3, 0.2, 0.2, 0.3, 0.6, 1.2, 2.6, 3.2, 2.4, 2.0, 2.2,
         2.4, 2.3, 2.2, 2.6, 3.4, 3.6, 3.2, 2.6, 2.4, 2.2, 1.8, 1.0]
    )
    hour_p /= hour_p.sum()
    hours = rng.choice(np.arange(24), size=n, p=hour_p)
    day = rng.integers(0, DAYS, size=n)
    minute = rng.integers(0, 60, size=n)
    second = rng.integers(0, 60, size=n)
    pickup = np.array(
        [START + timedelta(days=int(d), hours=int(h), minutes=int(m), seconds=int(s))
         for d, h, m, s in zip(day, hours, minute, second)]
    )

    # Distance: lognormal, clipped to a plausible range.
    distance = np.clip(rng.lognormal(mean=0.9, sigma=0.6, size=n), 0.3, 30.0)

    # Duration (minutes) = distance / speed * 60 + fixed overhead + noise.
    speed = _hour_congestion(hours)
    overhead = rng.normal(2.5, 1.0, size=n)          # pickup / traffic-light overhead
    noise = rng.normal(0.0, 2.5, size=n)
    duration_min = distance / speed * 60.0 + overhead + noise
    duration_min = np.clip(duration_min, 1.0, 180.0)

    passenger = rng.choice([1, 1, 1, 2, 2, 3, 4, 5, 6], size=n)
    payment = rng.choice([1, 2], size=n, p=[0.7, 0.3])  # 1=credit, 2=cash

    fare = 3.0 + 1.75 * distance + 0.35 * duration_min + rng.normal(0, 1.5, size=n)
    fare = np.clip(fare, 2.5, None)
    tolls = np.where(rng.random(n) < 0.08, rng.uniform(2, 12, size=n), 0.0)
    tip = np.where(payment == 1, fare * rng.uniform(0.0, 0.3, size=n), 0.0)
    surcharge = 1.0
    total = fare + tolls + tip + surcharge

    dropoff = np.array(
        [p + timedelta(minutes=float(d)) for p, d in zip(pickup, duration_min)]
    )

    df = pd.DataFrame(
        {
            "VendorID": rng.choice([1, 2], size=n),
            "tpep_pickup_datetime": pickup,
            "tpep_dropoff_datetime": dropoff,
            "passenger_count": passenger,
            "trip_distance": np.round(distance, 2),
            "PULocationID": pu,
            "DOLocationID": do,
            "payment_type": payment,
            "fare_amount": np.round(fare, 2),
            "tip_amount": np.round(tip, 2),
            "tolls_amount": np.round(tolls, 2),
            "improvement_surcharge": surcharge,
            "total_amount": np.round(total, 2),
        }
    )

    # Inject ~2% dirty rows so the cleaning stage is meaningful.
    dirty_idx = rng.choice(n, size=int(0.02 * n), replace=False)
    half = len(dirty_idx) // 2
    df.loc[dirty_idx[:half], "trip_distance"] = 0.0          # zero-distance trips
    df.loc[dirty_idx[half:], "fare_amount"] = -5.0           # negative fares
    df.loc[dirty_idx[:half], "tpep_dropoff_datetime"] = df.loc[
        dirty_idx[:half], "tpep_pickup_datetime"
    ]  # zero / negative duration

    df.to_csv(os.path.join(SAMPLE_DIR, "yellow_tripdata_sample.csv"), index=False)

    zones = pd.DataFrame(ZONES, columns=["LocationID", "Borough", "Zone", "service_zone"])
    zones.to_csv(os.path.join(SAMPLE_DIR, "taxi_zone_lookup.csv"), index=False)

    print(f"Wrote {len(df):,} trips and {len(zones)} zones to {SAMPLE_DIR}")


if __name__ == "__main__":
    generate()
