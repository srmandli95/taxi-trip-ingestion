#!/usr/bin/env python3
import os, sys, json, time, math, argparse, datetime as dt
from pathlib import Path
import requests

# Optional deps for quick Parquet flatten (weather)
try:
    import pandas as pd
except Exception:
    pd = None

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
RAW  = DATA / "raw"
RAW.mkdir(parents=True, exist_ok=True)

# ---------- Helpers ----------
def save_bytes(url: str, out_path: Path, headers=None):
    r = requests.get(url, headers=headers or {}, timeout=120)
    r.raise_for_status()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(r.content)
    return out_path

def save_json(url: str, out_path: Path, headers=None, payload=None, method="GET"):
    if method.upper() == "POST":
        r = requests.post(url, headers=headers or {}, data=payload or {}, timeout=180)
    else:
        r = requests.get(url, headers=headers or {}, params=payload or {}, timeout=180)
    r.raise_for_status()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(r.text)
    return out_path

def month_iter(start: str, end: str):
    """Yield (YYYY, MM) inclusive for 'YYYY-MM' strings."""
    y, m = map(int, start.split("-"))
    y2, m2 = map(int, end.split("-"))
    cur = dt.date(y, m, 1)
    endd = dt.date(y2, m2, 1)
    while cur <= endd:
        yield cur.year, cur.month
        # next month
        if cur.month == 12:
            cur = dt.date(cur.year + 1, 1, 1)
        else:
            cur = dt.date(cur.year, cur.month + 1, 1)

# ---------- 1) Yellow taxi monthly ----------
def fetch_tlc_yellow(start_month: str, end_month: str):
    base = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    saved = []
    for y, m in month_iter(start_month, end_month):
        fn = f"yellow_tripdata_{y}-{m:02d}.parquet"
        url = f"{base}/{fn}"
        out = RAW / "trips" / "yellow" / f"{y}" / f"{fn}"
        print(f"[TLC] {url} -> {out}")
        try:
            save_bytes(url, out)
            saved.append(out)
        except Exception as e:
            print(f"  ! skip {fn}: {e}")
    return saved

# ---------- 2) Taxi zones (lookup CSV) ----------
def fetch_taxi_zones():
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    out = RAW / "geo" / "taxi_zone_lookup.csv"
    print(f"[Zones] {url} -> {out}")
    save_bytes(url, out)
    return out

# ---------- 3) Weather (Open-Meteo Historical) ----------
# Docs: https://open-meteo.com/en/docs/historical-weather-api
def fetch_openmeteo_hourly(lat=40.71, lon=-74.01, start_date="2024-01-01", end_date="2024-01-31"):
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,precipitation",
        "timezone": "UTC",
    }
    out_json = RAW / "weather" / f"openmeteo_hourly_{start_date}_{end_date}.json"
    print(f"[Weather] {url} {params} -> {out_json}")
    save_json(url, out_json, payload=params)

    # Optional: write a small Parquet for quick EDA
    if pd is not None:
        try:
            obj = json.loads(out_json.read_text())
            hours = obj.get("hourly", {}).get("time", [])
            temp = obj.get("hourly", {}).get("temperature_2m", [])
            prec = obj.get("hourly", {}).get("precipitation", [])
            if hours and len(hours) == len(temp) == len(prec):
                df = pd.DataFrame({"ts_utc": hours, "temp_c": temp, "precip_mm": prec})
                out_pq = RAW / "weather" / f"openmeteo_hourly_{start_date}_{end_date}.parquet"
                df.to_parquet(out_pq, index=False)
                print(f"  wrote Parquet rows={len(df)} -> {out_pq}")
        except Exception as e:
            print(f"  (parquet flatten skipped): {e}")

    return out_json

# ---------- 4) NYC Permitted Events (Socrata) ----------
# Current dataset: tvpp-9vvx (NYC Permitted Event Information)
# You can set NYC_APP_TOKEN env var to improve rate limits.
def fetch_nyc_events(start_date="2024-01-01", end_date="2024-01-31", limit=50000):
    base = "https://data.cityofnewyork.us/resource/tvpp-9vvx.json"
    token = os.environ.get("NYC_APP_TOKEN")
    headers = {"X-App-Token": token} if token else {}
    where = f"start_date between '{start_date}T00:00:00.000' and '{end_date}T23:59:59.999'"
    params = {
        "$select": "*",
        "$where": where,
        "$order": "start_date DESC",
        "$limit": limit
    }
    out = RAW / "events" / f"nyc_permitted_events_{start_date}_{end_date}.json"
    print(f"[Events] {base} where={where} -> {out}")
    save_json(base, out, headers=headers, payload=params)
    return out

# ---------- 5) OSM Overpass (POIs) ----------
# Default: airports, theatres, bus/ferry terminals within Manhattan admin area
def fetch_osm_pois():
    url = "https://overpass-api.de/api/interpreter"
    query = r"""
[out:json][timeout:60];
area["name"="Manhattan"]->.a;
(
  node["aeroway"="aerodrome"](area.a);
  node["amenity"~"theatre|bus_station|ferry_terminal"](area.a);
);
out center;
"""
    out = RAW / "pois" / "osm_pois_manhattan.json"
    print(f"[OSM] Overpass -> {out}")
    save_json(url, out, method="POST", payload=query)
    return out

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(description="Download NYC taxi + enrichment data locally.")
    ap.add_argument("--trips-start", default="2024-01", help="Start month YYYY-MM for yellow taxi")
    ap.add_argument("--trips-end",   default="2024-01", help="End month YYYY-MM for yellow taxi")
    ap.add_argument("--wx-start",    default="2024-01-01", help="Weather start date YYYY-MM-DD")
    ap.add_argument("--wx-end",      default="2024-01-31", help="Weather end date YYYY-MM-DD")
    ap.add_argument("--lat", type=float, default=40.71)
    ap.add_argument("--lon", type=float, default=-74.01)
    ap.add_argument("--no-events", action="store_true", help="Skip NYC events")
    ap.add_argument("--no-pois",   action="store_true", help="Skip OSM POIs")
    args = ap.parse_args()

    print("=== Downloading Yellow Taxi Trips ===")
    fetch_tlc_yellow(args.trips_start, args.trips_end)

    print("=== Downloading Taxi Zones ===")
    fetch_taxi_zones()

    print("=== Downloading Weather (Open-Meteo) ===")
    fetch_openmeteo_hourly(args.lat, args.lon, args.wx_start, args.wx_end)

    if not args.no_events:
        print("=== Downloading NYC Events ===")
        fetch_nyc_events(args.wx_start, args.wx_end)

    if not args.no_pois:
        print("=== Downloading OSM POIs ===")
        fetch_osm_pois()

    print("\n✅ Done. Files are under ./data/raw/")
    print("   Open in Jupyter and explore!")

if __name__ == "__main__":
    sys.exit(main())
