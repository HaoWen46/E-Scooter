# Reconstruct Monthly Stations

We want a monthly panel of which Gogoro battery swap stations were open and where — one CSV per month, covering 2019-01 through 2023-12, so we can track the network's geographic expansion over time across six major Taiwan cities: 臺北市, 新北市, 桃園市, 臺中市, 臺南市, 高雄市.

The catch is that the raw data is a mess of lifecycle event logs, not clean snapshots. Stations get activated, removed, reactivated. Some were already gone by the time the data was scraped, so their coordinates aren't even in the main file. This script pieces it all together.

Output: `stations_monthly/stations_YYYY_MM.csv` — columns: `vmid`, `Longitude`, `Latitude`

## Run

Place all input files in the working directory, then:

```bash
uv pip install pandas numpy openpyxl
uv run python generate_monthly_stations.py
```

## Input Files

| File | Role |
|------|------|
| `gostations_cleaned.csv` | Station lifecycle events + county — drives activity status |
| `gostations_with_websites.csv` | Primary coordinate source (active stations) |
| `gostation_deleted.csv` | Coords for deleted stations (parsed from `map_url`) |
| `gostation_removed_from_construction.csv` | Same as above |
| `station_big.xlsx` | Fallback coords for historically removed stations |

## How It Works

### Figuring Out Which Stations Were Active Each Month

Rather than having a clean "open/closed" record per month, all we have is a log of events — when a station was activated, when it was removed. The script reconstructs monthly status from this by treating each station's history as a timeline:

1. Activation and removal timestamps are melted into a single event table.
2. For each station × month, it checks what happened: if the last event was an activation, the station ends the month active; if a removal, inactive.
3. Status is then forward-filled (a station stays active until explicitly removed) and backward-filled (it was presumably active before its first recorded activation).
4. Stations with no events at all fall back to their current status field.

This logic is taken verbatim from `batterystation_month.py`, which serves as the ground truth.

### Getting GPS Coordinates for Every Station

The primary data source only covers stations that were still active at scrape time — deleted stations are simply absent. But we need coordinates for historically active stations too, otherwise we lose them from the panel entirely. So we layer multiple sources:

1. `gostations_with_websites.csv` — the main source for active stations.
2. `gostation_deleted.csv` and `gostation_removed_from_construction.csv` — contain Google Maps URLs with embedded `ll=lat,lon` parameters that can be parsed out.
3. `station_big.xlsx` — a separate dataset that covers most of the remaining historically removed stations.

**One messy bit**: 28 stations had slightly mismatched names between `gostations_cleaned.csv` and `station_big.xlsx` — extra spaces, dropped brand prefixes, or swapped A/B suffixes. These were resolved manually and hardcoded as a name alias table in `build_coords()`. After this fix, no station in the six cities is left without coordinates.

### City Filter

Only stations whose `county` matches one of the six cities are kept. Everything else is dropped before the coordinate join.

## Validation

Spot-checked against `batterystation_month.py` (ground truth):

| Month | Taipei stations |
|-------|----------------|
| 2019-01 | 64 ✓ |
| 2023-12 | 230 ✓ |
