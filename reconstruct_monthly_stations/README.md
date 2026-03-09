# Reconstruct Monthly Stations

Reconstructs which Gogoro battery swap stations were active each month (2019-01 to 2023-12) across six major Taiwan cities: 臺北市, 新北市, 桃園市, 臺中市, 臺南市, 高雄市.

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

1. **Activity status** — replicates `batterystation_month.py` logic verbatim: melts activate/remove event columns, builds a full station×month panel, forward/backward fills status.
2. **Coordinates** — joined by station `Name` across the four sources above (priority order). 28 stations have slightly mismatched names between `gostations_cleaned.csv` and `station_big.xlsx`; a hardcoded alias dict in `build_coords()` resolves them. After aliasing, no station in the six cities is missing coordinates.
3. **City filter** — applied before the coordinate join using the `county` column in `gostations_cleaned.csv`.

## Validation

Spot-checked against `batterystation_month.py` (ground truth):

| Month | Taipei stations |
|-------|----------------|
| 2019-01 | 64 ✓ |
| 2023-12 | 230 ✓ |
