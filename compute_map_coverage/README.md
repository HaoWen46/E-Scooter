# Coverage Pipeline Documentation

## Overview

Two separate pipelines produce two kinds of output from the same underlying analysis (PyQGIS service area on scooter-accessible roads):

| Pipeline | Script | Output |
|----------|--------|--------|
| Monthly CSV | `compute_job.py` + `run_all_parallel.sh` | `out/coverage_{CITY}_{YYYY_MM}.csv` — 360 files (6 cities × 60 months) |
| Selective GPKG | `compute_gpkg.py` + `run_gpkg_batch.sh` | `out/gpkg/coverage_{CITY}_{YYYY_MM}.gpkg` — 18 files (6 cities × Dec 2019/2021/2023) |

---

## Shared Inputs

**Station data** (`data/stations_monthly/stations_YYYY_MM.csv`)
- Columns: `vmid, Longitude, Latitude` (WGS84 / EPSG:4326)
- Monthly snapshots of EV charging station positions

**Road + boundary data** (`base.gpkg`)
- `roads_scooter` — all-city roads excluding ROADCLASS1='HW' (highways), EPSG:3826
- `town_{CITY}` — district polygons for each city
- `b_{CITY}` — dissolved city boundary polygon

**CRS**: EPSG:3826 (TWD97/TM2) throughout analysis. Stations are reprojected from WGS84 at load time.

**Distance bands**: 500 m, 1000 m, 1500 m network distance.

---

## Monthly CSV Pipeline

### Purpose
Compute per-district road coverage statistics for every city × month combination (360 jobs total), for time-series analysis.

### Scripts

#### `compute_job.py` (core analysis)

Steps for one `CITY YYYY_MM` job:

1. **Load** `roads_scooter`, `town_{CITY}`, `b_{CITY}` from `base.gpkg`
2. **Load stations** CSV as delimited-text layer; reproject to EPSG:3826
3. **Clip** stations to city boundary (`native:clip`)
4. **Clip** roads to city boundary
5. **Service area** (`native:serviceareafromlayer`) at 500 m, 1000 m, 1500 m — outputs road line segments reachable within each distance from any station
   - Batch dissolve (groups of 20) then final dissolve to merge all station results per distance
6. **`native:sumlinelengths`** (chained 4×): accumulates `len_total`, `cum_500`, `cum_1000`, `cum_1500` per district polygon
7. **Export CSV** with computed band lengths and percentages

Output CSV columns:

| Column | Meaning |
|--------|---------|
| `TOWNID`, `TOWNNAME` | District identifiers |
| `len_total_m` | Total scooter road length in district (m) |
| `len_0_500_m` | Road length within 500 m of a station |
| `len_500_1000_m` | Road length 500–1000 m from a station |
| `len_1000_1500_m` | Road length 1000–1500 m from a station |
| `len_gt1500_m` | Road length beyond 1500 m |
| `reach500_pct` | cum_500 / len_total × 100 |
| `reach1k_pct` | cum_1000 / len_total × 100 |
| `reach1500_pct` | cum_1500 / len_total × 100 |

#### `run_one.sh` (job wrapper)

- Activates micromamba `qgis` env
- Skips job if CSV already exists (NFS-safe deduplication)
- Sets per-job isolation dirs (`TMPDIR`, `XDG_CACHE_HOME`, `XDG_CONFIG_HOME`, `QGIS_CUSTOM_CONFIG_PATH`) under `out/tmp/`
- Treats exit 134/139/137 (QGIS cleanup crash) as success if CSV was written
- Writes per-job log to `out/logs/{CITY}_{YYYY_MM}.log`

#### `run_all_parallel.sh` (dispatcher)

- Builds job list: 6 cities × months 2019-01 through 2023-12
- Optional `ONLY_MONTHS=1,2,3` env var to filter to specific months — used to distribute work across machines sharing NFS storage
- Per-hostname joblog: `out/logs/joblog_{hostname}` (avoids NFS collision)
- Memory-aware parallelism: reads `MemAvailable` and cgroup `memory.max`, computes `NJOBS = (budget - buffer) / per_job`; defaults to `MAX_JOBS=1`
- Runs via GNU `parallel --resume-failed --retries 2`

**Multi-machine NFS split example:**
```bash
# Machine 1: ONLY_MONTHS=1,2,3 nohup ./run_all_parallel.sh > out/logs/run_all.log 2>&1 &
# Machine 2: ONLY_MONTHS=4,5,6 ...
# Machine 3: ONLY_MONTHS=7,8,9 ...
# Machine 4: ONLY_MONTHS=10,11,12 ...
```

---

## GPKG Pipeline

### Purpose
Produce full spatial layers (not just statistics) for three reference months (Dec 2019, 2021, 2023) in each city — for map rendering and spatial inspection.

### Scripts

#### `compute_gpkg.py` (core analysis)

Steps are identical to `compute_job.py` through the service area + sumlinelengths stages. Additional steps:

- Adds `reach500_pct`, `reach1k_pct`, `reach1500_pct` as computed fields on the polygon layer (via `native:fieldcalculator`)
- Writes a **5-layer GeoPackage** using `QgsVectorFileWriter.writeAsVectorFormatV3`:

| Layer | Geometry | Content |
|-------|----------|---------|
| `towns_coverage` | Polygon | District polygons with all coverage stats |
| `sa_500` | LineString | Dissolved roads within 500 m of any station |
| `sa_1000` | LineString | Dissolved roads within 1000 m |
| `sa_1500` | LineString | Dissolved roads within 1500 m |
| `stations` | Point | Charging stations clipped to city (EPSG:3826) |

#### `run_gpkg_batch.sh` (dispatcher)

- Fixed job matrix: 6 cities × `[2019_12, 2021_12, 2023_12]` = 18 jobs
- Skips if GPKG already exists
- Same per-job isolation pattern as `run_one.sh`
- `MAX_JOBS=1` default (memory-intensive jobs)
- Joblog at `out/logs/joblog_gpkg`

---

## Environment

**Compute nodes**: 4 machines sharing NFS storage, primary node `master` (140.112.176.245), each with:
- CPU: 2× Intel Xeon Gold 6242 @ 2.80 GHz (16 cores/socket, 32 cores / 64 threads total)
- RAM: ~1006 GiB

**Work distribution**: `ONLY_MONTHS` env var splits the 360 CSV jobs across machines (e.g. months 1–3, 4–6, 7–9, 10–12). Each machine runs one job at a time (`MAX_JOBS=1`) due to per-job peak memory of ~22 GiB. GPKG batch (18 jobs) runs on a single machine.

**Package manager**: micromamba (conda-forge channel), environment name `qgis`

**Key packages**:

| Package | Version |
|---------|---------|
| Python | 3.14.3 |
| QGIS | 3.44.7 (Solothurn) |
| GDAL | 3.12.2 |
| PROJ | 9.7.1 |
| GEOS | 3.14.1 |
| PyQGIS / PyQt | 5.15.11 |
| NumPy | 2.4.2 |
| libspatialindex | 2.0.0 |
| GNU parallel | 20260122 |

**Headless**: `QT_QPA_PLATFORM=offscreen`

QGIS exits 134/139/137 during Python cleanup are harmless; all scripts detect and treat them as success when output files exist.
