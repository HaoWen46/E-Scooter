# Coverage Pipeline Documentation

## Overview

We want to know how well EV charging infrastructure serves scooter riders across Taiwan's six major cities, and how that has changed over time. The core metric is **road network coverage**: for each administrative district and each month, what fraction of the scooter-accessible road network lies within a given travel distance of a charging station?

We compute this for 6 cities × 60 months (2019–2023), producing 360 per-district CSV summaries. We also generate full spatial outputs (GeoPackage files) for three December snapshots — 2019, 2021, 2023 — to support map-based inspection of infrastructure evolution. These are kept separate because storing full spatial layers for all 360 jobs would be expensive and unnecessary; the CSVs carry the quantitative time series.

| Pipeline | Script | Output |
|----------|--------|--------|
| Monthly CSV | `compute_job.py` + `run_all_parallel.sh` | `out/coverage_{CITY}_{YYYY_MM}.csv` — 360 files (6 cities × 60 months) |
| Selective GPKG | `compute_gpkg.py` + `run_gpkg_batch.sh` | `out/gpkg/coverage_{CITY}_{YYYY_MM}.gpkg` — 18 files (6 cities × Dec 2019/2021/2023) |

---

## Inputs

### Charging station locations

Path: `data/stations_monthly/stations_YYYY_MM.csv`
- 60 files covering 2019-01 through 2023-12
- Columns: `vmid, Longitude, Latitude`
- Coordinates in WGS84 (EPSG:4326); reprojected to EPSG:3826 at load time
- Each file is a monthly snapshot of EV charging station locations in Taiwan

### Road network and administrative boundaries (`base.gpkg`)

The road network and district boundaries come from official city GIS data. Each city directory (`data/TP/`, `data/TC/`, etc.) contains 14 shapefiles, all prefixed by city code (TP→A, TC→B, TN→D, KS→E, NTP→F, TY→H):

| Shapefile | Geometry | Notes |
|-----------|----------|-------|
| `{PFX}_ROAD` | PolyLine | Road network — **used by pipeline** |
| `{PFX}_TOWN` | Polygon | District boundaries — **used by pipeline** |
| `{PFX}_ROADSP` | PolyLine | Road speed data |
| `{PFX}_RDNODE` | Point | Road nodes/intersections |
| `{PFX}_BRIDGE` | PolyLine | Bridges |
| `{PFX}_TUNNEL` | PolyLine | Tunnels |
| `{PFX}_RAIL` | PolyLine | Conventional rail |
| `{PFX}_HSRAIL` | PolyLine | High-speed rail |
| `{PFX}_MRT` | PolyLine | Metro/MRT lines |
| `{PFX}_LRT` | PolyLine | Light rail (KS, NTP only) |
| `{PFX}_MARK` | Point | Landmarks/markers |
| `{PFX}_RIVERA` | Polygon | River areas |
| `{PFX}_RIVERL` | PolyLine | River lines |
| `{PFX}_WATERA` | Polygon | Water bodies |
| `{PFX}_COUNTY` | Polygon | County boundary |

Only `{PFX}_ROAD` and `{PFX}_TOWN` are used. All shapefiles use EPSG:3826.

We pre-process these into `base.gpkg` before running either pipeline. The build is a two-step process (already completed; no need to rerun):

1. **`repair_to_gpkg.sh`** — loads `{PFX}_ROAD.shp` and `{PFX}_TOWN.shp` for all 6 cities and writes them into `repaired.gpkg` as 12 clean layers (`ROAD_TP`, `TOWN_TP`, …, `ROAD_TY`, `TOWN_TY`)

2. **`make_base.sh`** — reads `repaired.gpkg` and builds the 15 analysis-ready layers in `base.gpkg`:
   - Merges all 6 city road layers → `roads_all`
   - Removes `ROADCLASS1='HW'` (national highways) → `roads_scooter`
   - Merges all 6 city town layers → `town_all`; extracts per-city subsets → `town_{CITY}` ×6
   - Dissolves each city's districts into a single boundary polygon → `b_{CITY}` ×6

The highway exclusion step is intentional: scooters are legally prohibited from national highways in Taiwan, so including them would inflate coverage with roads that are effectively inaccessible to the target vehicle type. The `roads_scooter` layer is what both pipelines operate on.

---

## How Coverage Is Computed

For each city × month job, both pipelines follow the same analysis logic:

**1. Load and clip inputs.** Station coordinates (WGS84) are reprojected to EPSG:3826 and clipped to the city boundary. Roads are similarly clipped. This keeps each job self-contained to one city.

**2. Run service area analysis at three distances.** Using QGIS's `native:serviceareafromlayer`, we compute which road segments are reachable within 500 m, 1000 m, and 1500 m of travel along the road network from any station. We use **network distance**, not straight-line buffers — a station 400 m away as the crow flies might require 900 m of actual road travel, and straight-line buffers would overestimate reachability by ignoring topology. The raw output is one set of line segments per station per road; these are dissolved into a single merged layer per distance to represent the collective reach of all stations in the city.

For large cities this dissolve step can be memory-intensive, so we dissolve in batches of 20 before a final merge, preventing peak memory exhaustion.

**3. Measure road lengths per district.** We use QGIS's `native:sumlinelengths` (chained four times) to count, for each district polygon: total road length, and cumulative road length reachable within 500 m / 1000 m / 1500 m. The thresholds are **cumulative** — a road segment within 300 m of a station is counted in all three bands. This means reach percentages are always non-decreasing with distance, which is the natural interpretation of coverage.

We report coverage as **fraction of road length**, not fraction of geographic area. Road length is more meaningful for vehicle users: accessibility is experienced along roads, not across open space, and a district with few but long roads should not be penalised in the same way as a dense urban grid.

**4. Derive output.** Marginal band lengths (`len_500_1000_m`, `len_1000_1500_m`, `len_gt1500_m`) are derived by subtraction, giving a decomposition of the district's road network by proximity to stations. Reach percentages (`reach500_pct`, `reach1k_pct`, `reach1500_pct`) express cumulative coverage as a fraction of total district road length.

---

## Monthly CSV Pipeline

The CSV pipeline is the main time-series output. `compute_job.py` runs the analysis above for one city × month and writes a single CSV. `run_one.sh` wraps it with environment activation, output-existence checks (skip if already done), per-job QGIS isolation, and crash handling. `run_all_parallel.sh` dispatches all 360 jobs via GNU parallel.

### Output columns

| Column | Meaning |
|--------|---------|
| `TOWNID`, `TOWNNAME` | District identifiers |
| `len_total_m` | Total scooter road length in district (m) |
| `len_0_500_m` | Road length within 500 m of a station |
| `len_500_1000_m` | Road length 500–1000 m from a station |
| `len_1000_1500_m` | Road length 1000–1500 m from a station |
| `len_gt1500_m` | Road length beyond 1500 m (uncovered) |
| `reach500_pct` | cum_500 / len_total × 100 |
| `reach1k_pct` | cum_1000 / len_total × 100 |
| `reach1500_pct` | cum_1500 / len_total × 100 |

### Running

Each job peaks at roughly 22 GiB RAM (dominated by the service area dissolve). To avoid OOM, only one job runs at a time per machine (`MAX_JOBS=1`). Across four machines sharing NFS storage, we split by month group to avoid overlap — each machine takes a non-overlapping set of months via the `ONLY_MONTHS` env var:

```bash
# Machine 1
ONLY_MONTHS=1,2,3 nohup ./run_all_parallel.sh > out/logs/run_all.log 2>&1 &
# Machine 2
ONLY_MONTHS=4,5,6 nohup ./run_all_parallel.sh > out/logs/run_all.log 2>&1 &
# Machine 3
ONLY_MONTHS=7,8,9 nohup ./run_all_parallel.sh > out/logs/run_all.log 2>&1 &
# Machine 4
ONLY_MONTHS=10,11,12 nohup ./run_all_parallel.sh > out/logs/run_all.log 2>&1 &
```

Each machine writes to a per-hostname joblog (`out/logs/joblog_{hostname}`) to avoid NFS write conflicts. Jobs already completed (CSV exists) are skipped automatically.

---

## GPKG Pipeline

For spatial inspection and mapping, we also generate full GeoPackage files for December of 2019, 2021, and 2023 — three year-end snapshots that show where coverage stood at the beginning, middle, and end of the study period. December is used as the representative month for each year.

`compute_gpkg.py` runs the same analysis as the CSV pipeline but instead of writing a flat table, it writes a 5-layer GeoPackage that can be opened directly in any GIS application:

| Layer | Geometry | Content |
|-------|----------|---------|
| `towns_coverage` | Polygon | District polygons with all coverage stats as attributes |
| `sa_500` | LineString | Dissolved road segments within 500 m of any station |
| `sa_1000` | LineString | Dissolved road segments within 1000 m |
| `sa_1500` | LineString | Dissolved road segments within 1500 m |
| `stations` | Point | Charging stations clipped to city (EPSG:3826) |

`run_gpkg_batch.sh` runs the 18 jobs (6 cities × 3 months) sequentially on a single machine.

---

## Environment

**Compute nodes**: 4 machines sharing NFS storage, primary node `master` (140.112.176.245), each with:
- CPU: 2× Intel Xeon Gold 6242 @ 2.80 GHz (16 cores/socket, 32 cores / 64 threads total)
- RAM: ~1006 GiB

**Work distribution**: `ONLY_MONTHS` env var splits the 360 CSV jobs across machines (e.g. months 1–3, 4–6, 7–9, 10–12). Each machine runs one job at a time (`MAX_JOBS=1`) due to per-job peak memory of ~22 GiB. GPKG batch (18 jobs) runs on a single machine.

**Package manager**: micromamba 2.5.0 (conda-forge channel), environment name `qgis`

**Setup** (Linux x86-64, bash):

**Step 0 — Get the project files**

The scripts must live at `~/maps/` (hardcoded in all scripts as `BASEDIR="$HOME/maps"`). Copy the entire project directory there:

```bash
cp -r /path/to/project ~/maps
```

Make sure the following are present before running anything:
- `~/maps/base.gpkg` — pre-built road/boundary GeoPackage
- `~/maps/data/stations_monthly/stations_YYYY_MM.csv` — monthly station CSVs
- `~/maps/compute_job.py`, `run_one.sh`, `run_all_parallel.sh` (CSV pipeline)
- `~/maps/compute_gpkg.py`, `run_gpkg_batch.sh` (GPKG pipeline)

Make the shell scripts executable:

```bash
chmod +x ~/maps/run_one.sh ~/maps/run_all_parallel.sh ~/maps/run_gpkg_batch.sh
```

**Step 1 — Install micromamba**

micromamba is a self-contained conda-compatible package manager. It does not require root.

```bash
# Create ~/bin if it doesn't exist
mkdir -p ~/bin

# Download and extract the binary
curl -Ls https://micro.mamba.pm/api/micromamba/linux-64/latest \
  | tar -xvj -C ~/bin --strip-components=1 bin/micromamba
```

Add the following lines to `~/.bashrc` (open it with any text editor, paste at the bottom, save):

```bash
export PATH="$HOME/bin:$PATH"
export MAMBA_ROOT_PREFIX="$HOME/micromamba"
eval "$(micromamba shell hook --shell bash)"
```

Then reload the shell:

```bash
source ~/.bashrc
```

Verify it works:

```bash
micromamba --version
# Should print: 2.5.0 (or newer)
```

**Step 2 — Create the `qgis` environment**

This downloads and installs QGIS, GDAL, Python, and all dependencies from conda-forge. Expect ~2–5 GB of downloads and several minutes to complete.

```bash
micromamba create -n qgis -c conda-forge \
    qgis=3.44.7 gdal=3.12 python=3.14 numpy parallel --yes
```

**Step 3 — Verify QGIS works headlessly**

```bash
QT_QPA_PLATFORM=offscreen micromamba run -n qgis python3 -c \
    "import qgis.core; print(qgis.core.Qgis.QGIS_VERSION)"
# Expected: 3.44.7-Solothurn
```

If this prints the version without errors, the environment is ready. Scripts activate the env internally, so no manual `micromamba activate` is needed before running them.

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
