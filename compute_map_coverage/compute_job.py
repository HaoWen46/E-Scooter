#!/usr/bin/env python3
"""compute_job.py — PyQGIS headless script: service area → CSV per city×month.

Usage: python3 compute_job.py CITY YYYY_MM
Example: python3 compute_job.py TP 2023_01
"""

import csv
import os
import sys

# ---------------------------------------------------------------------------
# 0. Parse args
# ---------------------------------------------------------------------------
if len(sys.argv) != 3:
    print(f"Usage: {sys.argv[0]} CITY YYYY_MM", file=sys.stderr)
    sys.exit(1)

CITY = sys.argv[1]
YYYY_MM = sys.argv[2]

BASEDIR = os.path.expanduser("~/maps")
BASE_GPKG = os.path.join(BASEDIR, "base.gpkg")
STATION_CSV = os.path.join(
    BASEDIR, "data", "stations_monthly", f"stations_{YYYY_MM}.csv"
)
OUT_CSV = os.path.join(BASEDIR, "out", f"coverage_{CITY}_{YYYY_MM}.csv")
os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)

for path in (BASE_GPKG, STATION_CSV):
    if not os.path.isfile(path):
        print(f"ERROR: {path} not found", file=sys.stderr)
        sys.exit(1)

print(f"[{CITY} {YYYY_MM}] Starting compute_job.py")

# ---------------------------------------------------------------------------
# 1. Initialize QGIS
# ---------------------------------------------------------------------------
os.environ["QT_QPA_PLATFORM"] = "offscreen"
os.environ["QGIS_DISABLE_MESSAGE_HOOKS"] = "1"

from qgis.core import (
    QgsApplication,
    QgsCoordinateReferenceSystem,
    QgsVectorLayer,
)

qgs = QgsApplication([], False)
qgs.initQgis()

import processing
from processing.core.Processing import Processing

Processing.initialize()

CRS_3826 = QgsCoordinateReferenceSystem("EPSG:3826")


def gpkg_layer(gpkg, layername, name=None):
    """Load a layer from a GeoPackage."""
    uri = f"{gpkg}|layername={layername}"
    lyr = QgsVectorLayer(uri, name or layername, "ogr")
    if not lyr.isValid():
        print(f"  WARNING: failed to load {uri}")
    return lyr


def run_alg(alg_id, params):
    """Run a processing algorithm and return results dict."""
    result = processing.run(alg_id, params)
    return result


# ---------------------------------------------------------------------------
# 2. Load layers from base.gpkg
# ---------------------------------------------------------------------------
print(f"[{CITY} {YYYY_MM}] Loading layers...")

roads_scooter = gpkg_layer(BASE_GPKG, "roads_scooter")
town_city = gpkg_layer(BASE_GPKG, f"town_{CITY}")
boundary_city = gpkg_layer(BASE_GPKG, f"b_{CITY}")

for lyr in (roads_scooter, town_city, boundary_city):
    if not lyr.isValid():
        print(f"ERROR: layer {lyr.name()} invalid", file=sys.stderr)
        qgs.exitQgis()
        sys.exit(1)

# ---------------------------------------------------------------------------
# 3. Load & reproject stations
# ---------------------------------------------------------------------------
print(f"[{CITY} {YYYY_MM}] Loading stations from {STATION_CSV}")

station_uri = (
    f"file:///{STATION_CSV}?delimiter=,&xField=Longitude&yField=Latitude&crs=EPSG:4326"
)
stations_4326 = QgsVectorLayer(station_uri, "stations_4326", "delimitedtext")
if not stations_4326.isValid():
    print(f"ERROR: failed to load stations CSV", file=sys.stderr)
    qgs.exitQgis()
    sys.exit(1)

print(f"  Loaded {stations_4326.featureCount()} stations (EPSG:4326)")

# Reproject to EPSG:3826
result = run_alg("native:reprojectlayer", {
    "INPUT": stations_4326,
    "TARGET_CRS": CRS_3826,
    "OUTPUT": "TEMPORARY_OUTPUT",
})
stations_3826 = result["OUTPUT"]
print(f"  Reprojected: {stations_3826.featureCount()} stations (EPSG:3826)")

# ---------------------------------------------------------------------------
# 4. Clip stations to city boundary
# ---------------------------------------------------------------------------
print(f"[{CITY} {YYYY_MM}] Clipping stations to city boundary...")

result = run_alg("native:clip", {
    "INPUT": stations_3826,
    "OVERLAY": boundary_city,
    "OUTPUT": "TEMPORARY_OUTPUT",
})
stations_clipped = result["OUTPUT"]
n_stations = stations_clipped.featureCount()
print(f"  Stations in {CITY}: {n_stations}")

if n_stations == 0:
    print(f"[{CITY} {YYYY_MM}] No stations in city — writing empty CSV")

    # Write empty CSV with header
    with open(OUT_CSV, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "TOWNID", "TOWNNAME", "len_total_m",
            "len_0_500_m", "len_500_1000_m", "len_1000_1500_m", "len_gt1500_m",
            "reach500_pct", "reach1k_pct", "reach1500_pct",
        ])

    qgs.exitQgis()
    print(f"[{CITY} {YYYY_MM}] Done.")
    sys.exit(0)

# ---------------------------------------------------------------------------
# 5. Service area analysis (3 distances)
# ---------------------------------------------------------------------------
DISTANCES = [500, 1000, 1500]
sa_layers = {}

# Clip roads to city boundary first
result = run_alg("native:clip", {
    "INPUT": roads_scooter,
    "OVERLAY": boundary_city,
    "OUTPUT": "TEMPORARY_OUTPUT",
})
roads_city = result["OUTPUT"]
print(f"  Roads in {CITY}: {roads_city.featureCount()} features")

BATCH_SIZE = 20  # dissolve in batches to limit peak memory

for dist in DISTANCES:
    print(f"[{CITY} {YYYY_MM}] Service area: {dist}m...")
    result = run_alg("native:serviceareafromlayer", {
        "INPUT": roads_city,
        "START_POINTS": stations_clipped,
        "STRATEGY": 0,
        "TRAVEL_COST2": dist,
        "DEFAULT_DIRECTION": 2,
        "TOLERANCE": 0,
        "POINT_TOLERANCE": 200,
        "OUTPUT_LINES": "TEMPORARY_OUTPUT",
        "OUTPUT": "TEMPORARY_OUTPUT",
    })
    sa_lyr = result["OUTPUT_LINES"]
    n_feat = sa_lyr.featureCount()
    print(f"  sa_lines_{dist} (raw): {n_feat} features")

    # Batch dissolve: assign batch number, dissolve per batch, then final dissolve
    if n_feat > BATCH_SIZE:
        n_batches = (n_feat + BATCH_SIZE - 1) // BATCH_SIZE
        print(f"  Batch dissolve: {n_batches} batches of ~{BATCH_SIZE}...")

        # Add batch field via field calculator
        result_fc = run_alg("native:fieldcalculator", {
            "INPUT": sa_lyr,
            "FIELD_NAME": "_batch",
            "FIELD_TYPE": 1,  # integer
            "FORMULA": f"floor(@row_number / {BATCH_SIZE})",
            "OUTPUT": "TEMPORARY_OUTPUT",
        })
        del sa_lyr
        sa_with_batch = result_fc["OUTPUT"]

        # Dissolve per batch group
        result_bd = run_alg("native:dissolve", {
            "INPUT": sa_with_batch,
            "FIELD": ["_batch"],
            "OUTPUT": "TEMPORARY_OUTPUT",
        })
        del sa_with_batch
        sa_batched = result_bd["OUTPUT"]
        print(f"  After batch dissolve: {sa_batched.featureCount()} features")

        # Final dissolve to merge all batches
        result_fd = run_alg("native:dissolve", {
            "INPUT": sa_batched,
            "OUTPUT": "TEMPORARY_OUTPUT",
        })
        del sa_batched
        sa_dissolved = result_fd["OUTPUT"]
    else:
        result_d = run_alg("native:dissolve", {
            "INPUT": sa_lyr,
            "OUTPUT": "TEMPORARY_OUTPUT",
        })
        del sa_lyr
        sa_dissolved = result_d["OUTPUT"]

    sa_layers[dist] = sa_dissolved
    print(f"  sa_lines_{dist} (dissolved): {sa_dissolved.featureCount()} features")

# ---------------------------------------------------------------------------
# 5b. Compute road lengths per district (chained sumlinelengths)
# ---------------------------------------------------------------------------
print(f"[{CITY} {YYYY_MM}] Computing road lengths per district...")

result = run_alg("native:sumlinelengths", {
    "POLYGONS": town_city,
    "LINES": roads_city,
    "LEN_FIELD": "len_total",
    "COUNT_FIELD": "cnt_total",
    "OUTPUT": "TEMPORARY_OUTPUT",
})
sll1 = result["OUTPUT"]

result = run_alg("native:sumlinelengths", {
    "POLYGONS": sll1,
    "LINES": sa_layers[500],
    "LEN_FIELD": "cum_500",
    "COUNT_FIELD": "cnt_500",
    "OUTPUT": "TEMPORARY_OUTPUT",
})
sll2 = result["OUTPUT"]

result = run_alg("native:sumlinelengths", {
    "POLYGONS": sll2,
    "LINES": sa_layers[1000],
    "LEN_FIELD": "cum_1000",
    "COUNT_FIELD": "cnt_1000",
    "OUTPUT": "TEMPORARY_OUTPUT",
})
sll3 = result["OUTPUT"]

result = run_alg("native:sumlinelengths", {
    "POLYGONS": sll3,
    "LINES": sa_layers[1500],
    "LEN_FIELD": "cum_1500",
    "COUNT_FIELD": "cnt_1500",
    "OUTPUT": "TEMPORARY_OUTPUT",
})
sll_final = result["OUTPUT"]

# ---------------------------------------------------------------------------
# 5c. Export CSV
# ---------------------------------------------------------------------------
print(f"[{CITY} {YYYY_MM}] Exporting CSV → {OUT_CSV}")

with open(OUT_CSV, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "TOWNID", "TOWNNAME", "len_total_m",
        "len_0_500_m", "len_500_1000_m", "len_1000_1500_m", "len_gt1500_m",
        "reach500_pct", "reach1k_pct", "reach1500_pct",
    ])

    for feat in sll_final.getFeatures():
        field_names = [f.name() for f in sll_final.fields()]
        get = lambda name: feat[name] if name in field_names else 0.0

        townid = get("TOWNID") or ""
        townname = get("TOWNNAME") or ""
        len_total = float(get("len_total") or 0)
        cum_500 = float(get("cum_500") or 0)
        cum_1000 = float(get("cum_1000") or 0)
        cum_1500 = float(get("cum_1500") or 0)

        cum_500 = min(cum_500, len_total)
        cum_1000 = min(cum_1000, len_total)
        cum_1500 = min(cum_1500, len_total)

        len_0_500 = cum_500
        len_500_1000 = cum_1000 - cum_500
        len_1000_1500 = cum_1500 - cum_1000
        len_gt1500 = len_total - cum_1500

        if len_total > 0:
            r500 = cum_500 / len_total * 100
            r1k = cum_1000 / len_total * 100
            r1500 = cum_1500 / len_total * 100
        else:
            r500 = r1k = r1500 = 0.0

        writer.writerow([
            townid, townname,
            f"{len_total:.1f}",
            f"{len_0_500:.1f}", f"{len_500_1000:.1f}",
            f"{len_1000_1500:.1f}", f"{len_gt1500:.1f}",
            f"{r500:.2f}", f"{r1k:.2f}", f"{r1500:.2f}",
        ])

print(f"  CSV written: {OUT_CSV}")

# ---------------------------------------------------------------------------
# 6. Cleanup
# ---------------------------------------------------------------------------
qgs.exitQgis()
print(f"[{CITY} {YYYY_MM}] Done.")
