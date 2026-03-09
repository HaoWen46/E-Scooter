#!/usr/bin/env bash
# run_one.sh — correct version for parallel QGIS runs
# - isolates QGIS config/tmp per job (prevents cross-job stomping)
# - treats 134/139/137 as success IF outputs exist (QGIS cleanup crash / OOM kill after writing)
# Usage: ./run_one.sh CITY YYYY_MM

set -eo pipefail

CITY="${1:?Usage: $0 CITY YYYY_MM}"
YYYY_MM="${2:?Usage: $0 CITY YYYY_MM}"

BASEDIR="$HOME/maps"

# ---- Activate micromamba qgis environment ----
set +u
eval "$(micromamba shell hook --shell bash)"
micromamba activate qgis
set -u

# ---- Headless Qt / QGIS settings ----
export QT_QPA_PLATFORM=offscreen
export QGIS_DISABLE_MESSAGE_HOOKS=1
export PYTHONUNBUFFERED=1

# ---- Create output directories ----
mkdir -p "$BASEDIR/out/logs" "$BASEDIR/out/tmp"

LOGFILE="$BASEDIR/out/logs/${CITY}_${YYYY_MM}.log"
CSV="$BASEDIR/out/coverage_${CITY}_${YYYY_MM}.csv"

echo "=== run_one.sh: $CITY $YYYY_MM ===" | tee "$LOGFILE"
echo "Started: $(date)" | tee -a "$LOGFILE"

# ---- Skip if already done ----
if [ -f "$CSV" ]; then
  echo "CSV already exists — skipping." | tee -a "$LOGFILE"
  exit 0
fi

# ---- Per-job isolation (CRITICAL) ----
JOBKEY="${CITY}_${YYYY_MM}_$$"
JOBTMP="$BASEDIR/out/tmp/$JOBKEY"
mkdir -p "$JOBTMP"

export TMPDIR="$JOBTMP"
export XDG_CACHE_HOME="$JOBTMP/cache"
export XDG_CONFIG_HOME="$JOBTMP/config"
export XDG_RUNTIME_DIR="$JOBTMP/run"
export QGIS_CUSTOM_CONFIG_PATH="$JOBTMP/qgis_config"
mkdir -p "$XDG_CACHE_HOME" "$XDG_CONFIG_HOME" "$XDG_RUNTIME_DIR" "$QGIS_CUSTOM_CONFIG_PATH"

# (optional) let python use unique tmp
export JOB_TMP="$JOBTMP"

# ---- Run compute job ----
set +e
python3 "$BASEDIR/compute_job.py" "$CITY" "$YYYY_MM" 2>&1 | tee -a "$LOGFILE"
RC=${PIPESTATUS[0]}
set -e

echo "Finished: $(date), exit code: $RC" | tee -a "$LOGFILE"

# ---- Cleanup per-job isolation dir ----
rm -rf "$JOBTMP" 2>/dev/null || true

# ---- Treat late crashes / SIGKILL as success if CSV exists ----
if [ "$RC" -eq 139 ] || [ "$RC" -eq 134 ] || [ "$RC" -eq 137 ]; then
  if [ -f "$CSV" ]; then
    echo "  (exit $RC but CSV exists — treating as success)" | tee -a "$LOGFILE"
    exit 0
  fi
fi

exit "$RC"
