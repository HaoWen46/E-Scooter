#!/usr/bin/env bash
# run_gpkg_batch.sh — run compute_gpkg.py for 6 cities × Dec 2019/2021/2023
# Output: out/gpkg/coverage_{CITY}_{YYYY_MM}.gpkg
#
# Override parallelism:
#   MAX_JOBS=2 ./run_gpkg_batch.sh

set -eo pipefail

BASEDIR="$HOME/maps"
CITIES=(TP TC TN KS NTP TY)
MONTHS=(2019_12 2021_12 2023_12)

# ---- Activate env ----
set +u
eval "$(micromamba shell hook --shell bash)"
micromamba activate qgis
set -u

export QT_QPA_PLATFORM=offscreen
export QGIS_DISABLE_MESSAGE_HOOKS=1
export PYTHONUNBUFFERED=1

mkdir -p "$BASEDIR/out/gpkg" "$BASEDIR/out/logs" "$BASEDIR/out/tmp"

# ---- Job parallelism ----
MAX_JOBS="${MAX_JOBS:-1}"
AVAIL_GB=$(awk '/MemAvailable/ {print int($2/1024/1024)}' /proc/meminfo)
echo "MemAvailable: ${AVAIL_GB} GiB  MAX_JOBS: ${MAX_JOBS}"

# ---- Run one job ----
run_one_gpkg() {
    local CITY="$1"
    local YYYY_MM="$2"

    local GPKG="$BASEDIR/out/gpkg/coverage_${CITY}_${YYYY_MM}.gpkg"
    local LOGFILE="$BASEDIR/out/logs/${CITY}_${YYYY_MM}_gpkg.log"

    echo "=== run_gpkg: $CITY $YYYY_MM ===" | tee "$LOGFILE"
    echo "Started: $(date)" | tee -a "$LOGFILE"

    if [ -f "$GPKG" ]; then
        echo "GPKG already exists — skipping." | tee -a "$LOGFILE"
        return 0
    fi

    # Per-job isolation
    local JOBKEY="${CITY}_${YYYY_MM}_gpkg_$$"
    local JOBTMP="$BASEDIR/out/tmp/$JOBKEY"
    mkdir -p "$JOBTMP"
    export TMPDIR="$JOBTMP"
    export XDG_CACHE_HOME="$JOBTMP/cache"
    export XDG_CONFIG_HOME="$JOBTMP/config"
    export XDG_RUNTIME_DIR="$JOBTMP/run"
    export QGIS_CUSTOM_CONFIG_PATH="$JOBTMP/qgis_config"
    mkdir -p "$XDG_CACHE_HOME" "$XDG_CONFIG_HOME" "$XDG_RUNTIME_DIR" "$QGIS_CUSTOM_CONFIG_PATH"

    set +e
    python3 "$BASEDIR/compute_gpkg.py" "$CITY" "$YYYY_MM" 2>&1 | tee -a "$LOGFILE"
    RC=${PIPESTATUS[0]}
    set -e

    rm -rf "$JOBTMP" 2>/dev/null || true

    echo "Finished: $(date), exit code: $RC" | tee -a "$LOGFILE"

    # QGIS cleanup crash (134/139/137) is harmless if gpkg was written
    if [ "$RC" -eq 139 ] || [ "$RC" -eq 134 ] || [ "$RC" -eq 137 ]; then
        if [ -f "$GPKG" ]; then
            echo "  (exit $RC but GPKG exists — treating as success)" | tee -a "$LOGFILE"
            return 0
        fi
    fi

    return "$RC"
}

export -f run_one_gpkg
export BASEDIR

# ---- Build job list ----
JOBS=()
for CITY in "${CITIES[@]}"; do
    for YYYY_MM in "${MONTHS[@]}"; do
        JOBS+=("$CITY:::$YYYY_MM")
    done
done

echo "=== run_gpkg_batch.sh ==="
echo "Cities: ${CITIES[*]}"
echo "Months: ${MONTHS[*]}"
echo "Total jobs: ${#JOBS[@]}"
echo

JOBLOG="$BASEDIR/out/logs/joblog_gpkg"

parallel --joblog "$JOBLOG" --resume-failed \
    --retries 2 --halt never \
    --memfree 4G \
    --line-buffer -j "$MAX_JOBS" \
    'run_one_gpkg {1} {2}' \
    ::: "${CITIES[@]}" \
    ::: "${MONTHS[@]}" \
    || true

echo
echo "=== Job Summary ==="
TOTAL=$(tail -n +2 "$JOBLOG" | wc -l)
FAILED=$(tail -n +2 "$JOBLOG" | awk -F'\t' '$7 != 0 {count++} END {print count+0}')
echo "Total: $TOTAL  Passed: $((TOTAL - FAILED))  Failed: $FAILED"

if [ "$FAILED" -gt 0 ]; then
    echo
    echo "Failed jobs:"
    tail -n +2 "$JOBLOG" | awk -F'\t' '$7 != 0 {print $0}'
fi

echo
echo "GPKGs in: $BASEDIR/out/gpkg/"
ls -lh "$BASEDIR/out/gpkg/" 2>/dev/null || true
