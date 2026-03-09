#!/usr/bin/env bash
# run_all_parallel.sh — correct version (won't blow your 200GiB quota)
# - forces safe NJOBS based on your cgroup user.slice memory.max + MemAvailable
# - defaults conservative; override with env:
#     PERJOB_GB=22 BUFFER_GB=80 MAX_JOBS=2 ./run_all_parallel.sh
# - joblog is per-hostname (safe for shared NFS)
# - filter to specific months: ONLY_MONTHS=1,2,3 ./run_all_parallel.sh

set -eo pipefail

BASEDIR="$HOME/maps"
CITIES=(TP TC TN KS NTP TY)

# ---- Activate env + ensure parallel ----
set +u
eval "$(micromamba shell hook --shell bash)"
micromamba activate qgis

if ! command -v parallel &>/dev/null; then
  micromamba install -y -n qgis -c conda-forge parallel
fi
set -u

mkdir -p "$BASEDIR/out/logs" "$BASEDIR/out/tmp" "$BASEDIR/out/gpkg_zips"

# ---- Months: 2019_01 through 2023_12, optionally filtered ----
# ONLY_MONTHS=1,2,3  → only Jan, Feb, Mar across all years
MONTHS=()
if [ -n "${ONLY_MONTHS:-}" ]; then
  IFS=',' read -ra _FILTER <<< "$ONLY_MONTHS"
  for YEAR in $(seq 2019 2023); do
    for M in "${_FILTER[@]}"; do
      MONTHS+=("${YEAR}_$(printf '%02d' "$M")")
    done
  done
else
  for YEAR in $(seq 2019 2023); do
    for MONTH in $(seq -w 1 12); do
      MONTHS+=("${YEAR}_${MONTH}")
    done
  done
fi

JOBLOG="$BASEDIR/out/logs/joblog_$(hostname -s)"

# ---- Safe NJOBS calc ----
PERJOB_GB="${PERJOB_GB:-22}"   # measure once w/ /usr/bin/time -v and set this
BUFFER_GB="${BUFFER_GB:-80}"   # leave headroom (swap was full on your node)
MAX_JOBS="${MAX_JOBS:-1}"      # one job at a time to avoid OOM

AVAIL_GB=$(awk '/MemAvailable/ {print int($2/1024/1024)}' /proc/meminfo)

UID_NUM="$(id -u)"
USER_CG="/sys/fs/cgroup/user.slice/user-${UID_NUM}.slice"
USER_CG_MAX_GB=1000000
if [ -r "$USER_CG/memory.max" ]; then
  CGMAX="$(cat "$USER_CG/memory.max" 2>/dev/null || echo max)"
  if [ "$CGMAX" != "max" ]; then
    USER_CG_MAX_GB=$(( CGMAX / 1024 / 1024 / 1024 ))
  fi
fi

BUDGET_GB="$AVAIL_GB"
if [ "$USER_CG_MAX_GB" -lt "$BUDGET_GB" ]; then
  BUDGET_GB="$USER_CG_MAX_GB"
fi
BUDGET_GB=$(( BUDGET_GB - BUFFER_GB ))
[ "$BUDGET_GB" -lt 1 ] && BUDGET_GB=1

NJOBS=$(( BUDGET_GB / PERJOB_GB ))
[ "$NJOBS" -lt 1 ] && NJOBS=1
[ "$NJOBS" -gt "$MAX_JOBS" ] && NJOBS="$MAX_JOBS"

echo "=== run_all_parallel.sh ==="
echo "Cities: ${CITIES[*]}"
echo "Months: ${MONTHS[0]} through ${MONTHS[-1]} (${#MONTHS[@]} months)${ONLY_MONTHS:+  [filtered: $ONLY_MONTHS]}"
echo "Total jobs: $((${#CITIES[@]} * ${#MONTHS[@]}))"
echo
echo "MemAvailable: ${AVAIL_GB} GiB"
echo "User cgroup memory.max: ${USER_CG_MAX_GB} GiB"
echo "Assumed per-job peak: ${PERJOB_GB} GiB"
echo "Safety buffer: ${BUFFER_GB} GiB"
echo "Running with NJOBS=$NJOBS"
echo

parallel --joblog "$JOBLOG" --resume-failed \
  --retries 2 --halt never \
  --memfree 4G \
  --line-buffer -j "$NJOBS" \
  "$BASEDIR/run_one.sh" {1} {2} \
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
echo "Outputs in: $BASEDIR/out/"
echo "  CSVs:  out/coverage_*.csv"
echo "  GPKGs: out/gpkg_zips/*.zip"
echo "  Logs:  out/logs/"
echo "  Joblog: $JOBLOG"
