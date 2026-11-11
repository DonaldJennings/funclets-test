#!/usr/bin/env bash
# Runs one NDP stress scenario: starts the simulated storage worker, drives it
# with the simulated OSD, then reports whether the worker survived and whether
# any OSD op thread was left stuck.
#
#   tests/ndp_stress/run.sh BUILD_DIR SCENARIO
#
# Scenarios: light policy fdleak load readerror bigwrite rowrite noworker
#            slowfunclet
# Needs write access to /run/faasm (the socket path is fixed in cephcomm.h).
set -uo pipefail

BUILD=${1:?build dir}
SCENARIO=${2:?scenario}
LOGDIR=${LOGDIR:-$BUILD/logs}
mkdir -p "$LOGDIR" /run/faasm

# Pin the load readings low so every call is offloaded and these runs exercise
# the socket protocol. The "policy" scenario uses the real readings.
FORCE_OFFLOAD=(NDP_SIM_UTILISATION=0.1)

case "$SCENARIO" in
  light)       ENV=("${FORCE_OFFLOAD[@]}"); STORAGE=(--compute-ms 5)
               OSD=(--threads 1 --calls 20 --duration 30) ;;
  policy)      ENV=(OFFLOAD_CPU_THRESHOLD=0.95 OFFLOAD_RAM_THRESHOLD=0.95
                    OFFLOAD_LOAD_AVG_THRESHOLD=1000)
               STORAGE=(--compute-ms 5)
               OSD=(--threads 1 --calls 200 --duration 60) ;;
  fdleak)      ENV=("${FORCE_OFFLOAD[@]}"); STORAGE=(--compute-ms 1)
               OSD=(--threads 1 --calls 300 --duration 60) ;;
  load)        ENV=("${FORCE_OFFLOAD[@]}"); STORAGE=(--compute-ms 150 --delta-ms 1)
               OSD=(--threads 16 --duration 20 --objects 33
                    --object-bytes 262144 --drain 25) ;;
  readerror)   ENV=("${FORCE_OFFLOAD[@]}"); STORAGE=(--compute-ms 20)
               OSD=(--threads 4 --duration 10 --missing-every 25 --drain 25) ;;
  bigwrite)    ENV=("${FORCE_OFFLOAD[@]}"); STORAGE=(--compute-ms 5 --write-bytes 4194304)
               OSD=(--method rw --threads 2 --calls 10 --duration 30) ;;
  rowrite)     ENV=("${FORCE_OFFLOAD[@]}"); STORAGE=(--compute-ms 5 --write-bytes 1024)
               OSD=(--method ro --threads 2 --calls 10 --duration 30) ;;
  noworker)    ENV=(); STORAGE=(); NO_WORKER=1
               OSD=(--threads 2 --calls 4 --duration 60) ;;
  slowfunclet) ENV=("${FORCE_OFFLOAD[@]}")
               STORAGE=(--compute-ms 50 --slow-every 20 --slow-ms 32000)
               OSD=(--threads 4 --duration 45 --drain 25) ;;
  *) echo "unknown scenario $SCENARIO"; exit 2 ;;
esac

rm -f /run/faasm/faasm-ndp.sock
if [ -z "${NO_WORKER:-}" ]; then
    env "${ENV[@]}" "$BUILD/storage_sim" --report-every 5 "${STORAGE[@]}" \
        > "$LOGDIR/$SCENARIO.storage.log" 2>&1 &
    SPID=$!
    for _ in $(seq 50); do [ -S /run/faasm/faasm-ndp.sock ] && break; sleep 0.1; done
fi

echo "=== scenario: $SCENARIO ($BUILD)"
"$BUILD/osd_sim" "${OSD[@]}" 2>&1 | tee "$LOGDIR/$SCENARIO.osd.log" \
    | grep -E "^\[osd\]|Couldn't|Broken pipe|Resource temp" | uniq -c \
    | sed -E 's/^ +1 //' | head -40
OSD_RC=${PIPESTATUS[0]}

if [ -n "${NO_WORKER:-}" ]; then
    echo "--- storage worker: not started"
    exit 0
fi
sleep 1
if kill -0 "$SPID" 2>/dev/null; then
    kill -TERM "$SPID"; wait "$SPID" 2>/dev/null
    WORKER="alive"
else
    wait "$SPID"; RC=$?
    if [ "$RC" -gt 128 ]; then
        WORKER="DIED from signal $((RC - 128)) ($(kill -l $((RC - 128))))"
    else
        WORKER="DIED with exit code $RC"
    fi
fi
grep -E "\[storage\] (listening|final|progress)" "$LOGDIR/$SCENARIO.storage.log" \
    | sed -n '1p;$p'
grep -E "\[(error|critical)\]|terminate|what\(\)" "$LOGDIR/$SCENARIO.storage.log" \
    | sed -E 's/[0-9]{6,}/N/g' | sort | uniq -c | sort -rn | head -8
echo "--- storage worker: $WORKER"
case $OSD_RC in
  0) echo "--- osd: all op threads finished" ;;
  3) echo "--- osd: op threads STUCK (bricked)" ;;
  134) echo "--- osd: ABORTED (assert or suicide timeout)" ;;
  *) echo "--- osd: exit code $OSD_RC" ;;
esac
