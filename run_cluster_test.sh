#!/bin/bash
# run_cluster_test.sh
#
# Automates the 3-node jin-kv cluster test:
#   - starts all 3 nodes fresh
#   - waits for a stable leader
#   - proposes entries through the leader
#   - kills a follower, proposes more entries
#   - kills the leader, waits, revives the dead follower
#   - checks logs for double-leadership bugs and conflict resolution
#
# Usage: ./run_cluster_test.sh
# Run from the jin-kv project root.

set -uo pipefail

LOG1=node1.log
LOG2=node2.log
LOG3=node3.log

cleanup() {
    echo ""
    echo "=== cleaning up ==="
    for pid in "${PIDS[@]:-}"; do
        kill -9 "$pid" 2>/dev/null || true
    done
}
trap cleanup EXIT

echo "=== killing any orphaned server processes from previous runs ==="
pkill -9 -f "bin/server" 2>/dev/null || true
sleep 1

echo "=== resetting WAL and log files ==="
rm -f node1.wal node2.wal node3.wal "$LOG1" "$LOG2" "$LOG3"

echo "=== building ==="
mkdir -p bin
go build -o bin/server ./cmd/server || { echo "build failed"; exit 1; }

start_node() {
    # IMPORTANT: run the compiled binary directly, not "go run".
    # "go run" spawns a wrapper process and compiles to a temp binary
    # underneath it — $! would capture the wrapper's PID, and kill -9
    # on that PID leaves the real server process orphaned and still
    # running. Running ./bin/server directly means $! is the real PID.
    local id=$1
    local addr=$2
    local logfile=$3
    ./bin/server -id="$id" -addr="$addr" > "$logfile" 2>&1 &
    echo $!
}

find_leader() {
    # scans all 3 logs, returns the node id of whoever most recently
    # logged "BECAME LEADER", or empty if none found yet
    local latest_line=""
    local latest_node=""
    for f in "$LOG1:1" "$LOG2:2" "$LOG3:3"; do
        local file="${f%%:*}"
        local id="${f##*:}"
        if [[ -f "$file" ]]; then
            local line
            line=$(grep "BECAME LEADER" "$file" | tail -1)
            if [[ -n "$line" ]]; then
                latest_line="$line"
                latest_node="$id"
            fi
        fi
    done
    echo "$latest_node"
}

count_leadership_events() {
    # counts total "BECAME LEADER" lines across all logs — should be
    # low for a healthy cluster; a high count means leader churn
    cat "$LOG1" "$LOG2" "$LOG3" 2>/dev/null | grep -c "BECAME LEADER" || true
}

echo "=== starting all 3 nodes ==="
declare -A PID_FOR_ID
declare -A LOG_FOR_ID
PIDS=()

PID_FOR_ID[1]=$(start_node 1 :8080 "$LOG1")
LOG_FOR_ID[1]=$LOG1
PIDS+=("${PID_FOR_ID[1]}")

PID_FOR_ID[2]=$(start_node 2 :8081 "$LOG2")
LOG_FOR_ID[2]=$LOG2
PIDS+=("${PID_FOR_ID[2]}")

PID_FOR_ID[3]=$(start_node 3 :8082 "$LOG3")
LOG_FOR_ID[3]=$LOG3
PIDS+=("${PID_FOR_ID[3]}")

echo "node1 pid=${PID_FOR_ID[1]}  node2 pid=${PID_FOR_ID[2]}  node3 pid=${PID_FOR_ID[3]}"
echo "waiting 10s for election to stabilize..."
sleep 10

LEADER_ID=$(find_leader)
if [[ -z "$LEADER_ID" ]]; then
    echo "!!! no leader elected within 5s — check logs manually"
    exit 1
fi
echo "=== leader is node $LEADER_ID ==="

EVENTS=$(count_leadership_events)
echo "leadership events so far: $EVENTS"
if [[ "$EVENTS" -gt 1 ]]; then
    echo "!!! WARNING: more than one leadership event during initial election — possible re-election bug"
fi

# NOTE: proposing entries requires the stdin trigger to be wired into
# main.go (reading lines and calling r.Propose). This script assumes
# it's in place. If not yet added, this section will just do nothing
# harmful — the pipe target simply won't be read.
echo "=== proposing 3 entries via node $LEADER_ID ==="
LEADER_PID=${PID_FOR_ID[$LEADER_ID]}
{
    echo "set x=1"
    sleep 0.3
    echo "set y=2"
    sleep 0.3
    echo "set z=3"
} > "/proc/${LEADER_PID}/fd/0" 2>/dev/null || echo "(could not write to leader stdin — is it backgrounded with a real fd? see note below)"

echo "waiting 3s for these entries to replicate before partitioning..."
sleep 3

# pick a follower (any id that isn't the leader) to kill first
FOLLOWER_ID=""
for id in 1 2 3; do
    if [[ "$id" != "$LEADER_ID" ]]; then
        FOLLOWER_ID=$id
        break
    fi
done

echo "=== killing follower node $FOLLOWER_ID (simulating it falling behind) ==="
kill -9 "${PID_FOR_ID[$FOLLOWER_ID]}"

echo "=== proposing 2 more entries via node $LEADER_ID (follower $FOLLOWER_ID will miss these) ==="
{
    echo "set a=9"
    sleep 0.3
    echo "set b=8"
} > "/proc/${LEADER_PID}/fd/0" 2>/dev/null || true

echo "waiting 3s for these entries to replicate to remaining alive peers..."
sleep 3

echo "=== killing leader node $LEADER_ID (quorum temporarily lost) ==="
kill -9 "$LEADER_PID"

echo "waiting 5s (remaining lone node should NOT elect a leader)..."
sleep 5

echo "=== reviving node $FOLLOWER_ID (quorum restored: 2 of 3 alive) ==="
FOLLOWER_ADDR=":808$((FOLLOWER_ID - 1))"
NEW_PID=$(start_node "$FOLLOWER_ID" "$FOLLOWER_ADDR" "${LOG_FOR_ID[$FOLLOWER_ID]}")
PID_FOR_ID[$FOLLOWER_ID]=$NEW_PID
PIDS+=("$NEW_PID")

echo "waiting 10s for new election + conflict resolution to settle..."
sleep 10

echo ""
echo "================ RESULTS ================"

NEW_LEADER=$(find_leader)
echo "new leader after recovery: node $NEW_LEADER"

TOTAL_EVENTS=$(count_leadership_events)
echo "total leadership events across whole run: $TOTAL_EVENTS"
if [[ "$TOTAL_EVENTS" -gt 3 ]]; then
    echo "!!! WARNING: leader churned more than expected — check for the double-leadership bug"
fi

echo ""
echo "--- conflict/truncation markers in node $FOLLOWER_ID's log ---"
grep -i "conflict\|truncat" "${LOG_FOR_ID[$FOLLOWER_ID]}" || echo "(none found — conflict resolution may not have triggered, or logging is missing)"

echo ""
echo "--- last 15 lines of each node's log ---"
for id in 1 2 3; do
    echo "-- node $id --"
    tail -15 "${LOG_FOR_ID[$id]}" 2>/dev/null || echo "(log missing)"
    echo ""
done

echo "=== done — press Ctrl+C or wait, cleanup will kill remaining nodes ==="
sleep 2
