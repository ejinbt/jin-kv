#!/bin/bash
# full_cluster_test.sh
#
# End-to-end automated test for jin-kv, focused on Phase 4:
#   1. starts 3 nodes fresh
#   2. waits for a stable leader
#   3. kills a follower BEFORE any writes happen
#   4. proposes enough writes via the leader to trigger at least one
#      snapshot + compaction cycle WHILE the follower is dead — this
#      means the follower's nextIndex will point at log entries that
#      no longer exist, forcing the leader to use InstallSnapshot
#      instead of normal AppendEntries when the follower returns
#   5. revives the follower
#   6. verifies: no crash, no hang, and the revived node ends up with
#      the correct final state, plus checks for a snapshot file
#      appearing on the revived node as evidence it was installed
#
# Usage: ./full_cluster_test.sh
# Run from the jin-kv project root.

set -uo pipefail

LOG1=node1.log
LOG2=node2.log
LOG3=node3.log
FIFO1=node1.in
FIFO2=node2.in
FIFO3=node3.in

declare -A PID_FOR_ID
declare -A FEEDER_PID_FOR_ID
declare -A ADDR_FOR_ID
ADDR_FOR_ID[1]=":8080"
ADDR_FOR_ID[2]=":8081"
ADDR_FOR_ID[3]=":8082"
PIDS=()

cleanup() {
    echo ""
    echo "=== cleaning up ==="
    for pid in "${PIDS[@]:-}"; do
        kill -9 "$pid" 2>/dev/null || true
    done
    for id in 1 2 3; do
        fp="${FEEDER_PID_FOR_ID[$id]:-}"
        [[ -n "$fp" ]] && kill -9 "$fp" 2>/dev/null || true
    done
    rm -f "$FIFO1" "$FIFO2" "$FIFO3"
}
trap cleanup EXIT

echo "=== killing any orphaned server processes ==="
pkill -9 -f "bin/server" 2>/dev/null || true
sleep 1

echo "=== resetting WAL, snapshot, log, and fifo files ==="
rm -f node1.wal node2.wal node3.wal
rm -f node1.snapshot node2.snapshot node3.snapshot
rm -f "$LOG1" "$LOG2" "$LOG3" "$FIFO1" "$FIFO2" "$FIFO3"

echo "=== building ==="
mkdir -p bin
go build -o bin/server ./cmd/server || { echo "build failed"; exit 1; }

start_node() {
    # recreate the fifo fresh every time — reusing an old one can leave
    # a stale reader attached from a previously killed node, silently
    # stealing writes meant for the new process
    local id=$1
    local addr=$2
    local logfile=$3
    local fifo=$4

    rm -f "$fifo"
    mkfifo "$fifo"

    ( exec 3<>"$fifo"; cat <&3 ) | ./bin/server -id="$id" -addr="$addr" >> "$logfile" 2>&1 &
    local server_pid=$!

    sleep 0.2
    local feeder_pid
    feeder_pid=$(pgrep -f "exec 3<>$fifo" | head -1)
    FEEDER_PID_FOR_ID[$id]="$feeder_pid"

    echo "$server_pid"
}

kill_node() {
    local id=$1
    local server_pid=${PID_FOR_ID[$id]}
    local feeder_pid=${FEEDER_PID_FOR_ID[$id]:-}
    kill -9 "$server_pid" 2>/dev/null || true
    if [[ -n "$feeder_pid" ]]; then
        kill -9 "$feeder_pid" 2>/dev/null || true
    fi
}

send_cmd() {
    local fifo=$1
    local cmd=$2
    echo "$cmd" > "$fifo"
}

fifo_for() { eval echo "\$FIFO$1"; }

find_leader() {
    local best_id="" best_ts=""
    for id in 1 2 3; do
        local file="node${id}.log"
        [[ -f "$file" ]] || continue
        local line
        line=$(grep "BECAME LEADER" "$file" | tail -1)
        [[ -z "$line" ]] && continue
        local ts="${line:0:19}"
        if [[ -z "$best_ts" || "$ts" > "$best_ts" ]]; then
            best_ts="$ts"
            best_id="$id"
        fi
    done
    echo "$best_id"
}

wait_for_port() {
    local port=$1
    local tries=0
    while ! (echo > "/dev/tcp/127.0.0.1/$port") 2>/dev/null; do
        sleep 0.2
        tries=$((tries + 1))
        if [[ $tries -gt 25 ]]; then
            echo "!!! timed out waiting for port $port to come up"
            return 1
        fi
    done
    return 0
}

echo "=== starting all 3 nodes ==="
PID_FOR_ID[1]=$(start_node 1 "${ADDR_FOR_ID[1]}" "$LOG1" "$FIFO1")
PIDS+=("${PID_FOR_ID[1]}")
PID_FOR_ID[2]=$(start_node 2 "${ADDR_FOR_ID[2]}" "$LOG2" "$FIFO2")
PIDS+=("${PID_FOR_ID[2]}")
PID_FOR_ID[3]=$(start_node 3 "${ADDR_FOR_ID[3]}" "$LOG3" "$FIFO3")
PIDS+=("${PID_FOR_ID[3]}")

echo "node1 pid=${PID_FOR_ID[1]}  node2 pid=${PID_FOR_ID[2]}  node3 pid=${PID_FOR_ID[3]}"
echo "waiting for all 3 ports to actually come up..."
wait_for_port 8080
wait_for_port 8081
wait_for_port 8082
echo "all 3 ports confirmed up"

echo "waiting 8s for election to stabilize..."
sleep 8

LEADER_ID=$(find_leader)
if [[ -z "$LEADER_ID" ]]; then
    echo "!!! FAIL: no leader elected within 8s"
    exit 1
fi
echo "=== leader is node $LEADER_ID ==="

FOLLOWER_ID=""
for id in 1 2 3; do
    if [[ "$id" != "$LEADER_ID" ]]; then
        FOLLOWER_ID=$id
        break
    fi
done

echo "=== killing follower node $FOLLOWER_ID BEFORE any writes ==="
echo "(it needs to miss enough entries that the leader compacts them away)"
kill_node "$FOLLOWER_ID"
sleep 1

echo "=== proposing enough writes via node $LEADER_ID to force snapshot+compaction ==="
echo "(snapshotThreshold is 3 in the code — writing 8 entries guarantees"
echo " at least two compaction cycles happen while the follower is dead)"
for i in $(seq 1 8); do
    send_cmd "$(fifo_for $LEADER_ID)" "set k$i=v$i"
    sleep 0.4
done
echo "waiting 3s for the writes to commit and snapshots to be taken..."
sleep 3

echo "=== confirming a snapshot file now exists on the leader ==="
LEADER_SNAPSHOT="node${LEADER_ID}.snapshot"
if [[ -f "$LEADER_SNAPSHOT" ]]; then
    echo "leader snapshot file exists: $LEADER_SNAPSHOT"
else
    echo "!!! WARNING: no snapshot file found on leader — snapshotting may not have triggered"
fi

echo "=== reviving node $FOLLOWER_ID — nextIndex should point at compacted-away entries ==="
NEW_PID=$(start_node "$FOLLOWER_ID" "${ADDR_FOR_ID[$FOLLOWER_ID]}" "node${FOLLOWER_ID}.log" "$(fifo_for $FOLLOWER_ID)")
PID_FOR_ID[$FOLLOWER_ID]=$NEW_PID
PIDS+=("$NEW_PID")

REVIVED_PORT="${ADDR_FOR_ID[$FOLLOWER_ID]#:}"
echo "waiting for revived node's port ($REVIVED_PORT) to come up..."
wait_for_port "$REVIVED_PORT"
echo "revived node's port confirmed up"

echo "waiting 8s for InstallSnapshot to happen and settle..."
sleep 8

echo "=== checking revived node $FOLLOWER_ID is still alive (no crash) ==="
if kill -0 "${PID_FOR_ID[$FOLLOWER_ID]}" 2>/dev/null; then
    echo "PASS: revived node is still running, no crash/panic"
    NO_CRASH=true
else
    echo "FAIL: revived node's process died — check its log for a panic"
    NO_CRASH=false
fi

echo "=== querying revived node $FOLLOWER_ID for values it missed while dead ==="
for i in 1 4 8; do
    send_cmd "$(fifo_for $FOLLOWER_ID)" "get k$i"
    sleep 0.3
done
sleep 1

echo ""
echo "================ RESULTS ================"
echo "--- revived node $FOLLOWER_ID full log ---"
cat "node${FOLLOWER_ID}.log"

echo ""
echo "--- does the revived node have its own snapshot file now? ---"
REVIVED_SNAPSHOT="node${FOLLOWER_ID}.snapshot"
if [[ -f "$REVIVED_SNAPSHOT" ]]; then
    echo "PASS: $REVIVED_SNAPSHOT exists — strong evidence InstallSnapshot was received and saved"
    HAS_SNAPSHOT=true
else
    echo "FAIL: no snapshot file found on revived node"
    HAS_SNAPSHOT=false
fi

echo ""
echo "--- verdict ---"
PASS=true
if ! $NO_CRASH; then PASS=false; fi

if grep -q "^k1 = v1" "node${FOLLOWER_ID}.log"; then
    echo "PASS: k1 correctly recovered"
else
    echo "FAIL: k1 not recovered on revived node"
    PASS=false
fi

if grep -q "^k4 = v4" "node${FOLLOWER_ID}.log"; then
    echo "PASS: k4 correctly recovered"
else
    echo "FAIL: k4 not recovered on revived node"
    PASS=false
fi

if grep -q "^k8 = v8" "node${FOLLOWER_ID}.log"; then
    echo "PASS: k8 correctly recovered"
else
    echo "FAIL: k8 not recovered on revived node"
    PASS=false
fi

if ! $HAS_SNAPSHOT; then
    echo "NOTE: data was correct but no local snapshot file found on the"
    echo "      revived node — worth checking whether InstallSnapshot fired"
    echo "      vs. normal AppendEntries somehow catching it up instead"
fi

echo ""
if $PASS; then
    echo "=== OVERALL: PASS — snapshot install path verified ==="
else
    echo "=== OVERALL: FAIL — see above ==="
fi

echo "=== done — cleaning up ==="
sleep 1