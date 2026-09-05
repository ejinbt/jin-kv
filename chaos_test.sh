#!/bin/bash
# chaos_test.sh
#
# The real stress test for jin-kv, combining everything built across
# all 6 phases:
#   1. starts 3 nodes fresh
#   2. waits for a stable leader
#   3. starts a CONTINUOUS background write stream (one new key every
#      0.5s) that keeps running through the entire chaos sequence
#   4. while writes are flowing: kills a follower
#   5. a few seconds later, STILL while writing: kills the current
#      leader too (forces an election among the remaining survivors,
#      while the other node is also still dead)
#   6. a few seconds later: revives the first node that was killed
#   7. a few seconds later: revives the second one
#   8. stops the write stream
#   9. lets everything settle, then queries EVERY node for EVERY key
#      that was ever attempted, and checks that all live, healthy
#      nodes agree on the same final values
#
# This is intentionally harder than any previous test in the project:
# writes are happening DURING failures and recoveries, not just before
# or after them.
#
# Usage: ./chaos_test.sh
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
declare -A HTTPADDR_FOR_ID
ADDR_FOR_ID[1]=":8080"
ADDR_FOR_ID[2]=":8081"
ADDR_FOR_ID[3]=":8082"
HTTPADDR_FOR_ID[1]=":9080"
HTTPADDR_FOR_ID[2]=":9081"
HTTPADDR_FOR_ID[3]=":9082"
PIDS=()
WRITER_PID=""

cleanup() {
    echo ""
    echo "=== cleaning up ==="
    [[ -n "$WRITER_PID" ]] && kill -9 "$WRITER_PID" 2>/dev/null || true
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
rm -f chaos_writes.log

echo "=== building ==="
mkdir -p bin
go build -o bin/server ./cmd/server || { echo "build failed"; exit 1; }

start_node() {
    local id=$1
    local addr=$2
    local httpaddr=$3
    local logfile=$4
    local fifo=$5

    rm -f "$fifo"
    mkfifo "$fifo"

    ( exec 3<>"$fifo"; cat <&3 ) | ./bin/server -id="$id" -addr="$addr" -httpaddr="$httpaddr" >> "$logfile" 2>&1 &
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

find_leader() {
    local best_id="" best_ts=""
    for id in 1 2 3; do
        local file="node${id}.log"
        [[ -f "$file" ]] || continue
        # skip nodes that aren't actually running right now — a stale
        # "BECAME LEADER" line from before this node was killed should
        # never be reported as the current leader
        local pid="${PID_FOR_ID[$id]:-}"
        if [[ -n "$pid" ]] && ! kill -0 "$pid" 2>/dev/null; then
            continue
        fi
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

is_alive() {
    local id=$1
    kill -0 "${PID_FOR_ID[$id]}" 2>/dev/null
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
PID_FOR_ID[1]=$(start_node 1 "${ADDR_FOR_ID[1]}" "${HTTPADDR_FOR_ID[1]}" "$LOG1" "$FIFO1")
PIDS+=("${PID_FOR_ID[1]}")
PID_FOR_ID[2]=$(start_node 2 "${ADDR_FOR_ID[2]}" "${HTTPADDR_FOR_ID[2]}" "$LOG2" "$FIFO2")
PIDS+=("${PID_FOR_ID[2]}")
PID_FOR_ID[3]=$(start_node 3 "${ADDR_FOR_ID[3]}" "${HTTPADDR_FOR_ID[3]}" "$LOG3" "$FIFO3")
PIDS+=("${PID_FOR_ID[3]}")

echo "waiting for all ports to come up..."
wait_for_port 8080; wait_for_port 8081; wait_for_port 8082
wait_for_port 9080; wait_for_port 9081; wait_for_port 9082
echo "all ports confirmed up"

echo "waiting 8s for election to stabilize..."
sleep 8

LEADER_ID=$(find_leader)
if [[ -z "$LEADER_ID" ]]; then
    echo "!!! FAIL: no leader elected within 8s"
    exit 1
fi
echo "=== initial leader is node $LEADER_ID ==="

FIRST_TO_KILL=""
for id in 1 2 3; do
    if [[ "$id" != "$LEADER_ID" ]]; then
        FIRST_TO_KILL=$id
        break
    fi
done

echo ""
echo "=== starting continuous background write stream ==="
echo "(writes to whichever node is CURRENTLY THE LEADER at each moment,"
echo " re-checking every write in case leadership changes mid-stream)"

(
    i=0
    while true; do
        i=$((i + 1))
        current_leader=$(find_leader)
        if [[ -n "$current_leader" ]]; then
            port="${HTTPADDR_FOR_ID[$current_leader]#:}"
            resp=$(curl -s --max-time 1 "http://localhost:${port}/set?key=chaos$i&value=v$i" 2>/dev/null)
            echo "key=chaos$i write $i -> node $current_leader: $resp" >> chaos_writes.log
        else
            echo "write $i -> NO LEADER AVAILABLE, skipped" >> chaos_writes.log
        fi
        sleep 0.5
    done
) &
WRITER_PID=$!
echo "writer started, pid=$WRITER_PID"

sleep 3
echo ""
echo "=== [T+3s] killing follower node $FIRST_TO_KILL (writes continue) ==="
kill_node "$FIRST_TO_KILL"

sleep 4
CURRENT_LEADER=$(find_leader)
echo ""
echo "=== [T+7s] killing CURRENT leader node $CURRENT_LEADER too (writes continue, only 1 node alive now) ==="
SECOND_TO_KILL=$CURRENT_LEADER
kill_node "$SECOND_TO_KILL"

sleep 4
echo ""
echo "=== [T+11s] reviving node $FIRST_TO_KILL (2 of 3 alive again) ==="
NEW_PID=$(start_node "$FIRST_TO_KILL" "${ADDR_FOR_ID[$FIRST_TO_KILL]}" "${HTTPADDR_FOR_ID[$FIRST_TO_KILL]}" "node${FIRST_TO_KILL}.log" "$(eval echo "\$FIFO$FIRST_TO_KILL")")
PID_FOR_ID[$FIRST_TO_KILL]=$NEW_PID
PIDS+=("$NEW_PID")
wait_for_port "${ADDR_FOR_ID[$FIRST_TO_KILL]#:}"

sleep 4
echo ""
echo "=== [T+15s] reviving node $SECOND_TO_KILL (all 3 alive again) ==="
NEW_PID2=$(start_node "$SECOND_TO_KILL" "${ADDR_FOR_ID[$SECOND_TO_KILL]}" "${HTTPADDR_FOR_ID[$SECOND_TO_KILL]}" "node${SECOND_TO_KILL}.log" "$(eval echo "\$FIFO$SECOND_TO_KILL")")
PID_FOR_ID[$SECOND_TO_KILL]=$NEW_PID2
PIDS+=("$NEW_PID2")
wait_for_port "${ADDR_FOR_ID[$SECOND_TO_KILL]#:}"

sleep 5
echo ""
echo "=== [T+20s] stopping the write stream ==="
kill -9 "$WRITER_PID" 2>/dev/null || true
WRITER_PID=""

TOTAL_WRITES=$(grep -c "^write" chaos_writes.log || echo 0)
echo "total write attempts made: $TOTAL_WRITES"

echo ""
echo "=== letting the cluster settle for 10s ==="
sleep 10

FINAL_LEADER=$(find_leader)
echo "=== final leader: node $FINAL_LEADER ==="

echo ""
echo "=== process status right before verification ==="
for id in 1 2 3; do
    if is_alive "$id"; then
        echo "node $id: ALIVE (pid ${PID_FOR_ID[$id]})"
    else
        echo "node $id: DEAD (pid ${PID_FOR_ID[$id]:-unknown} not running) — check node${id}.log for a crash/panic"
    fi
done
echo ""
echo "--- last 10 lines of each node's log, for crash evidence ---"
for id in 1 2 3; do
    echo "-- node $id --"
    tail -10 "node${id}.log" 2>/dev/null || echo "(no log file)"
    echo ""
done

echo ""
echo "================ VERIFICATION ================"
echo "checking that all 3 nodes agree on the final value of every key"
echo "that was successfully proposed during the chaos window..."
echo ""

# extract keys that got an actual "proposed at index" success response
SUCCESSFUL_KEYS=$(grep "proposed at index" chaos_writes.log | grep -oE "key=chaos[0-9]+" | sed 's/key=//' | sort -u)
NUM_SUCCESSFUL=$(echo "$SUCCESSFUL_KEYS" | grep -c . || echo 0)
echo "keys with a successful propose response: $NUM_SUCCESSFUL"

MISMATCHES=0
CHECKED=0
UNANIMOUS_MISSING=0
UNANIMOUS_PRESENT=0

for key in $SUCCESSFUL_KEYS; do
    CHECKED=$((CHECKED + 1))

    # collect each node's answer into its own array slot — never join
    # into one string, since "not found" contains a space and would
    # get split apart by word-splitting, corrupting the comparison
    declare -a node_vals=()
    display=""
    for id in 1 2 3; do
        port="${HTTPADDR_FOR_ID[$id]#:}"
        v=$(curl -s --max-time 2 "http://localhost:${port}/get?key=${key}" 2>/dev/null)
        node_vals[$id]="$v"
        display="$display node$id=[$v]"
    done

    # count how many distinct answers exist across the three nodes,
    # treating "not found" and "" (unreachable) as their own values
    uniq_count=$(printf '%s\n' "${node_vals[1]}" "${node_vals[2]}" "${node_vals[3]}" | sort -u | wc -l)

    if [[ "$uniq_count" -eq 1 ]]; then
        # all three agree — either all have the value, or none do
        if [[ "${node_vals[1]}" == "not found" || -z "${node_vals[1]}" ]]; then
            UNANIMOUS_MISSING=$((UNANIMOUS_MISSING + 1))
        else
            UNANIMOUS_PRESENT=$((UNANIMOUS_PRESENT + 1))
        fi
    else
        echo "!!! REAL MISMATCH on $key:$display"
        MISMATCHES=$((MISMATCHES + 1))
    fi
done

echo ""
echo "--- summary ---"
echo "keys checked:                        $CHECKED"
echo "all 3 nodes agree, value present:    $UNANIMOUS_PRESENT"
echo "all 3 nodes agree, value absent:     $UNANIMOUS_MISSING"
echo "REAL mismatches (nodes disagree):    $MISMATCHES"
echo ""
echo "note: 'all agree, value absent' is NOT a failure. A \"proposed at"
echo "index N\" response only means the leader accepted the write into its"
echo "own log — not that it reached a majority. If that leader died before"
echo "replicating (which this test does deliberately), the write is"
echo "correctly lost. Raft only guarantees COMMITTED entries survive."
echo "Nodes disagreeing with each other is the only real failure."

echo ""
echo "--- sample of final state (first 5 successful keys) ---"
sample_keys=$(echo "$SUCCESSFUL_KEYS" | head -5)
for key in $sample_keys; do
    echo -n "$key: "
    for id in 1 2 3; do
        port="${HTTPADDR_FOR_ID[$id]#:}"
        v=$(curl -s --max-time 2 "http://localhost:${port}/get?key=${key}" 2>/dev/null)
        echo -n "node$id=$v  "
    done
    echo ""
done

echo ""
if [[ "$MISMATCHES" -eq 0 && "$CHECKED" -gt 0 ]]; then
    echo "=== OVERALL: PASS — cluster survived overlapping failures during"
    echo "    continuous writes and converged to identical state everywhere ==="
else
    echo "=== OVERALL: FAIL — see mismatches above ==="
fi

echo "=== done — cleaning up ==="
sleep 1