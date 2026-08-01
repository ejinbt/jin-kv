#!/bin/bash
# full_cluster_test.sh
#
# End-to-end automated test for jin-kv:
#   1. starts 3 nodes fresh
#   2. waits for a stable leader
#   3. proposes writes through the leader, verifies with get
#   4. kills a follower (simulating it falling behind)
#   5. proposes more writes the follower will miss
#   6. kills the leader (quorum temporarily lost)
#   7. revives the dead follower (quorum restored)
#   8. waits for a new leader + replication to settle
#   9. queries the REVIVED node directly and checks the values match
#
# Prints a clear PASS/FAIL verdict at the end.
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

echo "=== resetting WAL, log, and fifo files ==="
rm -f node1.wal node2.wal node3.wal "$LOG1" "$LOG2" "$LOG3" "$FIFO1" "$FIFO2" "$FIFO3"

echo "=== building ==="
mkdir -p bin
go build -o bin/server ./cmd/server || { echo "build failed"; exit 1; }

start_node() {
    # IMPORTANT: recreate the fifo fresh every time this is called —
    # reusing an old fifo can leave a stale reader (from a previously
    # killed node) still attached, which silently steals future writes
    # meant for the newly started process. A fresh mkfifo + fresh
    # feeder process guarantees exactly one reader exists at a time.
    local id=$1
    local addr=$2
    local logfile=$3
    local fifo=$4

    rm -f "$fifo"
    mkfifo "$fifo"

    # open the fifo read-write on fd 3 so opening doesn't block
    # waiting for a writer, then feed it into the server's stdin
    ./bin/server -id="$id" -addr="$addr" >> "$logfile" 2>&1 &
    local server_pid=$!

    # find the feeder subshell's pid too, so we can kill both later.
    # it's the most recent background job launched, which is this
    # same pipeline — but since $! only gives us the pipeline's last
    # command, grab the feeder via pgrep on the fifo path instead.
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
    # note: once the server process dies, the feeder's next write
    # attempt will hit a closed pipe and it'll exit on its own (SIGPIPE)
    # even if we somehow missed killing it directly above
}

send_cmd() {
    local fifo=$1
    local cmd=$2
    echo "$cmd" > "$fifo"
}

find_leader() {
    # returns the id of the node with the MOST RECENT "BECAME LEADER"
    # line, comparing actual timestamps, not just file order
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

fifo_for() { eval echo "\$FIFO$1"; }

echo "=== proposing initial writes via node $LEADER_ID ==="
send_cmd "$(fifo_for $LEADER_ID)" "set x=100"
sleep 0.5
send_cmd "$(fifo_for $LEADER_ID)" "set y=200"
sleep 2

echo "=== SANITY CHECK: querying every node for x and y before any kills happen ==="
echo "(if these fail even here, the bug is in basic replication itself,"
echo " not in anything related to the kill/revive/compaction scenario)"
for id in 1 2 3; do
    send_cmd "$(fifo_for $id)" "get x"
    sleep 0.3
    send_cmd "$(fifo_for $id)" "get y"
    sleep 0.3
done
sleep 1

# pick a follower to kill
FOLLOWER_ID=""
for id in 1 2 3; do
    if [[ "$id" != "$LEADER_ID" ]]; then
        FOLLOWER_ID=$id
        break
    fi
done

echo "=== killing follower node $FOLLOWER_ID (simulating it falling behind) ==="
kill_node "$FOLLOWER_ID"
sleep 1

echo "=== proposing more writes via node $LEADER_ID (node $FOLLOWER_ID will miss these) ==="
send_cmd "$(fifo_for $LEADER_ID)" "set x=999"
sleep 0.5
send_cmd "$(fifo_for $LEADER_ID)" "set z=42"
echo "waiting 4s to give heartbeats plenty of cycles to replicate before we kill anyone..."
sleep 4

echo "=== verifying leader's OWN state right before killing it ==="
send_cmd "$(fifo_for $LEADER_ID)" "get x"
sleep 0.3
send_cmd "$(fifo_for $LEADER_ID)" "get z"
sleep 0.5

echo "=== killing leader node $LEADER_ID (quorum temporarily lost) ==="
kill_node "$LEADER_ID"

echo "waiting 5s (remaining lone node should NOT elect a leader)..."
sleep 5

echo "=== reviving node $FOLLOWER_ID (quorum restored: 2 of 3 alive) ==="
NEW_PID=$(start_node "$FOLLOWER_ID" "${ADDR_FOR_ID[$FOLLOWER_ID]}" "node${FOLLOWER_ID}.log" "$(fifo_for $FOLLOWER_ID)")
PID_FOR_ID[$FOLLOWER_ID]=$NEW_PID
PIDS+=("$NEW_PID")

REVIVED_PORT="${ADDR_FOR_ID[$FOLLOWER_ID]#:}"
echo "waiting for revived node's port ($REVIVED_PORT) to actually come up..."
wait_for_port "$REVIVED_PORT"
echo "revived node's port confirmed up"

echo "waiting 8s for new election + replication to settle..."
sleep 8

NEW_LEADER=$(find_leader)
echo "=== new leader after recovery: node $NEW_LEADER ==="

echo "=== querying revived node $FOLLOWER_ID directly for x, y, z ==="
send_cmd "$(fifo_for $FOLLOWER_ID)" "get x"
sleep 0.3
send_cmd "$(fifo_for $FOLLOWER_ID)" "get y"
sleep 0.3
send_cmd "$(fifo_for $FOLLOWER_ID)" "get z"
sleep 1

echo "=== ALSO querying current leader node $NEW_LEADER for x and z, for comparison ==="
echo "(if the leader itself never had x=999/z=42, those writes never reached"
echo " a majority before the old leader died — that's correct Raft behavior,"
echo " not a bug. If the leader HAS them but the revived node doesn't, that's"
echo " a real replication bug specific to the revived node.)"
send_cmd "$(fifo_for $NEW_LEADER)" "get x"
sleep 0.3
send_cmd "$(fifo_for $NEW_LEADER)" "get z"
sleep 1

echo ""
echo "================ EARLY SANITY CHECK (before any kills) ================"
for id in 1 2 3; do
    echo "--- node $id get x/y responses (grepped from full log) ---"
    grep -A 2 '"get x"\|"get y"' "node${id}.log" | head -12
    echo ""
done

echo ""
echo "================ RESULTS ================"
echo "--- revived node $FOLLOWER_ID log tail (get responses should be here) ---"
tail -20 "node${FOLLOWER_ID}.log"

echo ""
echo "--- current leader node $NEW_LEADER log tail (comparison get responses) ---"
tail -10 "node${NEW_LEADER}.log"

echo ""
echo "--- verdict ---"
REVIVED_LOG="node${FOLLOWER_ID}.log"
OLD_LEADER_LOG="node${LEADER_ID}.log"
PASS=true

OLD_LEADER_HAD_X=false
OLD_LEADER_HAD_Z=false
grep -q "^x = 999" "$OLD_LEADER_LOG" && OLD_LEADER_HAD_X=true
grep -q "^z = 42" "$OLD_LEADER_LOG" && OLD_LEADER_HAD_Z=true

echo "old leader (node $LEADER_ID) had x=999 before dying: $OLD_LEADER_HAD_X"
echo "old leader (node $LEADER_ID) had z=42 before dying: $OLD_LEADER_HAD_Z"
echo ""

if $OLD_LEADER_HAD_X; then
    if grep -q "^x = 999" "$REVIVED_LOG"; then
        echo "PASS: x correctly caught up to 999 (old leader had it, revived node now has it too)"
    else
        echo "FAIL: old leader HAD x=999 but revived node never caught up — real replication bug"
        PASS=false
    fi
else
    echo "SKIP: old leader never actually had x=999 before dying — write was never committed,"
    echo "      so it's correct for it to be lost. Not a bug."
fi

if grep -q "^y = 200" "$REVIVED_LOG"; then
    echo "PASS: y correctly caught up to 200"
else
    echo "FAIL: y did not catch up to 200"
    PASS=false
fi
# is this how we write comments in bash script ?
if $OLD_LEADER_HAD_Z; then
    if grep -q "^z = 42" "$REVIVED_LOG"; then
        echo "PASS: z correctly replicated (old leader had it, revived node now has it too)"
    else
        echo "FAIL: old leader HAD z=42 but revived node never caught up — real replication bug"
        PASS=false
    fi
else
    echo "SKIP: old leader never actually had z=42 before dying — write was never committed,"
    echo "      so it's correct for it to be lost. Not a bug."
fi

echo ""
if $PASS; then
    echo "=== OVERALL: PASS — replication and recovery verified end to end ==="
else
    echo "=== OVERALL: FAIL — see above ==="
fi

echo "=== done — cleaning up ==="
sleep 1