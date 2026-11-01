#!/bin/bash
# usage: run_exp.sh N INTERVAL MOVES SEED OUTDIR
N=$1; INTERVAL=$2; MOVES=$3; SEED=$4; OUT=$5
BASEDIR=$(cd "$(dirname "$0")/.." && pwd)
HOST=$(hostname)
LIMIT=${LIMIT:-300}
mkdir -p "$OUT"
export CLASSPATH=$BASEDIR/comp512p2.jar:$BASEDIR
export TIMONOCHROME=true UPDATEDISPLAY=false
group=19
gameid=game-$group-99
group_str=""
for i in $(seq 1 $N); do group_str="$group_str,$HOST:40${i}$group"; done
group_str=${group_str#,}
cd "$OUT"
pids=()
for i in $(seq 1 $N); do
  java comp512st.tests.TreasureIslandAppAuto $HOST:40${i}$group $group_str $gameid $N $i $MOVES $INTERVAL $SEED$i > $gameid-$i-display.log 2>&1 &
  pids+=($!)
done
( sleep $LIMIT; kill -9 "${pids[@]}" 2>/dev/null; echo TIMEOUT > killed ) &
watcher=$!
wait "${pids[@]}"
kill $watcher 2>/dev/null
expected=$((N * MOVES))
ref=""
status=OK
for i in $(seq 1 $N); do
  c=$(grep -c 'Move request' $gameid-$i.log 2>/dev/null)
  h=$(grep 'Move request' $gameid-$i.log 2>/dev/null | md5 -q)
  m=$(tail -25 $gameid-$i-display.log | md5 -q)
  [ "$c" != "$expected" ] && status="MISSING(p$i:$c/$expected)"
  [ -z "$ref" ] && ref="$h$m"
  [ "$h$m" != "$ref" ] && status="MISMATCH"
done
[ -f killed ] && status="TIMEOUT"
echo "N=$N interval=$INTERVAL moves=$MOVES seed=$SEED status=$status" | tee status.txt
