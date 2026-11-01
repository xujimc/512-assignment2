#!/bin/bash
# usage: sweep.sh DATADIR
DATA=$1
HERE=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$DATA"
run() {
  local name=$1 n=$2 iv=$3 mv=$4 seed=$5
  local d="$DATA/$name/n${n}_i${iv}_${seed}"
  [ -f "$d/status.txt" ] && return
  "$HERE/run_exp.sh" $n $iv $mv $seed "$d" 2>/dev/null
  for f in "$d"/*processinfo*.log; do
    i=$(echo "$f" | sed -E 's/.*-([0-9]+)-processinfo-\.log/\1/')
    grep "PERF" "$f" > "$d/perf-$i.txt"
    rm -f "$f"
  done
  rm -f "$d"/*-display.log.lck "$d"/*.lck
}
for seed in a b; do
  for n in 2 3 5 7 9; do
    for iv in 0 1 2 5 10 20 50 100 200; do
      run sweep $n $iv 100 $seed
    done
  done
done
for seed in f1 f2 f3 f4 f5; do
  for n in 5 9; do
    run fair $n 0 200 $seed
  done
done
echo ALL_DONE
