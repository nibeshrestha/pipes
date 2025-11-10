#!/bin/bash

RUNS=1
BLKPROPTIMES=(400 500 600)

for BPT in ${BLKPROPTIMES[@]}
do
    for i in $(seq 1 1 "$RUNS")
    do
        if fab remote --client-rate $BPT \
            | tee /dev/tty \
            | grep -i "error\|exception\|traceback"
        then
            echo "Failed to complete remote benchmark"
            fab kill
            exit 2
        fi
    fab kill
    sleep 20
    done
done