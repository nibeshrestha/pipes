#!/bin/bash

RUNS=1
BLKPROPTIMES=(3150 3180 3200 3220 3230 3240 3250 3260 3270 3280)

for BPT in ${BLKPROPTIMES[@]}
do
    for i in $(seq 1 1 "$RUNS")
    do
        if fab remote --bpt $BPT \
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