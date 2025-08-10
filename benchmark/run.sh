#!/bin/bash

RUNS=1
BURSTS=(100 200 300 400 500 600 700 800 )
BLKPROPTIMES=(520 530 540 550 560 570 580 590 600 610 620 630 640)

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