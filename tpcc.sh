#!/usr/bin/bash -x

SYSTEMS=(2pl-no-wait occ-serial sto-disk sto-memory sto-counter sto-sketch sto-counter-lazy sto-sketch-lazy tictoc-disk tictoc-memory tictoc-counter tictoc-sketch tictoc-counter-lazy tictoc-sketch-lazy mvcc-memory mvcc-sketch mvcc-counter mvcc-disk mvcc-counter-lazy mvcc-sketch-lazy)
WORKLOADS=(tpcc-wh4 tpcc-wh8 tpcc-wh16 tpcc-wh32 tpcc-wh60 tpcc-wh1000)

LOG_DIR=$HOME/tpcc_logs
OUTPUT_DIR=$HOME/tpcc_results

DEV=/dev/nvme0n1

NRUNS=3

CACHE_SIZE=256
RUN_SEC=60

mkdir -p $LOG_DIR

for work in ${WORKLOADS[@]}
do
    for sys in ${SYSTEMS[@]}
    do
        if [[ "$sys" == mvcc-* && "$work" == *-upserts ]]; then
            continue
        fi

        # for thr in 1 2 4 8 12 16 20 24 28 32 36 40 44 48 52 56 60
        for thr in 60
        do
            for run in $(seq 1 ${NRUNS})
            do
                LOG_FILE="$LOG_DIR/${sys}_${work}_${thr}_${run}.log"

                # Skip if log file already exists and contains the desired line
                if [ -f "$LOG_FILE" ] && grep -q "# Transaction throughput (KTPS)" "$LOG_FILE"; then
                    continue
                fi

                if [[ "$work" == tpcc-wh60 ]]; then
                    CACHE_SIZE=512
                elif [[ "$work" == tpcc-wh1000 ]]; then
                    CACHE_SIZE=6144
                fi

                # Retry until the output file contains the desired line
                while true
                do
                    sudo blkdiscard $DEV
                    timeout -k 10 300 python3 -u ./tpcc.py -s "$sys" -w "$work" -t "$thr" -c "$CACHE_SIZE" -r "$RUN_SEC" -d "$DEV" | tee "$LOG_FILE"

                    # Check if the log file contains the required line
                    if grep -q "# Transaction throughput (KTPS)" "$LOG_FILE"; then
                        break
                    fi
                done
            done
        done
    done
done

mkdir -p $OUTPUT_DIR
python3 parse.py $LOG_DIR $OUTPUT_DIR