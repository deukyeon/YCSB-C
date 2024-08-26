#!/usr/bin/bash -x

SYSTEMS=(2pl-no-wait occ-serial occ-parallel sto-disk sto-memory sto-counter sto-sketch tictoc-disk tictoc-memory tictoc-counter tictoc-sketch mvcc-memory mvcc-sketch mvcc-counter mvcc-disk)
WORKLOADS=(tpcc-wh4 tpcc-wh8 tpcc-wh16 tpcc-wh32 tpcc-wh4-upserts tpcc-wh8-upserts tpcc-wh16-upserts tpcc-wh32-upserts)

LOG_DIR=$HOME/tpcc_logs
OUTPUT_DIR=$HOME/tpcc_results

DEV=/dev/nvme0n1

NRUNS=1

CACHE_SIZE=256
RUN_SEC=240

mkdir -p $LOG_DIR

for work in ${WORKLOADS[@]}
do 
    # [ $work == "tpcc-wh4" ] && CACHE_SIZE=296
    # [ $work == "tpcc-wh8" ] && CACHE_SIZE=336
    # [ $work == "tpcc-wh16" ] && CACHE_SIZE=416
    # [ $work == "tpcc-wh32" ] && CACHE_SIZE=576
    # [ $work == "tpcc-wh4-upserts" ] && CACHE_SIZE=296
    # [ $work == "tpcc-wh8-upserts" ] && CACHE_SIZE=336
    # [ $work == "tpcc-wh16-upserts" ] && CACHE_SIZE=416
    # [ $work == "tpcc-wh32-upserts" ] && CACHE_SIZE=576
    for sys in ${SYSTEMS[@]}
    do
        for thr in 1 4 8 12 16 20 24 28 32 36 40 44 48 52 56 60
        do
            for run in $(seq 1 ${NRUNS})
            do
                LOG_FILE="$LOG_DIR/${sys}_${work}_${thr}_${run}.log"

                # Skip if log file already exists and contains the desired line
                if [ -f "$LOG_FILE" ] && grep -q "# Transaction throughput (KTPS)" "$LOG_FILE"; then
                    continue
                fi

                # Retry until the output file contains the desired line
                while true
                do
                    timeout -k 10 900 python3 -u ./tpcc.py -s "$sys" -w "$work" -t "$thr" -c "$CACHE_SIZE" -r "$RUN_SEC" -d "$DEV" | tee "$LOG_FILE"

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