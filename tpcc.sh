#!/usr/bin/bash -x

SYSTEMS=(2pl-no-wait occ-serial occ-parallel sto-disk sto-memory sto-counter sto-sketch tictoc-disk tictoc-memory tictoc-counter tictoc-sketch)
#  mvcc-memory mvcc-sketch mvcc-counter mvcc-disk will be added later.
WORKLOADS=(tpcc-wh4 tpcc-wh8 tpcc-wh16 tpcc-wh32 tpcc-wh4-upserts tpcc-wh8-upserts tpcc-wh16-upserts tpcc-wh32-upserts)

LOG_DIR=$HOME/tpcc_logs
OUTPUT_DIR=$HOME/tpcc_results

DEV=/dev/nvme0n1

NRUNS=1

mkdir -p $LOG_DIR

for work in ${WORKLOADS[@]}
do 
    for sys in ${SYSTEMS[@]}
    do
	for thr in 1 4 8 12 16 20 24 28 32 36 40 44 48 52 56 60
	do
            for run in $(seq 1 ${NRUNS})
            do
                LOG_FILE=$LOG_DIR/${sys}_${work}_${thr}_${run}.log
                [ -f "$LOG_FILE" ] && continue
                ./tpcc.py -s $sys -w $work -t $thr -c 6144 -r 60 -d $DEV | tee $LOG_FILE
            done
        done
    done
done

mkdir -p $OUTPUT_DIR
python3 parse.py $LOG_DIR $OUTPUT_DIR