#!/usr/bin/bash -x

SYSTEMS=(2pl-no-wait occ-serial occ-parallel sto-disk sto-memory sto-counter sto-sketch tictoc-disk tictoc-memory tictoc-counter tictoc-sketch)
# mvcc-memory mvcc-sketch mvcc-counter mvcc-disk will be added later.
WORKLOADS=(write_intensive read_intensive write_intensive_medium read_intensive_medium)

LOG_DIR=$HOME/ycsb_logs
OUTPUT_DIR=$HOME/ycsb_results

DEV=/dev/nvme0n1

NRUNS=1

mkdir -p $LOG_DIR

for work in ${WORKLOADS[@]}
do 
    for sys in ${SYSTEMS[@]}
    do
        for thr in 1 4 8 12 16 20 24 28 32 36 40 44 48 52 56 60
        do
            for run in {0..$NRUNS}
            do
                LOG_FILE=$LOG_DIR/${sys}_${work}_${thr}_${run}.log
                [ -f "$LOG_FILE" ] && continue
                ./ycsb.py -s $sys -w $work -t $thr -c 6144 -r 240 -d $DEV | tee $LOG_FILE
            done
        done
    done
done

mkdir -p $OUTPUT_DIR
python3 parse.py $LOG_DIR $OUTPUT_DIR