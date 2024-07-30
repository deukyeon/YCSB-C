#!/usr/bin/bash -x

SYSTEMS=(2pl-no-wait occ-serial occ-parallel sto-disk sto-memory sto-counter sto-sketch tictoc-disk tictoc-memory tictoc-counter tictoc-sketch)
# mvcc-memory mvcc-sketch mvcc-counter mvcc-disk will be added later.
WORKLOADS=(write_intensive read_intensive write_intensive_medium read_intensive_medium)
NRUNS=2

LOG_DIR=$HOME/ycsb_logs
OUTPUT_DIR=$HOME/ycsb_results

DEV=/dev/md0

mkdir -p $LOG_DIR

for work in ${WORKLOADS[@]}
do 
    for sys in ${SYSTEMS[@]}
    do
	for thr in 1 4 8 12 16 20 24 28 32 36 40 44 48 52 56 60
	do
            for run in $(seq 1 ${NRUNS})
            do
                ./ycsb.py -s $sys -w $work -t $thr -c 6144 -r 60 -d $DEV | tee $LOG_DIR/${sys}_${work}_${thr}_${run}.log
            done
        done
    done
done

mkdir -p $OUTPUT_DIR
python3 parse.py $LOG_DIR $OUTPUT_DIR