#!/usr/bin/bash

set -e

SYSTEMS=(mvcc-memory mvcc-sketch mvcc-counter)
WORKLOADS=(write_intensive read_intensive)
NRUNS=2

for work in ${WORKLOADS[@]}
do 
    for sys in ${SYSTEMS[@]}
    do
	for thr in 1 4 8 12 16 20 24 28 32 36 40 44 48 52 56 60
	do
            for run in $(seq 1 ${NRUNS})
            do
                ./ycsb.py -s $sys -w $work -t $thr -c 6144 -r 60 > $HOME/${sys}_${work}_${thr}_${run}.log
            done
        done
    done
done
