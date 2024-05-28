#!/usr/bin/bash

set -e

SYSTEMS=(mvcc-memory mvcc-sketch mvcc-counter)
WORKLOADS=(write-intensive)
NRUNS=2

for thr in 1 2 4 8 12 16 20 24 28 32 36 40 44 48 52 56 60
do
    for sys in ${SYSTEMS[@]}
    do
        for work in ${WORKLOADS[@]}
        do 
            for run in {1..${NRUNS}}
            do
                ./ycsb.py -s $sys -w ${work} -t 60 -c 6144 -r 60 > $HOME/${sys}_${work}_${thr}_${run}.log
            done
        done
    done
done
