#!/bin/bash

result_dir=ycsb_results

! test -d $result_dir && mkdir -p $result_dir

for sys in tictoc-memory tictoc-counter tictoc-sketch sto-memory sto-counter sto-sketch 2pl-no-wait baseline-serial baseline-parallel tictoc-disk sto-disk
do
    for wl in read_intensive write_intensive read_intensive_medium write_intensive_medium
    do
	python3 run_ycsb.py -s $sys -w $wl -r 60 -c 6144 -f
	cp $sys-$wl $sys-$wl.csv $result_dir
    done
done
