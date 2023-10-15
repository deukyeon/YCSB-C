#!/bin/bash

result_dir=tpcc_results

! test -d $result_dir && mkdir -p $result_dir

for sys in tictoc-memory tictoc-counter tictoc-sketch sto-memory sto-counter sto-sketch 2pl-no-wait baseline-serial baseline-parallel tictoc-disk sto-disk
do
    for wl in tpcc-wh4 tpcc-wh8 tpcc-wh16 tpcc-wh32 tpcc-wh4-upserts tpcc-wh8-upserts tpcc-wh16-upserts tpcc-wh32-upserts
    do
	python3 run_tpcc.py -s $sys -w $wl -r 60 -f
	cp $sys-$wl $sys-$wl.csv $result_dir
    done
done
