#!/usr/bin/bash -x

set -e

SPLINTERDB_HOME=$(pwd)/../splinterdb

TICTOC_CACHE_OUT=tictoc-cache-dynamic.out
FANTASTICC_OUT=fantasticc-dynamic.out

function compile_ycsb() {
	make
}

function compile_tictoc_cache() {
	cd $SPLINTERDB_HOME
	# make clean
	git checkout src/experimental_mode.h
	sed -i 's/#define EXPERIMENTAL_MODE_KEEP_ALL_KEYS.*/#define EXPERIMENTAL_MODE_KEEP_ALL_KEYS 1/g' src/experimental_mode.h
	make
	cd -
}

function compile_fantasticc() {
	cd $SPLINTERDB_HOME
	# make clean
	git checkout src/experimental_mode.h
	make
	cd -
}

function run() {
	export LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so
	echo 1 | sudo tee /proc/sys/vm/drop_caches
	./ycsbc -db transactional_splinterdb -threads 32 -L workloads/read_intensive.spec -W workloads/read_intensive.spec -W workloads/write_intensive.spec &>$1
}


compile_tictoc_cache
compile_ycsb
run $TICTOC_CACHE_OUT
compile_fantasticc
run $FANTASTICC_OUT

tail $TICTOC_CACHE_OUT $FANTASTICC_OUT
