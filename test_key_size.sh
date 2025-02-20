#!/bin/bash -x

WORKLOAD=write_intensive_test

KEY_SIZE=${1:-24}
VALUE_SIZE=${2:-100}

LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so \
    ./ycsbc -db transactional_splinterdb -threads 60 -benchmark_seconds 30.0 -client txn \
    -L workloads/"$WORKLOAD".spec -W workloads/"$WORKLOAD".spec \
    -p splinterdb.filename /dev/md127 -p splinterdb.cache_size_mb 6144 \
    -p splinterdb.num_normal_bg_threads 0 -p splinterdb.num_memtable_bg_threads 0 \
    -p splinterdb.disable_upsert 1 -p splinterdb.io_contexts_per_process 64 -p splinterdb.disk_size_gb 1787 \
    -p splinterdb.max_key_size "$KEY_SIZE" \
    -w zeropadding $(("$KEY_SIZE" - 4)) -w fieldlength "$VALUE_SIZE"
