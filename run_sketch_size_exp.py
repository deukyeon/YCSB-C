#!/usr/bin/env python3

import os
import signal
import sys
import time

def signal_handler(sig, frame):
    print("Received signal:", sig)
    print("Exiting...")
    os.remove(results_path)
    sys.exit(0)

# Register the signal handler for SIGINT
signal.signal(signal.SIGINT, signal_handler)

ycsb_path = os.getcwd()
splinterdb_path = "../splinterdb"

results_path = os.path.join(ycsb_path, "sketch_size_exp")
if not os.path.exists(results_path):
    os.mkdir(results_path)

results_path = os.path.join(results_path, f"{time.time()}")
os.mkdir(results_path)

# assume the branch of splinterdb is deukyeon/fantastiCC-refactor
os.chdir(splinterdb_path)
os.system("git checkout src/experimental_mode.h")
os.system("sed -i 's/#define EXPERIMENTAL_MODE_TICTOC_SKETCH [ ]*0/#define EXPERIMENTAL_MODE_TICTOC_SKETCH 1/g' src/experimental_mode.h")

for rows in [1, 2, 4]:
    os.system(f"sed -i 's/txn_splinterdb_cfg->sktch_config.rows = [0-9]\+;/txn_splinterdb_cfg->sktch_config.rows = {rows};/' src/transaction_impl/transaction_tictoc_sketch.h")
    cols = 262144
    while cols >= 1:
        os.system(f"sed -i 's/txn_splinterdb_cfg->sktch_config.cols = [0-9]\+;/txn_splinterdb_cfg->sktch_config.cols = {cols};/' src/transaction_impl/transaction_tictoc_sketch.h")

        os.system("sudo -E make clean")
        os.system("sudo -E make install")
        os.chdir(ycsb_path)
        os.system("make clean")
        os.system("make")

        output_path = os.path.join(results_path, f"rows_{rows}_cols_{cols}")
        os.system(f"LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so ./ycsbc -db transactional_splinterdb -threads 60 -L workloads/read_intensive.spec -p splinterdb.filename workloads/read_intensive.spec -p splinterdb.cache_size_mb 4096 -p splinterdb.filename /dev/md0 > {output_path} 2>&1")

        cols = cols // 2