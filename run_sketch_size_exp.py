#!/usr/bin/env python3

import os
import sys
import time
import getopt
import subprocess

def print_help():
    print("\t-h,--help: Print this help message", file=sys.stderr)
    print("\t-p,--parse [results path]: Parse the results in the given directory", file=sys.stderr)
    exit(1)

try:
    opts, args = getopt.getopt(sys.argv[1:], "hp:", ["help", "parse="])
except getopt.GetoptError as err:
    print_help()

def parse_result(results_path):
    results = []
    for file in os.listdir(results_path):
        if not file.startswith("rows_"):
            continue
        f = open(f"{results_path}/{file}", "r")
        lines = f.readlines()
        f.close()

        run_data = False

        for line in lines:
            if run_data:
                fields = line.split()
                run_threads = fields[-2]
                run_tputs = fields[-1]
                run_data = False

            if line.startswith("# Transaction throughput (KTPS)"):
                run_data = True

            if line.startswith("# Abort count"):
                fields = line.split()
                abort_counts = fields[-1]

            if line.startswith("Abort rate"):
                fields = line.split()
                abort_rates = fields[-1]

        filename_splits = file.split("_")
        rows = int(filename_splits[1])
        cols = int(filename_splits[3])

        size_bytes = rows * cols * (128 // 8)

        results.append((str(size_bytes), str(rows), str(cols), run_threads, run_tputs, abort_counts, abort_rates))

    results.sort(key=lambda x: (int(x[0]), int(x[1]), int(x[2])))

    output = open(f"{results_path}/results.csv", "w")
    print("size_bytes\trows\tcols\trun_threads\trun_tputs\tabort_counts\tabort_rates", file=output)
    for result in results:
        print("\t".join(result), file=output)
    output.close()

for o, a in opts:
    if o in ('-h', '--help'):
        print_help()
    if o in ('-p', '--parse'):
        parse_result(a)
        exit(0)


def run_cmd(cmd):
    subprocess.call(cmd, shell=True)

ycsb_path = os.getcwd()
splinterdb_path = os.path.abspath("../splinterdb")

results_path = os.path.join(ycsb_path, "sketch_size_exp")
if not os.path.exists(results_path):
    os.mkdir(results_path)

results_path = os.path.join(results_path, f"{time.time()}")
os.mkdir(results_path)

# assume the branch of splinterdb is deukyeon/fantastiCC-refactor
os.chdir(splinterdb_path)
run_cmd("git checkout src/experimental_mode.h")
run_cmd("sed -i 's/#define EXPERIMENTAL_MODE_TICTOC_SKETCH [ ]*0/#define EXPERIMENTAL_MODE_TICTOC_SKETCH 1/g' src/experimental_mode.h")

os.environ['CC'] = "clang"
os.environ['LD'] = "clang"

max_size = 8 * 1024 * 1024
rowsxcols = max_size // (128 // 8)
for rows in [1, 2, 4]:
    cols = rowsxcols // rows
    while cols >= 1:
        os.chdir(splinterdb_path)
        run_cmd(f"sed -i 's/txn_splinterdb_cfg->sktch_config.rows = [0-9]\+;/txn_splinterdb_cfg->sktch_config.rows = {rows};/' src/transaction_impl/transaction_tictoc_sketch.h")
        run_cmd(f"sed -i 's/txn_splinterdb_cfg->sktch_config.cols = [0-9]\+;/txn_splinterdb_cfg->sktch_config.cols = {cols};/' src/transaction_impl/transaction_tictoc_sketch.h")
        run_cmd("sudo -E make clean")
        run_cmd("sudo -E make install")

        os.chdir(ycsb_path)
        run_cmd("make clean")
        run_cmd("make")

        output_path = os.path.join(results_path, f"rows_{rows}_cols_{cols}")
        run_cmd(f"LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so ./ycsbc \
                -db transactional_splinterdb \
                -threads 60 \
                -L workloads/read_intensive.spec \
                -W workloads/read_intensive.spec \
                -p splinterdb.filename /dev/md0 \
                -p splinterdb.cache_size_mb 4096 \
                > {output_path} 2>&1")

        cols = cols // 2