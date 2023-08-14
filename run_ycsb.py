#!/usr/bin/python3

import os
import subprocess
import sys
import getopt
from exp_system import ExpSystem

available_systems = [
    'splinterdb',
    'tictoc-disk',
    'silo-disk',
    'baseline-serial',
    'baseline-parallel',
    'silo-memory',
    'tictoc-memory',
    'tictoc-counter',
    'tictoc-sketch',
    'sto-sketch',
    'sto-counter',
    'sto-memory',
    '2pl'
]

system_branch_map = {
    'splinterdb': 'deukyeon/fantastiCC-refactor',
    'tictoc-disk': 'deukyeon/fantastiCC-refactor',
    'silo-disk': 'deukyeon/silo-disk',
    'baseline-serial': 'deukyeon/baseline',
    'baseline-parallel': 'deukyeon/baseline',
    'silo-memory': 'deukyeon/fantastiCC-refactor',
    'tictoc-memory': 'deukyeon/fantastiCC-refactor',
    'tictoc-counter': 'deukyeon/fantastiCC-refactor',
    'tictoc-sketch': 'deukyeon/fantastiCC-refactor',
    'sto-sketch': 'deukyeon/fantastiCC-refactor',
    'sto-counter': 'deukyeon/fantastiCC-refactor',
    'sto-memory': 'deukyeon/fantastiCC-refactor',
    '2pl': 'deukyeon/fantastiCC-refactor'
}

system_sed_map = {
    'baseline-parallel': ["sed -i 's/\/\/ #define PARALLEL_VALIDATION/#define PARALLEL_VALIDATION/g' src/transaction_private.h"],
    'silo-memory': ["sed -i 's/#define EXPERIMENTAL_MODE_SILO_MEMORY [ ]*0/#define EXPERIMENTAL_MODE_SILO_MEMORY 1/g' src/experimental_mode.h"],
    'tictoc-disk': ["sed -i 's/#define EXPERIMENTAL_MODE_TICTOC_DISK [ ]*0/#define EXPERIMENTAL_MODE_TICTOC_DISK 1/g' src/experimental_mode.h"],
    'tictoc-memory': ["sed -i 's/#define EXPERIMENTAL_MODE_TICTOC_MEMORY [ ]*0/#define EXPERIMENTAL_MODE_TICTOC_MEMORY 1/g' src/experimental_mode.h"],
    'tictoc-counter': ["sed -i 's/#define EXPERIMENTAL_MODE_TICTOC_COUNTER [ ]*0/#define EXPERIMENTAL_MODE_TICTOC_COUNTER 1/g' src/experimental_mode.h"],
    'tictoc-sketch': ["sed -i 's/#define EXPERIMENTAL_MODE_TICTOC_SKETCH [ ]*0/#define EXPERIMENTAL_MODE_TICTOC_SKETCH 1/g' src/experimental_mode.h"],
    'sto-memory': ["sed -i 's/#define EXPERIMENTAL_MODE_STO_MEMORY [ ]*0/#define EXPERIMENTAL_MODE_STO_MEMORY 1/g' src/experimental_mode.h"],
    'sto-sketch': ["sed -i 's/#define EXPERIMENTAL_MODE_STO_SKETCH [ ]*0/#define EXPERIMENTAL_MODE_STO_SKETCH 1/g' src/experimental_mode.h"],
    'sto-counter': ["sed -i 's/#define EXPERIMENTAL_MODE_STO_COUNTER [ ]*0/#define EXPERIMENTAL_MODE_STO_COUNTER 1/g' src/experimental_mode.h"],
    '2pl': ["sed -i 's/#define EXPERIMENTAL_MODE_2PL [ ]*0/#define EXPERIMENTAL_MODE_2PL 1/g' src/experimental_mode.h"],
}

available_workloads = [
    'write_intensive',
    'write_intensive_large_value',
    'write_intensive_test',
]


def printHelp():
    print("Usage:", sys.argv[0], "-s [system] -w [workload] -d [device] -f -p ", file=sys.stderr)
    print("\t-s,--system [system]: Choose one of the followings --",
          available_systems, file=sys.stderr)
    print("\t-w,--workload [workload]: Choose one of the followings --",
          available_workloads, file=sys.stderr)
    print("\t-d,--device [device]: Choose the device for SplinterDB (default: /dev/md0)", file=sys.stderr)
    print("\t-f,--force: Force to run (Delete all existing logs)", file=sys.stderr)
    print("\t-p,--parse: Parse the logs without running", file=sys.stderr)
    exit(1)


def run_shell_command(cmd, parse=True, shell=False):
    if parse and not shell:
        cmd = cmd.split()
    sp = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, shell=shell)
    out, err = sp.communicate()
    if out:
        print(out.decode())
    if err:
        print(err.decode(), file=sys.stderr)
    return out, err

def parseLogfile(logfile_path, csv, system, conf, seq):
    f = open(logfile_path, "r")
    lines = f.readlines()
    f.close()

    # load_threads = []
    # load_tputs = []
    run_threads = []
    run_tputs = []

    abort_counts = []
    abort_rates = []

    # load_data = False
    run_data = False

    for line in lines:
        # if load_data:
        #     fields = line.split()
        #     load_threads.append(fields[-2])
        #     load_tputs.append(fields[-1])
        #     load_data = False

        # if line.startswith("# Load throughput (KTPS)"):
        #     load_data = True

        if run_data:
            fields = line.split()
            run_threads.append(fields[-2])
            run_tputs.append(fields[-1])
            run_data = False

        if line.startswith("# Transaction throughput (KTPS)"):
            run_data = True

        if line.startswith("# Abort count"):
            fields = line.split()
            abort_counts.append(fields[-1])

        if line.startswith("Abort rate"):
            fields = line.split()
            abort_rates.append(fields[-1])

    # print csv
    # for tuple in zip(run_threads, load_tputs, run_tputs, abort_counts, abort_rates):
    for tuple in zip(run_threads, run_tputs, abort_counts, abort_rates):
        print(system, conf, ','.join(tuple), seq, sep=',', file=csv)


def main(argc, argv):
    parse_result_only = False
    force_to_run = False

    opts, _ = getopt.getopt(sys.argv[1:], 's:w:d:pfh', 
                            ['system=', 'workload=', 'device=', 'parse', 'force', 'help'])
    system = None
    conf = None
    dev_name = '/dev/md0'

    for opt, arg in opts:
        if opt in ('-s', '--system'):
            system = arg
            if system not in available_systems:
                printHelp()
        elif opt in ('-w', '--workload'):
            conf = arg
            if conf not in available_workloads:
                printHelp()
        elif opt in ('-d', '--device'):
            dev_name = arg
            if not os.path.exists(dev_name):
                print(f'Device {dev_name} does not exist.', file=sys.stderr)
                exit(1)
        elif opt in ('-p', '--parse'):
            parse_result_only = True
        elif opt in ('-f', '--force'):
            force_to_run = True
        elif opt in ('-h', '--help'):
            printHelp()

    if not system or not conf:
        printHelp()

    label = system + '-' + conf
    csv_path = f'{label}.csv'
    csv = open(csv_path, 'w')
    print("system,conf,threads,goodput,aborts,abort_rate,seq", file=csv)
    num_repeats = 2
    if parse_result_only:
        for i in range(0, num_repeats):
            log_path = f'/tmp/{label}.{i}.log'
            parseLogfile(log_path, csv, system, conf, i)
        return

    ExpSystem.build(system, '../splinterdb')

    db = 'splinterdb' if system == 'splinterdb' else 'transactional_splinterdb'
    spec_file = 'workloads/' + conf + '.spec'

    # This is the maximum number of threads that run YCSB clients.
    max_num_threads = min(os.cpu_count(), 32)
    
    # This is the maximum number of threads that can be run in parallel in the system.
    max_total_threads = 60

    cache_size_mb = 4096

    cmds = []
    for thread in [1] + list(range(4, max_num_threads + 1, 4)):
        num_normal_bg_threads = thread
        num_memtable_bg_threads = (thread + 9) // 10

        total_num_threads = thread + num_normal_bg_threads + num_memtable_bg_threads
        if total_num_threads > max_total_threads:
            num_normal_bg_threads -= (total_num_threads - max_total_threads)

        cmd = f'LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so \
            ./ycsbc -db {db} -threads {thread} -client txn \
            -P {spec_file} -W {spec_file} \
            -p splinterdb.filename {dev_name} \
            -p splinterdb.cache_size_mb {cache_size_mb} \
            -p splinterdb.num_normal_bg_threads {num_normal_bg_threads} \
            -p splinterdb.num_memtable_bg_threads {num_memtable_bg_threads}'
        cmds.append(cmd)

    for i in range(0, num_repeats):
        log_path = f'/tmp/{label}.{i}.log'
        if os.path.isfile(log_path):
            if force_to_run:
                os.remove(log_path)
            else:
                continue
        logfile = open(log_path, 'w')
        specfile = open(spec_file, 'r')
        logfile.writelines(specfile.readlines())
        specfile.close()
        for cmd in cmds:
            # run_shell_command('fallocate -l 500GB splinterdb.db')
            logfile.write(f'{cmd}\n')
            run_shell_command(
                'echo 1 | sudo tee /proc/sys/vm/drop_caches > /dev/null')

            # run load phase
            run_shell_command(f'LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so ./ycsbc -db {db} -threads {max_num_threads} -L {spec_file} -p splinterdb.filename {dev_name} -p splinterdb.cache_size_mb 4096', shell=True)

            out, _ = run_shell_command(cmd, shell=True)
            if out:
                logfile.write(out.decode())
            # run_shell_command('rm -f splinterdb.db')
        logfile.close()
    
    for i in range(0, num_repeats):
        log_path = f'/tmp/{label}.{i}.log'
        parseLogfile(log_path, csv, system, conf, i)
    csv.close()


if __name__ == '__main__':
    main(len(sys.argv), sys.argv)
