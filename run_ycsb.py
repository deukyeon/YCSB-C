#!/usr/bin/python3

import os
import subprocess
import sys

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
    'sto-memory'
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
    'sto-memory': 'deukyeon/fantastiCC-refactor'
}

system_sed_map = {
    'baseline-parallel': ["sed -i 's/\/\/ #define PARALLEL_VALIDATION/#define PARALLEL_VALIDATION/g' src/transaction_private.h"],
    'silo-memory': ["sed -i 's/#define EXPERIMENTAL_MODE_SILO [ ]*0/#define EXPERIMENTAL_MODE_SILO 1/g' src/experimental_mode.h"],
    'tictoc-disk': ["sed -i 's/#define EXPERIMENTAL_MODE_TICTOC [ ]*0/#define EXPERIMENTAL_MODE_TICTOC 1/g' src/experimental_mode.h",
                    "sed -i 's/#define EXPERIMENTAL_MODE_DISK [ ]*0/#define EXPERIMENTAL_MODE_DISK 1/g' src/experimental_mode.h"],
    'tictoc-memory': ["sed -i 's/#define EXPERIMENTAL_MODE_TICTOC [ ]*0/#define EXPERIMENTAL_MODE_TICTOC 1/g' src/experimental_mode.h",
                      "sed -i 's/#define EXPERIMENTAL_MODE_MEMORY [ ]*0/#define EXPERIMENTAL_MODE_MEMORY 1/g' src/experimental_mode.h"],
    'tictoc-counter': ["sed -i 's/#define EXPERIMENTAL_MODE_TICTOC [ ]*0/#define EXPERIMENTAL_MODE_TICTOC 1/g' src/experimental_mode.h",
                       "sed -i 's/#define EXPERIMENTAL_MODE_COUNTER [ ]*0/#define EXPERIMENTAL_MODE_COUNTER 1/g' src/experimental_mode.h"],
    'tictoc-sketch': ["sed -i 's/#define EXPERIMENTAL_MODE_TICTOC [ ]*0/#define EXPERIMENTAL_MODE_TICTOC 1/g' src/experimental_mode.h",
                      "sed -i 's/#define EXPERIMENTAL_MODE_SKETCH [ ]*0/#define EXPERIMENTAL_MODE_SKETCH 1/g' src/experimental_mode.h"],
    'sto-memory': ["sed -i 's/#define EXPERIMENTAL_MODE_STO [ ]*0/#define EXPERIMENTAL_MODE_STO 1/g' src/experimental_mode.h",
                   "sed -i 's/#define EXPERIMENTAL_MODE_MEMORY [ ]*0/#define EXPERIMENTAL_MODE_MEMORY 1/g' src/experimental_mode.h"],
    'sto-sketch': ["sed -i 's/#define EXPERIMENTAL_MODE_STO [ ]*0/#define EXPERIMENTAL_MODE_STO 1/g' src/experimental_mode.h",
                   "sed -i 's/#define EXPERIMENTAL_MODE_SKETCH [ ]*0/#define EXPERIMENTAL_MODE_SKETCH 1/g' src/experimental_mode.h"],
    'sto-counter': ["sed -i 's/#define EXPERIMENTAL_MODE_STO [ ]*0/#define EXPERIMENTAL_MODE_STO 1/g' src/experimental_mode.h",
                    "sed -i 's/#define EXPERIMENTAL_MODE_COUNTER [ ]*0/#define EXPERIMENTAL_MODE_COUNTER 1/g' src/experimental_mode.h"]
}

available_workloads = [
    'write_intensive',
    'write_intensive_large_value',
    'write_intensive_test',
]


def printHelp():
    print("Usage:", sys.argv[0], "[system] [workload]", file=sys.stderr)
    print("\t[system]: Choose one of the followings --",
          available_systems, file=sys.stderr)
    print("\t[workload]: Choose one of the followings --",
          available_workloads, file=sys.stderr)
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


def buildSystem(sys):
    current_dir = os.getcwd()
    splinterdb_dir = '../splinterdb'
    os.chdir(splinterdb_dir)
    run_shell_command('git checkout -- .')
    run_shell_command(f'git checkout {system_branch_map[sys]}')
    run_shell_command('sudo -E make clean')
    if sys in system_sed_map:
        for sed in system_sed_map[sys]:
            run_shell_command(sed, shell=True)
    run_shell_command('sudo -E make install')
    os.chdir(current_dir)
    run_shell_command('make clean')
    run_shell_command('make')


def parseLogfile(logfile_path, csv, system, conf, seq):
    f = open(logfile_path, "r")
    lines = f.readlines()
    f.close()

    load_threads = []
    load_tputs = []
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
    if argc < 2:
        printHelp()

    system = argv[1]
    if system not in available_systems:
        printHelp()

    conf = argv[2]
    if conf not in available_workloads:
        printHelp()

    parse_result_only = False
    if len(argv) > 3:
        parse_result_only = bool(argv[3])

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

    buildSystem(system)

    db = 'splinterdb' if system == 'splinterdb' else 'transactional_splinterdb'
    spec_file = 'workloads/' + conf + '.spec'

    # This is the maximum number of threads that run YCSB clients.
    max_num_threads = min(os.cpu_count(), 32)
    
    # This is the maximum number of threads that can be run in parallel in the system.
    max_total_threads = 60

    dev_name = '/dev/md0'
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
