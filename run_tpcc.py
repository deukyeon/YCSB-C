#!/usr/bin/python3

import os
import subprocess
import sys
import getopt

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

def printHelp():
    print("Usage:", sys.argv[0], "-s [system] -u -d [device] -f -p", file=sys.stderr)
    print("\t-s,--system [system]: Choose one of the followings --",
          available_systems, file=sys.stderr)
    print("\t-u,--upsert: Enable the upsertification", file=sys.stderr)
    print("\t-d,--device [device]: Choose the device for SplinterDB (default: /dev/md0)", file=sys.stderr)
    print("\t-f,--force: Force to run (Delete all existing logs)", file=sys.stderr)
    print("\t-p,--parse: Parse the logs without running", file=sys.stderr)
    exit(1)


def run_shell_command(cmd, parse=True, shell=False):
    if not parse:
        print(" ".join(cmd))
    else:
        print(cmd)

    if parse and not shell:
        cmd = cmd.split()
    sp = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=shell)
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


def parseLogfile(logfile_path, csv, system, conf, seq, num_wh):
    f = open(logfile_path, "r")
    lines = f.readlines()
    f.close()

    run_threads = []
    run_tputs = []

    abort_counts = []
    abort_rates = []

    run_data = False

    for line in lines:
        if run_data:
            fields = line.split()
            run_threads.append(fields[-2])
            run_tputs.append(fields[-1])
            run_data = False

        if line.startswith("# Transaction goodput (KTPS)"):
            run_data = True

        if line.startswith("# Abort count"):
            fields = line.split()
            abort_counts.append(fields[-1])

        if line.startswith("Abort rate"):
            fields = line.split()
            abort_rates.append(fields[-1])

    # print csv
    for tuple in zip(run_threads, run_tputs, abort_counts, abort_rates):
        print(system, conf, num_wh, ','.join(tuple), seq, sep=',', file=csv)


def change_num_warehouses(num_wh):
    run_shell_command("git checkout core/tpcc_config.h", shell=True)
    run_shell_command(f"sed -i 's/#define NUM_WH.*/#define NUM_WH {num_wh}/g' core/tpcc_config.h", shell=True)
    run_shell_command("make clean", shell=True)
    run_shell_command("make", shell=True)


def main(argc, argv):
    if argc < 2:
        printHelp()

    system = argv[1]
    if system not in available_systems:
        printHelp()

    conf = 'tpcc'

    upsert_opt = ''
    if len(argv) > 2 and argv[2] == 'upsert':
        conf = 'tpcc-upsert'
        upsert_opt = '-upserts'

    parse_result_only = False
    if len(argv) > 3:
        parse_result_only = bool(argv[3])
    
    force_to_run = False

    opts, _ = getopt.getopt(sys.argv[1:], 's:ud:pf', 
                            ['system=', 'upsert', 'device', 'parse', 'force'])
    system = None
    conf = None
    dev_name = '/dev/md0'
    
    for opt, arg in opts:
        if opt in ('-s', '--system'):
            system = arg
            if system not in available_systems:
                printHelp()
        elif opt in ('-u', '--upsert'):
            conf = 'tpcc-upsert'
            upsert_opt = '-upserts'
        elif opt in ('-d', '--device'):
            dev_name = arg
            if not os.path.exists(dev_name):
                print(f'Device {dev_name} does not exist.', file=sys.stderr)
                exit(1)
        elif opt in ('-p', '--parse'):
            parse_result_only = True
        elif opt in ('-f', '--force'):
            force_to_run = True

    num_repeats = 5
    num_warehouses = [4, 8, 32]

    label = system + '-' + conf
    csv = open(f'{label}.csv', 'w')
    print("system,conf,num_wh,threads,goodput,aborts,abort_rate,seq", file=csv)
    if parse_result_only:
        for i in range(0, num_repeats):
            for wh in num_warehouses:
                log_path = f'/tmp/{label}-wh{wh}.{i}.log'
                parseLogfile(log_path, csv, system, conf, i, wh)
        return

    buildSystem(system)

    db = 'splinterdb' if system == 'splinterdb' else 'transactional_splinterdb'

    # This is the maximum number of threads that run YCSB clients.
    max_num_threads = min(os.cpu_count(), 32)
    
    # This is the maximum number of threads that can be run in parallel in the system.
    max_total_threads = 60

    cache_size_mb = 128

    cmds = []
    for thread in [1] + list(range(4, max_num_threads + 1, 4)):
        num_normal_bg_threads = thread
        num_memtable_bg_threads = (thread + 9) // 10

        total_num_threads = thread + num_normal_bg_threads + num_memtable_bg_threads
        if total_num_threads > max_total_threads:
            num_normal_bg_threads -= (total_num_threads - max_total_threads)
        splinterdb_opts = '-p splinterdb.filename {dev_name} \
                            -p splinterdb.cache_size_mb {cache_size_mb} \
                            -p splinterdb.num_normal_bg_threads {num_normal_bg_threads} \
                            -p splinterdb.num_memtable_bg_threads {num_memtable_bg_threads}'
        cmd = f'LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so ./ycsbc -db {db} \
            -threads {thread} -benchmark tpcc {splinterdb_opts} {upsert_opt}'
        cmds.append(cmd)

    for i in range(0, num_repeats):
        for wh in num_warehouses:
            change_num_warehouses(wh)
            log_path = f'/tmp/{label}-wh{wh}.{i}.log'
            if force_to_run:
                os.remove(log_path)
            if os.path.isfile(log_path):
                continue
            logfile = open(log_path, 'w')
            for cmd in cmds:
                logfile.write(f'{cmd}\n')
                out, _ = run_shell_command(cmd, shell=True)
                if out:
                    logfile.write(out.decode())
            logfile.close()
            
    for i in range(0, num_repeats):
        for wh in num_warehouses:
            log_path = f'/tmp/{label}-wh{wh}.{i}.log'
            parseLogfile(log_path, csv, system, conf, i, wh)
    csv.close()

if __name__ == '__main__':
    main(len(sys.argv), sys.argv)
