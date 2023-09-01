#!/usr/bin/python3

import os
import subprocess
import sys
import getopt
from exp_system import *

def printHelp():
    print("Usage:", sys.argv[0], "-s [system] -u -d [device] -f -p -b -c [cache_size_mb] -h", file=sys.stderr)
    print("\t-s,--system [system]: Choose one of the followings --",
          available_systems, file=sys.stderr)
    print("\t-u,--upsert: Enable the upsertification", file=sys.stderr)
    print("\t-d,--device [device]: Choose the device for SplinterDB (default: /dev/md0)", file=sys.stderr)
    print("\t-f,--force: Force to run (Delete all existing logs)", file=sys.stderr)
    print("\t-p,--parse: Parse the logs without running", file=sys.stderr)
    print("\t-b,--bgthreads: Enable background threads", file=sys.stderr)
    print("\t-c,--cachesize: Set the cache size in MB (default: 128)", file=sys.stderr)
    print("\t-h,--help: Print this help message", file=sys.stderr)
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
    opts, _ = getopt.getopt(sys.argv[1:], 's:ud:pfbc:h', 
                            ['system=', 'upsert', 'device=', 'parse', 'force', 'bgthreads', 'cachesize=', 'help'])
    system = None
    dev_name = '/dev/md0'
    conf = 'tpcc'
    upsert_opt = ''
    parse_result_only = False
    force_to_run = False
    enable_bgthreads = False
    cache_size_mb = 128

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
        elif opt in ('-b', '--bgthreads'):
            enable_bgthreads = True
        elif opt in ('-c', '--cachesize'):
            cache_size_mb = int(arg)
        elif opt in ('-h', '--help'):
            printHelp()

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

    ExpSystem.build(system, '../splinterdb')

    db = 'splinterdb' if system == 'splinterdb' else 'transactional_splinterdb'
    
    # This is the maximum number of threads that can be run in parallel in the system.
    max_total_threads = 60

    # This is the maximum number of threads that run YCSB clients.
    max_num_threads = min(os.cpu_count(), max_total_threads)

    cmds = []
    for thread in [1] + list(range(4, max_num_threads + 1, 4)):
        if enable_bgthreads:
            num_normal_bg_threads = thread
            num_memtable_bg_threads = (thread + 9) // 10

            total_num_threads = thread + num_normal_bg_threads + num_memtable_bg_threads
            if total_num_threads > max_total_threads:
                num_normal_bg_threads = max(0, num_normal_bg_threads - (total_num_threads - max_total_threads))
            
            total_num_threads = thread + num_normal_bg_threads + num_memtable_bg_threads
            if total_num_threads > max_total_threads:
                num_memtable_bg_threads = max(0, num_memtable_bg_threads - (total_num_threads - max_total_threads))
        else:
            num_normal_bg_threads = 0
            num_memtable_bg_threads = 0

        splinterdb_opts = f'-p splinterdb.filename {dev_name} \
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
            if os.path.isfile(log_path):
                if force_to_run:
                    os.remove(log_path)
                else:
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
