#!/usr/bin/python3

import os
import subprocess
import sys
import getopt
from exp_system import *

def printHelp():
    print("Usage:", sys.argv[0], "-s [system] -w [workload] -d [device] -f -p -b -c [cache_size_mb] -r [run_seconds] -h", file=sys.stderr)
    print("\t-s,--system [system]: Choose one of the followings --",
          available_systems, file=sys.stderr)
    print("\t-w,--workload [workload]: Specify a spec file in workloads", file=sys.stderr)
    print("\t-d,--device [device]: Choose the device for SplinterDB (default: /dev/md0)", file=sys.stderr)
    print("\t-f,--force: Force to run (Delete all existing logs)", file=sys.stderr)
    print("\t-p,--parse: Parse the logs without running", file=sys.stderr)
    print("\t-b,--bgthreads: Enable background threads", file=sys.stderr)
    print("\t-c,--cachesize: Set the cache size in MB (default: 256)", file=sys.stderr)
    print("\t-r,--run_seconds: Set the run time in seconds (default: 0, which means disabled)", file=sys.stderr)
    print("\t-h,--help: Print this help message", file=sys.stderr)
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

    run_threads = []
    run_tputs = []

    abort_counts = []
    abort_rates = []

    payment_abort_rates = []
    neworder_abort_rates = []
    payment_failure_rates = []
    neworder_failure_rates = []
    payment_total_attempts = []
    neworeder_total_attempts = []

    run_data = False

    commit_latency_data = False
    abort_latency_data = False

    dist_stats_keys = [
        'Min',
        'Max',
        'Avg',
        'P50',
        'P90',
        'P95',
        'P99',
        'P99.9']
    commit_latencies = {}
    abort_latencies = {}
    for key in dist_stats_keys:
        commit_latencies[key] = []
        abort_latencies[key] = []

    for line in lines:
        if line.startswith("# Transaction throughput (KTPS)"):
            run_data = True
            continue

        if run_data:
            fields = line.split()
            run_threads.append(fields[-2])
            run_tputs.append(fields[-1])
            run_data = False

        if line.startswith("# Abort count"):
            fields = line.split()
            abort_counts.append(fields[-1])

        if line.startswith("Abort rate"):
            fields = line.split()
            abort_rates.append(fields[-1])

        if line.startswith("# Commit Latencies"):
            commit_latency_data = True
            continue

        if line.startswith("# Abort Latencies"):
            abort_latency_data = True
            no_abort_latency_data = True
            continue
            
        if commit_latency_data:
            fields = line.split(sep=':')
            if fields[0] not in dist_stats_keys:
                commit_latency_data = False
                continue
            commit_latencies[fields[0]].append(fields[1].strip())
            if fields[0] == dist_stats_keys[-1]:
                commit_latency_data = False
            
        if abort_latency_data:
            fields = line.split(sep=':')
            if fields[0] not in dist_stats_keys:
                if no_abort_latency_data:
                    for key in dist_stats_keys:
                        abort_latencies[key].append('0')
                abort_latency_data = False
                continue
            abort_latencies[fields[0]].append(fields[1].strip())
            no_abort_latency_data = False
            if fields[0] == dist_stats_keys[-1]:
                abort_latency_data = False

        if line.startswith("# (Payment) Abort rate(%)"):
            fields = line.split()
            payment_abort_rates.append(fields[-1])
        if line.startswith("# (NewOrder) Abort rate(%)"):
            fields = line.split()
            neworder_abort_rates.append(fields[-1])
        if line.startswith("# (Payment) Failure rate(%)"):
            fields = line.split()
            payment_failure_rates.append(fields[-1])
        if line.startswith("# (NewOrder) Failure rate(%)"):
            fields = line.split()
            neworder_failure_rates.append(fields[-1])
        if line.startswith("# (Payment) Total attempts"):
            fields = line.split()
            payment_total_attempts.append(fields[-1])
        if line.startswith("# (NewOrder) Total attempts"):
            fields = line.split()
            neworeder_total_attempts.append(fields[-1])

    # print csv
    for tuple in zip(run_threads, run_tputs, abort_counts, abort_rates, payment_abort_rates, neworder_abort_rates, payment_failure_rates, neworder_failure_rates, payment_total_attempts, neworeder_total_attempts, commit_latencies['Min'], commit_latencies['Max'], commit_latencies['Avg'], commit_latencies['P50'], commit_latencies['P90'], commit_latencies['P95'], commit_latencies['P99'], commit_latencies['P99.9'], abort_latencies['Min'], abort_latencies['Max'], abort_latencies['Avg'], abort_latencies['P50'], abort_latencies['P90'], abort_latencies['P95'], abort_latencies['P99'], abort_latencies['P99.9']):
        print(system, conf, ','.join(tuple), seq, sep=',', file=csv)


def generateOutputFile(input, output=sys.stdout):
    import pandas as pd
    import numpy as np
    df = pd.read_csv(input)
    df = df.select_dtypes(include=np.number)
    df = df.groupby(by='threads').agg('mean')
    df.drop('seq', axis=1, inplace=True)
    print(df.to_string(), file=output)


def main(argc, argv):
    parse_result_only = False
    force_to_run = False
    enable_bgthreads = False
    cache_size_mb = 256
    run_seconds = 0

    opts, _ = getopt.getopt(sys.argv[1:], 's:w:d:pfbc:r:h', 
                            ['system=', 'workload=', 'device=', 'parse', 'force', 'bgthreads', 'cachesize=', 'run_seconds=', 'help'])
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
            spec_file = 'workloads/' + conf + '.spec'
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
        elif opt in ('-r', '--run_seconds'):
            run_seconds = float(arg)
        elif opt in ('-h', '--help'):
            printHelp()

    if not system:
        print("Invalid system", file=sys.stderr)
        printHelp()

    if not conf or not os.path.isfile(spec_file):
        print("Invalid workload", file=sys.stderr)
        printHelp()

    label = system + '-' + conf
    num_repeats = 1

    def parse():
        csv_path = f'{label}.csv'
        csv = open(csv_path, 'w')
        print("system,conf,threads,goodput,aborts,abort_rate,payment_abort_rate, neworder_abort_rate, payment_failure_rate, neworder_failure_rate, payment_total_attempts, neworeder_total_attempts,commit_latency_min,commit_latency_max,commit_latency_avg,commit_latency_p50,commit_latency_p90,commit_latency_p95,commit_latency_p99,commit_latency_p99.9,abort_latency_min,abort_latency_max,abort_latency_avg,abort_latency_p50,abort_latency_p90,abort_latency_p95,abort_latency_p99,abort_latency_p99.9,seq", file=csv)
        for i in range(0, num_repeats):
            log_path = f'/tmp/{label}.{i}.log'
            parseLogfile(log_path, csv, system, conf, i)
        csv.close()
        
        with open(f'{label}', 'w') as out:
            generateOutputFile(csv_path, out)
            
    if parse_result_only:
        parse()
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

        cmd = f'LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so \
            ./ycsbc -db {db} -threads {thread} -benchmark tpcc -W {spec_file} \
            -benchmark_seconds {run_seconds} \
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
            out, _ = run_shell_command(cmd, shell=True)
            if out:
                logfile.write(out.decode())
            # run_shell_command('rm -f splinterdb.db')
        logfile.close()
    
    parse()

if __name__ == '__main__':
    main(len(sys.argv), sys.argv)
