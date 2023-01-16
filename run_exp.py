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
    'tictoc-cache-unlimit',
    'fantastiCC'
]

available_workloads = [
    'high_contention',
    'medium_contention',
    'read_only'
]


def printHelp():
    print("Usage:", sys.argv[0], "[system] [workload]", file=sys.stderr)
    print("\t[system]: Choose one of the followings --",
          available_systems, file=sys.stderr)
    print("\t[workload]: Choose one of the followings --",
          available_workloads, file=sys.stderr)
    exit(1)


if len(sys.argv) < 2:
    printHelp()

system = sys.argv[1]
if system not in available_systems:
    printHelp()

workload = sys.argv[2]
if workload not in available_workloads:
    printHelp()

label = system + '-' + workload

db = 'splinterdb' if system == 'splinterdb' else 'transactional_splinterdb'
spec_file = 'workloads/' + workload + '.spec'
num_txns_per_thread = 10000
ops_per_txn = 1

specfile = open(spec_file, 'r')
specfile_data = specfile.readlines()
for line in specfile_data:
    if line.startswith('opspertransaction'):
        ops_per_txn = int(line.split(sep='=')[-1])
specfile.close()

max_num_threads = min(os.cpu_count(), 32)

cmds = []
for thread in [1, 2] + list(range(4, max_num_threads + 1, 4)):
    operation_count = thread * num_txns_per_thread * ops_per_txn
    cmd = './ycsbc'
    cmd += ' -db ' + db
    cmd += ' -threads ' + str(thread)
    cmd += ' -L ' + spec_file
    cmd += ' -W ' + spec_file
    cmd += ' -w operationcount ' + str(operation_count)
    cmds.append(cmd)


def parseLogfile(logfile_path):
    f = open(logfile_path, "r")
    lines = f.readlines()
    f.close()

    load_threads = []
    load_tputs = []
    run_threads = []
    run_tputs = []

    abort_counts = []
    abort_rates = []

    load_data = False
    run_data = False

    for line in lines:
        if load_data:
            fields = line.split()
            load_threads.append(fields[-2])
            load_tputs.append(fields[-1])
            load_data = False

        if line.startswith("# Load throughput (KTPS)"):
            load_data = True

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
    print("threads,load,workload,aborts")
    for i in range(0, len(load_threads)):
        print(load_threads[i], load_tputs[i],
              run_tputs[i], abort_counts[i], abort_rates[i],
              sep=',')


num_repeats = 5
for i in range(0, num_repeats):
    log_path = f'/tmp/{label}.{i}.log'
    logfile = open(log_path, 'w')
    logfile.writelines(specfile_data)
    for cmd in cmds:
        logfile.write(f'{cmd}\n')
        popen = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
        popen.wait()
        output = popen.stdout.read().decode()
        logfile.write(output)
    logfile.close()
    parseLogfile(log_path)
