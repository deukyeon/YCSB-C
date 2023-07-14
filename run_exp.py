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
    'tictoc-singlecounter',
    'tictoc-sketch',
    'sto-sketch',
    'sto-singlecounter',
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
    'tictoc-singlecounter': 'deukyeon/fantastiCC-refactor',
    'tictoc-sketch': 'deukyeon/fantastiCC-refactor',
    'sto-sketch': 'deukyeon/fantastiCC-refactor',
    'sto-singlecounter': 'deukyeon/fantastiCC-refactor',
    'sto-memory': 'deukyeon/fantastiCC-refactor'
}

system_sed_map = {
    'baseline-parallel': ["sed -i 's/\/\/ #define PARALLEL_VALIDATION/#define PARALLEL_VALIDATION/g' src/transaction_private.h"],
    'silo-memory': ["sed -i 's/#define EXPERIMENTAL_MODE_SILO [ ]*0/#define EXPERIMENTAL_MODE_SILO 1/g' src/experimental_mode.h"],
    'tictoc-disk': ["sed -i 's/#define EXPERIMENTAL_MODE_TICTOC_DISK [ ]*0/#define EXPERIMENTAL_MODE_TICTOC_DISK 1/g' src/experimental_mode.h"],
    'tictoc-memory': ["sed -i 's/#define EXPERIMENTAL_MODE_TICTOC_MEMORY [ ]*0/#define EXPERIMENTAL_MODE_TICTOC_MEMORY 1/g' src/experimental_mode.h",
                      "sed -i 's/#define EXPERIMENTAL_MODE_KEEP_ALL_KEYS [ ]*0/#define EXPERIMENTAL_MODE_KEEP_ALL_KEYS 1/g' src/experimental_mode.h"],
    'tictoc-singlecounter': ["sed -i 's/#define EXPERIMENTAL_MODE_TICTOC_MEMORY [ ]*0/#define EXPERIMENTAL_MODE_TICTOC_MEMORY 1/g' src/experimental_mode.h"],
    'tictoc-sketch': ["sed -i 's/#define EXPERIMENTAL_MODE_SKETCH [ ]*0/#define EXPERIMENTAL_MODE_SKETCH 1/g' src/experimental_mode.h"],
    'sto-memory': ["sed -i 's/#define EXPERIMENTAL_MODE_STO_MEMORY [ ]*0/#define EXPERIMENTAL_MODE_STO_MEMORY 1/g' src/experimental_mode.h",
                   "sed -i 's/#define EXPERIMENTAL_MODE_KEEP_ALL_KEYS [ ]*0/#define EXPERIMENTAL_MODE_KEEP_ALL_KEYS 1/g' src/experimental_mode.h"],
    'sto-sketch': ["sed -i 's/#define EXPERIMENTAL_MODE_STO_SKETCH [ ]*0/#define EXPERIMENTAL_MODE_STO_SKETCH 1/g' src/experimental_mode.h"],
    'sto-singlecounter': ["sed -i 's/#define EXPERIMENTAL_MODE_STO_MEMORY [ ]*0/#define EXPERIMENTAL_MODE_STO_MEMORY 1/g' src/experimental_mode.h"]
}

available_workloads = [
    'high_contention',
    'medium_contention',
    'write_intensive',
    'write_intensive_10M',
    'write_intensive_100M',
    'write_intensive_10M_16ops',
    'write_intensive_10M_uniform',
    'rmw_intensive',
    'rmw_intensive_10M',
    'rmw_intensive_10M_16ops',
    'rmw_intensive_10M_uniform',
    'read_intensive',
    'read_intensive_10M'
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
    for tuple in zip(run_threads, load_tputs, run_tputs, abort_counts, abort_rates, strict=True):
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
    print("system,conf,threads,load,goodput,aborts,abort_rate,seq", file=csv)
    num_repeats = 5
    if parse_result_only:
        for i in range(0, num_repeats):
            log_path = f'/tmp/{label}.{i}.log'
            parseLogfile(log_path, csv, system, conf, i)
        return

    buildSystem(system)

    db = 'splinterdb' if system == 'splinterdb' else 'transactional_splinterdb'
    spec_file = 'workloads/' + conf + '.spec'

    max_num_threads = min(os.cpu_count(), 32)

    cpulist = []
    for i in range(max_num_threads // 2):
        cpulist.append(i)
        cpulist.append(i + 16)

    cmds = []
    # for thread in [1, 2] + list(range(4, max_num_threads + 1, 4)):
    for thread in [1] + list(range(4, max_num_threads + 1, 4)):
    # for thread in [32]:
        cpulist_str = ",".join(map(str, cpulist[:thread]))
        cmd = f'LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so numactl -C {cpulist_str} ./ycsbc -db {db} -threads {thread} -L {spec_file} -W {spec_file} -p splinterdb.filename /dev/nvme1n1'
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
            out, _ = run_shell_command(cmd, shell=True)
            if out:
                logfile.write(out.decode())
            # run_shell_command('rm -f splinterdb.db')
        logfile.close()
        parseLogfile(log_path, csv, system, conf, i)
    csv.close()


if __name__ == '__main__':
    main(len(sys.argv), sys.argv)
