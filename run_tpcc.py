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
    'fantastiCC'
]

system_branch_map = {
    'splinterdb': 'deukyeon/tictoc',
    'tictoc-disk': 'deukyeon/fantastiCC-refactor',
    'silo-disk': 'deukyeon/silo-disk',
    'baseline-serial': 'deukyeon/baseline',
    'baseline-parallel': 'deukyeon/baseline',
    'silo-memory': 'deukyeon/fantastiCC-refactor',
    'tictoc-memory': 'deukyeon/fantastiCC-refactor',
    'fantastiCC': 'deukyeon/fantastiCC-refactor'
}

system_sed_map = {
    'baseline-parallel': ['sed', '-i', 's/\/\/ #define PARALLEL_VALIDATION/#define PARALLEL_VALIDATION/g', 'src/transaction_private.h'],
    'silo-memory': ['sed', '-i', 's/#define EXPERIMENTAL_MODE_SILO [ ]*0/#define EXPERIMENTAL_MODE_SILO 1/g', 'src/experimental_mode.h'],
    'tictoc-disk': ['sed', '-i', 's/#define EXPERIMENTAL_MODE_TICTOC_DISK [ ]*0/#define EXPERIMENTAL_MODE_TICTOC_DISK 1/g', 'src/experimental_mode.h'],
    'tictoc-memory': ['sed', '-i', 's/#define EXPERIMENTAL_MODE_KEEP_ALL_KEYS [ ]*0/#define EXPERIMENTAL_MODE_KEEP_ALL_KEYS 1/g', 'src/experimental_mode.h']
}

systems_with_iceberg = [k for k, v in system_branch_map.items() if v == 'deukyeon/fantastiCC-refactor']

def printHelp():
    print("Usage:", sys.argv[0], "[system] [workload]", file=sys.stderr)
    print("\t[system]: Choose one of the followings --",
          available_systems, file=sys.stderr)
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
    if sys in systems_with_iceberg:
        run_shell_command('git submodule update --init --recursive third-party')
    run_shell_command('make clean')
    if sys in system_sed_map:
        run_shell_command(system_sed_map[sys], parse=False)
    run_shell_command('make')
    os.chdir(current_dir)


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
    for tuple in zip(run_threads, run_tputs, abort_counts, abort_rates, strict=True):
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
    if len(argv) > 2:
        assert(argv[2] == 'upsert')
        conf = 'tpcc-upsert'
        upsert_opt = '-upserts'

    buildSystem(system)

    label = system + '-' + conf

    db = 'splinterdb' if system == 'splinterdb' else 'transactional_splinterdb'

    max_num_threads = min(os.cpu_count(), 32)

    cpulist = []
    for i in range(max_num_threads // 2):
        cpulist.append(i)
        cpulist.append(i + 16)

    num_warehouses = [4, 8, 32]

    splinterdb_opts = '-p splinterdb.filename /dev/nvme1n1 -p splinterdb.cache_size_mb 32000'

    cmds = []
    # for thread in [1, 2] + list(range(4, max_num_threads + 1, 4)):
    for thread in [1] + list(range(4, max_num_threads + 1, 4)):
        cmd = f'LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so numactl -C {",".join(map(str, cpulist[:thread]))} ./ycsbc -db {db} -threads {thread} -benchmark tpcc {splinterdb_opts} {upsert_opt}'
        cmds.append(cmd)

    csv = open(f'{label}.csv', 'w')
    print("system,conf,num_wh,threads,goodput,aborts,abort_rate,seq", file=csv)
    num_repeats = 1
    for i in range(0, num_repeats):
        for wh in num_warehouses:
            change_num_warehouses(wh)
            log_path = f'/tmp/{label}-wh{wh}.{i}.log'
            logfile = open(log_path, 'w')
            for cmd in cmds:
                logfile.write(f'{cmd}\n')
                out, _ = run_shell_command(cmd, shell=True)
                if out:
                    logfile.write(out.decode())
            logfile.close()
            parseLogfile(log_path, csv, system, conf, i, wh)
    csv.close()

if __name__ == '__main__':
    main(len(sys.argv), sys.argv)
