#!/usr/bin/python3

import os
import getopt
import sys


# Get an option named 'enabled' with no argument
opts, args = getopt.getopt(sys.argv[1:], 'et:', ['enabled', 'threads='])

bg_threads_enabled = False
num_threads = 16
for opt, arg in opts:
    if opt in ('-e', '--enabled'):
        bg_threads_enabled = True
    elif opt in ('-t', '--threads'):
        num_threads = int(arg)

if bg_threads_enabled:
    num_normal_bg_threads = num_threads
    num_memtable_bg_threads = (num_threads + 9) // 10
else:
    num_normal_bg_threads = 0
    num_memtable_bg_threads = 0


ld_preload = 'LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so'
numactl = 'numactl -N 0'
ycsbc = f'./ycsbc -db transactional_splinterdb \
    -threads {num_threads} \
    -L workloads/write_intensive_test.spec \
    -W workloads/write_intensive_test.spec \
    -p splinterdb.filename /dev/nvme1n1 \
    -p splinterdb.cache_size_mb 256 \
    -p splinterdb.num_normal_bg_threads {num_normal_bg_threads} \
    -p splinterdb.num_memtable_bg_threads {num_memtable_bg_threads}'

cmd = f'{ld_preload} {numactl} {ycsbc}'

print(f'Running command: {cmd}')
print(f'Total # of threads: {num_threads + num_normal_bg_threads + num_memtable_bg_threads}')

os.system(cmd)