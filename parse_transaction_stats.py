#!/usr/bin/python3

import sys
import numpy as np
import getopt


def print_usage(retcode=0):
    print(f'Usage: {sys.argv[0]} -t <num_threads>')
    sys.exit(retcode)


num_threads = 0
opts, args = getopt.getopt(sys.argv[1:], 'ht:', ['help', 'threads='])
for opt, arg in opts:
    if opt in ('-h', '--help'):
        print_usage(0)
    elif opt in ('-t', '--threads'):
        num_threads = int(arg)

if num_threads == 0:
    print_usage(1)

transaction_times = [[] for _ in range(num_threads)]
execution_times = [[] for _ in range(num_threads)]
validation_times = [[] for _ in range(num_threads)]
write_times = [[] for _ in range(num_threads)]

abort_transaction_times = [[] for _ in range(num_threads)]
abort_execution_times = [[] for _ in range(num_threads)]
abort_validation_times = [[] for _ in range(num_threads)]

for i in range(num_threads):
    filename = f'transaction_stats_{i}.txt'
    with open(filename, 'r') as f:
        for line in f:
            if line.startswith('abort_transaction_times'):
                commit_data = False
                continue
            if line.startswith('commit_transaction_times'):
                commit_data = True
                continue

            fields = line.split()
            if commit_data:
                if fields[0] == 'T':
                    transaction_times[i].append(int(fields[1]))
                if fields[0] == 'E':
                    execution_times[i].append(int(fields[1]))
                if fields[0] == 'V':
                    validation_times[i].append(int(fields[1]))
                if fields[0] == 'W':
                    write_times[i].append(int(fields[1]))
            else:
                if fields[0] == 'T':
                    abort_transaction_times[i].append(int(fields[1]))
                if fields[0] == 'E':
                    abort_execution_times[i].append(int(fields[1]))
                if fields[0] == 'V':
                    abort_validation_times[i].append(int(fields[1]))


def print_stats(data):
    print("thread", "min", "med", "avg", "99", "max")
    aggregated = []
    for i in range(num_threads):
        print(i, int(np.min(data[i])), int(np.median(data[i])),
              int(np.mean(data[i])), int(np.percentile(data[i], 99)), int(np.max(data[i])))
        aggregated.extend(data[i])
    print("all", int(np.min(aggregated)), int(np.median(aggregated)), int(np.mean(aggregated)),
          int(np.percentile(aggregated, 99)), int(np.max(aggregated)))


print("Statistics by threads (commit)")
print("Transaction times (ns)")
print_stats(transaction_times)
print("Execution times")
print_stats(execution_times)
print("Validation times")
print_stats(validation_times)
print("Write times")
print_stats(write_times)

print("Statistics by threads (abort)")
print("Transaction times (ns)")
print_stats(abort_transaction_times)
print("Execution times")
print_stats(abort_execution_times)
print("Validation times")
print_stats(abort_validation_times)
