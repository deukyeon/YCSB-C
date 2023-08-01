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

transaction_times = [[]] * num_threads
execution_times = [[]] * num_threads
validation_times = [[]] * num_threads
write_times = [[]] * num_threads
            
abort_transaction_times = [[]] * num_threads
abort_execution_times = [[]] * num_threads
abort_validation_times = [[]] * num_threads

commit_data = True

for i in range(num_threads):
    filename = f'transaction_stats_{i}.txt'
    with open(filename, 'r') as f:
        for line in f:
            if line.startswith('abort_transaction_times'):
                commit_data = False
                continue
            if line.startswith('commit_transaction_times'):
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

print("Statistics by threads (commit)")
print("Transaction times (ns): min,mid,avg,99,max")
for i in range(num_threads):
    print(f'{i}: {int(np.min(transaction_times[i]))},{int(np.median(transaction_times[i]))},{int(np.mean(transaction_times[i]))},{int(np.percentile(transaction_times[i], 99))},{int(np.max(transaction_times[i]))}')
print("Execution times: min,mid,avg,99,max")
for i in range(num_threads):
    print(f'{i}: {int(np.min(execution_times[i]))},{int(np.median(execution_times[i]))},{int(np.mean(execution_times[i]))},{int(np.percentile(execution_times[i], 99))},{int(np.max(execution_times[i]))}')
print("Validation times: min,mid,avg,99,max")
for i in range(num_threads):
    print(f'{i}: {int(np.min(validation_times[i]))},{int(np.median(validation_times[i]))},{int(np.mean(validation_times[i]))},{int(np.percentile(validation_times[i], 99))},{int(np.max(validation_times[i]))}')
print("Write times: min,mid,avg,99,max")
for i in range(num_threads):
    print(f'{i}: {int(np.min(write_times[i]))},{int(np.median(write_times[i]))},{int(np.mean(write_times[i]))},{int(np.percentile(write_times[i], 99))},{int(np.max(write_times[i]))}')

print("Statistics by threads (abort)")
print("Transaction times (ns): min,mid,avg,99,max")
for i in range(num_threads):
    print(f'{i}: {int(np.min(abort_transaction_times[i]))},{int(np.median(abort_transaction_times[i]))},{int(np.mean(abort_transaction_times[i]))},{int(np.percentile(abort_transaction_times[i], 99))},{int(np.max(abort_transaction_times[i]))}')
print("Execution times: min,mid,avg,99,max")
for i in range(num_threads):
    print(f'{i}: {int(np.min(abort_execution_times[i]))},{int(np.median(abort_execution_times[i]))},{int(np.mean(abort_execution_times[i]))},{int(np.percentile(abort_execution_times[i], 99))},{int(np.max(abort_execution_times[i]))}')
print("Validation times: min,mid,avg,99,max")
for i in range(num_threads):
    print(f'{i}: {int(np.min(abort_validation_times[i]))},{int(np.median(abort_validation_times[i]))},{int(np.mean(abort_validation_times[i]))},{int(np.percentile(abort_validation_times[i], 99))},{int(np.max(abort_validation_times[i]))}')

