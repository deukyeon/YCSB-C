import re
import csv
import sys

# Function to parse the input file
def parse_input_file(file_path):
    with open(file_path, 'r') as file:
        input_text = file.read()

    # Define regex patterns for the required information
    load_throughput_pattern = r"# Load throughput \(KTPS\)\n.*\s+(\d+\.\d+)"
    thread_goodput_pattern = r"# Transaction throughput \(KTPS\)\n.*\s+(\d+)\s+(\d+\.\d+)"
    abort_count_pattern = r"# Abort count:\s+(\d+)"
    abort_rate_pattern = r"Abort rate:\s+(.+)"
    commit_latency_pattern = r"# Commit Latencies \(us\)\nMin:\s+(.+)\nMax:\s+(.+)\nAvg:\s+(.+)\nP50:\s+(.+)\nP90:\s+(.+)\nP95:\s+(.+)\nP99:\s+(.+)\nP99\.9:\s+(.+)"
    abort_latency_pattern = r"# Abort Latencies \(us\)\nMin:\s+(.+)\nMax:\s+(.+)\nAvg:\s+(.+)\nP50:\s+(.+)\nP90:\s+(.+)\nP95:\s+(.+)\nP99:\s+(.+)\nP99\.9:\s+(.+)"

    # Extract the required values using regex
    load_throughput = re.search(load_throughput_pattern, input_text)
    load_throughput = load_throughput.group(1) if load_throughput else 0

    thread_goodput = re.search(thread_goodput_pattern, input_text)
    if thread_goodput:
        thread_count = thread_goodput.group(1)
        goodput = thread_goodput.group(2)
    else:
        thread_count = 0
        goodput = 0

    abort_count = re.search(abort_count_pattern, input_text)
    abort_count = abort_count.group(1) if abort_count else 0

    abort_rate = re.search(abort_rate_pattern, input_text)
    abort_rate = abort_rate.group(1) if abort_rate else 0

    commit_latency = re.search(commit_latency_pattern, input_text)
    commit_latency = commit_latency.groups() if commit_latency else [0]*8

    abort_latency = re.search(abort_latency_pattern, input_text)
    abort_latency = abort_latency.groups() if abort_latency else [0]*8

    # Return the extracted values
    return {
        "Thread Count": thread_count,
        # "Load Throughput (KTPS)": load_throughput,
        "Goodput (KTPS)": goodput,
        "Abort Count": abort_count,
        "Abort Rate": abort_rate,
        "Commit Latencies (us)": {
            "Min": commit_latency[0],
            "Max": commit_latency[1],
            "Avg": commit_latency[2],
            "P50": commit_latency[3],
            "P90": commit_latency[4],
            "P95": commit_latency[5],
            "P99": commit_latency[6],
            "P99.9": commit_latency[7]
        },
        "Abort Latencies (us)": {
            "Min": abort_latency[0],
            "Max": abort_latency[1],
            "Avg": abort_latency[2],
            "P50": abort_latency[3],
            "P90": abort_latency[4],
            "P95": abort_latency[5],
            "P99": abort_latency[6],
            "P99.9": abort_latency[7]
        }
    }

# Function to write the extracted values to a CSV file
def write_to_csv(results, output_file):
    # headers = [
    #     "Thread Count", "Load Throughput (KTPS)", "Goodput (KTPS)", "Abort Rate",
    #     "Commit Latency Min (us)", "Commit Latency Max (us)", "Commit Latency Avg (us)",
    #     "Commit Latency P50 (us)", "Commit Latency P90 (us)", "Commit Latency P95 (us)",
    #     "Commit Latency P99 (us)", "Commit Latency P99.9 (us)",
    #     "Abort Latency Min (us)", "Abort Latency Max (us)", "Abort Latency Avg (us)",
    #     "Abort Latency P50 (us)", "Abort Latency P90 (us)", "Abort Latency P95 (us)",
    #     "Abort Latency P99 (us)", "Abort Latency P99.9 (us)"
    # ]
    headers = [
        "threads", "goodput", "aborts", "abort_rate", "commit_latency_min", "commit_latency_max", "commit_latency_avg", "commit_latency_p50", "commit_latency_p90", "commit_latency_p95", "commit_latency_p99", "commit_latency_p99.9", "abort_latency_min", "abort_latency_max", "abort_latency_avg", "abort_latency_p50", "abort_latency_p90", "abort_latency_p95", "abort_latency_p99", "abort_latency_p99.9"]

    rows = []
    for result in results:
        row = [
            result["Thread Count"], result["Goodput (KTPS)"], result["Abort Count"], result["Abort Rate"]
        ]
        row.extend(result["Commit Latencies (us)"].values())
        row.extend(result["Abort Latencies (us)"].values())
        rows.append(row)

    # Write rows to CSV file
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(rows)


def generateOutputFile(input, output=sys.stdout):
    import pandas as pd
    import numpy as np
    df = pd.read_csv(input)
    df = df.select_dtypes(include=np.number)
    df = df.groupby(by='threads').agg('mean')
    print(df.to_string(), file=output)
    
    
input_dir = sys.argv[1]
output_dir = sys.argv[2]

# Example usage
for system in ['2pl-no-wait', 'baseline-serial', 'baseline-parallel', 'sto-disk', 'sto-memory', 'sto-counter', 'sto-sketch', 'tictoc-disk', 'tictoc-memory', 'tictoc-counter', 'tictoc-sketch', 'mvcc-memory', 'mvcc-sketch', 'mvcc-counter']:
    # mvcc-disk will be added later.
    for workload in ['write_intensive', 'read_intensive']:
        input_file_paths = []
        for thr in [1] + list(range(4, 64, 4)):
            for seq in [1, 2]:
                input_file_paths.append(f'{input_dir}/{system}_{workload}_{thr}_{seq}.log')
        output_file_path = f'{output_dir}/{system}-{workload}.csv'  # Replace with the path to your output CSV file

        results = [parse_input_file(file_path) for file_path in input_file_paths]
        write_to_csv(results, output_file_path)

        with open(f'{output_dir}/{system}-{workload}', 'w') as out:
            generateOutputFile(output_file_path, out)

