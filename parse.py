import re
import csv

# Function to parse the input file
def parse_input_file(file_path):
    with open(file_path, 'r') as file:
        input_text = file.read()

    # Define regex patterns for the required information
    load_throughput_pattern = r"# Load throughput \(KTPS\)\n.*\s+(\d+\.\d+)"
    thread_goodput_pattern = r"# Transaction throughput \(KTPS\)\n.*\s+(\d+)\s+(\d+\.\d+)"
    abort_rate_pattern = r"Abort rate:\s+(\d+)"
    commit_latency_pattern = r"# Commit Latencies \(us\)\nMin:\s+(.+)\nMax:\s+(.+)\nAvg:\s+(.+)\nP50:\s+(.+)\nP90:\s+(.+)\nP95:\s+(.+)\nP99:\s+(.+)\nP99\.9:\s+(.+)"
    abort_latency_pattern = r"# Abort Latencies \(us\)\nMin:\s+(.+)\nMax:\s+(.+)\nAvg:\s+(.+)\nP50:\s+(.+)\nP90:\s+(.+)\nP95:\s+(.+)\nP99:\s+(.+)\nP99\.9:\s+(.+)"

    # Extract the required values using regex
    load_throughput = re.search(load_throughput_pattern, input_text)
    load_throughput = load_throughput.group(1) if load_throughput else None

    thread_goodput = re.search(thread_goodput_pattern, input_text)
    if thread_goodput:
        thread_count = thread_goodput.group(1)
        goodput = thread_goodput.group(2)
    else:
        thread_count = None
        goodput = None

    abort_rate = re.search(abort_rate_pattern, input_text)
    abort_rate = abort_rate.group(1) if abort_rate else None

    commit_latency = re.search(commit_latency_pattern, input_text)
    commit_latency = commit_latency.groups() if commit_latency else [None]*8

    abort_latency = re.search(abort_latency_pattern, input_text)
    abort_latency = abort_latency.groups() if abort_latency else [None]*8

    # Return the extracted values
    return {
        "Thread Count": thread_count,
        "Load Throughput (KTPS)": load_throughput,
        "Goodput (KTPS)": goodput,
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
    headers = [
        "Thread Count", "Load Throughput (KTPS)", "Goodput (KTPS)", "Abort Rate",
        "Commit Latency Min (us)", "Commit Latency Max (us)", "Commit Latency Avg (us)",
        "Commit Latency P50 (us)", "Commit Latency P90 (us)", "Commit Latency P95 (us)",
        "Commit Latency P99 (us)", "Commit Latency P99.9 (us)",
        "Abort Latency Min (us)", "Abort Latency Max (us)", "Abort Latency Avg (us)",
        "Abort Latency P50 (us)", "Abort Latency P90 (us)", "Abort Latency P95 (us)",
        "Abort Latency P99 (us)", "Abort Latency P99.9 (us)"
    ]

    rows = []
    for result in results:
        row = [
            result["Thread Count"], result["Load Throughput (KTPS)"], result["Goodput (KTPS)"], result["Abort Rate"]
        ]
        row.extend(result["Commit Latencies (us)"].values())
        row.extend(result["Abort Latencies (us)"].values())
        rows.append(row)

    # Write rows to CSV file
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(rows)

# Example usage
input_file_paths = []
for thr in [1, 2] + list(range(4, 64, 4)):
    input_file_paths.append(f'mvcc-counter_write_intensive_{thr}_1.log')
output_file_path = 'output.csv'  # Replace with the path to your output CSV file

results = [parse_input_file(file_path) for file_path in input_file_paths]
write_to_csv(results, output_file_path)

print(f"Results have been written to {output_file_path}")

