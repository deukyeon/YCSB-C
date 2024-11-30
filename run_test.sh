LOG_DIR=$HOME/newbaseline

mkdir -p $LOG_DIR

for prot in tictoc sto mvcc
do
    for sys in memory sketch counter hashtable
    do
        for work in write_intensive read_intensive
        do
            LOG_FILE="$LOG_DIR/$prot-$sys-$work"
            # Skip if log file already exists and contains the desired line
            if [ -f "$LOG_FILE" ] && grep -q "# Transaction throughput (KTPS)" "$LOG_FILE"; then
                continue
            fi

            echo "python3 ycsb.py -s $prot-$sys -w $work -d /dev/sdb2 -t 60 -r 240"

            # Retry until the output file contains the desired line
            while true
            do
                python3 ycsb.py -s $prot-$sys -w $work -d /dev/nvme0n1 -c 6144 -t 60 -r 240 > $LOG_FILE

                # Check if the log file contains the required line
                if grep -q "# Transaction throughput (KTPS)" "$LOG_FILE"; then
                    break
                fi
            done
        done
    done
done
