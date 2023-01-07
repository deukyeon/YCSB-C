#!/bin/bash

DB=${1:-"transactional_splinterdb"}
THREADS=${2:-1}
DIST=${3:-"zipfian"}
FIELDLENGTH=${4:-1024}

RECORDCOUNT=10000000
test "$FIELDLENGTH" == "100" && RECORDCOUNT=100000000
OPSPERTRANSACTION=${5:-16}
#TRANSACTIONCOUNT=10000
#OPERATIONCOUNT=$(($OPSPERTRANSACTION * $TRANSACTIONCOUNT))
TXNPERTHREAD=10000
OPERATIONCOUNT=$(($OPSPERTRANSACTION * $THREADS * $TXNPERTHREAD))

THETA=${6:-0.9}

LOADSPEC=workloads/load.spec
WORKLOADSPEC=workloads/workloada.spec

RUN="./ycsbc -db $DB -threads $THREADS \
-L $LOADSPEC \
-w fieldlength $FIELDLENGTH \
-w recordcount $RECORDCOUNT \
-W $WORKLOADSPEC \
-w requestdistribution $DIST \
-w operationcount $OPERATIONCOUNT \
-w opspertransaction $OPSPERTRANSACTION \
-w theta $THETA"

echo "=== Run command ==="
echo $RUN
echo "--- Load file ($LOADSPEC) ---"
cat $LOADSPEC
echo "--- Workload file ($WORKLOADSPEC) ---"
cat $WORKLOADSPEC
echo "=== Running the benchamrk ==="
$RUN
