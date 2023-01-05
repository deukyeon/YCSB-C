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
TXNPERTHREAD=1000
OPERATIONCOUNT=$(($OPSPERTRANSACTION * $THREADS * $TXNPERTHREAD))

THETA=${6:-0.9}

gdb --args ./ycsbc -db $DB -threads $THREADS \
-L workloads/load.spec \
-w fieldlength $FIELDLENGTH \
-w recordcount $RECORDCOUNT \
-W workloads/workloada.spec \
-w requestdistribution $DIST \
-w operationcount $OPERATIONCOUNT \
-w opspertransaction $OPSPERTRANSACTION \
-w theta $THETA
