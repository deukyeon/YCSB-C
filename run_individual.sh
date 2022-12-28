#!/bin/bash

DB=${1:-"transactional_splinterdb"}
THREADS=${2:-1}
DIST=${3:-"zipfian"}
FIELDLENGTH=${4:-1024}

RECORDCOUNT=84000000
test "$FIELDLENGTH" == "100" && RECORDCOUNT=673000000
TRANSACTIONCOUNT=1000000
OPSPERTRANSACTION=${5:-16}
OPERATIONCOUNT=$(($OPSPERTRANSACTION * $TRANSACTIONCOUNT))
# TXNPERTHREAD=1000000
# OPERATIONCOUNT=$(($OPSPERTRANSACTION * $THREADS * $TXNPERTHREAD))

THETA=${6:0.9}

./ycsbc -db $DB -threads $THREADS \
-L workloads/load.spec \
-w fieldlength $FIELDLENGTH \
-w recordcount $RECORDCOUNT \
-W workloads/workloada.spec \
-w requestdistribution $DIST \
-w operationcount $OPERATIONCOUNT \
-w opspertransaction $OPSPERTRANSACTION \
-w theta $THETA
