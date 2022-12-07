#!/bin/bash

DB=${1:-"transactional_splinterdb"}
THREADS=${2:-1}
DIST=${3:-"uniform"}
FIELDLENGTH=${4:-1024}

RECORDCOUNT=84000000
test "$FIELDLENGTH" == "100" && RECORDCOUNT=673000000
TXNPERTHREAD=1000000
OPSPERTRANSACTION=2
OPERATIONCOUNT=$(($OPSPERTRANSACTION * $THREADS * $TXNPERTHREAD))

./ycsbc -db $DB -threads $THREADS \
-L workloads/load.spec \
-w fieldlength $FIELDLENGTH \
-w recordcount $RECORDCOUNT \
-W workloads/workloada.spec \
-w requestdistribution $DIST \
-w operationcount $OPERATIONCOUNT \
-w opspertransaction $OPSPERTRANSACTION
