#!/bin/bash -x

if [[ "x$1" == "xsplinterdb" || "x$1" == "xtransactional_splinterdb" ]]
then
  DB=$1
else
  echo "Usage: $0 [splinterdb|transactional_splinterdb] [(optional)label]"
  exit 1
fi
LABEL=${2:-$DB}

FIELDLENGTH=100
THREADS=(1 2 4 8 12 16 20 24 28 32)
OPSPERTXN=16
THETA=0.9

REPEAT=5

echo "Experiments for $LABEL"

for i in $(seq $REPEAT)
do
    echo "Run for the uniform distribution"

    OUT=~/$LABEL-uniform.out.$i

    rm -f $OUT

    for t in ${THREADS[@]}
    do
	bash run_individual.sh $DB $t uniform $FIELDLENGTH $OPSPERTXN 0 >> $OUT 2>&1
    done

    echo "Output: $OUT"
    echo "To parse the result, run 'python plot_throughput.py $OUT'"

    OUT=~/$LABEL-zipf.out.$i

    rm -f $OUT

    echo "Run for the zipfian distribution (theta=$THETA)"

    for t in ${THREADS[@]}
    do
	bash run_individual.sh $DB $t zipfian $FIELDLENGTH $OPSPERTXN $THETA >> $OUT 2>&1
    done

    echo "Output: $OUT"
    echo "To parse the result, run 'python plot_throughput.py $OUT'"
done
