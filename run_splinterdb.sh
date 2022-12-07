#!/bin/bash -x

if [[ "x$1" == "xsplinterdb" || "x$1" == "xtransactional_splinterdb" ]]
then
  DB=$1
else
  echo "Usage: $0 [splinterdb|transactional_splinterdb] [(optional)label]"
  exit 1
fi

LABEL=${2:-$DB}

THREADS=(1 2 4 8 12 16 20 24 28 32)

echo "Run for the uniform distribution"

OUT=$LABEL-uniform.out

rm -f $OUT

for t in ${THREADS[@]}
do
  bash run_individual.sh $DB $t uniform 100 >> $OUT 2>&1
done

OUT=$LABEL-zipf.out

rm -f $OUT

echo "Run for the zipfian distribution"

for t in ${THREADS[@]}
do
  bash run_individual.sh $DB $t zipfian 100 >> $OUT 2>&1
done
