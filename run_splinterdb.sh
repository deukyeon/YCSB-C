#!/bin/bash -x

if [[ "x$1" == "xsplinterdb" || "x$1" == "xtransactional_splinterdb" ]]
then
  DB=$1
else
  echo "Usage: $0 [splinterdb|transactional_splinterdb] [(optional)fieldlength] [(optional)label]"
  exit 1
fi

LABEL=${3:-$DB}

FIELDLENGTH=${2:-1024}

THREADS=(1 2 4 8 12 16 20 24 28 32)

echo "Run for the uniform distribution"

OUT=~/$LABEL-uniform.out

rm -f $OUT

for t in ${THREADS[@]}
do
  bash run_individual.sh $DB $t uniform $FIELDLENGTH >> $OUT 2>&1
done

OUT=~/$LABEL-zipf.out

rm -f $OUT

echo "Run for the zipfian distribution"

for t in ${THREADS[@]}
do
  bash run_individual.sh $DB $t zipfian $FIELDLENGTH >> $OUT 2>&1
done
