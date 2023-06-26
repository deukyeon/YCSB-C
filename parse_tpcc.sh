# SYSTEMS=(baseline-serial baseline-parallel tictoc-disk tictoc-memory tictoc-singlecounter tictoc-sketch)
SYSTEMS=(tictoc-singlecounter tictoc-sketch)
for sys in ${SYSTEMS[@]}
do
    python run_tpcc.py $sys - True
    python run_tpcc.py $sys upsert True
done

