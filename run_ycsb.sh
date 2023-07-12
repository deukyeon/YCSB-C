# SYSTEMS=(splinterdb baseline-serial baseline-parallel tictoc-disk tictoc-memory tictoc-singlecounter tictoc-sketch)
SYSTEMS=(splinterdb tictoc-disk tictoc-memory tictoc-singlecounter tictoc-sketch)
WORKLOADS=(write_intensive write_intensive_10M rmw_intensive_10M)

for sys in ${SYSTEMS[@]}
do
    for workload in ${WORKLOADS[@]}
    do
	python run_exp.py $sys $workload
	python run_exp.py $sys $workload True
    done
done

