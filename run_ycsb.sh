SYSTEMS=(splinterdb baseline-serial baseline-parallel tictoc-disk tictoc-memory tictoc-singlecounter tictoc-sketch)

for sys in ${SYSTEMS[@]}
do
    # 10M records
    for workload in write_intensive_10M rmw_intensive_10M
    do
	python run_exp.py $sys $workload
    done

    for workload in write_intensive_10M_uniform rmw_intensive_10M_uniform
    do
	python run_exp.py $sys $workload
    done

    for workload in read_intensive_10M
    do
	python run_exp.py $sys $workload
    done

    # 100M records
    for workload in write_intensive rmw_intensive
    do
	python run_exp.py $sys $workload
    done
done

