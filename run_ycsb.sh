for sys in splinterdb baseline-serial baseline-parallel tictoc-disk tictoc-memory fantastiCC
do
    # 10M records
    for workload in write_intensive_10M rmw_intensive_10M
    do
	python run_exp.py $sys $workload
    done

    # 100M records
    for workload in write_intensive rmw_intensive
    do
	python run_exp.py $sys $workload
    doen
done

