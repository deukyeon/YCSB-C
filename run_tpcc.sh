for sys in baseline-serial baseline-parallel tictoc-disk tictoc-memory fantastiCC
do
    python run_tpcc.py $sys
done

