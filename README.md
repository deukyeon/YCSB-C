# Benchmarks for transactional SplinterDB

## Build

See the original [README](./README-original.md).

Before running benchmarks make sure that the path of SplinterDB. The run scripts below assume the splinterdb is in the parent directory.

```
/foo
  |-- splinterdb
  |-- YCSB-C
```

## Run YCSB

```
./ycsb.py -s [transactional system] -w [workload] -t [#threads] -r [benchmark time] -c [cache size] -d [database file]
```

`transactional system` is the type of transactional system, e.g., tictoc-disk, tictoc-memory, tictoc-sketch.
`workload` should be the filename (without the extention '.spec') in the workloads directory.

It outputs to STDOUT, so outputs need to be redirected to be kept.

## Run TPCC


```
./tpcc.py -s [transactional system] -w [workload] -t [#threads] -r [benchmark time] -c [cache size] -d [database file]
```

Parameters are the same as YCSB.
