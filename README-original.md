# YCSB-C

- Yahoo! Cloud Serving Benchmark in C++, a C++ version of YCSB (https://github.com/brianfrankcooper/YCSB/wiki)
- Based on basicthinker's YCSB-C (https://github.com/basicthinker/YCSB-C)
- Adds support for SplinterDB (https://github.com/vmware/splinterdb)
- Performance improvements
- New features (e.g. running Load and Workloads in separate executions)

## Quick Start for Debian-based systems

Install SplinterDB (https://github.com/vmware/splinterdb)

```sh
$ sudo apt-get install libtbb-dev librocksdb-dev libhiredis-dev
$ make
```

As the driver for Redis is linked by default, change the runtime library path
to include the hiredis library by:
```
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib
```

Run Workload A with a [SplinterDB](https://splinterdb.org)-based
implementation of the database, for example:
```sh
$ ./ycsbc -db splinterdb -threads 4 -L workloads/load.spec -W workloads/workloada.spec
```

Note that we do not have load and run commands as the original YCSB. Specify
how many records to load by the `recordcount` property. Reference properties
listed in the `.spec` files in the workloads dir.

To inspect generated data, use the `basic` db and set property `basicdb.verbose`, e.g.
```sh
$ ./ycsbc -db basic -p basicdb.verbose 1 -L workloads/load.spec -w recordcount 3 -w fieldlength 5
```
```
# Loading records:      3
A new thread begins working.
INSERT usertable user12161962213042174405 [ field0=q____ ]
INSERT usertable user09929646806074584996 [ field0=h____ ]
INSERT usertable user16626593026977353223 [ field0=h____ ]
# Load throughput (KTPS)
basic   workloads/load.spec     1       7.16204
```
The `field0=` is not written to the database, it is only for display.

Workload properties may be set in the `.spec` files, or overridden on the
command line with the `-w` flags.  Common overrides:
- `zeropadding`: generated keys will have length `max(24, 4 + zeropadding)`.
   There's no way to generate keys shorter than 24 bytes.
- `fieldlength`: the length of the generated values
- `recordcount`: number of records to insert during the load step
- `operationcount`: number of operations to perform during a workload

Putting that all together, to use `max(24, 4 + 21) = 25` byte keys and 3 byte values, load 5 records and then run Workload A with 6 operations, run this:
```sh
$ ./ycsbc -db basic -p basicdb.verbose 1 -L workloads/load.spec -w zeropadding 21 -w fieldlength 3 -w recordcount 5 -W workloads/workloada.spec -w operationcount 6
```
```
# Loading records:      5
A new thread begins working.
INSERT usertable user012161962213042174405 [ field0=q__ ]
INSERT usertable user009929646806074584996 [ field0=h__ ]
INSERT usertable user016626593026977353223 [ field0=h__ ]
INSERT usertable user014394277620009763814 [ field0=x__ ]
INSERT usertable user003232700585171816769 [ field0=h__ ]
# Load throughput (KTPS)
basic   workloads/load.spec     1       7.89507
# Transaction count:    6
A new thread begins working.
UPDATE usertable user012161962213042174405 [ field0=iii ]
READ usertable user014394277620009763814 < all fields >
READ usertable user012161962213042174405 < all fields >
READ usertable user009929646806074584996 < all fields >
UPDATE usertable user009929646806074584996 [ field0=vvv ]
UPDATE usertable user014394277620009763814 [ field0=vvv ]
# Transaction throughput (KTPS)
basic   workloads/workloada.spec        1       29.4284
```

Or to replicate the "Workload A" experiment from Figure 5(b) in [Conway et al, 2020](https://www.usenix.org/system/files/atc20-conway.pdf) on SplinterDB with 12 threads, you might run
```sh
$ ./ycsbc -db splinterdb -threads 12 -L workloads/load.spec -w fieldlength 1024 -w recordcount 84000000 -W workloads/workloada.spec -w operationcount 10000000
```
