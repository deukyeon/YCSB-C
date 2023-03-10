#ifndef _TPCC_CONFIG_H_
#define _TPC__CONFIG_H_

#define THREAD_CNT					4
#define ABORT_PENALTY 				100000

// # of transactions to run for warmup
#define WARMUP						0
#define PERC_PAYMENT 				0.5
#define FIRSTNAME_MINLEN 			8
#define FIRSTNAME_LEN 				16
#define LASTNAME_LEN 				16
#define DIST_PER_WARE				10
#define WH_UPDATE					true
#define NUM_WH 						1
#define PART_CNT                    1

#define W_CODE                     0
#define I_CODE                     (1ULL << 61)
#define C_CODE                     (2ULL << 61)
#define S_CODE                     (3ULL << 61)
#define NO_CODE                    (4ULL << 61)
#define O_CODE                     (10ULL << 60)
#define OL_CODE                    (11ULL << 60)
#define H_CODE                     (6ULL << 61)
#define D_CODE                     (7ULL << 61)

#endif // _TPCC_CONFIG_H