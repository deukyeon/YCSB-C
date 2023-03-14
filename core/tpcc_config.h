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
#define MAX_ORDERS_PER_DISTRICT     2000000
#define MAX_OL_PER_ORDER            15
#define WH_UPDATE					true
#define NUM_WH 						1
#define TOTAL_NUM_TRANSACTIONS      1

#endif // _TPCC_CONFIG_H