#ifndef _TPCC_GLOBAL_H_
#define _TPCC_GLOBAL_H_

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <cassert>
#include "tpcc_config.h"

// random generator per warehouse
extern drand48_data ** tpcc_buffer;

#define m_assert(cond, ...) \
	if (!(cond)) {\
		printf("ASSERTION FAILURE [%s : %d] ", \
		__FILE__, __LINE__); \
		printf(__VA_ARGS__);\
		assert(false);\
	}

typedef uint64_t ts_t; // time stamp type

/******************************************/
// Global Parameters
/******************************************/
extern uint32_t g_thread_cnt;
extern ts_t g_abort_penalty;
extern uint32_t g_num_wh;
extern uint32_t g_part_cnt;
extern double g_perc_payment;
extern bool g_wh_update;
extern uint32_t g_max_items;
extern uint32_t g_cust_per_dist;

uint64_t Rand(uint64_t max, uint64_t thd_id);
uint64_t NURand(uint64_t A, uint64_t x, uint64_t y, uint64_t thd_id);
uint64_t URand(uint64_t x, uint64_t y, uint64_t thd_id);
uint64_t Lastname(uint64_t num, char* name);
uint64_t MakeAlphaString(int min, int max, char* str, uint64_t thd_id);
uint64_t MakeNumberString(int min, int max, char* str, uint64_t thd_id);

uint64_t wKey(uint64_t w_id);
uint64_t iKey(uint64_t i_id);
uint64_t dKey(uint64_t d_id, uint64_t d_w_id);
uint64_t cKey(uint64_t c_id, uint64_t c_d_id, uint64_t c_w_id);
uint64_t olKey(uint64_t w_id, uint64_t d_id, uint64_t o_id);
uint64_t oKey(uint64_t w_id, uint64_t d_id, uint64_t o_id);
uint64_t noKey(uint64_t w_id, uint64_t d_id, uint64_t o_id);
uint64_t sKey(uint64_t s_i_id, uint64_t s_w_id);

#endif // _TPCC_GLOBAL_H_