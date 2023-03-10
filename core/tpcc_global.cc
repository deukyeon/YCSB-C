#include "tpcc_global.h"

drand48_data ** tpcc_buffer;

ts_t g_abort_penalty = ABORT_PENALTY;
uint32_t g_thread_cnt = THREAD_CNT;
uint32_t g_num_wh = NUM_WH;
double g_perc_payment = PERC_PAYMENT;
bool g_wh_update = WH_UPDATE;
uint32_t g_part_cnt = PART_CNT;
uint32_t g_max_items = 100000;
uint32_t g_cust_per_dist = 3000;

/**********************************************/	
// helper functions
/**********************************************/

uint64_t wKey(uint64_t w_id)  {
	return W_CODE + w_id;
}

uint64_t iKey(uint64_t i_id)  {
	return I_CODE + i_id;
}

uint64_t dKey(uint64_t d_id, uint64_t d_w_id)  {
	return D_CODE + d_w_id * DIST_PER_WARE + d_id; 
}

uint64_t cKey(uint64_t c_id, uint64_t c_d_id, uint64_t c_w_id) {
	return C_CODE + (c_w_id * DIST_PER_WARE + c_d_id) * g_cust_per_dist + c_id;
}

uint64_t olKey(uint64_t w_id, uint64_t d_id, uint64_t o_id) {
	return OL_CODE + (w_id * DIST_PER_WARE + d_id) * g_cust_per_dist + o_id; 
}

uint64_t oKey(uint64_t w_id, uint64_t d_id, uint64_t o_id) {
	return O_CODE + (w_id * DIST_PER_WARE + d_id) * g_cust_per_dist + o_id; 
}

uint64_t noKey(uint64_t w_id, uint64_t d_id, uint64_t o_id) {
	return NO_CODE + (w_id * DIST_PER_WARE + d_id) * g_cust_per_dist + o_id; 
}

uint64_t sKey(uint64_t s_i_id, uint64_t s_w_id) {
	return S_CODE + s_w_id * g_max_items + s_i_id;
}

uint64_t Lastname(uint64_t num, char* name) {
  	static const char *n[] =
    	{"BAR", "OUGHT", "ABLE", "PRI", "PRES",
     	"ESE", "ANTI", "CALLY", "ATION", "EING"};
  	strcpy(name, n[num/100]);
  	strcat(name, n[(num/10)%10]);
  	strcat(name, n[num%10]);
  	return strlen(name);
}

uint64_t Rand(uint64_t max, uint64_t thd_id) {
	int64_t rint64 = 0;
	lrand48_r(tpcc_buffer[thd_id], &rint64);
	return rint64 % max;
}

uint64_t URand(uint64_t x, uint64_t y, uint64_t thd_id) {
    return x + Rand(y - x + 1, thd_id);
}

uint64_t NURand(uint64_t A, uint64_t x, uint64_t y, uint64_t thd_id) {
  static bool C_255_init = false;
  static bool C_1023_init = false;
  static bool C_8191_init = false;
  static uint64_t C_255, C_1023, C_8191;
  int C = 0;
  switch(A) {
    case 255:
      if(!C_255_init) {
        C_255 = (uint64_t) URand(0,255, thd_id);
        C_255_init = true;
      }
      C = C_255;
      break;
    case 1023:
      if(!C_1023_init) {
        C_1023 = (uint64_t) URand(0,1023, thd_id);
        C_1023_init = true;
      }
      C = C_1023;
      break;
    case 8191:
      if(!C_8191_init) {
        C_8191 = (uint64_t) URand(0,8191, thd_id);
        C_8191_init = true;
      }
      C = C_8191;
      break;
    default:
      m_assert(false, "Error! NURand\n");
      exit(-1);
  }
  return(((URand(0,A, thd_id) | URand(x,y, thd_id))+C)%(y-x+1))+x;
}

uint64_t MakeAlphaString(int min, int max, char* str, uint64_t thd_id) {
    char char_list[] = {'1','2','3','4','5','6','7','8','9','a','b','c',
                        'd','e','f','g','h','i','j','k','l','m','n','o',
                        'p','q','r','s','t','u','v','w','x','y','z','A',
                        'B','C','D','E','F','G','H','I','J','K','L','M',
                        'N','O','P','Q','R','S','T','U','V','W','X','Y','Z'};
    uint64_t cnt = URand(min, max, thd_id);
    for (uint32_t i = 0; i < cnt; i++) 
		  str[i] = char_list[URand(0L, 60L, thd_id)];
    for (int i = cnt; i < max; i++)
		  str[i] = '\0';

    return cnt;
}

uint64_t MakeNumberString(int min, int max, char* str, uint64_t thd_id) {
  uint64_t cnt = URand(min, max, thd_id);
  for (uint32_t i = 0; i < cnt; i++) {
    uint64_t r = URand(0L,9L, thd_id);
    str[i] = '0' + r;
  }
  return cnt;
}