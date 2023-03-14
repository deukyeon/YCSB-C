//
//  client.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef TPCC_CLIENT_H_
#define TPCC_CLIENT_H_

#include "tpcc_workload.h"
#include "db.h"
#include "utils.h"
#include <atomic>
#include <chrono>
#include <cmath>
#include <iostream>
#include <string>
#include <thread>

namespace tpcc {

class TPCCClient {
public:
   TPCCClient(int thread_id, TPCCWorkload *wl) : _thread_id(thread_id), _wl(wl)
   {
      abort_cnt = 0;
      txn_cnt   = 0;
   }

   virtual bool run_transactions();

   virtual ~TPCCClient()
   {
      //TPCCClient::total_abort_cnt.fetch_add(abort_cnt);
   }

   // getters for txn_cnt and abort_cnt
   int
   GetTxnCnt() const
   {
      return txn_cnt;
   }
   int
   GetAbortCnt() const
   {
      return abort_cnt;
   }

   //static std::atomic<unsigned long> total_abort_cnt;

protected:
   int                _thread_id;
   TPCCWorkload       *_wl;
   unsigned long      abort_cnt;
   unsigned long      txn_cnt;
};

inline bool
TPCCClient::run_transactions()
{
   TPCCTransaction txn;

   for (uint32_t i = 0; i < TOTAL_NUM_TRANSACTIONS; i++) {
      txn.init(_thread_id);
      if (_wl->run_transaction(&txn) == ycsbc::DB::kOK) {
         txn_cnt++;
      } else {
         abort_cnt++;
      }
   }

   return true;
}

} // namespace tpcc

#endif // TPCC_CLIENT_H_
