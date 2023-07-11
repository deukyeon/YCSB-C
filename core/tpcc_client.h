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

      abort_cnt_payment   = 0;
      abort_cnt_new_order = 0;

      attempts_payment   = 0;
      attempts_new_order = 0;
   }

   virtual bool
   run_transactions();

   virtual ~TPCCClient()
   {
      // TPCCClient::total_abort_cnt.fetch_add(abort_cnt);
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

   int
   GetAbortCntPayment() const
   {
      return abort_cnt_payment;
   }

   int
   GetAbortCntNewOrder() const
   {
      return abort_cnt_new_order;
   }

   int
   GetAttemptsPayment() const
   {
      return attempts_payment;
   }

   int
   GetAttemptsNewOrder() const
   {
      return attempts_new_order;
   }

   // static std::atomic<unsigned long> total_abort_cnt;

protected:
   int           _thread_id;
   TPCCWorkload *_wl;
   unsigned long abort_cnt;
   unsigned long txn_cnt;

   unsigned long abort_cnt_payment;
   unsigned long abort_cnt_new_order;

   unsigned long attempts_payment;
   unsigned long attempts_new_order;
};

inline bool
TPCCClient::run_transactions()
{
   TPCCTransaction txn;

   for (uint32_t i = 0; i < g_total_num_transactions; i++) {
      txn.init(_thread_id);

      tpcc::tpcc_txn_type current_txn_type = txn.type;

      switch (current_txn_type) {
         case TPCC_PAYMENT:
            ++attempts_payment;
            break;
         case TPCC_NEW_ORDER:
            ++attempts_new_order;
            break;
         default:
            assert(false);
      }

      bool     need_retry = false;
      uint32_t retry      = 0;
      do {
         if ((need_retry =
                 _wl->run_transaction(&txn) == ycsbc::DB::kErrorConflict)) {

            switch (current_txn_type) {
               case TPCC_PAYMENT:
                  ++abort_cnt_payment;
                  break;
               case TPCC_NEW_ORDER:
                  ++abort_cnt_new_order;
                  break;
               default:
                  assert(false);
            }
            ++abort_cnt;

            const int sleep_for = std::pow(2.0, retry * g_abort_penalty_us);
            std::this_thread::sleep_for(std::chrono::microseconds(sleep_for));
            ++retry;
         } else {
            ++txn_cnt;
         }
      } while (need_retry && retry <= g_max_txn_retry);
   }

   return true;
}

} // namespace tpcc

#endif // TPCC_CLIENT_H_
