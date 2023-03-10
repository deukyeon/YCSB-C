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
   TPCCClient(int id, ycsbc::DB &db) : id_(id), db_(db)
   {
      abort_cnt = 0;
      txn_cnt   = 0;
      srand48_r(id, &drand_buffer);
   }

   virtual bool DoInsert();
   virtual bool DoTransaction();

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
   int                id_;
   ycsbc::DB          &db_;
   unsigned long      abort_cnt;
   unsigned long      txn_cnt;
   drand48_data       drand_buffer;
};

inline bool
TPCCClient::DoInsert()
{
   int status = -1;
   if (db_.IsTransactionSupported()) {
      ycsbc::Transaction *txn = NULL;
      db_.Begin(&txn);
      //status = db_.Insert(txn, workload_.NextTable(), key, pairs);
      db_.Commit(&txn);
   } else {
      //status = db_.Insert(NULL, workload_.NextTable(), key, pairs);
   }

   return (status == ycsbc::DB::kOK);
}

inline bool
TPCCClient::DoTransaction()
{
   // bool need_retry = false;
   // int  retry      = 0;
   // do {
   //    int          status = -1;
   //    ycsbc::Transaction *txn    = NULL;
   //    db_.Begin(&txn);
   //    if ((need_retry = db_.Commit(&txn) == ycsbc::DB::kErrorConflict)) {
   //       ++abort_cnt;
   //       // const int sleep_for =
   //       //     std::min((int)std::pow(2.0, (float)retry++),
   //       //     workload_.max_txn_retry_ms());

   //       // double r = 0;
   //       // drand48_r(&drand_buffer, &r);
   //       // const int sleep_for = (r * workload_.max_txn_abort_panelty_us());
	//       const int sleep_for = std::pow(2.0, retry * workload_.max_txn_abort_panelty_us());
   //       std::this_thread::sleep_for(std::chrono::microseconds(sleep_for));
	//       ++retry;
   //    } else {
   //       ++txn_cnt;
   //    }
   // } while (need_retry && retry <= workload_.max_txn_retry());
   return 0;
}

} // namespace tpcc

#endif // TPCC_CLIENT_H_
