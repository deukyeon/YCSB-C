#pragma once

#include "../core/db.h"
#include "../core/generator.h"
#include "../core/discrete_generator.h"
#include "transaction.h"

#include <map>

namespace retwis {

struct ClientOutput {
   uint64_t txn_cnt;
   uint64_t commit_cnt;
   uint64_t abort_cnt;

   std::map<Transaction::Type, uint64_t> txn_cnt_by_type;
   std::map<Transaction::Type, uint64_t> commit_cnt_by_type;
   std::map<Transaction::Type, uint64_t> abort_cnt_by_type;

   ClientOutput() : txn_cnt(0), commit_cnt(0), abort_cnt(0)
   {
      txn_cnt_by_type[Transaction::Type::ADD_USER] = 0;
      txn_cnt_by_type[Transaction::Type::POST]     = 0;
      txn_cnt_by_type[Transaction::Type::FOLLOW]   = 0;
      txn_cnt_by_type[Transaction::Type::TIMELINE] = 0;

      commit_cnt_by_type[Transaction::Type::ADD_USER] = 0;
      commit_cnt_by_type[Transaction::Type::POST]     = 0;
      commit_cnt_by_type[Transaction::Type::FOLLOW]   = 0;
      commit_cnt_by_type[Transaction::Type::TIMELINE] = 0;

      abort_cnt_by_type[Transaction::Type::ADD_USER] = 0;
      abort_cnt_by_type[Transaction::Type::POST]     = 0;
      abort_cnt_by_type[Transaction::Type::FOLLOW]   = 0;
      abort_cnt_by_type[Transaction::Type::TIMELINE] = 0;
   }
};

class Client {
public:
   Client(ycsbc::DB *db, uint64_t id, uint64_t total_num_clients);
   virtual ~Client();

   void
   init();
   void
   run_transactions();

   ClientOutput
   get_output()
   {
      return output;
   }

private:
   void
   debug(const char *format, ...);

   ycsbc::DB *db;
   uint64_t   id;
   uint64_t   total_num_clients;

   std::default_random_engine                   user_id_generator_engine;
   ycsbc::Generator<uint64_t>                  *user_id_generator;
   std::default_random_engine                   target_id_generator_engine;
   ycsbc::Generator<uint64_t>                  *target_id_generator;
   std::default_random_engine                   txn_chooser_engine;
   ycsbc::DiscreteGenerator<Transaction::Type> *txn_chooser;

   ClientOutput output;
};

} // namespace retwis