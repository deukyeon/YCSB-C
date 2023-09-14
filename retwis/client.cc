#include "client.h"
#include "config.h"

#include "../core/uniform_generator.h"
#include "../core/scrambled_zipfian_generator.h"

#include <chrono>
#include <thread>
#include <stdarg.h>
#include <mutex>

namespace retwis {

Client::Client(ycsbc::DB *db, uint64_t id, uint64_t total_num_clients)
   : db(db), id(id), total_num_clients(total_num_clients)
{
   user_id_generator_engine.seed(id * 3423452437 + 8349344563457);
   target_id_generator_engine.seed(id * 3423452437 + 8349344563457);
   txn_chooser_engine.seed(id * 3423452437 + 8349344563457);

   txn_chooser =
      new ycsbc::DiscreteGenerator<Transaction::Type>(txn_chooser_engine);
   txn_chooser->AddValue(Transaction::Type::ADD_USER,
                         RETWIS_ADD_USER_PROPORTION);
   txn_chooser->AddValue(Transaction::Type::POST, RETWIS_POST_PROPORTION);
   txn_chooser->AddValue(Transaction::Type::FOLLOW, RETWIS_FOLLOW_PROPORTION);
   txn_chooser->AddValue(Transaction::Type::TIMELINE,
                         RETWIS_TIMELINE_PROPORTION);

   user_id_generator = new ycsbc::ScrambledZipfianGenerator(
      user_id_generator_engine, 0, RETWIS_INIT_USERS - 1);
   target_id_generator = new ycsbc::UniformGenerator(
      target_id_generator_engine, 0, RETWIS_INIT_USERS - 1);
}

Client::~Client()
{
   delete user_id_generator;
   delete target_id_generator;
   delete txn_chooser;
}

void
Client::init()
{
   for (uint64_t i = 0; i < RETWIS_INIT_USERS / total_num_clients; ++i) {
      std::string user_id = std::to_string(get_next_global_uid());
      debug("Add user %s\n", user_id.c_str());
      TransactionAddUser txn_add_user(
         db, user_id, "user" + user_id, "password" + user_id);
      txn_add_user.run();
   }
}

void
Client::run_transactions()
{
   auto run = [](Transaction &txn) {
      uint64_t abort_cnt = 0;
      bool     is_abort  = false;
      do {
         is_abort = txn.run();

         const int sleep_for =
            std::pow(2.0, abort_cnt) * RETWIS_TXN_RETRY_PENALTY_NS;
         if (sleep_for > 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(sleep_for));
         }
         abort_cnt += (int)(is_abort);
      } while (is_abort && abort_cnt <= RETWIS_MAX_TXN_RETRIES);
      return abort_cnt;
   };

   for (int i = 0; i < RETWIS_NUM_TRANSACTIONS_PER_CLIENT; ++i) {
      uint64_t          abort_cnt = 0;
      Transaction::Type type      = txn_chooser->Next();
      switch (type) {
         case Transaction::Type::ADD_USER:
         {
            std::string        user_id = std::to_string(get_next_global_uid());
            TransactionAddUser txn_add_user(
               db, user_id, "user" + user_id, "password" + user_id);
            abort_cnt = run(txn_add_user);
            break;
         }
         case Transaction::Type::POST:
         {
            std::string     user_id = std::to_string(user_id_generator->Next());
            std::string     post_id = std::to_string(get_next_global_pid());
            TransactionPost txn_post(db, user_id, post_id);
            abort_cnt = run(txn_post);
            break;
         }
         case Transaction::Type::FOLLOW:
         {
            std::string user_id = std::to_string(user_id_generator->Next());
            std::string target_id;
            do {
               target_id = std::to_string(target_id_generator->Next());
            } while (target_id == user_id);
            TransactionFollow txn_follow(db, user_id, target_id);
            abort_cnt = run(txn_follow);
            break;
         }
         case Transaction::Type::TIMELINE:
         {
            std::string user_id = std::to_string(user_id_generator->Next());
            const int   count   = 5; // 1 + rand() % 10;
            TransactionTimeline txn_timeline(db, user_id, count);
            abort_cnt = run(txn_timeline);
            break;
         }
      }

      output.txn_cnt++;
      output.commit_cnt += (abort_cnt == 0);
      output.abort_cnt += abort_cnt;

      output.txn_cnt_by_type[type]++;
      output.commit_cnt_by_type[type] += (abort_cnt == 0);
      output.abort_cnt_by_type[type] += abort_cnt;
   }
}

void
Client::debug(const char *format, ...)
{
#if 0
        static std::mutex mtx;
        const std::lock_guard<std::mutex> lock(mtx);
        va_list args;
        va_start(args, format);
        printf("Client %lu: ", id);
        vprintf(format, args);
        va_end(args);
#else
   (void)format;
#endif
}

} // namespace retwis