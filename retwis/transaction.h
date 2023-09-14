#pragma once

#include "../core/db.h"
#include "global.h"
#include <stdarg.h>
#include <mutex>

namespace retwis {

#define run_and_return_if_abort(call)                                          \
   do {                                                                        \
      if ((call) == ycsbc::DB::kErrorConflict) {                               \
         return true;                                                          \
      }                                                                        \
   } while (0)

class Transaction {
public:
   enum Type : int { ADD_USER, POST, FOLLOW, TIMELINE };

   Transaction(ycsbc::DB *db, std::string uid) : db(db), txn(nullptr), me(uid)
   {
      default_empty_buffer = std::string(max_value_buffer_size, '\n');
      values.push_back(std::make_pair("string", default_empty_buffer));
      value = &values[0];
   }
   virtual ~Transaction() {}

   /*
    * It returns true if transaction aborts.
    */
   virtual bool
   run() = 0;

protected:
   ycsbc::DB          *db;
   ycsbc::Transaction *txn;
   std::string         me;

   std::string                    default_empty_buffer;
   std::vector<ycsbc::DB::KVPair> values;
   ycsbc::DB::KVPair             *value;

   void
   debug(const char *format, ...)
   {
#if 0
        static std::mutex mtx;
        const std::lock_guard<std::mutex> lock(mtx);
        va_list args;
        va_start(args, format);
        printf("Transaction with User ID: %s: ", me.c_str());
        vprintf(format, args);
        va_end(args);
#else
      (void)format;
#endif
   }
};

class TransactionAddUser : public Transaction {
public:
   TransactionAddUser(ycsbc::DB  *db,
                      std::string next_user_id,
                      std::string username,
                      std::string password);

   bool
   run();

private:
   std::string username;
   std::string password;
};


class TransactionFollow : public Transaction {
public:
   TransactionFollow(ycsbc::DB *db, std::string uid, std::string target_uid);

   bool
   run();

private:
   std::string target_uid;
   bool        is_follow;
};

class TransactionPost : public Transaction {
public:
   TransactionPost(ycsbc::DB *db, std::string uid, std::string next_post_id);

   bool
   run();

private:
   std::string next_post_id;
};

class TransactionTimeline : public Transaction {
public:
   TransactionTimeline(ycsbc::DB *db, std::string uid, int count);

   bool
   run();

private:
   int count;
};

} // namespace retwis